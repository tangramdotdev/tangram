use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
};

impl Session {
	pub(crate) async fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.create_group_local(arg).await,
			tg::Location::Remote(remote) => self.create_group_remote(arg, remote).await,
		}
	}

	async fn create_group_local(
		&self,
		arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let permission =
			tg::grant::Permission::Group(tg::grant::permission::group::Permission::Write);
		let authorized = self
			.authorize(
				tg::grant::Resource::Specifier(arg.specifier.clone()),
				permission,
			)
			.await?;
		if authorized.is_some_and(|permissions| !permissions.contains(permission)) {
			return Err(tg::error!("unauthorized"));
		}
		self.pull_ancestors(&arg.specifier, arg.ancestors.pull)
			.await?;
		let session = self.clone();
		let output = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					let group = session
						.create_group_with_transaction(transaction, arg, &mut batch)
						.await?;
					let output = tg::group::create::Output { group };
					session
						.server
						.enqueue_database_outbox_with_transaction(transaction, &batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(output))
				}
				.boxed()
			})
			.await?;
		Ok(output)
	}

	async fn create_group_remote(
		&self,
		mut arg: tg::group::create::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::group::create::Output> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let mut output = client.create_group(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to create the group"),
		)?;
		self.invalidate_remote_cache(&remote.name).await;
		output.group.location = Some(tg::Location::Remote(remote));

		Ok(output)
	}

	async fn create_group_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::group::create::Arg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<tg::Group> {
		if let Some(id) =
			Self::try_get_id_for_specifier_with_transaction(transaction, &arg.specifier).await?
		{
			if arg.ancestors.create && id.kind() == tg::id::Kind::Group {
				let id = id.try_into()?;
				let mut group = Self::try_get_group_with_transaction(transaction, &id)
					.await?
					.ok_or_else(|| tg::error!("failed to find the group"))?;
				group.token = self.create_read_token(&id.clone().into())?;

				return Ok(group);
			}
			return Err(tg::error!("specifier is already in use"));
		}
		let parent = if arg.ancestors.create {
			self.create_parent_groups_with_transaction(transaction, &arg.specifier, batch)
				.await?
		} else {
			Self::resolve_parent_for_specifier_with_transaction(transaction, &arg.specifier).await?
		};
		let mut group = self
			.insert_group_with_transaction(transaction, &arg.specifier, parent.as_ref(), batch)
			.await?;
		group.token = self.create_read_token(&group.id.clone().into())?;

		Ok(group)
	}

	pub(crate) async fn create_parent_groups_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<Option<tg::Id>> {
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let mut parent = None;
		for specifier in specifier.ancestors() {
			let id = match Self::try_get_id_for_specifier_with_transaction(transaction, &specifier)
				.await?
			{
				Some(id) => id,
				None => self
					.insert_group_with_transaction(transaction, &specifier, parent.as_ref(), batch)
					.await?
					.id
					.into(),
			};
			if id.kind() == tg::id::Kind::Tag {
				return Err(tg::error!("a tag cannot be a parent"));
			}
			parent = Some(id);
		}

		Ok(parent)
	}

	async fn insert_group_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
		parent: Option<&tg::Id>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<tg::Group> {
		let id = tg::group::Id::new();
		Self::insert_specifier_with_transaction(transaction, &id.clone().into(), specifier).await?;
		let name = specifier.name().to_owned();
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into groups (id, name, parent)
				values ({p}1, {p}2, {p}3);
			"
		);
		transaction
			.execute(
				statement.into(),
				db::params![
					id.to_string(),
					name.clone(),
					parent.map(ToString::to_string)
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		batch.items.push(tangram_index::batch::Item::PutGroup(
			tangram_index::group::put::Arg {
				id: id.clone(),
				parent: parent.cloned(),
				specifier: specifier.clone(),
			},
		));
		if !matches!(
			self.context.principal,
			tg::Principal::Anonymous | tg::Principal::Root
		) {
			let principal = &self.context.principal;
			let arg = tg::grant::create::Arg {
				principal: principal.try_to_grant_principal()?.into(),
				permissions: tg::Either::Left(
					tg::grant::Permission::Group(tg::grant::permission::group::Permission::Admin)
						.into(),
				),
				resource: tg::Referent::with_node(tg::grant::Resource::Id(id.clone().into())),
			};
			self.create_grant_with_transaction(transaction, arg, batch)
				.await?;
		}
		let group = tg::Group {
			id,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			name,
			parent: parent.cloned(),
			specifier: specifier.clone(),
			token: None,
		};

		Ok(group)
	}

	pub(crate) async fn resolve_parent_for_specifier_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<Option<tg::Id>> {
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let Some(parent) = specifier.parent() else {
			return Ok(None);
		};
		let parent = Self::try_get_id_for_specifier_with_transaction(transaction, &parent)
			.await?
			.ok_or_else(|| tg::error!("the parent does not exist"))?;
		if parent.kind() == tg::id::Kind::Tag {
			return Err(tg::error!("a tag cannot be a parent"));
		}

		Ok(Some(parent))
	}

	pub(crate) async fn create_group_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.create_group(arg).await?;
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}
}
