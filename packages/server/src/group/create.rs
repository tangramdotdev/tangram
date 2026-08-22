use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{collections::BTreeMap, ops::ControlFlow},
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
			tg::Location::Local(_) if !self.server.is_primary_region() => {
				self.create_group_primary_region(arg).await
			},
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
		self.pull_ancestors(&arg.specifier, arg.ancestors.pull)
			.await?;
		let options = tangram_futures::retry::Options::default();
		let session = self.clone();
		let output = tangram_futures::retry(&options, || {
			let arg = arg.clone();
			let session = session.clone();
			async move {
				match session.create_group_local_attempt(arg).await? {
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(()) => Ok(ControlFlow::Continue(tg::error!(
						"the named node ids kept changing while authorizing the write"
					))),
				}
			}
		})
		.await?;
		self.server
			.spawn_publish_database_outbox_notification_task();
		Ok(output)
	}

	async fn create_group_local_attempt(
		&self,
		arg: tg::group::create::Arg,
	) -> tg::Result<ControlFlow<tg::group::create::Output>> {
		let ids_by_specifier = self
			.try_get_ids_and_ancestors_for_specifiers(std::slice::from_ref(&arg.specifier))
			.await?;
		let permission = tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Write,
		);
		let authorized = self
			.authorize(
				tg::Selector::<tg::Id>::Specifier(arg.specifier.clone()),
				permission,
			)
			.await?;
		if authorized.is_some_and(|permissions| !permissions.contains(permission)) {
			return Err(tg::error!("unauthorized"));
		}
		crate::checkpoint!(self.server, "group.create.authorized", specifier = %arg.specifier)
			.await;
		let session = self.clone();
		let output = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let ids_by_specifier = ids_by_specifier.clone();
				let session = session.clone();
				async move {
					session
						.create_group_local_with_transaction(transaction, arg, &ids_by_specifier)
						.await
				}
				.boxed()
			})
			.await?;
		Ok(output)
	}

	async fn create_group_local_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::group::create::Arg,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
	) -> tg::Result<ControlFlow<ControlFlow<tg::group::create::Output>, crate::database::Error>> {
		let batch_size = self.server.config.sync.get.database.batch_size;
		match Self::verify_ids_for_specifiers_with_transaction(
			transaction,
			ids_by_specifier,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(true) => (),
			ControlFlow::Break(false) => {
				return Ok(ControlFlow::Break(ControlFlow::Continue(())));
			},
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let mut batch = tangram_index::batch::Arg::default();
		let group = match self
			.create_group_with_transaction(transaction, arg, &mut batch)
			.await?
		{
			ControlFlow::Break(group) => group,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		match self
			.server
			.enqueue_database_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let output = tg::group::create::Output { group };

		Ok(ControlFlow::Break(ControlFlow::Break(output)))
	}

	async fn create_group_primary_region(
		&self,
		mut arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client.create_group(arg).await.map_err(|error| {
			tg::error!(!error, "failed to create the group in the primary region")
		})?;

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
		let location = tg::Location::Remote(remote);
		self.update_tokens_for_location(&mut output.group.tokens, &location)?;
		output.group.location = Some(location);

		Ok(output)
	}

	async fn create_group_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::group::create::Arg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<tg::Group, crate::database::Error>> {
		let id = match Self::try_get_id_for_specifier_with_transaction(transaction, &arg.specifier)
			.await?
		{
			ControlFlow::Break(id) => id,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		if let Some(id) = id {
			if arg.ancestors.create && id.kind() == tg::id::Kind::Group {
				let id = id.try_into()?;
				let group = match Self::try_get_group_with_transaction(transaction, &id).await? {
					ControlFlow::Break(group) => group,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let mut group = group.ok_or_else(|| tg::error!("failed to find the group"))?;
				if let Some(token) = self.create_read_token(&id.clone().into())? {
					group.tokens.insert_local(token);
				}

				return Ok(ControlFlow::Break(group));
			}
			return Err(tg::error!("specifier is already in use"));
		}
		let parent = if arg.ancestors.create {
			match self
				.create_parent_groups_with_transaction(transaction, &arg.specifier, batch)
				.await?
			{
				ControlFlow::Break(parent) => parent,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		} else {
			match Self::resolve_parent_for_specifier_with_transaction(transaction, &arg.specifier)
				.await?
			{
				ControlFlow::Break(parent) => parent,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		};
		let group = match self
			.insert_group_with_transaction(transaction, &arg.specifier, parent.as_ref(), batch)
			.await?
		{
			ControlFlow::Break(group) => group,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let mut group = group;
		if let Some(token) = self.create_read_token(&group.id.clone().into())? {
			group.tokens.insert_local(token);
		}

		Ok(ControlFlow::Break(group))
	}

	pub(crate) async fn create_parent_groups_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<Option<tg::Id>, crate::database::Error>> {
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let mut parent = None;
		for specifier in specifier.ancestors() {
			let id = match Self::try_get_id_for_specifier_with_transaction(transaction, &specifier)
				.await?
			{
				ControlFlow::Break(id) => id,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let id = if let Some(id) = id {
				id
			} else {
				let group = match self
					.insert_group_with_transaction(transaction, &specifier, parent.as_ref(), batch)
					.await?
				{
					ControlFlow::Break(group) => group,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				group.id.into()
			};
			if id.kind() == tg::id::Kind::Tag {
				return Err(tg::error!("a tag cannot be a parent"));
			}
			parent = Some(id);
		}

		Ok(ControlFlow::Break(parent))
	}

	async fn insert_group_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
		parent: Option<&tg::Id>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<tg::Group, crate::database::Error>> {
		let id = tg::group::Id::new();
		match Self::insert_specifier_with_transaction(transaction, &id.clone().into(), specifier)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let name = specifier.name().to_owned();
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into groups (id, name, parent)
				values ({p}1, {p}2, {p}3);
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![
					id.to_string(),
					name.clone(),
					parent.map(ToString::to_string)
				],
			)
			.await;
		crate::database::retry!(result, "failed to execute the statement");
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
			let subject = self.context.principal.try_to_subject()?;
			let arg = tg::grant::create::Arg {
				permissions: tg::Either::Left(
					tg::authorization::Permission::Group(
						tg::authorization::permission::group::Permission::Admin,
					)
					.into(),
				),
				resource: tg::Referent::with_node(tg::Selector::Id(id.clone().into())),
				subject: subject.into(),
			};
			match self
				.create_grant_with_transaction(transaction, arg, batch)
				.await?
			{
				ControlFlow::Break(_) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		}
		let group = tg::Group {
			id,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			name,
			parent: parent.cloned(),
			specifier: specifier.clone(),
			tokens: tg::authorization::Tokens::default(),
		};

		Ok(ControlFlow::Break(group))
	}

	pub(crate) async fn resolve_parent_for_specifier_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<ControlFlow<Option<tg::Id>, crate::database::Error>> {
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let Some(parent) = specifier.parent() else {
			return Ok(ControlFlow::Break(None));
		};
		let parent =
			match Self::try_get_id_for_specifier_with_transaction(transaction, &parent).await? {
				ControlFlow::Break(parent) => parent,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			.ok_or_else(|| tg::error!("the parent does not exist"))?;
		if parent.kind() == tg::id::Kind::Tag {
			return Err(tg::error!("a tag cannot be a parent"));
		}

		Ok(ControlFlow::Break(Some(parent)))
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
