use {
	crate::{Session, node::Node},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
	tangram_index::prelude::*,
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
		if self.context.principal.is_none() {
			return Err(tg::error!("unauthorized"));
		}
		if let Some(ancestor) = self
			.try_get_nearest_existing_ancestor(&arg.specifier)
			.await? && ancestor.kind == tg::id::Kind::Group
			&& !self
				.authorize(ancestor.id, tg::grant::Permission::Write)
				.await?
		{
			return Err(tg::error!("unauthorized"));
		}
		let session = self.clone();
		let (output, batch) = self
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
					Ok::<_, crate::database::Error>(ControlFlow::Break((output, batch)))
				}
				.boxed()
			})
			.await?;
		if !batch.is_empty() {
			self.server
				.index
				.batch(batch)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the group"))?;
		}
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
		client.create_group(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to create the group"),
		)
	}

	async fn create_group_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::group::create::Arg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<tg::Group> {
		let node = self
			.create_group_with_ancestors_with_transaction(transaction, &arg.specifier, batch)
			.await?;
		Ok(tg::Group {
			id: node.id.try_into()?,
			name: node.name,
			parent: node.parent,
			specifier: node.specifier,
		})
	}

	async fn create_group_with_ancestors_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<Node> {
		if Self::try_get_node_by_specifier_with_transaction(transaction, specifier)
			.await?
			.is_some()
		{
			return Err(tg::error!("specifier is already in use"));
		}
		let node = self
			.ensure_group_with_ancestors_with_transaction(transaction, specifier, batch)
			.await?;
		Ok(node)
	}

	pub(crate) async fn ensure_group_with_ancestors_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<Node> {
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let mut parent = None;
		let mut node = None;
		for ancestor in specifier.prefixes() {
			if let Some(existing) =
				Self::try_get_node_by_specifier_with_transaction(transaction, &ancestor).await?
			{
				if existing.kind == tg::id::Kind::Tag {
					return Err(tg::error!("specifier is already in use"));
				}
				parent = Some(existing.id.clone());
				node = Some(existing);
				continue;
			}
			let created = self
				.create_group_node_with_transaction(transaction, &ancestor, parent.as_ref(), batch)
				.await?;
			parent = Some(created.id.clone());
			node = Some(created);
		}
		node.ok_or_else(|| tg::error!("invalid specifier"))
	}

	pub(crate) async fn create_group_node_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
		parent: Option<&tg::Id>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<Node> {
		let id = tg::group::Id::new();
		let node = Self::create_node_with_transaction(
			transaction,
			&id.clone().into(),
			tg::id::Kind::Group,
			specifier,
			parent,
		)
		.await?;
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
					node.name.clone(),
					node.parent.as_ref().map(ToString::to_string)
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		batch.put_groups.push(tangram_index::group::put::Arg {
			id: id.clone(),
			name: node.name.clone(),
			parent: node.parent.clone(),
		});
		if let Some(principal) = self
			.context
			.principal
			.as_ref()
			.filter(|principal| !principal.is_root())
		{
			let arg = tg::grant::create::Arg {
				principal: tg::grant::Principal::from(principal.clone()).into(),
				permission: tg::grant::Permission::Admin,
				resource: tg::grant::Resource::Id(id.clone().into()),
			};
			self.create_grant_with_transaction(transaction, arg, batch)
				.await?;
		}
		Ok(node)
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
