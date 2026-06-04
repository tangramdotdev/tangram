use {
	crate::{Session, context::Authentication},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	pub(crate) async fn create_tag_grant(
		&self,
		arg: tg::tag::grants::create::Arg,
	) -> tg::Result<tg::TagGrant> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.create_tag_grant_local(arg).await,
			tg::Location::Remote(remote) => self.create_tag_grant_remote(arg, remote).await,
		}
	}

	async fn create_tag_grant_local(
		&self,
		arg: tg::tag::grants::create::Arg,
	) -> tg::Result<tg::TagGrant> {
		let created_by = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
			.map(|user| user.id.clone());
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let created_by = created_by.clone();
				let session = session.clone();
				async move {
					let grant = session
						.create_tag_grant_with_transaction(transaction, &arg, created_by.as_ref())
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(grant))
				}
				.boxed()
			})
			.await
	}

	async fn create_tag_grant_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: &tg::tag::grants::create::Arg,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<tg::TagGrant> {
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(transaction, &arg.tag).await?
		else {
			return Err(tg::error!("failed to find the tag"));
		};
		if node.kind != tg::id::Kind::Tag {
			return Err(tg::error!("failed to find the tag"));
		}
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into tag_grants (tag, principal, permission, created_at, created_by)
				values ({p}1, {p}2, {p}3, {p}4, {p}5)
				on conflict (tag, principal, permission) do nothing;
			"
		);
		transaction
			.execute(
				statement.into(),
				db::params![
					node.id.to_string(),
					arg.principal.to_string(),
					arg.permission.to_string(),
					created_at,
					created_by.map(ToString::to_string)
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Self::increment_visibility_with_transaction(
			transaction,
			&node.id,
			&arg.principal.to_string(),
		)
		.await?;
		Ok(tg::TagGrant {
			created_at,
			created_by: created_by.cloned(),
			permission: arg.permission,
			principal: arg.principal.clone(),
			resource: node.id,
		})
	}

	async fn create_tag_grant_remote(
		&self,
		mut arg: tg::tag::grants::create::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::TagGrant> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client.create_tag_grant(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to create the tag grant"),
		)
	}

	pub(crate) async fn create_tag_grant_request(
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
		let output = self.create_tag_grant(arg).await?;
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let body = serde_json::to_vec(&output).unwrap();
				(Some(mime::APPLICATION_JSON), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		Ok(response.body(body).unwrap())
	}
}
