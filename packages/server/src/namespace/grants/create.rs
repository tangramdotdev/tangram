use {
	crate::{Session, context::Authentication},
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	pub(crate) async fn create_namespace_grant(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> tg::Result<tg::Grant> {
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
			tg::Location::Local(_) => self.create_namespace_grant_local(arg).await,
			tg::Location::Remote(remote) => self.create_namespace_grant_remote(arg, remote).await,
		}
	}

	async fn create_namespace_grant_local(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> tg::Result<tg::Grant> {
		if matches!(arg.principal, tg::Principal::All) && arg.permission != tg::Permission::Read {
			return Err(tg::error!("all grants may only be read"));
		}
		self.authorize_namespace(&arg.namespace, tg::Permission::Admin)
			.await?;
		let created_by = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
			.map(|user| user.id.clone());

		let mut connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let namespace_id =
			Self::get_or_create_namespace_with_transaction(&transaction, &arg.namespace).await?;
		match &arg.principal {
			tg::Principal::All | tg::Principal::Root => {},
			tg::Principal::Group(group) => {
				Self::try_get_group_with_transaction(&transaction, &group.to_string())
					.await?
					.ok_or_else(|| tg::error!("failed to find the group"))?;
			},
			tg::Principal::User(user) => {
				Self::try_get_user_with_transaction(&transaction, &user.to_string())
					.await?
					.ok_or_else(|| tg::error!("failed to find the user"))?;
			},
		}
		let grant = Self::create_namespace_grant_with_transaction(
			&transaction,
			&arg.namespace,
			namespace_id,
			&arg.principal,
			arg.permission,
			created_by.as_ref(),
		)
		.await?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(grant)
	}

	async fn create_namespace_grant_remote(
		&self,
		mut arg: tg::namespace::grants::create::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::Grant> {
		let client = self
			.get_remote_session(&remote.name)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					remote = %remote.name,
					"failed to get the remote client"
				)
			})?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client.create_namespace_grant(arg).await.map_err(|error| {
			tg::error!(
				!error,
				remote = %remote.name,
				"failed to create the namespace grant"
			)
		})
	}

	pub(crate) async fn create_namespace_grant_request(
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
		let output = self
			.create_namespace_grant(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the namespace grant"))?;
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
		Ok(response.body(body).unwrap())
	}
}
