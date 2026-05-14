use {
	super::Grantee,
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	pub(crate) async fn create_namespace_grant(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> tg::Result<tg::Grant> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		let grantee = Self::grantee(arg.user, arg.group, arg.public)?;
		if matches!(grantee, Grantee::Public) && arg.permission != tg::Permission::Read {
			return Err(tg::error!("public grants may only be read"));
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
		let grant = match grantee {
			Grantee::User(user) => {
				let user = Self::try_get_user_with_transaction(&transaction, &user)
					.await?
					.ok_or_else(|| tg::error!("failed to find the user"))?;
				Self::create_namespace_grant_for_user_with_transaction(
					&transaction,
					&arg.namespace,
					namespace_id,
					&user.id,
					arg.permission,
					created_by.as_ref(),
				)
				.await?
			},
			Grantee::Group(group) => {
				let group = Self::try_get_group_with_transaction(&transaction, &group)
					.await?
					.ok_or_else(|| tg::error!("failed to find the group"))?;
				Self::create_namespace_grant_for_group_with_transaction(
					&transaction,
					&arg.namespace,
					namespace_id,
					&group.id,
					arg.permission,
					created_by.as_ref(),
				)
				.await?
			},
			Grantee::Public => {
				Self::create_namespace_grant_for_public_with_transaction(
					&transaction,
					&arg.namespace,
					namespace_id,
					created_by.as_ref(),
				)
				.await?
			},
		};
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(grant)
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
