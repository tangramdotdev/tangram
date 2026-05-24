use {
	super::Grantee,
	crate::{Session, context::Authentication},
	tangram_client::prelude::*,
	tangram_database::prelude::*,
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
		let grantee = Self::tag_grantee(arg.user, arg.group, arg.all)?;
		if matches!(grantee, Grantee::All) && arg.permission != tg::Permission::Read {
			return Err(tg::error!("all grants may only be read"));
		}
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
		self.authorize_tag_with_transaction(&transaction, &arg.tag, tg::Permission::Admin)
			.await?;
		let namespace_id = Self::try_get_tag_namespace_id_with_transaction(&transaction, &arg.tag)
			.await?
			.ok_or_else(|| tg::error!("failed to find the tag"))?;
		let grant = match grantee {
			Grantee::User(user) => {
				let user = Self::try_get_user_with_transaction(&transaction, &user)
					.await?
					.ok_or_else(|| tg::error!("failed to find the user"))?;
				Self::create_tag_grant_for_user_with_transaction(
					&transaction,
					&arg.tag,
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
				Self::create_tag_grant_for_group_with_transaction(
					&transaction,
					&arg.tag,
					namespace_id,
					&group.id,
					arg.permission,
					created_by.as_ref(),
				)
				.await?
			},
			Grantee::All => {
				Self::create_tag_grant_for_all_with_transaction(
					&transaction,
					&arg.tag,
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
		let output = self
			.create_tag_grant(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the tag grant"))?;
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
