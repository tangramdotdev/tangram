use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn revoke_user_namespace_permission(
		&self,
		user: &str,
		arg: tg::user::revoke::Arg,
	) -> tg::Result<Option<()>> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		self.authorize_namespace(&arg.namespace, tg::Permission::Admin)
			.await?;

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
		let Some(user) = Self::try_get_user_with_transaction(&transaction, user).await? else {
			return Ok(None);
		};
		let Some(namespace_id) =
			Self::try_get_namespace_id_with_transaction(&transaction, &arg.namespace).await?
		else {
			return Ok(None);
		};
		let output = Self::revoke_namespace_from_user_with_transaction(
			&transaction,
			namespace_id,
			&user.id,
			arg.permission,
		)
		.await?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(output)
	}

	pub(crate) async fn revoke_user_namespace_permission_request(
		&self,
		request: http::Request<BoxBody>,
		user: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		let Some(()) = self
			.revoke_user_namespace_permission(user, arg)
			.await
			.map_err(
				|error| tg::error!(!error, %user, "failed to revoke the namespace permission"),
			)?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}
		Ok(http::Response::builder().empty().unwrap().boxed_body())
	}
}
