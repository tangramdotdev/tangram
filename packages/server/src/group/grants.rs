use {
	crate::{Session, context::Authentication},
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn list_group_namespace_grants(
		&self,
		group: &str,
	) -> tg::Result<Option<tg::group::grants::Output>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let authentication = &self.context.authentication;
		if authentication
			.as_ref()
			.is_none_or(|authentication| authentication.is_runner() || authentication.is_sandbox())
		{
			return Err(tg::error!("unauthorized"));
		}

		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let Some(group) = Self::try_get_group_with_transaction(&transaction, group).await? else {
			return Ok(None);
		};
		if let Some(Authentication::User(current_user)) = authentication
			&& !Self::user_has_namespace_permission_with_transaction(
				&transaction,
				&current_user.id,
				&group.namespace,
				tg::Permission::Admin,
			)
			.await?
		{
			return Err(tg::error!("unauthorized"));
		}
		let data =
			Self::list_namespace_grants_for_group_with_transaction(&transaction, &group.id).await?;
		Ok(Some(tg::group::grants::Output { data }))
	}

	pub(crate) async fn list_group_namespace_grants_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params::<tg::group::grants::Arg>()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		let Some(output) = self.list_group_namespace_grants(&arg.group).await.map_err(
			|error| tg::error!(!error, group = %arg.group, "failed to list the namespace grants"),
		)?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
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
