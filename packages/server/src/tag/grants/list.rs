use {
	crate::{Session, context::Authentication},
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn list_tag_grants(
		&self,
		arg: tg::tag::grants::list::Arg,
	) -> tg::Result<Option<tg::tag::grants::list::Output>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
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
		self.authorize_tag_with_transaction(&transaction, &arg.tag, tg::Permission::Admin)
			.await?;
		let Some(namespace_id) =
			Self::try_get_tag_namespace_id_with_transaction(&transaction, &arg.tag).await?
		else {
			return Ok(None);
		};
		let data =
			Self::list_tag_grants_for_tag_with_transaction(&transaction, &arg.tag, namespace_id)
				.await?;
		Ok(Some(tg::tag::grants::list::Output { data }))
	}

	pub(crate) async fn list_tag_grants_request(
		&self,
		request: http::Request<BoxBody>,
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
		let Some(output) = self
			.list_tag_grants(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the tag grants"))?
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
