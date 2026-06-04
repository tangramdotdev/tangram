use {
	crate::{Session, context::Authentication, tag::delete::get_tag_data_with_transaction},
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_get_tag(
		&self,
		tag: &tg::tag::Selector,
	) -> tg::Result<Option<tg::tag::get::Output>> {
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
		let Some(node) = Self::try_get_node_by_selector_with_transaction(&transaction, tag).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::Tag
			|| !self
				.node_is_visible_with_transaction(&transaction, &node.id)
				.await?
		{
			return Ok(None);
		}
		let data = get_tag_data_with_transaction(&transaction, &node).await?;
		Ok(Some(tg::tag::get::Output { data }))
	}

	pub(crate) async fn try_get_tag_request(
		&self,
		request: http::Request<BoxBody>,
		path: &[&str],
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let tag = path.join(":").parse()?;
		let Some(output) = self.try_get_tag(&tag).await? else {
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
