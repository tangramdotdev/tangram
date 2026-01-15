use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _},
};

impl Server {
	pub(crate) async fn try_get_tag_with_context(
		&self,
		context: &Context,
		pattern: &tg::tag::Pattern,
		arg: tg::tag::get::Arg,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let arg = tg::tag::list::Arg {
			length: Some(1),
			local: arg.local,
			pattern: pattern.clone(),
			recursive: false,
			remotes: arg.remotes,
			reverse: true,
		};
		let tg::tag::list::Output { data } = self
			.list_tags_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, %pattern, "failed to list tags"))?;
		let Some(output) = data.into_iter().next() else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	pub(crate) async fn handle_get_tag_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		pattern: &[&str],
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Parse the tag pattern.
		let pattern = pattern
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag pattern"))?;

		// Get the tag.
		let Some(output) = self
			.try_get_tag_with_context(context, &pattern, arg)
			.await?
		else {
			return Ok(http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.body(Body::empty())
				.unwrap());
		};

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), Body::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();
		Ok(response)
	}
}
