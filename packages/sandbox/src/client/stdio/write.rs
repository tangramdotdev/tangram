use {
	super::Arg,
	crate::client::Client,
	futures::{Stream, StreamExt as _, TryStreamExt as _, future},
	tangram_client::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

impl Client {
	pub async fn write_stdio(
		&self,
		id: &tg::process::Id,
		arg: Arg,
		stream: impl Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::stdio::write::Event>> + Send + 'static>
	{
		let method = http::Method::POST;
		let path = format!("/processes/{id}/stdio");
		let uri = Uri::builder()
			.path(&path)
			.query_params(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let stream = stream.map(
			|result: tg::Result<tg::process::stdio::read::Event>| match result {
				Ok(event) => event.try_into(),
				Err(error) => error.try_into(),
			},
		);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::TEXT_EVENT_STREAM.to_string(),
			)
			.sse(stream)
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let stream = response
			.sse()
			.map_err(|source| tg::error!(!source, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			});
		Ok(stream)
	}
}
