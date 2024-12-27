use futures::{future, Stream, TryStreamExt};
use tangram_http::{incoming::response::Ext as _, Outgoing};
use tokio::io::AsyncRead;

use crate as tg;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub compress: Option<tg::blob::compress::Format>,

	pub format: tg::artifact::archive::Format,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Output {
	pub object: tg::object::Id,
}

impl tg::Client {
	pub(crate) async fn import_object(
		&self,
		arg: tg::object::import::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<Output>>> + Send + 'static> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/objects/import?{query}");

		// Create the request body from the reader.
		let body = Outgoing::reader(reader);

		// Create the request.
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.body(body)
			.unwrap();

		// Get the response.
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let content_type = response
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if !matches!(
			content_type
				.as_ref()
				.map(|content_type| (content_type.type_(), content_type.subtype())),
			Some((mime::TEXT, mime::EVENT_STREAM)),
		) {
			return Err(tg::error!(?content_type, "invalid content type"));
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
