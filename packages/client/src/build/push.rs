use crate as tg;
use futures::{future, Stream, StreamExt as _, TryStreamExt as _};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub logs: bool,
	pub outcomes: bool,
	pub recursive: bool,
	pub remote: String,
	pub targets: bool,
}

impl tg::Build {
	pub async fn push<H>(
		&self,
		handle: &H,
		arg: tg::build::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static>
	where
		H: tg::Handle,
	{
		let id = self.id();
		let stream = handle.push_build(id, arg).await?;
		Ok(stream.boxed())
	}
}

impl tg::Client {
	pub async fn push_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/push");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.json(arg)
			.unwrap();
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
