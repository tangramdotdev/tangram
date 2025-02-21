use crate::{self as tg, util::serde::is_false};
use futures::{Stream, TryStreamExt as _, future};
use tangram_either::Either;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub commands: bool,

	pub items: Vec<Either<tg::process::Id, tg::object::Id>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub logs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub outputs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub recursive: bool,

	pub remote: String,
}

impl tg::Client {
	pub async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let method = http::Method::POST;
		let uri = "/push";
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
