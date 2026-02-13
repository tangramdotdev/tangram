use {
	crate::prelude::*,
	futures::{Stream, StreamExt as _, TryStreamExt as _, future},
	std::path::PathBuf,
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{is_false, is_true, return_true},
};

pub use crate::checkin::Lock;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub artifact: tg::artifact::Id,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub dependencies: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub extension: Option<String>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub force: bool,

	#[serde(
		default = "tg::checkin::default_lock",
		skip_serializing_if = "tg::checkin::is_default_lock"
	)]
	pub lock: Option<Lock>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub path: PathBuf,
}

pub async fn checkout<H>(handle: &H, arg: Arg) -> tg::Result<PathBuf>
where
	H: tg::Handle,
{
	let stream = handle.checkout(arg).await?.boxed();
	let output = stream
		.try_last()
		.await?
		.and_then(|event| event.try_unwrap_output().ok())
		.ok_or_else(|| tg::error!("stream ended without output"))?;
	Ok(output.path)
}

impl tg::Client {
	pub async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		let method = http::Method::POST;
		let uri = "/checkout";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
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
