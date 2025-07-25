use crate::{
	self as tg,
	util::serde::{is_false, is_true, return_true},
};
use futures::{Stream, TryStreamExt as _, future};
use std::{path::PathBuf, pin::pin};
use tangram_futures::stream::TryExt as _;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub destructive: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub deterministic: bool,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub ignore: bool,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub lock: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub locked: bool,

	pub path: PathBuf,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub updates: Vec<tg::tag::Pattern>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub referent: tg::Referent<tg::artifact::Id>,
}

pub async fn checkin<H>(handle: &H, arg: tg::checkin::Arg) -> tg::Result<tg::Artifact>
where
	H: tg::Handle,
{
	let stream = handle.checkin(arg).await?;
	let output = pin!(stream)
		.try_last()
		.await?
		.and_then(|event| event.try_unwrap_output().ok())
		.ok_or_else(|| tg::error!("stream ended without output"))?;
	let artifact = tg::Artifact::with_id(output.referent.item);
	Ok(artifact)
}

impl tg::Client {
	pub async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		let method = http::Method::POST;
		let uri = "/checkin";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
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
