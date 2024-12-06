use crate::{
	self as tg,
	util::serde::{is_false, is_true, return_true},
};
use futures::{future, Stream, TryStreamExt as _};
use std::{path::PathBuf, pin::pin};
use tangram_futures::stream::TryStreamExt as _;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub dependencies: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub force: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub path: PathBuf,
}

impl tg::Artifact {
	pub async fn check_out<H>(&self, handle: &H, arg: Arg) -> tg::Result<PathBuf>
	where
		H: tg::Handle,
	{
		let id = self.id(handle).await?;
		let stream = handle.check_out_artifact(&id, arg).await?;
		let output = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;
		Ok(output.path)
	}
}

impl tg::Client {
	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkout::Output>>>,
	> {
		let method = http::Method::POST;
		let uri = format!("/artifacts/{id}/checkout");
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

impl Default for Arg {
	fn default() -> Self {
		Self {
			force: false,
			path: None,
			dependencies: true,
		}
	}
}
