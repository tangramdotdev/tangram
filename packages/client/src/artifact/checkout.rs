use crate::{
	self as tg,
	util::serde::{is_false, is_true, return_true},
};
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use std::pin::pin;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub bundle: bool,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub dependencies: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub force: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<tg::Path>,
}

impl tg::Artifact {
	pub async fn check_out<H>(&self, handle: &H, arg: Arg) -> tg::Result<tg::Path>
	where
		H: tg::Handle,
	{
		let id = self.id(handle).await?;
		let stream = handle.check_out_artifact(&id, arg).await?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			if let tg::Progress::End(path) = event {
				return Ok(path);
			}
		}
		Err(tg::error!("checkout failed"))
	}
}

impl tg::Client {
	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::Progress<tg::Path>>>> {
		let method = http::Method::POST;
		let uri = format!("/artifacts/{id}/checkout");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.sse().map(|result| {
			result
				.map_err(|source| tg::error!(!source, "failed to read an event"))?
				.try_into()
		});
		Ok(output)
	}
}

impl Default for Arg {
	fn default() -> Self {
		Self {
			bundle: false,
			force: false,
			path: None,
			dependencies: true,
		}
	}
}
