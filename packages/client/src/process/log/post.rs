use crate::{self as tg, util::serde::BytesBase64};
use bytes::Bytes;
use serde_with::serde_as;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,

	#[serde_as(as = "i32")]
	pub kind: tg::process::log::Kind,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub added: bool,
}

impl tg::Process {
	pub async fn add_log<H>(&self, handle: &H, arg: tg::process::log::post::Arg) -> tg::Result<bool>
	where
		H: tg::Handle,
	{
		let id = self.id();
		let output = handle.try_add_process_log(id, arg).await?;
		Ok(output.added)
	}
}

impl tg::Client {
	pub async fn try_add_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> tg::Result<Output> {
		let method = http::Method::POST;
		let uri = format!("/processes/{id}/log");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
