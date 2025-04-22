use crate as tg;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Error>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub exit: Option<u8>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub output: Option<tg::value::Data>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub finished: bool,
}

impl tg::Process {
	pub async fn finish<H>(&self, handle: &H, arg: tg::process::finish::Arg) -> tg::Result<bool>
	where
		H: tg::Handle,
	{
		let id = self.id();
		let output = handle.try_finish_process(id, arg).await?;
		Ok(output.finished)
	}
}

impl tg::Client {
	pub async fn try_finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<Output> {
		let method = http::Method::POST;
		let uri = format!("/processes/{id}/finish");
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
		let output = response.json().await?;
		Ok(output)
	}
}
