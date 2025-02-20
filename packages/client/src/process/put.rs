use crate::{self as tg, util::serde::is_false};
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(flatten)]
	pub data: tg::process::Data,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "is_false")]
	pub commands_complete: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub complete: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub logs_complete: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub outputs_complete: bool,
}

impl Arg {
	pub fn objects(&self) -> Vec<tg::object::Id> {
		self.data.objects()
	}
}

impl tg::Client {
	pub async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<tg::process::put::Output> {
		let method = http::Method::PUT;
		let uri = format!("/processes/{id}");
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
