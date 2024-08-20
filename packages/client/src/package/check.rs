use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub package: tg::artifact::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub diagnostics: Vec<tg::Diagnostic>,
}

impl tg::Client {
	pub async fn check_package(&self, arg: Arg) -> tg::Result<tg::package::check::Output> {
		let method = http::Method::POST;
		let uri = "/packages/check";
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
