use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub reference: tg::Reference,

	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub locked: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Output {
	pub package: tg::package::Id,
}

impl tg::Client {
	pub async fn create_package(
		&self,
		arg: tg::package::create::Arg,
	) -> tg::Result<tg::package::create::Output> {
		let method = http::Method::POST;
		let uri = "/packages";
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
