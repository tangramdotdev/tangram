use {
	crate::prelude::*,
	std::path::PathBuf,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{is_false, is_true, return_true},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub hostname: Option<String>,

	pub host: String,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub mounts: Vec<Mount>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub network: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub user: Option<String>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Mount {
	#[tangram_serialize(id = 0)]
	pub source: PathBuf,

	#[tangram_serialize(id = 1)]
	pub target: PathBuf,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	#[tangram_serialize(id = 2, default = "return_true", skip_serializing_if = "is_true")]
	pub readonly: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::sandbox::Id,
}

impl tg::Client {
	pub async fn create_sandbox(&self, arg: Arg) -> tg::Result<tg::sandbox::create::Output> {
		let method = http::Method::POST;
		let uri = "/sandbox/create";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
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
		let output = response
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the response"))?;
		Ok(output)
	}
}
