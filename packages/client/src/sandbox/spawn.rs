use {
	crate::prelude::*,
	std::path::PathBuf,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub command: PathBuf,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub args: Vec<String>,

	pub stdin: tg::process::Stdio,

	pub stdout: tg::process::Stdio,

	pub stderr: tg::process::Stdio,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub pid: i32,
}

impl tg::Client {
	pub async fn sandbox_spawn(
		&self,
		id: &tg::sandbox::Id,
		arg: Arg,
	) -> tg::Result<Output> {
		let method = http::Method::POST;
		let uri = format!("/sandbox/{id}/spawn");
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
