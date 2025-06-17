use crate as tg;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	pub size: tg::pty::Size,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::pty::Id,
}

impl tg::Client {
	pub async fn create_pty(&self, arg: Arg) -> tg::Result<tg::pty::create::Output> {
		let method = http::Method::POST;
		let uri = "/ptys";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
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
