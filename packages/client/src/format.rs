use crate as tg;
use std::path::PathBuf;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub path: PathBuf,
}

impl tg::Client {
	pub async fn format(&self, arg: tg::format::Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/format";
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
		Ok(())
	}
}
