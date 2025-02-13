use crate as tg;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub module: tg::Module,
}

impl tg::Client {
	pub async fn format_package(&self, arg: tg::package::format::Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/packages/format";
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
