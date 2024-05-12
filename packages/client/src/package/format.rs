use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

impl tg::Client {
	pub async fn format_package(&self, dependency: &tg::Dependency) -> tg::Result<()> {
		let method = http::Method::POST;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/format");
		let request = http::request::Builder::default().method(method).uri(uri);
		let request = request.empty().unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
