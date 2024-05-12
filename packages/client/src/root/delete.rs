use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

impl tg::Client {
	pub async fn delete_root(&self, name: &str) -> tg::Result<()> {
		let method = http::Method::DELETE;
		let name = urlencoding::encode(name);
		let uri = format!("/roots/{name}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
