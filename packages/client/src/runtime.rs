use crate as tg;
use tangram_http::{response::Ext as _, request::builder::Ext as _};

impl tg::Client {
	pub async fn get_js_runtime_doc(&self) -> tg::Result<serde_json::Value> {
		let method = http::Method::GET;
		let uri = "/runtimes/js/doc";
		let request = http::request::Builder::default().method(method).uri(uri);
		let request = request.empty().unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
