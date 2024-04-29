use crate as tg;
use tangram_http::{incoming::ResponseExt as _, Outgoing};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Health {
	pub version: Option<String>,
}

impl tg::Client {
	pub async fn health(&self) -> tg::Result<Health> {
		let method = http::Method::GET;
		let uri = "/health";
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
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
