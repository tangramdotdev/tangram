use crate as tg;
use tangram_http::{incoming::ResponseExt as _, Outgoing};

impl tg::Client {
	pub async fn clean(&self) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/clean";
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
		Ok(())
	}
}
