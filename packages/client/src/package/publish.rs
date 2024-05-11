use crate as tg;
use tangram_http::{incoming::ResponseExt as _, outgoing::RequestBuilderExt as _};

impl tg::Client {
	pub async fn publish_package(&self, id: &tg::artifact::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/packages/{id}/publish");
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
