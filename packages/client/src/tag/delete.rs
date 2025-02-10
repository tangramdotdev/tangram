use crate as tg;
use tangram_http::{response::Ext as _, request::builder::Ext as _};

impl tg::Client {
	pub async fn delete_tag(&self, tag: &tg::Tag) -> tg::Result<()> {
		let method = http::Method::DELETE;
		let uri = format!("/tags/{tag}");
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
