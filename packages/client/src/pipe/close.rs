use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

impl tg::Client {
	pub async fn close_pipe(&self, id: &tg::pipe::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/pipes/{id}/close");
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
