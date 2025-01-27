use crate::{self as tg, Client};
use bytes::Bytes;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

impl Client {
	pub async fn write_pipe(&self, id: &tg::pipe::Id, bytes: Bytes) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/pipes/{id}/write");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.bytes(bytes)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
