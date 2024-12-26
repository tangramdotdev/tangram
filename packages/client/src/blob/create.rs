use crate as tg;
use tangram_http::{incoming::response::Ext as _, Outgoing};
use tokio::io::AsyncRead;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub blob: tg::blob::Id,
	pub metadata: tg::object::Metadata,
}

impl tg::Client {
	pub async fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::blob::create::Output> {
		let method = http::Method::POST;
		let uri = "/blobs";
		let body = Outgoing::reader(reader);
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
