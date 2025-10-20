use {
	crate as tg,
	tangram_http::{Body, response::Ext as _},
	tokio::io::AsyncRead,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub blob: tg::blob::Id,
}

impl tg::Client {
	pub async fn write(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::write::Output> {
		let method = http::Method::POST;
		let uri = "/write";
		let body = Body::with_reader(reader);
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
