use {
	crate::prelude::*,
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
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the error response"))?;
			return Err(error);
		}
		let output = response
		.json()
		.await
		.map_err(|source| tg::error!(!source, "failed to deserialize the response"))?;
		Ok(output)
	}
}
