use crate::{self as tg, util::http::Outgoing};
use futures::TryStreamExt as _;
use http_body_util::{BodyExt as _, StreamBody};
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

impl tg::Client {
	pub async fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
		_transaction: Option<&()>,
	) -> tg::Result<tg::blob::Id> {
		let method = http::Method::POST;
		let uri = "/blobs";
		let body = Outgoing::new(StreamBody::new(
			ReaderStream::new(reader)
				.map_ok(hyper::body::Frame::data)
				.map_err(|source| tg::error!(!source, "failed to read from the reader")),
		));
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}
}
