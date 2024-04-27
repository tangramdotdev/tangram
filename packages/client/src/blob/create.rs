use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};
use futures::TryStreamExt as _;
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
		let body = Outgoing::stream(
			ReaderStream::new(reader)
				.map_err(|source| tg::error!(!source, "failed to read from the reader")),
		);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.unwrap();
		let response = self.send(request).await?;
		let response = response.success().await?;
		let output = response.json().await?;
		Ok(output)
	}
}
