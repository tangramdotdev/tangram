use crate::Server;
use std::pin::Pin;
use tangram_client as tg;
use tangram_http::{incoming::RequestExt, Incoming, Outgoing};
use tokio::io::AsyncRead;

impl Server {
	pub async fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::compress::Arg,
	) -> tg::Result<tg::blob::compress::Output> {
		let blob = tg::Blob::with_id(id.clone());
		let reader = blob.reader(self).await?;
		let reader: Pin<Box<dyn AsyncRead + Send + 'static>> = match arg.format {
			tg::blob::compress::Format::Bz2 => {
				Box::pin(async_compression::tokio::bufread::BzEncoder::new(reader))
			},
			tg::blob::compress::Format::Gz => {
				Box::pin(async_compression::tokio::bufread::GzipEncoder::new(reader))
			},
			tg::blob::compress::Format::Xz => {
				Box::pin(async_compression::tokio::bufread::XzEncoder::new(reader))
			},
			tg::blob::compress::Format::Zstd => {
				Box::pin(async_compression::tokio::bufread::ZstdEncoder::new(reader))
			},
		};
		let blob = tg::Blob::with_reader(self, reader, None).await?;
		let id = blob.id(self, None).await?;
		let output = tg::blob::compress::Output { blob: id };
		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_compress_blob_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.compress_blob(&id, arg).await?;
		let response = http::Response::builder()
			.body(Outgoing::json(output))
			.unwrap();
		Ok(response)
	}
}
