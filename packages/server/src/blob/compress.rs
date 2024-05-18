use crate::Server;
use std::pin::Pin;
use tangram_client as tg;
use tokio::io::AsyncRead;

impl Server {
	pub async fn compress_blob(
		&self,
		blob: &tg::Blob,
		format: tg::blob::compress::Format,
	) -> tg::Result<tg::Blob> {
		let reader = blob.reader(self).await?;
		let reader: Pin<Box<dyn AsyncRead + Send + 'static>> = match format {
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
		let blob = tg::Blob::with_reader(self, reader).await?;
		Ok(blob)
	}
}
