use super::Runtime;
use std::pin::Pin;
use tangram_client as tg;
use tokio::io::AsyncRead;

impl Runtime {
	pub async fn compress(
		&self,
		build: &tg::Build,
		_remote: Option<String>,
	) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the target.
		let target = build.target(server).await?;

		// Get the args.
		let args = target.args(server).await?;

		// Get the blob.
		let blob: tg::Blob = args
			.iter()
			.nth(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("expected a blob"))?;

		// Get the format.
		let format = args
			.iter()
			.nth(2)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse::<tg::blob::compress::Format>()
			.map_err(|source| tg::error!(!source, "invalid format"))?;

		// Compress the blob.
		let reader = blob.reader(server).await?;
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
		let blob = tg::Blob::with_reader(server, reader).await?;

		Ok(blob.into())
	}
}