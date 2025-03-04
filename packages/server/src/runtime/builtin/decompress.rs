use super::Runtime;
use crate::Server;
use std::{pin::Pin, time::Duration};
use tangram_client as tg;
use tangram_futures::read::shared_position_reader::SharedPositionReader;
use tokio::io::{AsyncRead, AsyncReadExt as _};

impl Runtime {
	pub async fn decompress(&self, process: &tg::Process) -> tg::Result<tg::Value> {
		let server = &self.server;
		let command = process.command(server).await?;

		// Get the args.
		let args = command.args(server).await?;

		// Get the blob.
		let blob: tg::Blob = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("expected a blob"))?;

		// Detect the format.
		let format = detect_compression(server, &blob).await?;

		// Create the reader.
		let reader = blob.read(server, tg::blob::read::Arg::default()).await?;
		let reader = SharedPositionReader::with_reader_and_position(reader, 0)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the shared position reader"))?;

		// Spawn a task to log progress.
		let position = reader.shared_position();
		let size = blob.length(server).await?;
		let log_task = tokio::spawn({
			let server = server.clone();
			let process = process.clone();
			async move {
				loop {
					let position = position.load(std::sync::atomic::Ordering::Relaxed);
					let indicator = tg::progress::Indicator {
						current: Some(position),
						format: tg::progress::IndicatorFormat::Bytes,
						name: String::new(),
						title: "decompressing".to_owned(),
						total: Some(size),
					};
					let message = format!("{indicator}\n");
					let arg = tg::process::log::post::Arg {
						bytes: message.into(),
						remote: process.remote().cloned(),
					};
					if !server
						.try_post_process_log(process.id(), arg)
						.await
						.map_or(true, |ok| ok.added)
					{
						break;
					}
					tokio::time::sleep(Duration::from_secs(1)).await;
				}
			}
		});
		let log_task_abort_handle = log_task.abort_handle();
		scopeguard::defer! {
			log_task_abort_handle.abort();
		};

		// Decompress the blob.
		let reader: Pin<Box<dyn AsyncRead + Send + 'static>> = match format {
			tg::blob::compress::Format::Bz2 => {
				Box::pin(async_compression::tokio::bufread::BzDecoder::new(reader))
			},
			tg::blob::compress::Format::Gz => {
				Box::pin(async_compression::tokio::bufread::GzipDecoder::new(reader))
			},
			tg::blob::compress::Format::Xz => {
				Box::pin(async_compression::tokio::bufread::XzDecoder::new(reader))
			},
			tg::blob::compress::Format::Zstd => {
				Box::pin(async_compression::tokio::bufread::ZstdDecoder::new(reader))
			},
		};

		let blob = tg::Blob::with_reader(server, reader).await?;

		log_task.abort();

		// Log that the decompression finished.
		let message = "finished decompressing\n";
		let arg = tg::process::log::post::Arg {
			bytes: message.into(),
			remote: process.remote().cloned(),
		};
		server.try_post_process_log(process.id(), arg).await.ok();

		Ok(blob.into())
	}
}

async fn detect_compression(
	server: &Server,
	blob: &tg::Blob,
) -> tg::Result<tg::blob::compress::Format> {
	// Read first 6 bytes.
	let mut magic_reader = blob
		.read(
			server,
			tg::blob::read::Arg {
				length: Some(6),
				..Default::default()
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create magic bytes reader"))?;
	let mut magic_bytes = [0u8; 6];
	let bytes_read = magic_reader
		.read(&mut magic_bytes)
		.await
		.map_err(|source| tg::error!(!source, "failed to read magic bytes"))?;

	let result = tg::blob::compress::Format::from_magic(&magic_bytes[..bytes_read]);
	match result {
		Some(format) => Ok(format),
		None => Err(tg::error!(%id = blob.id(server).await?, "unrecognized compression format")),
	}
}
