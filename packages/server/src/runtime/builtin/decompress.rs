use super::Runtime;
use std::{pin::Pin, time::Duration};
use tangram_client as tg;
use tangram_futures::read::shared_position_reader::SharedPositionReader;
use tokio::io::{AsyncBufReadExt as _, AsyncRead};

impl Runtime {
	pub async fn decompress(&self, process: &tg::Process) -> tg::Result<tg::Value> {
		let server = &self.server;
		let command = process.command(server).await?;

		// Get the args.
		let args = command.args(server).await?;

		// Get the blob.
		let input = args
			.first()
			.ok_or_else(|| tg::error!("invalid number of arguments"))?;
		let blob = match input {
			tg::Value::Object(tg::Object::Branch(branch)) => tg::Blob::Branch(branch.clone()),
			tg::Value::Object(tg::Object::Leaf(leaf)) => tg::Blob::Leaf(leaf.clone()),
			tg::Value::Object(tg::Object::File(file)) => file.contents(server).await?,
			_ => return Err(tg::error!("expected a blob or a file")),
		};

		// Create the reader.
		let reader = blob.read(server, tg::blob::read::Arg::default()).await?;
		let mut reader = SharedPositionReader::with_reader_and_position(reader, 0)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the shared position reader"))?;

		// Detect the compression format.
		let buffer = reader
			.fill_buf()
			.await
			.map_err(|source| tg::error!(!source, "failed to fill the buffer"))?;
		let format = super::util::detect_compression_format(buffer)?
			.ok_or_else(|| tg::error!("invalid compression format"))?;

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
						stream: tg::process::log::Stream::Stderr,
					};
					let result = server.try_post_process_log(process.id(), arg).await;
					if let Err(error) = result {
						tracing::error!(?error, "failed to post process log");
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
			stream: tg::process::log::Stream::Stderr,
		};
		server.try_post_process_log(process.id(), arg).await.ok();

		let output = if input.is_blob() {
			blob.into()
		} else if input.is_file() {
			tg::File::with_contents(blob).into()
		} else {
			unreachable!()
		};

		Ok(output)
	}
}
