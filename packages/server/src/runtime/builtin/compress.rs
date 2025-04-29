use super::Runtime;
use crate::runtime::util;
use std::{pin::Pin, time::Duration};
use tangram_client as tg;
use tangram_futures::read::shared_position_reader::SharedPositionReader;
use tokio::io::AsyncRead;

impl Runtime {
	pub async fn compress(&self, process: &tg::Process) -> tg::Result<crate::runtime::Output> {
		let server = &self.server;
		let command = process.command(server).await?;

		// Get the args.
		let args = command.args(server).await?;

		// Get the blob.
		let input = args
			.first()
			.ok_or_else(|| tg::error!("invalid number of arguments"))?;
		let blob = match input {
			tg::Value::Object(tg::Object::Blob(blob)) => blob.clone(),
			tg::Value::Object(tg::Object::File(file)) => file.contents(server).await?,
			_ => return Err(tg::error!("expected a blob or a file")),
		};

		// Get the format.
		let format = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse::<tg::CompressionFormat>()
			.map_err(|source| tg::error!(!source, "invalid format"))?;

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
						title: "compressing".to_owned(),
						total: Some(size),
					};
					let message = format!("{indicator}\n");
					util::log(&server, &process, tg::process::log::Stream::Stderr, message).await;
					tokio::time::sleep(Duration::from_secs(1)).await;
				}
			}
		});
		let log_task_abort_handle = log_task.abort_handle();
		scopeguard::defer! {
			log_task_abort_handle.abort();
		};

		// Compress the blob.
		let reader: Pin<Box<dyn AsyncRead + Send + 'static>> = match format {
			tg::CompressionFormat::Bz2 => {
				Box::pin(async_compression::tokio::bufread::BzEncoder::new(reader))
			},
			tg::CompressionFormat::Gz => {
				Box::pin(async_compression::tokio::bufread::GzipEncoder::new(reader))
			},
			tg::CompressionFormat::Xz => {
				Box::pin(async_compression::tokio::bufread::XzEncoder::new(reader))
			},
			tg::CompressionFormat::Zstd => {
				Box::pin(async_compression::tokio::bufread::ZstdEncoder::new(reader))
			},
		};
		let blob = tg::Blob::with_reader(server, reader).await?;

		log_task.abort();

		// Log that the compression finished.
		let message = "finished compressing\n";
		util::log(
			server,
			process,
			tg::process::log::Stream::Stderr,
			message.to_owned(),
		)
		.await;

		let output = if input.is_blob() {
			blob.into()
		} else if input.is_file() {
			tg::File::with_contents(blob).into()
		} else {
			unreachable!()
		};

		let output = crate::runtime::Output {
			checksum: None,
			error: None,
			exit: 0,
			output: Some(output),
		};

		Ok(output)
	}
}
