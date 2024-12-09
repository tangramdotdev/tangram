use super::Runtime;
use byte_unit::Byte;
use num::ToPrimitive as _;
use std::{fmt::Write, pin::Pin, time::Duration};
use tangram_client as tg;
use tangram_futures::read::SharedPositionReader;
use tokio::io::AsyncRead;

impl Runtime {
	pub async fn decompress(
		&self,
		build: &tg::Build,
		remote: Option<String>,
	) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the target.
		let target = build.target(server).await?;

		// Get the args.
		let args = target.args(server).await?;

		// Get the blob.
		let blob: tg::Blob = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("expected a blob"))?;

		// Get the format.
		let format = args
			.get(2)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse::<tg::blob::compress::Format>()
			.map_err(|source| tg::error!(!source, "invalid format"))?;

		// Create the reader.
		let reader = blob.reader(server).await?;
		let reader = SharedPositionReader::new(reader)
			.await
			.map_err(|source| tg::error!(!source, "io error"))?;

		// Spawn a task to log progress.
		let position = reader.shared_position();
		let size = blob.size(server).await?;
		let log_task = tokio::spawn({
			let server = server.clone();
			let build = build.clone();
			let remote = remote.clone();
			async move {
				let bar_length = 20;
				let first_message = "decompressing\n".to_string();
				let arg = tg::build::log::post::Arg {
					bytes: first_message.into(),
					remote: remote.clone(),
				};
				let result = build.add_log(&server, arg).await;
				if result.is_err() {
					return;
				}
				loop {
					let position = position.load(std::sync::atomic::Ordering::Relaxed);
					let last = position * bar_length / size;
					let mut bar = String::new();
					write!(bar, " [").unwrap();
					for _ in 0..last {
						write!(bar, "=").unwrap();
					}
					if position < size {
						write!(bar, ">").unwrap();
					} else {
						write!(bar, "=").unwrap();
					}

					for _ in last..bar_length {
						write!(bar, " ").unwrap();
					}
					write!(bar, "]").unwrap();

					let percent = 100.0 * position.to_f64().unwrap() / size.to_f64().unwrap();
					let position = Byte::from_u64(position);
					let size = Byte::from_u64(size);
					let message = format!("{bar} {position:#} of {size:#} {percent:.2}%\n");
					let arg = tg::build::log::post::Arg {
						bytes: message.into(),
						remote: remote.clone(),
					};
					let result = build.add_log(&server, arg).await;
					if result.is_err() {
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
		let arg = tg::build::log::post::Arg {
			bytes: message.into(),
			remote: remote.clone(),
		};
		build.add_log(server, arg).await.ok();

		Ok(blob.into())
	}
}
