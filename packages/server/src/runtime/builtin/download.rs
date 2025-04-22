use super::Runtime;
use crate::{runtime::util, temp::Temp};
use futures::TryStreamExt as _;
use num::ToPrimitive as _;
use std::{
	sync::{Arc, atomic::AtomicU64},
	time::Duration,
};
use tangram_client as tg;
use tokio_util::io::StreamReader;
use url::Url;

impl Runtime {
	pub async fn download(&self, process: &tg::Process) -> tg::Result<tg::Value> {
		let server = &self.server;
		let command = process.command(server).await?;

		// Ensure the process has a checksum.
		if process.load(server).await?.checksum.is_none() {
			return Err(tg::error!("a download must have a checksum"));
		}

		// Get the args.
		let args = command.args(server).await?;

		// Get the URL.
		let url = args
			.first()
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse::<Url>()
			.map_err(|source| tg::error!(!source, "invalid url"))?;

		// Send the request.
		let response = reqwest::get(url.clone())
			.await
			.map_err(|source| tg::error!(!source, %url, "failed to perform the request"))?
			.error_for_status()
			.map_err(|source| tg::error!(!source, %url, "expected a success status"))?;

		// Log that the download started.
		let message = format!("downloading from \"{url}\"\n");
		util::log(server, process, tg::process::log::Stream::Stderr, message).await;

		// Create the log task.
		let downloaded = Arc::new(AtomicU64::new(0));
		let content_length = response.content_length();
		let log_task = tokio::spawn({
			let server = server.clone();
			let process = process.clone();
			let downloaded = downloaded.clone();
			async move {
				loop {
					let downloaded = downloaded.load(std::sync::atomic::Ordering::Relaxed);
					let indicator = tg::progress::Indicator {
						current: Some(downloaded),
						format: tg::progress::IndicatorFormat::Bytes,
						name: String::new(),
						title: "downloading".to_owned(),
						total: content_length,
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

		// Create the reader.
		let mut reader = StreamReader::new(
			response
				.bytes_stream()
				.map_err(std::io::Error::other)
				.inspect_ok({
					let n = downloaded.clone();
					move |bytes| {
						n.fetch_add(
							bytes.len().to_u64().unwrap(),
							std::sync::atomic::Ordering::Relaxed,
						);
					}
				}),
		);

		// Download to a temp file.
		let temp = Temp::new(server);
		let mut file = tokio::fs::File::create(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed create the temp file"))?;
		tokio::io::copy(&mut reader, &mut file)
			.await
			.map_err(|source| tg::error!(!source, "failed to write to the temp file"))?;
		drop(file);

		// Check in the temp file.
		let arg = tg::artifact::checkin::Arg {
			cache: false,
			destructive: true,
			deterministic: true,
			ignore: false,
			locked: false,
			lockfile: false,
			path: temp.path().to_owned(),
		};
		let blob = tg::Artifact::check_in(server, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the downloaded file"))?
			.unwrap_file()
			.contents(server)
			.await?;

		// Abort and await the log task.
		log_task.abort();
		match log_task.await {
			Ok(()) => Ok(()),
			Err(error) if error.is_cancelled() => Ok(()),
			Err(error) => Err(error),
		}
		.unwrap();

		// Log that the download finished.
		let message = format!("finished download from \"{url}\"\n");
		util::log(server, process, tg::process::log::Stream::Stderr, message).await;

		Ok(blob.into())
	}
}
