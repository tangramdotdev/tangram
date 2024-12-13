use super::Runtime;
use futures::TryStreamExt as _;
use num::ToPrimitive as _;
use std::{
	sync::{atomic::AtomicU64, Arc},
	time::Duration,
};
use tangram_client as tg;
use tokio_util::io::StreamReader;
use url::Url;

impl Runtime {
	pub async fn download(
		&self,
		build: &tg::Build,
		remote: Option<String>,
	) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the target.
		let target = build.target(server).await?;

		// Ensure the target has a checksum.
		if target.checksum(server).await?.is_none() {
			return Err(tg::error!("a download must have a checksum"));
		}

		// Get the args.
		let args = target.args(server).await?;

		// Get the URL.
		let url = args
			.get(1)
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

		// Spawn a task to log progress.
		let downloaded = Arc::new(AtomicU64::new(0));
		let content_length = response.content_length();
		let log_task = tokio::spawn({
			let server = server.clone();
			let build = build.clone();
			let remote = remote.clone();
			let url = url.clone();
			let downloaded = downloaded.clone();
			async move {
				let first_message = format!("downloading from \"{url}\"\n");
				let arg = tg::build::log::post::Arg {
					bytes: first_message.into(),
					remote: remote.clone(),
				};
				let result = build.add_log(&server, arg).await;
				if result.is_err() {
					return;
				}
				loop {
					let downloaded = downloaded.load(std::sync::atomic::Ordering::Relaxed);
					let indicator = tg::progress::Indicator {
						current: Some(downloaded),
						format: tg::progress::IndicatorFormat::Bytes,
						name: String::new(),
						title: "downloading".to_owned(),
						total: content_length,
					};
					let message = indicator.to_string();
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

		// Create the reader.
		let reader = StreamReader::new(
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

		// Create the blob.
		let blob = tg::Blob::with_reader(server, reader)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the blob"))?;

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
		let arg = tg::build::log::post::Arg {
			bytes: message.into(),
			remote: remote.clone(),
		};
		build.add_log(server, arg).await.ok();

		Ok(blob.into())
	}
}
