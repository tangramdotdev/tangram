use super::Runtime;
use crate::temp::Temp;
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
		process: &tg::process::Id,
		command: &tg::Command,
		remote: Option<String>,
	) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Ensure the command has a checksum.
		if command.checksum(server).await?.is_none() {
			return Err(tg::error!("a download must have a checksum"));
		}

		// Get the args.
		let args = command.args(server).await?;

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
			let process = process.clone();
			let remote = remote.clone();
			let url = url.clone();
			let downloaded = downloaded.clone();
			async move {
				let first_message = format!("downloading from \"{url}\"\n");
				let arg = tg::process::log::post::Arg {
					bytes: first_message.into(),
					kind: tg::process::log::Kind::Stderr,
					remote: remote.clone(),
				};
				if !server
					.try_add_process_log(&process, arg)
					.await
					.map_or(true, |ok| ok.added)
				{
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
					let mut message = indicator.to_string();
					message.push('\n');
					let arg = tg::process::log::post::Arg {
						bytes: message.into(),
						kind: tg::process::log::Kind::Stderr,
						remote: remote.clone(),
					};
					if !server
						.try_add_process_log(&process, arg)
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

		// Create a temp file to download to.
		let temp = Temp::new(server);
		let mut file = tokio::fs::File::create(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed create the temp file"))?;
		tokio::io::copy(&mut reader, &mut file)
			.await
			.map_err(|source| tg::error!(!source, "failed to write to the temp file"))?;
		drop(file);

		// Create the blob.
		let output = server
			.create_blob_with_path(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the blob"))?;
		let blob = tg::Blob::with_id(output.blob);

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
		let arg = tg::process::log::post::Arg {
			bytes: message.into(),
			kind: tg::process::log::Kind::Stderr,
			remote: remote.clone(),
		};
		server.try_add_process_log(process, arg).await.ok();

		Ok(blob.into())
	}
}
