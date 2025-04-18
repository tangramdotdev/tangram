use super::Runtime;
use crate::temp::Temp;
use futures::TryStreamExt as _;
use num::ToPrimitive as _;
use std::{
	sync::{Arc, atomic::AtomicU64},
	time::Duration,
};
use tangram_client as tg;
use tokio::io::{AsyncBufReadExt as _, BufReader};
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
		let url = args
			.first()
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse::<Url>()
			.map_err(|source| tg::error!(!source, "invalid url"))?;
		let extract = args
			.get(1)
			.map(|value| {
				value
					.try_unwrap_bool_ref()
					.copied()
					.map_err(|_| tg::error!("expected a boolean"))
			})
			.transpose()?;

		// Send the request.
		let response = reqwest::get(url.clone())
			.await
			.map_err(|source| tg::error!(!source, %url, "failed to perform the request"))?
			.error_for_status()
			.map_err(|source| tg::error!(!source, %url, "expected a success status"))?;

		// Log that the download started.
		let message = format!("downloading from \"{url}\"\n");
		let arg = tg::process::log::post::Arg {
			bytes: message.into(),
			remote: process.remote().cloned(),
			stream: tg::process::log::Stream::Stderr,
		};
		server.try_post_process_log(process.id(), arg).await.ok();

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

		// Create the reader.
		let stream = response
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
			});
		let reader = StreamReader::new(stream);
		let mut reader = BufReader::with_capacity(8192, reader);

		// Fill the buffer.
		let header = reader
			.fill_buf()
			.await
			.map_err(|source| tg::error!(!source, "failed to fill the buffer"))?;

		// Detect the archive format.
		let extract = match extract {
			Some(true) => Some(
				super::util::detect_archive_format(header)?
					.ok_or_else(|| tg::error!("failed to determine the archive format"))?,
			),
			Some(false) => None,
			None => super::util::detect_archive_format(header)?,
		};

		// Download or extract to the temp.
		let temp = Temp::new(server);
		if let Some((format, compression)) = extract {
			// Extract to the temp.
			match format {
				tg::artifact::archive::Format::Tar => {
					self.extract_tar(&temp, reader, compression).await?;
				},
				tg::artifact::archive::Format::Zip => {
					self.extract_zip(&temp, reader).await?;
				},
			}
		} else {
			// Download to the temp.
			let mut file = tokio::fs::File::create(temp.path())
				.await
				.map_err(|source| tg::error!(!source, "failed create the temp file"))?;
			tokio::io::copy(&mut reader, &mut file)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to the temp file"))?;
		}

		// Check in the temp.
		let arg = tg::artifact::checkin::Arg {
			cache: false,
			destructive: true,
			deterministic: true,
			ignore: false,
			locked: false,
			lockfile: false,
			path: temp.path().to_owned(),
		};
		let artifact = tg::Artifact::check_in(server, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the downloaded file"))?;

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
			remote: process.remote().cloned(),
			stream: tg::process::log::Stream::Stderr,
		};
		server.try_post_process_log(process.id(), arg).await.ok();

		let output = if extract.is_some() {
			artifact.into()
		} else {
			artifact
				.try_unwrap_file()
				.map_err(|_| tg::error!("expected a file"))?
				.contents(server)
				.await?
				.into()
		};

		Ok(output)
	}
}
