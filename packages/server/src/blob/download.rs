use crate::Server;
use futures::TryStreamExt as _;
use num::ToPrimitive as _;
use std::sync::{atomic::AtomicU64, Arc};
use tangram_client as tg;
use tokio_util::io::StreamReader;
use url::Url;

impl Server {
	pub async fn download_blob(
		&self,
		url: &Url,
		checksum: &tg::Checksum,
		build: &tg::Build,
	) -> tg::Result<tg::Blob> {
		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let response = reqwest::get(url.clone())
			.await
			.map_err(|source| tg::error!(!source, %url, "failed to perform the request"))?
			.error_for_status()
			.map_err(|source| tg::error!(!source, %url, "expected a sucess status"))?;

		// Spawn a task to log progress.
		let downloaded = Arc::new(AtomicU64::new(0));
		let content_length = response.content_length();
		let log_task = tokio::spawn({
			let server = self.clone();
			let build = build.clone();
			let url = url.clone();
			let downloaded = downloaded.clone();
			async move {
				loop {
					let downloaded = downloaded.load(std::sync::atomic::Ordering::Relaxed);
					let message = if let Some(content_length) = content_length {
						let percent =
							100.0 * downloaded.to_f64().unwrap() / content_length.to_f64().unwrap();
						let downloaded = byte_unit::Byte::from_u64(downloaded);
						let content_length = byte_unit::Byte::from_u64(content_length);
						format!("downloading from \"{url}\": {downloaded} of {content_length} {percent:.2}%\n")
					} else {
						let downloaded = byte_unit::Byte::from_u64(downloaded);
						format!("downloading from \"{url}\": {downloaded}\n")
					};
					let result = build.add_log(&server, message.into()).await;
					if result.is_err() {
						break;
					}
					tokio::time::sleep(std::time::Duration::from_secs(1)).await;
				}
			}
		});
		let log_task_abort_handle = log_task.abort_handle();
		scopeguard::defer! {
			log_task_abort_handle.abort();
		};

		// Create a stream of chunks.
		let checksum_writer = tg::checksum::Writer::new(checksum.algorithm());
		let checksum_writer = Arc::new(std::sync::Mutex::new(Some(checksum_writer)));
		let stream = response
			.bytes_stream()
			.map_err(std::io::Error::other)
			.inspect_ok({
				let n = downloaded.clone();
				let checksum_writer = checksum_writer.clone();
				move |chunk| {
					n.fetch_add(
						chunk.len().to_u64().unwrap(),
						std::sync::atomic::Ordering::Relaxed,
					);
					checksum_writer
						.lock()
						.unwrap()
						.as_mut()
						.unwrap()
						.update(chunk);
				}
			});

		// Create the blob and validate.
		let blob = tg::Blob::with_reader(self, StreamReader::new(stream))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the blob"))?;

		// Abort the log task.
		log_task.abort();

		// Log that the download finished.
		let message = format!("finished download from \"{url}\"\n");
		build.add_log(self, message.into()).await.ok();

		// Verify the checksum.
		let checksum_writer = checksum_writer.lock().unwrap().take().unwrap();
		let actual = checksum_writer.finalize();
		if &actual != checksum {
			return Err(
				tg::error!(%url = url, %actual, %expected = checksum, "the checksum did not match"),
			);
		}

		Ok(blob)
	}
}
