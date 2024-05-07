use crate::Server;
use futures::TryStreamExt as _;
use std::sync::Arc;
use tangram_client as tg;
use tangram_http::{
	incoming::RequestExt as _, outgoing::ResponseBuilderExt as _, Incoming, Outgoing,
};
use tokio_util::io::StreamReader;

impl Server {
	pub async fn download_blob(
		&self,
		arg: tg::blob::download::Arg,
	) -> tg::Result<tg::blob::download::Output> {
		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let response = reqwest::get(arg.url.clone())
			.await
			.map_err(|source| tg::error!(!source, %url = arg.url, "failed to perform the request"))?
			.error_for_status()
			.map_err(|source| tg::error!(!source, %url = arg.url, "expected a sucess status"))?;

		// // Spawn a task to log progress.
		// let n = Arc::new(AtomicU64::new(0));
		// let content_length = response.content_length();
		// let log_task = tokio::spawn({
		// 	let server = self.clone();
		// 	let build = build.clone();
		// 	let url = arg.url.clone();
		// 	let n = n.clone();
		// 	async move {
		// 		loop {
		// 			let n = n.load(std::sync::atomic::Ordering::Relaxed);
		// 			let message =
		// 				if let Some(content_length) = content_length {
		// 					let percent =
		// 						100.0 * n.to_f64().unwrap() / content_length.to_f64().unwrap();
		// 					format!("downloading from \"{url}\": {n} of {content_length} {percent:.2}%\n")
		// 				} else {
		// 					format!("downloading from \"{url}\": {n}\n")
		// 				};
		// 			build.add_log(&server, message.into()).await.ok();
		// 			tokio::time::sleep(std::time::Duration::from_secs(1)).await;
		// 		}
		// 	}
		// });
		// let log_task_abort_handle = log_task.abort_handle();
		// scopeguard::defer! {
		// 	log_task_abort_handle.abort();
		// };

		// Create a stream of chunks.
		let checksum_writer = tg::checksum::Writer::new(arg.checksum.algorithm());
		let checksum_writer = Arc::new(std::sync::Mutex::new(Some(checksum_writer)));
		let stream = response
			.bytes_stream()
			.map_err(std::io::Error::other)
			.inspect_ok({
				// let n = n.clone();
				let checksum_writer = checksum_writer.clone();
				move |chunk| {
					// n.fetch_add(
					// 	chunk.len().to_u64().unwrap(),
					// 	std::sync::atomic::Ordering::Relaxed,
					// );
					checksum_writer
						.lock()
						.unwrap()
						.as_mut()
						.unwrap()
						.update(chunk);
				}
			});

		// Create the blob and validate.
		let blob = tg::Blob::with_reader(self, StreamReader::new(stream), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the blob"))?;

		// Abort the log task.
		// log_task.abort();

		// // Log that the download finished.
		// let message = format!("finished download from \"{}\"\n", arg.url);
		// build
		// 	.add_log(self, message.into())
		// 	.await
		// 	.map_err(|source| tg::error!(!source, "failed to add the log"))?;

		// Verify the checksum.
		let checksum_writer = checksum_writer.lock().unwrap().take().unwrap();
		let actual = checksum_writer.finalize();
		if actual != arg.checksum {
			return Err(
				tg::error!(%url = arg.url, %actual, %expected = arg.checksum, "the checksum did not match"),
			);
		}

		// Create the output.
		let blob = blob.id(self, None).await?;
		let output = tg::blob::download::Output { blob };

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_download_blob_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.download_blob(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
