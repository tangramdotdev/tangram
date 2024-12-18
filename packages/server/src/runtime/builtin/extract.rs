use super::Runtime;
use crate::temp::Temp;
use std::time::Duration;
use tangram_client as tg;
use tangram_futures::read::SharedPositionReader;
use tokio_util::io::SyncIoBridge;

impl Runtime {
	pub async fn extract(
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
		let format = if let Some(value) = args.get(2) {
			let format = value
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("expected a string"))?
				.parse::<tg::artifact::archive::Format>()
				.map_err(|source| tg::error!(!source, "invalid format"))?;
			Some(format)
		} else {
			None
		};

		// Create the reader.
		let reader = blob.reader(server).await?;
		let reader = SharedPositionReader::new(reader)
			.await
			.map_err(|source| tg::error!(!source, "io error"))?;

		let position = reader.shared_position();
		let size = blob.size(server).await?;
		let log_task = tokio::spawn({
			let server = server.clone();
			let build = build.clone();
			let remote = remote.clone();
			async move {
				loop {
					let position = position.load(std::sync::atomic::Ordering::Relaxed);
					let indicator = tg::progress::Indicator {
						current: Some(position),
						format: tg::progress::IndicatorFormat::Bytes,
						name: String::new(),
						title: "extracting".to_owned(),
						total: Some(size),
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

		// Create a temporary path.
		let temp = Temp::new(server);
		let path = temp.as_ref().join("archive");

		// Extract in a blocking task.
		tokio::task::spawn_blocking({
			let reader = SyncIoBridge::new(reader);
			let path = path.clone();
			move || {
				let format = format
					.ok_or_else(|| tg::error!("archive format detection is unimplemented"))?;
				match format {
					tg::artifact::archive::Format::Tar => {
						let mut archive = tar::Archive::new(reader);
						archive.set_preserve_permissions(false);
						archive.set_unpack_xattrs(false);
						archive.unpack(path).map_err(|source| {
							tg::error!(!source, "failed to extract the archive")
						})?;
					},
					tg::artifact::archive::Format::Zip => {
						let mut archive = zip::ZipArchive::new(reader).map_err(|source| {
							tg::error!(!source, "failed to extract the archive")
						})?;
						archive.extract(&path).map_err(|source| {
							tg::error!(!source, "failed to extract the archive")
						})?;
					},
				}
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.unwrap()?;

		log_task.abort();

		// Log that the extraction finished.
		let message = "finished extracting\n";
		let arg = tg::build::log::post::Arg {
			bytes: message.into(),
			remote: remote.clone(),
		};
		build.add_log(server, arg).await.ok();

		// Check in the extracted artifact.
		let arg = tg::artifact::checkin::Arg {
			cache: true,
			destructive: true,
			deterministic: true,
			ignore: false,
			locked: true,
			lockfile: false,
			path,
		};
		let artifact = tg::Artifact::check_in(server, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the extracted archive"))?;

		Ok(artifact.into())
	}
}
