use super::Runtime;
use crate::tmp::Tmp;
use byte_unit::Byte;
use num::ToPrimitive;
use std::time::Duration;
use tangram_client as tg;
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
		let reader = blob.progress_reader(server).await?;
		let extracted = reader.position();
		let content_length = reader.size();
		let log_task = tokio::spawn({
			let server = server.clone();
			let build = build.clone();
			let remote = remote.clone();
			async move {
				loop {
					let extracted = extracted.load(std::sync::atomic::Ordering::Relaxed);
					let percent =
						100.0 * extracted.to_f64().unwrap() / content_length.to_f64().unwrap();
					let extracted = Byte::from_u64(extracted);
					let content_length = Byte::from_u64(content_length);
					let message =
						format!("extracting: {extracted:#} of {content_length:#} {percent:.2}%\n");
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
		let tmp = Tmp::new(server);
		let path = tmp.as_ref().join("archive");

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
		let message = format!("finished extracting\n");
		let arg = tg::build::log::post::Arg {
			bytes: message.into(),
			remote: remote.clone(),
		};
		build.add_log(server, arg).await.ok();

		// Check in the extracted artifact.
		let arg = tg::artifact::checkin::Arg {
			destructive: true,
			deterministic: true,
			ignore: false,
			locked: true,
			path,
		};
		let artifact = tg::Artifact::check_in(server, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the extracted archive"))?;

		Ok(artifact.into())
	}
}
