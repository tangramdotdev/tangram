use super::Runtime;
use crate::tmp::Tmp;
use tangram_client as tg;
use tokio_util::io::SyncIoBridge;

impl Runtime {
	pub async fn extract(
		&self,
		build: &tg::Build,
		_remote: Option<String>,
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
