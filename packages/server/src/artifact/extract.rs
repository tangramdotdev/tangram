use crate::{tmp::Tmp, Server};
use tangram_client as tg;
use tokio_util::io::SyncIoBridge;

impl Server {
	pub async fn extract_artifact(
		&self,
		blob: &tg::Blob,
		format: Option<tg::artifact::archive::Format>,
	) -> tg::Result<tg::Artifact> {
		// Create the reader.
		let reader = blob.reader(self).await?;

		// Create a temporary path.
		let tmp = Tmp::new(self);
		let path = tmp.as_ref().join("archive");

		// Extract in a blocking task.
		tokio::task::spawn_blocking({
			let reader = SyncIoBridge::new(reader);
			let path = path.clone();
			move || {
				let format = format
					.ok_or_else(|| tg::error!("archive format detection not yet implemented"))?;
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
		let path = path.try_into()?;
		let artifact = tg::Artifact::check_in(self, path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the extracted archive"))?;

		Ok(artifact)
	}
}
