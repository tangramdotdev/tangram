use crate::{
	tmp::Tmp,
	util::http::{full, Incoming, Outgoing},
	Http, Server,
};
use http_body_util::BodyExt as _;
use tangram_client as tg;
use tokio_util::io::SyncIoBridge;

impl Server {
	pub async fn extract_artifact(
		&self,
		arg: tg::artifact::ExtractArg,
	) -> tg::Result<tg::artifact::ExtractOutput> {
		// Create the reader.
		let blob = tg::Blob::with_id(arg.blob);
		let reader = blob.reader(self).await?;

		// Create a temporary path.
		let tmp = Tmp::new(self);
		let path = tmp.as_ref().join("archive");

		// Extract in a blocking task.
		tokio::task::spawn_blocking({
			let reader = SyncIoBridge::new(reader);
			let path = path.clone();
			move || {
				match arg.format {
					tg::artifact::ArchiveFormat::Tar => {
						let mut archive = tar::Archive::new(reader);
						archive.set_preserve_permissions(false);
						archive.set_unpack_xattrs(false);
						archive.unpack(path).map_err(|source| {
							tg::error!(!source, "failed to extract the archive")
						})?;
					},
					tg::artifact::ArchiveFormat::Zip => {
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
		let artifact = tg::Artifact::check_in(self, &path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the extracted archive"))?;

		// Create the output.
		let id = artifact.id(self, None).await?;
		let output = tg::artifact::ExtractOutput { id };

		Ok(output)
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_extract_artifact_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;

		// Extract the blob.
		let output = self.handle.extract_artifact(arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the response"))?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}
}
