use super::Runtime;
use tangram_client as tg;

impl Runtime {
	pub async fn checksum(
		&self,
		build: &tg::Build,
		_remote: Option<String>,
	) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the target.
		let target = build.target(server).await?;

		// Get the args.
		let args = target.args(server).await?;

		// Get the object.
		let object = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_unwrap_object()
			.ok()
			.ok_or_else(|| tg::error!("expected an object"))?;

		// Get the algorithm.
		let algorithm = args
			.get(2)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse::<tg::checksum::Algorithm>()
			.map_err(|source| tg::error!(!source, "invalid algorithm"))?;

		// Compute the checksum.
		let checksum = if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
			self.checksum_artifact(&artifact, algorithm).await?
		} else if let Ok(blob) = tg::Blob::try_from(object.clone()) {
			self.checksum_blob(&blob, algorithm).await?
		} else {
			return Err(tg::error!("invalid object"));
		};

		Ok(checksum.to_string().into())
	}

	async fn checksum_artifact(
		&self,
		artifact: &tg::Artifact,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		match algorithm {
			tg::checksum::Algorithm::None => Ok(tg::Checksum::None),
			tg::checksum::Algorithm::Unsafe => Ok(tg::Checksum::Unsafe),
			_ => {
				let blob = artifact
					.archive(&self.server, tg::artifact::archive::Format::Tgar)
					.await?;
				self.checksum_blob(&blob, algorithm).await
			},
		}
	}

	async fn checksum_blob(
		&self,
		blob: &tg::Blob,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		let server = &self.server;
		let mut writer = tg::checksum::Writer::new(algorithm);
		let mut reader = blob.read(server, tg::blob::read::Arg::default()).await?;
		tokio::io::copy(&mut reader, &mut writer)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to copy from the reader to the writer")
			})?;
		let checksum = writer.finalize();
		Ok(checksum)
	}
}
