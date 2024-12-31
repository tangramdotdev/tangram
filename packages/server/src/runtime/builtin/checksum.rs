use super::Runtime;
use num::ToPrimitive;
use tangram_client as tg;

mod writer;

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

	async fn checksum_blob(
		&self,
		blob: &tg::Blob,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		match algorithm {
			tg::checksum::Algorithm::None => Ok(tg::Checksum::None),
			tg::checksum::Algorithm::Unsafe => Ok(tg::Checksum::Unsafe),
			algorithm => {
				let mut writer = writer::Writer::new(algorithm);
				writer.write_header().await?;
				let reader = blob
					.read(&self.server, tg::blob::read::Arg::default())
					.await?;
				writer.write_blob(reader).await?;
				writer.finish().await
			},
		}
	}

	async fn checksum_artifact(
		&self,
		artifact: &tg::Artifact,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		match algorithm {
			tg::checksum::Algorithm::None => Ok(tg::Checksum::None),
			tg::checksum::Algorithm::Unsafe => Ok(tg::Checksum::Unsafe),
			algorithm => {
				let mut writer = writer::Writer::new(algorithm);
				writer.write_header().await?;
				self.checksum_artifact_inner(&mut writer, artifact).await?;
				writer.finish().await
			},
		}
	}

	async fn checksum_artifact_inner(
		&self,
		writer: &mut writer::Writer,
		artifact: &tg::Artifact,
	) -> tg::Result<()> {
		match artifact {
			tg::Artifact::Directory(directory) => {
				let entries = directory.entries(&self.server).await?;
				writer
					.write_directory(entries.len().to_u64().unwrap())
					.await?;
				for (entry, artifact) in entries {
					writer.write_directory_entry(entry.as_str()).await?;
					Box::pin(self.checksum_artifact_inner(writer, &artifact.clone())).await?;
				}
				Ok(())
			},
			tg::Artifact::File(file) => {
				if !file.dependencies(&self.server).await?.is_empty() {
					return Err(tg::error!("cannot checksum a file with dependencies"));
				}
				let executable = file.executable(&self.server).await?;
				let size = file.size(&self.server).await?;
				let mut reader = file
					.read(&self.server, tg::blob::read::Arg::default())
					.await?;
				writer.write_file(executable, size, &mut reader).await
			},
			tg::Artifact::Symlink(symlink) => {
				let target = symlink
					.target(&self.server)
					.await?
					.ok_or_else(|| tg::error!("cannot checksum a symlink without a target"))?;
				writer
					.write_symlink(target.to_string_lossy().as_ref())
					.await
			},
		}
	}
}
