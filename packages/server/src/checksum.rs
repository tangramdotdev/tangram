use {
	crate::Server, num::ToPrimitive as _, tangram_client as tg, tangram_futures::write::Ext as _,
	tokio::io::AsyncWriteExt as _,
};

impl Server {
	pub(crate) async fn checksum_blob(
		&self,
		blob: &tg::Blob,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		let mut writer = tg::checksum::Writer::new(algorithm);
		let mut reader = blob.read(self, tg::read::Options::default()).await?;
		tokio::io::copy(&mut reader, &mut writer)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the file contents"))?;
		let checksum = writer.finalize();
		Ok(checksum)
	}

	pub(crate) async fn checksum_artifact(
		&self,
		artifact: &tg::Artifact,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		let mut writer = tg::checksum::Writer::new(algorithm);
		writer
			.write_uvarint(0)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the archive version"))?;
		self.checksum_artifact_inner(&mut writer, artifact).await?;
		let checksum = writer.finalize();
		Ok(checksum)
	}

	async fn checksum_artifact_inner(
		&self,
		writer: &mut tg::checksum::Writer,
		artifact: &tg::Artifact,
	) -> tg::Result<()> {
		match artifact {
			tg::Artifact::Directory(directory) => {
				let entries = directory.entries(self).await?;
				writer
					.write_uvarint(0)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the artifact kind"))?;
				let len = entries.len().to_u64().unwrap();
				writer.write_uvarint(len).await.map_err(|source| {
					tg::error!(!source, "failed to write the number of directory entries")
				})?;
				for (name, artifact) in entries {
					let len = name.len().to_u64().unwrap();
					writer
						.write_uvarint(len)
						.await
						.map_err(|source| tg::error!(!source, "failed to write the name length"))?;
					writer
						.write_all(name.as_bytes())
						.await
						.map_err(|source| tg::error!(!source, "failed to write the name"))?;
					Box::pin(self.checksum_artifact_inner(writer, &artifact.clone())).await?;
				}
			},
			tg::Artifact::File(file) => {
				if !file.dependencies(self).await?.is_empty() {
					return Err(tg::error!("cannot checksum a file with dependencies"));
				}
				let executable = file.executable(self).await?;
				let length = file.length(self).await?;
				let mut reader = file.read(self, tg::read::Options::default()).await?;
				writer
					.write_uvarint(1)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the artifact kind"))?;
				writer
					.write_uvarint(executable.into())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the executable bit"))?;
				writer
					.write_uvarint(length)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the file length"))?;
				tokio::io::copy(&mut reader, writer)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the file contents"))?;
			},
			tg::Artifact::Symlink(symlink) => {
				if symlink.artifact(self).await?.is_some() {
					return Err(tg::error!("cannot checksum a symlink with an artifact"));
				}
				let path = symlink
					.path(self)
					.await?
					.ok_or_else(|| tg::error!("cannot checksum a symlink without a path"))?;
				let target = path.to_string_lossy();
				writer
					.write_uvarint(2)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the artifact kind"))?;
				let len = target.len().to_u64().unwrap();
				writer.write_uvarint(len).await.map_err(|source| {
					tg::error!(!source, "failed to write the target path length")
				})?;
				writer
					.write_all(target.as_bytes())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the target path"))?;
			},
		}
		Ok(())
	}
}
