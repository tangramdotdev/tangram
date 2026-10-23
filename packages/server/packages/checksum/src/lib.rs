use {
	futures::{FutureExt as _, future::BoxFuture},
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_futures::write::Ext as _,
	tangram_server_core::Session,
	tokio::io::AsyncWriteExt as _,
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn checksum_blob(
		&self,
		session: &Session,
		blob: &tg::Blob,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		let mut writer = tg::checksum::Writer::new(algorithm);
		let mut reader = blob
			.read_with_instance(session, tg::read::Options::default())
			.await
			.map_err(|error| tg::error!(!error, "failed to read the blob"))?;
		tokio::io::copy(&mut reader, &mut writer)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the file contents"))?;
		let checksum = writer.finalize();
		Ok(checksum)
	}

	pub(crate) async fn checksum_artifact(
		&self,
		session: &Session,
		artifact: &tg::Artifact,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		let mut writer = tg::checksum::Writer::new(algorithm);
		writer
			.write_uvarint(0)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the archive version"))?;
		self.checksum_artifact_inner(session, &mut writer, artifact)
			.await
			.map_err(|error| tg::error!(!error, "failed to checksum the artifact"))?;
		let checksum = writer.finalize();
		Ok(checksum)
	}

	async fn checksum_artifact_inner(
		&self,
		session: &Session,
		writer: &mut tg::checksum::Writer,
		artifact: &tg::Artifact,
	) -> tg::Result<()> {
		match artifact {
			tg::Artifact::Directory(directory) => {
				let entries = directory
					.entries_with_instance(session)
					.await
					.map_err(|error| tg::error!(!error, "failed to get directory entries"))?;
				writer
					.write_uvarint(0)
					.await
					.map_err(|error| tg::error!(!error, "failed to write the artifact kind"))?;
				let len = entries.len().to_u64().unwrap();
				writer.write_uvarint(len).await.map_err(|error| {
					tg::error!(!error, "failed to write the number of directory entries")
				})?;
				for (name, artifact) in entries {
					let len = name.len().to_u64().unwrap();
					writer
						.write_uvarint(len)
						.await
						.map_err(|error| tg::error!(!error, "failed to write the name length"))?;
					writer
						.write_all(name.as_bytes())
						.await
						.map_err(|error| tg::error!(!error, "failed to write the name"))?;
					Box::pin(self.checksum_artifact_inner(session, writer, &artifact.clone()))
						.await
						.map_err(|error| tg::error!(!error, %name, "failed to checksum entry"))?;
				}
			},
			tg::Artifact::File(file) => {
				let dependencies = file
					.dependencies_with_instance(session)
					.await
					.map_err(|error| tg::error!(!error, "failed to get file dependencies"))?;
				if !dependencies.is_empty() {
					return Err(tg::error!("cannot checksum a file with dependencies"));
				}
				let executable = file
					.executable_with_instance(session)
					.await
					.map_err(|error| tg::error!(!error, "failed to get executable bit"))?;
				let length = file
					.length_with_instance(session)
					.await
					.map_err(|error| tg::error!(!error, "failed to get file length"))?;
				let mut reader = file
					.read_with_instance(session, tg::read::Options::default())
					.await
					.map_err(|error| tg::error!(!error, "failed to read the file"))?;
				writer
					.write_uvarint(1)
					.await
					.map_err(|error| tg::error!(!error, "failed to write the artifact kind"))?;
				writer
					.write_uvarint(executable.into())
					.await
					.map_err(|error| tg::error!(!error, "failed to write the executable bit"))?;
				writer
					.write_uvarint(length)
					.await
					.map_err(|error| tg::error!(!error, "failed to write the file length"))?;
				tokio::io::copy(&mut reader, writer)
					.await
					.map_err(|error| tg::error!(!error, "failed to write the file contents"))?;
			},
			tg::Artifact::Symlink(symlink) => {
				let artifact = symlink
					.artifact_with_instance(session)
					.await
					.map_err(|error| tg::error!(!error, "failed to get symlink artifact"))?;
				if artifact.is_some() {
					return Err(tg::error!("cannot checksum a symlink with an artifact"));
				}
				let path = symlink
					.path_with_instance(session)
					.await
					.map_err(|error| tg::error!(!error, "failed to get symlink path"))?
					.ok_or_else(|| tg::error!("cannot checksum a symlink without a path"))?;
				let target = path.to_string_lossy();
				writer
					.write_uvarint(2)
					.await
					.map_err(|error| tg::error!(!error, "failed to write the artifact kind"))?;
				let len = target.len().to_u64().unwrap();
				writer.write_uvarint(len).await.map_err(|error| {
					tg::error!(!error, "failed to write the target path length")
				})?;
				writer
					.write_all(target.as_bytes())
					.await
					.map_err(|error| tg::error!(!error, "failed to write the target path"))?;
			},
		}
		Ok(())
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::checksum::Component for Component {
	fn checksum_blob<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		blob: &'a tg::Blob,
		algorithm: tg::checksum::Algorithm,
	) -> BoxFuture<'a, tg::Result<tg::Checksum>> {
		self.checksum_blob(session, blob, algorithm).boxed()
	}

	fn checksum_artifact<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		artifact: &'a tg::Artifact,
		algorithm: tg::checksum::Algorithm,
	) -> BoxFuture<'a, tg::Result<tg::Checksum>> {
		self.checksum_artifact(session, artifact, algorithm).boxed()
	}
}
