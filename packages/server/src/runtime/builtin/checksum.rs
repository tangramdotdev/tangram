use crate::Server;

use super::Runtime;
use num::ToPrimitive;
use tangram_client as tg;
use tokio::io::AsyncWrite;

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

	async fn checksum_artifact(
		&self,
		artifact: &tg::Artifact,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		match algorithm {
			tg::checksum::Algorithm::None => Ok(tg::Checksum::None),
			tg::checksum::Algorithm::Unsafe => Ok(tg::Checksum::Unsafe),
			_ => {
				let blob = self.create_blob_from_artifact(artifact).await?;
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

	async fn create_blob_from_artifact(&self, artifact: &tg::Artifact) -> tg::Result<tg::Blob> {
		// Create a duplex stream.
		let (reader, writer) = tokio::io::duplex(8192);

		// Create the archive future.
		let server = self.server.clone();
		let archive_future = async move {
			// Create the writer.
			let mut writer = writer::Writer::new(writer);

			// Write the archive header.
			writer.write_header().await?;

			// Archive the artifact.
			create_blob_from_artifact_inner(&server, &mut writer, artifact).await?;

			// Finish the archive.
			writer.finish().await?;

			Ok::<_, tg::Error>(())
		};

		// Create the blob future.
		let blob_future = tg::Blob::with_reader(&self.server, reader);

		let blob = match futures::future::join(archive_future, blob_future).await {
			(_, Ok(blob)) => blob,
			(Err(source), _) | (_, Err(source)) => {
				return Err(tg::error!(
					!source,
					"failed to join the archive and blob futures"
				));
			},
		};

		Ok(blob)
	}
}

async fn create_blob_from_artifact_inner<W>(
	server: &Server,
	writer: &mut writer::Writer<W>,
	artifact: &tg::Artifact,
) -> tg::Result<()>
where
	W: AsyncWrite + Unpin + Send + Sync,
{
	match artifact {
		tg::Artifact::Directory(directory) => {
			let entries = directory.entries(server).await?;
			writer
				.write_directory(entries.len().to_u64().unwrap())
				.await?;
			for (entry, artifact) in entries {
				writer.write_directory_entry(entry.as_str()).await?;
				Box::pin(create_blob_from_artifact_inner(
					server,
					writer,
					&artifact.clone(),
				))
				.await?;
			}
			Ok(())
		},
		tg::Artifact::File(file) => {
			if !file.dependencies(server).await?.is_empty() {
				return Err(tg::error!("cannot archive a file with dependencies"));
			}
			let executable = file.executable(server).await?;
			let size = file.size(server).await?;
			let mut reader = file.read(server, tg::blob::read::Arg::default()).await?;
			writer.write_file(executable, size, &mut reader).await
		},
		tg::Artifact::Symlink(symlink) => {
			let target = symlink
				.target(server)
				.await?
				.ok_or_else(|| tg::error!("cannot archive a symlink without a target"))?;
			writer
				.write_symlink(target.to_string_lossy().as_ref())
				.await
		},
	}
}
