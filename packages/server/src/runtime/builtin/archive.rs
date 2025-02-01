use super::Runtime;
use crate::Server;
use std::path::Path;
use tangram_client as tg;
use tokio_util::compat::{
	FuturesAsyncWriteCompatExt as _, TokioAsyncReadCompatExt as _, TokioAsyncWriteCompatExt as _,
};

impl Runtime {
	pub async fn archive(&self, process: &tg::Process) -> tg::Result<tg::Value> {
		let server = &self.server;
		let command = process.command(server).await?;

		// Get the args.
		let args = command.args(server).await?;

		// Get the artifact.
		let artifact: tg::Artifact = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("expected an artifact"))?;

		// Get the format.
		let format = args
			.get(2)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse::<tg::artifact::archive::Format>()
			.map_err(|source| tg::error!(!source, "invalid format"))?;

		// Create the archive task.
		let blob = match format {
			tg::artifact::archive::Format::Tar => tar(server, &artifact).await?,
			tg::artifact::archive::Format::Zip => zip(server, &artifact).await?,
		};

		Ok(blob.into())
	}
}

async fn tar(server: &Server, artifact: &tg::Artifact) -> tg::Result<tg::Blob> {
	// Create a duplex stream.
	let (reader, writer) = tokio::io::duplex(8192);

	// Create the archive future.
	let archive_future = async move {
		// Create the tar builder.
		let mut builder = async_tar::Builder::new(writer.compat_write());

		// Archive the artifact.
		let directory = artifact
			.try_unwrap_directory_ref()
			.ok()
			.ok_or_else(|| tg::error!("can only tar a directory"))?;
		for (name, artifact) in directory.entries(server).await? {
			tar_inner(server, &mut builder, Path::new(&name), &artifact).await?;
		}

		// Finish writing the archive.
		builder
			.finish()
			.await
			.map_err(|source| tg::error!(!source, "failed to finish the archive"))?;

		Ok::<_, tg::Error>(())
	};

	// Create the blob future.
	let blob_future = tg::Blob::with_reader(server, reader);

	// Join the futures.
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

async fn tar_inner<W>(
	server: &Server,
	builder: &mut async_tar::Builder<W>,
	path: &Path,
	artifact: &tg::Artifact,
) -> tg::Result<()>
where
	W: futures::io::AsyncWrite + Unpin + Send + Sync,
{
	match artifact {
		tg::Artifact::Directory(directory) => {
			let mut header = async_tar::Header::new_gnu();
			header.set_size(0);
			header.set_entry_type(async_tar::EntryType::Directory);
			header.set_mode(0o755);
			builder
				.append_data(&mut header, path, &[][..])
				.await
				.map_err(|source| tg::error!(!source, "failed to append directory"))?;
			for (name, artifact) in directory.entries(server).await? {
				Box::pin(tar_inner(server, builder, &path.join(name), &artifact)).await?;
			}
			Ok(())
		},
		tg::Artifact::File(file) => {
			if !file.dependencies(server).await?.is_empty() {
				return Err(tg::error!("cannot archive a file with dependencies"));
			}
			let size = file.size(server).await?;
			let reader = file.read(server, tg::blob::read::Arg::default()).await?;
			let executable = file.executable(server).await?;
			let reader = reader.compat();
			let mut header = async_tar::Header::new_gnu();
			header.set_size(size);
			header.set_entry_type(async_tar::EntryType::Regular);
			let permissions = if executable { 0o0755 } else { 0o0644 };
			header.set_mode(permissions);
			builder
				.append_data(&mut header, path, reader)
				.await
				.map_err(|source| tg::error!(!source, "failed to append file"))
		},
		tg::Artifact::Symlink(symlink) => {
			let target = symlink
				.target(server)
				.await?
				.ok_or_else(|| tg::error!("cannot archive a symlink without a target"))?;
			let mut header = async_tar::Header::new_gnu();
			header.set_size(0);
			header.set_entry_type(async_tar::EntryType::Symlink);
			header.set_mode(0o777);
			header
				.set_link_name(target.to_string_lossy().as_ref())
				.map_err(|source| tg::error!(!source, "failed to set symlink target"))?;
			builder
				.append_data(&mut header, path, &[][..])
				.await
				.map_err(|source| tg::error!(!source, "failed to append symlink"))
		},
	}
}

async fn zip(server: &Server, artifact: &tg::Artifact) -> tg::Result<tg::Blob> {
	// Create a duplex stream.
	let (reader, writer) = tokio::io::duplex(8192);

	// Create the archive future.
	let archive_future = async move {
		// Create the tar builder.
		let mut builder = async_zip::base::write::ZipFileWriter::new(writer.compat_write());

		// Archive the artifact.
		let directory = artifact
			.try_unwrap_directory_ref()
			.ok()
			.ok_or_else(|| tg::error!("can only zip a directory"))?;
		for (name, artifact) in directory.entries(server).await? {
			zip_inner(server, &mut builder, Path::new(&name), &artifact).await?;
		}

		// Finish writing the archive.
		builder
			.close()
			.await
			.map(|_| ())
			.map_err(|source| tg::error!(!source, "failed to write the archive"))?;

		Ok::<_, tg::Error>(())
	};

	// Create the blob future.
	let blob_future = tg::Blob::with_reader(server, reader);

	// Join the futures.
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

async fn zip_inner<W>(
	server: &Server,
	builder: &mut async_zip::base::write::ZipFileWriter<W>,
	path: &Path,
	artifact: &tg::Artifact,
) -> tg::Result<()>
where
	W: futures::io::AsyncWrite + Unpin + Send + Sync,
{
	match artifact {
		tg::Artifact::Directory(directory) => {
			let filename = format!("{}/", path.to_string_lossy());
			let entry =
				async_zip::ZipEntryBuilder::new(filename.into(), async_zip::Compression::Deflate)
					.unix_permissions(0o755);
			builder
				.write_entry_whole(entry.build(), &[][..])
				.await
				.map_err(|source| tg::error!(!source, "could not write the directory entry"))?;
			for (name, artifact) in directory.entries(server).await? {
				Box::pin(zip_inner(
					server,
					builder,
					&path.join(name),
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
			let permissions = if executable { 0o0755 } else { 0o0644 };
			let entry = async_zip::ZipEntryBuilder::new(
				path.to_string_lossy().as_ref().into(),
				async_zip::Compression::Deflate,
			)
			.unix_permissions(permissions);
			let mut entry_writer = builder
				.write_entry_stream(entry)
				.await
				.unwrap()
				.compat_write();
			let mut file_reader = file.read(server, tg::blob::read::Arg::default()).await?;
			tokio::io::copy(&mut file_reader, &mut entry_writer)
				.await
				.map_err(|source| tg::error!(!source, "could not write the file entry"))?;
			entry_writer.into_inner().close().await.unwrap();
			Ok(())
		},
		tg::Artifact::Symlink(symlink) => {
			let target = symlink
				.target(server)
				.await?
				.ok_or_else(|| tg::error!("cannot archive a symlink without a target"))?;
			let entry = async_zip::ZipEntryBuilder::new(
				path.to_string_lossy().as_ref().into(),
				async_zip::Compression::Deflate,
			)
			.unix_permissions(0o120_777);
			builder
				.write_entry_whole(entry.build(), target.to_string_lossy().as_bytes())
				.await
				.map_err(|source| tg::error!(!source, "could not write the symlink entry"))
		},
	}
}
