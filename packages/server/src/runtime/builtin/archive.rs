use super::Runtime;
use crate::Server;
use futures::{future::Either, FutureExt};
use num::ToPrimitive as _;
use std::{
	future::Future,
	path::{Path, PathBuf},
	str::FromStr,
};
use tangram_client as tg;
use tangram_tgar as tgar;
use tokio::io::DuplexStream;
use tokio_util::compat::{
	FuturesAsyncWriteCompatExt, TokioAsyncReadCompatExt as _, TokioAsyncWriteCompatExt,
};

impl Runtime {
	pub async fn archive(
		&self,
		build: &tg::Build,
		_remote: Option<String>,
	) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the target.
		let target = build.target(server).await?;

		// Get the args.
		let args = target.args(server).await?;

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

		// Create the output file. Duplex due to tokio #6914.
		let (reader, writer) = tokio::io::duplex(8192);

		// Create the archive task.
		let archive_task: Either<_, Either<_, _>> = match format {
			tg::artifact::archive::Format::Tar => tar(server, &artifact, writer).left_future(),
			tg::artifact::archive::Format::Zip => {
				zip(server, &artifact, writer).left_future().right_future()
			},
			tg::artifact::archive::Format::Tgar => tgar(server, &artifact, writer)
				.right_future()
				.right_future(),
		};

		match futures::future::join(archive_task, tg::Blob::with_reader(server, reader)).await {
			(Ok(()), Ok(blob)) => Ok(blob.into()),
			(Ok(()), Err(source)) => Err(tg::error!(!source, "unable to generate the blob")),
			(Err(source), Ok(_)) => Err(tg::error!(!source, "unable to generate the archive")),
			(Err(archive_err), Err(blob_err)) => Err(tg::error!(
				"unable to generate the archive: {archive_err}, and unable to generate the blob: {blob_err}"
			)),
		}
	}
}

fn tar(
	server: &Server,
	artifact: &tg::Artifact,
	writer: DuplexStream,
) -> impl Future<Output = tg::Result<()>> {
	let artifact = artifact.clone();
	let server = server.clone();
	async move {
		// Create the tar builder.
		let mut tar = async_tar::Builder::new(writer.compat_write());

		// Create the path from the artifact id.
		let path = artifact.id(&server).await?.to_string();
		let path = PathBuf::from_str(path.as_str())
			.map_err(|source| tg::error!(!source, "failed to create the artifact path"))
			.unwrap();

		// Archive the artifact.
		tar_inner(&server, &artifact, &mut tar, &path).await?;

		// Finish writing the archive.
		tar.finish()
			.await
			.map_err(|source| tg::error!(!source, "failed to write the archive"))
	}
}

async fn tar_inner<W>(
	server: &Server,
	artifact: &tg::Artifact,
	tar: &mut async_tar::Builder<W>,
	path: &PathBuf,
) -> tg::Result<()>
where
	W: futures::AsyncWrite + Unpin + Send + Sync,
{
	match artifact {
		tg::Artifact::Directory(directory) => {
			let mut header = async_tar::Header::new_gnu();
			header.set_size(0);
			header.set_entry_type(async_tar::EntryType::Directory);
			header.set_mode(0o755);
			tar.append_data(&mut header, path, &[][..])
				.await
				.map_err(|source| tg::error!(!source, "failed to append directory"))?;
			for (entry, artifact) in directory.entries(server).await? {
				Box::pin(tar_inner(server, &artifact.clone(), tar, &path.join(entry))).await?;
			}
			Ok(())
		},
		tg::Artifact::File(file) => {
			if !file.dependencies(server).await?.is_empty() {
				return Err(tg::error!("cannot archive a file with dependencies"));
			}
			let reader = file.reader(server).await?;
			let size = reader.size();
			let executable = file.executable(server).await?;
			let reader = reader.compat();
			let mut header = async_tar::Header::new_gnu();
			header.set_size(size);
			header.set_entry_type(async_tar::EntryType::Regular);
			let permissions = if executable { 0o0755 } else { 0o0644 };
			header.set_mode(permissions);
			tar.append_data(&mut header, path, reader)
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
			tar.append_data(&mut header, path, &[][..])
				.await
				.map_err(|source| tg::error!(!source, "failed to append symlink"))
		},
	}
}

fn tgar(
	server: &Server,
	artifact: &tg::Artifact,
	writer: DuplexStream,
) -> impl Future<Output = tg::Result<()>> {
	let artifact = artifact.clone();
	let server = server.clone();
	async move {
		// Create the tgar builder.
		let mut tgar = tgar::Builder::new(writer).await?;

		// Write the archive header.
		tgar.append_archive_header().await?;

		// Archive the artifact.
		tgar_inner(&server, &artifact, &mut tgar).await?;

		tgar.finish().await
	}
}

async fn tgar_inner<W>(
	server: &Server,
	artifact: &tg::Artifact,
	tgar: &mut tgar::Builder<W>,
) -> tg::Result<()>
where
	W: tokio::io::AsyncWrite + Unpin + Send + Sync,
{
	match artifact {
		tg::Artifact::Directory(directory) => {
			let entries = directory.entries(server).await?;
			tgar.append_directory(entries.len().to_u64().unwrap())
				.await?;
			for (entry, artifact) in entries {
				tgar.append_directory_entry(entry.as_str()).await?;
				Box::pin(tgar_inner(server, &artifact.clone(), tgar)).await?;
			}
			Ok(())
		},
		tg::Artifact::File(file) => {
			if !file.dependencies(server).await?.is_empty() {
				return Err(tg::error!("cannot archive a file with dependencies"));
			}
			let executable = file.executable(server).await?;
			let mut reader = file.reader(server).await?;
			let length = reader.size();
			tgar.append_file(executable, length, &mut reader).await
		},
		tg::Artifact::Symlink(symlink) => {
			let target = symlink
				.target(server)
				.await?
				.ok_or_else(|| tg::error!("cannot archive a symlink without a target"))?;
			tgar.append_symlink(target.to_string_lossy().as_ref()).await
		},
	}
}

fn zip(
	server: &Server,
	artifact: &tg::Artifact,
	writer: DuplexStream,
) -> impl Future<Output = tg::Result<()>> {
	let artifact = artifact.clone();
	let server = server.clone();
	async move {
		// Create the zip writer.
		let mut zip = async_zip::base::write::ZipFileWriter::with_tokio(writer);

		// Create the path from the artifact id.
		let path = artifact.id(&server).await?.to_string();
		let path = PathBuf::from_str(path.as_str())
			.map_err(|source| tg::error!(!source, "failed to create the artifact path"))
			.unwrap();

		// Archive the artifact.
		zip_inner(&server, &artifact, &mut zip, &path).await?;

		// Finish writing the archive.
		zip.close()
			.await
			.map(|_| ())
			.map_err(|source| tg::error!(!source, "failed to write the archive"))
	}
}

async fn zip_inner<W>(
	server: &Server,
	artifact: &tg::Artifact,
	zip: &mut async_zip::tokio::write::ZipFileWriter<W>,
	path: &Path,
) -> tg::Result<()>
where
	W: tokio::io::AsyncWrite + Unpin + Send + Sync,
{
	match artifact {
		tg::Artifact::Directory(directory) => {
			let entry_filename = format!("{}/", path.to_string_lossy());
			let builder = async_zip::ZipEntryBuilder::new(
				entry_filename.into(),
				async_zip::Compression::Deflate,
			)
			.unix_permissions(0o755);
			zip.write_entry_whole(builder.build(), &[][..])
				.await
				.map_err(|source| tg::error!(!source, "could not write the directory entry",))?;

			for (entry, artifact) in directory.entries(server).await? {
				Box::pin(zip_inner(server, &artifact.clone(), zip, &path.join(entry))).await?;
			}
			Ok(())
		},
		tg::Artifact::File(file) => {
			if !file.dependencies(server).await?.is_empty() {
				return Err(tg::error!("cannot archive a file with dependencies"));
			}
			let executable = file.executable(server).await?;
			let permissions = if executable { 0o0755 } else { 0o0644 };
			let builder = async_zip::ZipEntryBuilder::new(
				path.to_string_lossy().as_ref().into(),
				async_zip::Compression::Deflate,
			)
			.unix_permissions(permissions);
			let mut entry_writer = zip
				.write_entry_stream(builder)
				.await
				.unwrap()
				.compat_write();
			let mut file_reader = file.reader(server).await?;
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
			let builder = async_zip::ZipEntryBuilder::new(
				path.to_string_lossy().as_ref().into(),
				async_zip::Compression::Deflate,
			)
			.unix_permissions(0o120_777);
			zip.write_entry_whole(builder.build(), target.to_string_lossy().as_bytes())
				.await
				.map_err(|source| tg::error!(!source, "could not write the symlink entry",))
		},
	}
}
