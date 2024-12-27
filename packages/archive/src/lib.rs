use self::{reader::Reader, writer::Writer};
use futures::future;
use num::ToPrimitive as _;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_client as tg;
use tokio::io::{AsyncRead, AsyncWrite};

pub mod reader;
pub mod writer;

pub async fn archive<H>(handle: &H, artifact: &tg::Artifact) -> tg::Result<tg::Blob>
where
	H: tg::Handle,
{
	// Create a duplex stream.
	let (reader, writer) = tokio::io::duplex(8192);

	// Create the archive future.
	let archive_future = async move {
		// Create the writer.
		let mut writer = Writer::new(writer);

		// Write the archive header.
		writer.write_header().await?;

		// Archive the artifact.
		archive_inner(handle, &mut writer, artifact).await?;

		// Finish the archive.
		writer.finish().await?;

		Ok::<_, tg::Error>(())
	};

	// Create the blob future.
	let blob_future = tg::Blob::with_reader(handle, reader);

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

async fn archive_inner<H, W>(
	handle: &H,
	writer: &mut Writer<W>,
	artifact: &tg::Artifact,
) -> tg::Result<()>
where
	H: tg::Handle,
	W: AsyncWrite + Unpin + Send + Sync,
{
	match artifact {
		tg::Artifact::Directory(directory) => {
			let entries = directory.entries(handle).await?;
			writer
				.write_directory(entries.len().to_u64().unwrap())
				.await?;
			for (entry, artifact) in entries {
				writer.write_directory_entry(entry.as_str()).await?;
				Box::pin(archive_inner(handle, writer, &artifact.clone())).await?;
			}
			Ok(())
		},
		tg::Artifact::File(file) => {
			if !file.dependencies(handle).await?.is_empty() {
				return Err(tg::error!("cannot archive a file with dependencies"));
			}
			let executable = file.executable(handle).await?;
			let size = file.size(handle).await?;
			let mut reader = file.read(handle, tg::blob::read::Arg::default()).await?;
			writer.write_file(executable, size, &mut reader).await
		},
		tg::Artifact::Symlink(symlink) => {
			let target = symlink
				.target(handle)
				.await?
				.ok_or_else(|| tg::error!("cannot archive a symlink without a target"))?;
			writer
				.write_symlink(target.to_string_lossy().as_ref())
				.await
		},
	}
}

pub async fn extract<H, R>(handle: &H, reader: R) -> tg::Result<tg::Artifact>
where
	H: tg::Handle,
	R: AsyncRead + Unpin,
{
	let mut reader = Reader::new(reader);
	let version = reader.read_header().await?;
	if version != 0 {
		return Err(tg::error!("unsupported tgar version"));
	}
	let artifact = extract_inner(handle, &mut reader).await?;
	Ok(artifact)
}

async fn extract_inner<H, R>(handle: &H, reader: &mut Reader<R>) -> tg::Result<tg::Artifact>
where
	H: tg::Handle,
	R: AsyncRead + Unpin,
{
	match reader.read_artifact_kind().await? {
		tg::artifact::Kind::Directory => {
			let mut entries = BTreeMap::new();
			let count = reader.read_directory().await?;
			for _ in 0..count {
				let name = reader.read_directory_entry_name().await?;
				let artifact = Box::pin(extract_inner(handle, reader)).await?;
				entries.insert(name, artifact);
			}
			let directory = tg::Directory::with_entries(entries);
			let artifact = tg::Artifact::Directory(directory);
			Ok(artifact)
		},
		tg::artifact::Kind::File => {
			let (reader_, writer) = tokio::io::duplex(8192);
			let blob_future = tg::Blob::with_reader(handle, reader_);
			let read_future = reader.read_file(writer);
			let file = match future::join(blob_future, read_future).await {
				(Ok(blob), Ok(executable)) => {
					tg::File::builder(blob).executable(executable).build()
				},
				(Err(source), _) | (_, Err(source)) => {
					return Err(tg::error!(!source, "failed to read the file"));
				},
			};
			let artifact = tg::Artifact::File(file);
			Ok(artifact)
		},
		tg::artifact::Kind::Symlink => {
			let target = reader.read_symlink().await?;
			let target = PathBuf::from(target);
			let symlink = tg::Symlink::with_target(target);
			let artifact = tg::Artifact::Symlink(symlink);
			Ok(artifact)
		},
	}
}
