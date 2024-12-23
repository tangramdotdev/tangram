use super::Runtime;
use crate::Server;
use futures::{AsyncReadExt as _, StreamExt as _};
use std::{
	path::{Path, PathBuf},
	time::Duration,
};
use tangram_client as tg;
use tangram_futures::read::SharedPositionReader;
use tokio::io::{AsyncBufRead, AsyncSeek};
use tokio_util::compat::{FuturesAsyncReadCompatExt as _, TokioAsyncReadCompatExt as _};

impl Runtime {
	pub async fn extract(
		&self,
		build: &tg::Build,
		remote: Option<String>,
	) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the target.
		let target = build.target(server).await?;

		// Get the args.
		let args = target.args(server).await?;

		// Get the blob.
		let blob: tg::Blob = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("expected a blob"))?;

		// Get the format.
		let format = if let Some(value) = args.get(2) {
			let format = value
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("expected a string"))?
				.parse::<tg::artifact::archive::Format>()
				.map_err(|source| tg::error!(!source, "invalid format"))?;
			Some(format)
		} else {
			None
		};

		// Create the reader.
		let reader = blob.reader(server).await?;
		let reader = SharedPositionReader::new(reader)
			.await
			.map_err(|source| tg::error!(!source, "io error"))?;

		// Create the log task.
		let position = reader.shared_position();
		let size = blob.size(server).await?;
		let log_task = tokio::spawn({
			let server = server.clone();
			let build = build.clone();
			let remote = remote.clone();
			async move {
				loop {
					let position = position.load(std::sync::atomic::Ordering::Relaxed);
					let indicator = tg::progress::Indicator {
						current: Some(position),
						format: tg::progress::IndicatorFormat::Bytes,
						name: String::new(),
						title: "extracting".to_owned(),
						total: Some(size),
					};
					let message = indicator.to_string();
					let arg = tg::build::log::post::Arg {
						bytes: message.into(),
						remote: remote.clone(),
					};
					let result = build.add_log(&server, arg).await;
					if result.is_err() {
						break;
					}
					tokio::time::sleep(Duration::from_secs(1)).await;
				}
			}
		});
		let log_task_abort_handle = log_task.abort_handle();
		scopeguard::defer! {
			log_task_abort_handle.abort();
		};

		let format =
			format.ok_or_else(|| tg::error!("archive format detection is unimplemented"))?;

		// Extract the artifact.
		let artifact = match format {
			tg::artifact::archive::Format::Tar => tar(server, reader).await?,
			tg::artifact::archive::Format::Tgar => tangram_archive::extract(server, reader).await?,
			tg::artifact::archive::Format::Zip => zip(server, reader).await?,
		};

		// Abort and await the log task.
		log_task.abort();
		match log_task.await {
			Ok(()) => Ok(()),
			Err(error) if error.is_cancelled() => Ok(()),
			Err(error) => Err(error),
		}
		.unwrap();

		// Log that the extraction finished.
		let message = "finished extracting\n";
		let arg = tg::build::log::post::Arg {
			bytes: message.into(),
			remote: remote.clone(),
		};
		build.add_log(server, arg).await.ok();

		Ok(artifact.into())
	}
}

async fn tar<R>(server: &Server, reader: R) -> tg::Result<tg::Artifact>
where
	R: tokio::io::AsyncRead + Unpin + Send + Sync + 'static,
{
	// Create the reader.
	let reader = async_tar::Archive::new(reader.compat());

	// Get the entries.
	let mut entries: Vec<(PathBuf, tg::Artifact)> = Vec::new();
	let mut iter = reader
		.entries()
		.map_err(|source| tg::error!(!source, "failed to get the entries from the archive"))?;
	while let Some(entry) = iter.next().await {
		let entry = entry
			.map_err(|source| tg::error!(!source, "failed to get the entry from the archive"))?;
		let header = entry.header();
		let path = PathBuf::from(
			entry
				.path()
				.map_err(|source| tg::error!(!source, "failed to get the entry path"))?
				.as_ref(),
		);
		match header.entry_type() {
			async_tar::EntryType::Directory => {
				let directory = tg::Directory::with_entries([].into());
				let artifact = tg::Artifact::Directory(directory);
				entries.push((path, artifact));
			},
			async_tar::EntryType::Symlink => {
				let target = header
					.link_name()
					.map_err(|source| tg::error!(!source, "failed to read the symlink target"))?
					.ok_or_else(|| tg::error!("no symlink target stored in the archive"))?;
				let symlink = tg::Symlink::with_target(target.as_ref().into());
				let artifact = tg::Artifact::Symlink(symlink);
				entries.push((path, artifact));
			},
			async_tar::EntryType::Link => {
				let target = header
					.link_name()
					.map_err(|source| {
						tg::error!(!source, "failed to read the hard link target path")
					})?
					.ok_or_else(|| tg::error!("no hard link target path stored in the archive"))?;
				let target = Path::new(target.as_ref());
				if let Some((_, artifact)) = entries.iter().find(|(path, _)| path == target) {
					entries.push((path, artifact.clone()));
				} else {
					return Err(tg::error!(
						"could not find the hard link target in the archive"
					));
				}
			},
			async_tar::EntryType::XGlobalHeader
			| async_tar::EntryType::XHeader
			| async_tar::EntryType::GNULongName
			| async_tar::EntryType::GNULongLink => {
				continue;
			},
			_ => {
				let mode = header
					.mode()
					.map_err(|source| tg::error!(!source, "failed to read the entry mode"))?;
				let executable = mode & 0o111 != 0;
				let blob = tg::Blob::with_reader(server, entry.compat()).await?;
				let file = tg::File::builder(blob).executable(executable).build();
				let artifact = tg::Artifact::File(file);
				entries.push((path, artifact));
			},
		}
	}

	// Build the directory.
	let mut builder = tg::directory::Builder::default();
	for (path, artifact) in entries {
		if !path
			.components()
			.all(|component| matches!(component, std::path::Component::CurDir))
		{
			builder = builder.add(server, &path, artifact).await?;
		}
	}
	let directory = builder.build();

	Ok(directory.into())
}

async fn zip<R>(server: &Server, reader: R) -> tg::Result<tg::Artifact>
where
	R: AsyncBufRead + AsyncSeek + Unpin + Send + Sync + 'static,
{
	// Create the reader.
	let mut reader = async_zip::base::read::seek::ZipFileReader::new(reader.compat())
		.await
		.map_err(|source| tg::error!(!source, "failed to create the zip reader"))?;

	let mut entries: Vec<(PathBuf, tg::Artifact)> = Vec::new();
	for index in 0..reader.file().entries().len() {
		// Get the entry.
		let entry = reader.file().entries().get(index).unwrap();

		// Get the path.
		let path = PathBuf::from(
			entry
				.filename()
				.as_str()
				.map_err(|source| tg::error!(!source, "failed to get the entry filename"))?,
		);

		// Check if the entry is a directory.
		let is_dir = entry
			.dir()
			.map_err(|source| tg::error!(!source, "failed to get type of entry"))?;

		// Check if the entry is a symlink.
		let is_symlink = entry
			.unix_permissions()
			.is_some_and(|permissions| permissions & 0o120_000 == 0o120_000);

		// Check if the entry is executable.
		let is_executable = entry
			.unix_permissions()
			.is_some_and(|permissions| permissions & 0o000_111 != 0);

		// Create the artifacts.
		if is_dir {
			let directory = tg::Directory::with_entries([].into());
			let artifact = tg::Artifact::Directory(directory);
			entries.push((path, artifact));
		} else if is_symlink {
			let mut entry = reader
				.reader_without_entry(index)
				.await
				.map_err(|source| tg::error!(!source, "unable to get the entry reader"))?;
			let mut buffer = Vec::new();
			entry
				.read_to_end(&mut buffer)
				.await
				.map_err(|source| tg::error!(!source, "failed to read symlink target"))?;
			let target = core::str::from_utf8(&buffer)
				.map_err(|source| tg::error!(!source, "symlink target not valid UTF-8"))?;
			let symlink = tg::Symlink::with_target(target.into());
			let artifact = tg::Artifact::Symlink(symlink);
			entries.push((path, artifact));
		} else {
			let entry = reader
				.reader_without_entry(index)
				.await
				.map_err(|source| tg::error!(!source, "unable to get the entry reader"))?;
			let output = server.create_blob(entry.compat()).await?;
			let blob = tg::Blob::with_id(output.blob);
			let file = tg::File::builder(blob).executable(is_executable).build();
			let artifact = tg::Artifact::File(file);
			entries.push((path, artifact));
		}
	}

	// Build the directory.
	let mut builder = tg::directory::Builder::default();
	for (path, artifact) in entries {
		if !path
			.components()
			.all(|component| matches!(component, std::path::Component::CurDir))
		{
			builder = builder.add(server, &path, artifact).await?;
		}
	}
	let directory = builder.build();

	Ok(directory.into())
}
