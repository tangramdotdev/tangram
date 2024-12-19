use super::Runtime;
use crate::Server;
use futures::{stream::FuturesOrdered, AsyncReadExt, StreamExt, TryStreamExt};
use std::{cmp::Ordering, collections::BTreeMap, path::PathBuf, time::Duration};
use tangram_archive as tgar;
use tangram_client as tg;
use tangram_futures::read::SharedPositionReader;
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

		// Create the extract task.
		let artifact = match format {
			tg::artifact::archive::Format::Tar => tar(server, reader).await?,
			tg::artifact::archive::Format::Tgar => tgar(server, reader).await?,
			tg::artifact::archive::Format::Zip => zip(server, reader).await?,
		};

		log_task.abort();

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
	// Create the tar reader.
	let tar = async_tar::Archive::new(reader.compat());

	let artifacts: BTreeMap<usize, (PathBuf, Option<tg::Artifact>)> = tar
		.entries()
		.map_err(|source| tg::error!(!source, "could not get entries from the archive"))?
		.enumerate()
		.map(|(index, entry)| {
			// Get the entry.
			let entry = entry
				.map_err(|source| tg::error!(!source, "could not get entry from the archive"))?;

			// Get the entry path.
			let path = PathBuf::from(
				entry
					.path()
					.map_err(|source| tg::error!(!source, "could not get the entry path"))?
					.as_ref(),
			);

			// Create non-dir artifacts.
			Ok::<_, tg::Error>(async move {
				let header = entry.header();
				match header.entry_type() {
					async_tar::EntryType::Regular => {
						let blob = tg::Blob::with_reader(server, entry.compat()).await?;
						let artifact = tg::Artifact::File(tg::File::with_contents(blob));
						Ok((index, (path, Some(artifact))))
					},
					async_tar::EntryType::Symlink => {
						let target = header
							.link_name()
							.map_err(|source| {
								tg::error!(!source, "could not read the symlink target")
							})?
							.ok_or_else(|| tg::error!("no symlink target stored in the archive"))?;
						let symlink = tg::Symlink::with_target(target.as_ref().into());
						let artifact = tg::Artifact::Symlink(symlink);
						Ok((index, (path, Some(artifact))))
					},
					async_tar::EntryType::Directory => Ok((index, (path, None))),
					_ => Err(tg::error!("unsupported entry type")),
				}
			})
		})
		.try_collect::<FuturesOrdered<_>>()
		.await?
		.try_collect()
		.await?;

	finish_extract(server, artifacts).await
}

async fn zip<R>(server: &Server, reader: R) -> tg::Result<tg::Artifact>
where
	R: tokio::io::AsyncBufRead + tokio::io::AsyncSeek + Unpin + Send + Sync + 'static,
{
	// Create the zip reader.
	let mut zip = async_zip::base::read::seek::ZipFileReader::with_tokio(reader)
		.await
		.map_err(|source| tg::error!(!source, "could not create zip file reader"))?;

	// Create non-dir artifacts.
	let mut artifacts: BTreeMap<usize, (PathBuf, Option<tg::Artifact>)> = BTreeMap::new();
	for index in 0..zip.file().entries().len() {
		// Get the entry.
		let entry = zip.file().entries().get(index).unwrap();

		// Get the entry filename.
		let path = entry
			.filename()
			.as_str()
			.map_err(|source| tg::error!(!source, "could not get entry filename"))?;

		// Create the path for entry.
		let path = PathBuf::from(path);

		// Check if the entry is a symlink.
		let is_symlink = match entry.unix_permissions() {
			Some(permissions) => matches!(permissions & 0o120_000, 0o120_000),
			None => false,
		};

		//Check if the entry is a directory.
		let is_dir = entry
			.dir()
			.map_err(|source| tg::error!(!source, "could not get type of entry"))?;

		// Create the artifacts.
		if is_dir {
			artifacts.insert(index, (path, None));
		} else if is_symlink {
			let mut reader = zip
				.reader_without_entry(index)
				.await
				.map_err(|source| tg::error!(!source, "unable to get the entry reader"))?;
			let mut buf = Vec::new();
			reader
				.read_to_end(&mut buf)
				.await
				.map_err(|source| tg::error!(!source, "could not read symlink target"))?;
			let target = core::str::from_utf8(&buf)
				.map_err(|source| tg::error!(!source, "symlink target not valid UTF-8"))?;
			let symlink = tg::Symlink::with_target(target.into());
			let artifact = tg::Artifact::Symlink(symlink);
			artifacts.insert(index, (path, Some(artifact)));
		} else {
			let reader = zip
				.reader_without_entry(index)
				.await
				.map_err(|source| tg::error!(!source, "unable to get the entry reader"))?;
			let output = server.create_blob(reader.compat()).await?;
			let blob = tg::Blob::with_id(output.blob);
			let artifact = tg::Artifact::File(tg::File::with_contents(blob));
			artifacts.insert(index, (path, Some(artifact)));
		}
	}

	finish_extract(server, artifacts).await
}

async fn tgar<R>(server: &Server, reader: R) -> tg::Result<tg::Artifact>
where
	R: tokio::io::AsyncBufRead + tokio::io::AsyncSeek + Unpin + Send + Sync,
{
	let mut tgar = tgar::read::Reader::new(reader);
	let version = tgar.read_header().await?;
	if version != 1 {
		return Err(tg::error!("unsupported tgar version"));
	}
	tgar_get_artifact(server, &mut tgar).await
}

async fn tgar_get_artifact<R>(
	server: &Server,
	tgar: &mut tgar::read::Reader<R>,
) -> tg::Result<tg::Artifact>
where
	R: tokio::io::AsyncBufRead + tokio::io::AsyncSeek + Unpin + Send + Sync,
{
	match tgar.read_artifact_type().await? {
		tg::artifact::Kind::Directory => {
			let mut entries = BTreeMap::new();
			for _ in 0..tgar.read_directory().await? {
				let entry_name = tgar.read_directory_entry_name().await?;
				let entry = Box::pin(tgar_get_artifact(server, tgar)).await?;
				entries.insert(entry_name, entry);
			}
			Ok(tg::Artifact::Directory(tg::Directory::with_entries(
				entries,
			)))
		},
		tg::artifact::Kind::File => {
			let (reader, writer) = tokio::io::duplex(8192);
			match futures::future::join(
				tg::Blob::with_reader(server, reader),
				tgar.read_file(writer),
			)
			.await
			{
				(Ok(blob), Ok(executable)) => {
					let file = tg::File::builder(blob).executable(executable).build();
					Ok(tg::Artifact::File(file))
				},
				(Ok(_), Err(source)) => Err(tg::error!(!source, "unable to read the file")),
				(Err(source), Ok(_)) => Err(tg::error!(!source, "unable to generate the blob")),
				(Err(blob_err), Err(file_err)) => Err(tg::error!(
				"unable to read the file: {file_err}, and unable to generate the blob: {blob_err}"
			)),
			}
		},
		tg::artifact::Kind::Symlink => Ok(tg::Artifact::Symlink(tg::Symlink::with_target(
			PathBuf::from(tgar.read_symlink().await?),
		))),
	}
}

async fn finish_extract(
	server: &Server,
	artifacts: BTreeMap<usize, (PathBuf, Option<tg::Artifact>)>,
) -> tg::Result<tg::Artifact> {
	// Get the root index and path.
	let mut root_index = None;
	let mut root_path = None;
	artifacts.iter().try_for_each(|(index, (path, _))| {
		// Get the parent path of the entry's filename.
		let parent_path = path
			.parent()
			.ok_or_else(|| tg::error!("could not get the path parent"))?
			.to_path_buf();

		// Get the parent index.
		if parent_path.to_string_lossy().is_empty() {
			if root_index.is_some() {
				return Err(tg::error!(
					"can only extract an archive with one root artifact"
				));
			}
			root_index = Some(*index);
			root_path = Some(path);
		}
		Ok(())
	})?;

	// Create the output artifact.
	match artifacts.len().cmp(&1) {
		Ordering::Less => Err(tg::error!("did not find any artifacts in the archive")),
		Ordering::Greater => {
			if let Some(root_index) = root_index {
				if artifacts.contains_key(&root_index) {
					let mut builder = tg::directory::Builder::default();
					for (index, (path, artifact)) in &artifacts {
						let path = if let Some(root_path) = root_path {
							path.strip_prefix(root_path).map_err(|source| {
								tg::error!(!source, "found archive path not under the root path")
							})?
						} else {
							path.as_path()
						};
						if *index != root_index {
							if let Some(artifact) = artifact {
								builder = builder.add(server, path, artifact.clone()).await?;
							} else {
								builder = builder
									.add(
										server,
										path,
										tg::Artifact::Directory(tg::Directory::with_entries(
											BTreeMap::new(),
										)),
									)
									.await?;
							}
						}
					}
					Ok(tg::Artifact::Directory(builder.build()))
				} else {
					Err(tg::error!("did not find the root artifact"))
				}
			} else {
				Err(tg::error!("could not calculate the root index"))
			}
		},
		Ordering::Equal => match artifacts.into_iter().last() {
			Some((_, (_, Some(artifact)))) => Ok(artifact),
			Some((_, (_, None))) => Ok(tg::Artifact::Directory(tg::Directory::with_entries(
				BTreeMap::new(),
			))),
			_ => Err(tg::error!("could not find the root artifact")),
		},
	}
}
