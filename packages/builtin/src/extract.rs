use {
	async_zip::base::read::stream::ZipFileReader,
	futures::{AsyncReadExt as _, StreamExt as _},
	std::{
		os::unix::fs::PermissionsExt as _,
		path::{Component, Path, PathBuf},
		pin::pin,
	},
	tangram_client::prelude::*,
	tangram_futures::{
		read::{Ext as _, shared_position_reader::SharedPositionReader},
		stream::{Ext as _, TryExt as _},
		task::Task,
	},
	tokio::io::AsyncBufReadExt as _,
	tokio_util::compat::FuturesAsyncReadCompatExt as _,
};

pub(crate) async fn extract<H>(
	handle: &H,
	args: tg::value::data::Array,
	_cwd: std::path::PathBuf,
	_env: tg::value::data::Map,
	_executable: tg::command::data::Executable,
	logger: crate::Logger,
	temp_path: Option<&Path>,
) -> tg::Result<crate::Output>
where
	H: tg::Handle,
{
	// Get the blob.
	let input = args
		.first()
		.ok_or_else(|| tg::error!("invalid number of arguments"))?;
	let blob = match input {
		tg::value::Data::Object(id) => {
			let object = tg::Object::with_id(id.clone());
			match object {
				tg::Object::Blob(blob) => blob,
				tg::Object::File(file) => file.contents_with_handle(handle).await?,
				_ => return Err(tg::error!("expected a blob or a file")),
			}
		},
		_ => {
			return Err(tg::error!("expected a blob or a file"));
		},
	};

	// Create the reader.
	let reader = blob
		.read_with_handle(handle, tg::read::Options::default())
		.await?;
	let mut reader = SharedPositionReader::with_reader_and_position(reader, 0)
		.await
		.map_err(|source| tg::error!(!source, "failed to create the shared position reader"))?;

	// Detect the archive and compression formats.
	let buffer = reader
		.fill_buf()
		.await
		.map_err(|source| tg::error!(!source, "failed to fill the buffer"))?;
	let (format, compression) = super::util::detect_archive_format(buffer)?
		.ok_or_else(|| tg::error!("invalid archive format"))?;

	// Spawn a task to log progress.
	let position = reader.shared_position();
	let size = blob.length_with_handle(handle).await?;
	let (sender, receiver) = async_channel::bounded::<tg::Result<tg::progress::Event<()>>>(1024);
	let progress_task = Task::spawn({
		let position = position.clone();
		|_| async move {
			loop {
				let current = position.load(std::sync::atomic::Ordering::Relaxed);
				let indicator = tg::progress::Indicator {
					current: Some(current),
					format: tg::progress::IndicatorFormat::Bytes,
					name: String::new(),
					title: "extracting".to_owned(),
					total: Some(size),
				};
				let event = tg::progress::Event::Indicators::<()>(vec![indicator]);
				let result = sender.send(Ok(event)).await;
				if result.is_err() {
					break;
				}
				tokio::time::sleep(std::time::Duration::from_secs(1)).await;
			}
		}
	});
	let stream = receiver.attach(progress_task);
	let log_task = Task::spawn({
		let logger = logger.clone();
		|_| async move { crate::log_progress_stream(&logger, stream).await.ok() }
	});

	// Create a temp.
	let temp = temp_path
		.map_or_else(tangram_util::fs::Temp::new, tangram_util::fs::Temp::new_in)
		.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;
	tokio::fs::create_dir(temp.path())
		.await
		.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;

	// Extract to the temp.
	match format {
		tg::ArchiveFormat::Tar => {
			extract_tar(temp.path(), &mut reader, compression).await?;
		},
		tg::ArchiveFormat::Zip => {
			extract_zip(temp.path(), &mut reader).await?;
		},
	}

	// Check in the temp.
	let stream = handle
		.checkin(tg::checkin::Arg {
			options: tg::checkin::Options {
				destructive: true,
				ignore: false,
				lock: None,
				root: true,
				..Default::default()
			},
			path: temp.path().to_owned(),
			updates: Vec::new(),
		})
		.await?;
	let output = pin!(stream)
		.try_last()
		.await?
		.and_then(|event| event.try_unwrap_output().ok())
		.ok_or_else(|| tg::error!("stream ended without output"))?;
	let artifact = tg::Artifact::with_id(output.artifact.item);

	// Abort the log task.
	log_task.abort();

	// Log that the extraction finished.
	let message = "finished extracting\n";
	logger(
		tg::process::stdio::Stream::Stderr,
		message.to_owned().into_bytes(),
	)
	.await?;

	let output = artifact.into();

	let output = crate::Output {
		checksum: None,
		error: None,
		exit: 0,
		output: Some(output),
	};

	Ok(output)
}

pub(crate) async fn extract_tar(
	temp: &Path,
	reader: &mut (impl tokio::io::AsyncBufRead + Send + Unpin + 'static),
	compression: Option<tg::CompressionFormat>,
) -> tg::Result<()> {
	let reader = match compression {
		Some(tg::CompressionFormat::Bz2) => {
			async_compression::tokio::bufread::BzDecoder::new(reader).boxed()
		},
		Some(tg::CompressionFormat::Gz) => {
			async_compression::tokio::bufread::GzipDecoder::new(reader).boxed()
		},
		Some(tg::CompressionFormat::Xz) => {
			async_compression::tokio::bufread::XzDecoder::new(reader).boxed()
		},
		Some(tg::CompressionFormat::Zstd) => {
			async_compression::tokio::bufread::ZstdDecoder::new(reader).boxed()
		},
		None => reader.boxed(),
	};
	let mut archive = tokio_tar::ArchiveBuilder::new(reader).build();
	let mut entries = archive
		.entries()
		.map_err(|source| tg::error!(!source, "failed to read the archive"))?;
	while let Some(entry) = entries.next().await {
		let mut entry =
			entry.map_err(|source| tg::error!(!source, "failed to read the archive entry"))?;
		if entry.header().entry_type().is_pax_global_extensions() {
			continue;
		}
		let path = entry
			.path()
			.map_err(|source| tg::error!(!source, "failed to read the archive entry path"))?;
		let path = resolve_tar_entry_path(temp, &path)?;
		let kind = entry.header().entry_type();

		if kind.is_dir() {
			tokio::fs::create_dir_all(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			continue;
		}

		let parent = path.parent().expect("expected the entry to have a parent");
		tokio::fs::create_dir_all(parent)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		if kind.is_symlink() {
			let target = entry
				.link_name()
				.map_err(|source| tg::error!(!source, "failed to read symlink target"))?
				.ok_or_else(|| tg::error!("expected the entry to have a symlink target"))?;
			tokio::fs::symlink(&target, &path)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			continue;
		}

		if kind.is_hard_link() {
			let target = entry
				.link_name()
				.map_err(|source| tg::error!(!source, "failed to read hard link target"))?
				.ok_or_else(|| tg::error!("expected the entry to have a hard link target"))?;
			let target = resolve_tar_entry_path(temp, &target)?;
			tokio::fs::hard_link(&target, &path)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the hard link"))?;
			continue;
		}

		let is_executable = entry
			.header()
			.mode()
			.ok()
			.is_some_and(|permissions| permissions & 0o000_111 != 0);
		let mut file = tokio::fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the file"))?;
		tokio::io::copy(&mut entry, &mut file)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the file"))?;
		if is_executable {
			let permissions = std::fs::Permissions::from_mode(0o755);
			tokio::fs::set_permissions(&path, permissions)
				.await
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}
	}

	Ok(())
}

fn resolve_tar_entry_path(root: &Path, path: &Path) -> tg::Result<PathBuf> {
	let mut output = root.to_owned();
	for component in path.components() {
		match component {
			Component::CurDir => (),
			Component::Normal(component) => output.push(component),
			_ => {
				return Err(tg::error!(
					path = %path.display(),
					"invalid archive entry path"
				));
			},
		}
	}
	Ok(output)
}

pub(crate) async fn extract_zip(
	temp: &Path,
	reader: &mut (impl tokio::io::AsyncBufRead + Send + Unpin + 'static),
) -> tg::Result<()> {
	// Create the reader.
	let mut reader = Some(ZipFileReader::with_tokio(reader));

	// Extract.
	loop {
		let Some(mut entry) = reader
			.take()
			.unwrap()
			.next_with_entry()
			.await
			.map_err(|source| tg::error!(!source, "failed to read first entry"))?
		else {
			break;
		};

		// Get the reader.
		let entry_reader = entry.reader_mut();

		// Get the path.
		let filename = entry_reader
			.entry()
			.filename()
			.as_str()
			.map_err(|source| tg::error!(!source, "failed to get the entry filename"))?;
		let path = temp.join(filename);

		// Check if the entry is a directory.
		let is_dir = entry_reader
			.entry()
			.dir()
			.map_err(|source| tg::error!(!source, "failed to get type of entry"))?;

		// Check if the entry is a symlink.
		let is_symlink = entry_reader
			.entry()
			.unix_permissions()
			.is_some_and(|permissions| permissions & 0o120_000 == 0o120_000);

		// Check if the entry is executable.
		let is_executable = entry_reader
			.entry()
			.unix_permissions()
			.is_some_and(|permissions| permissions & 0o000_111 != 0);

		if is_dir {
			tokio::fs::create_dir_all(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
		} else if is_symlink {
			let mut buffer = Vec::new();
			entry_reader
				.read_to_end(&mut buffer)
				.await
				.map_err(|source| tg::error!(!source, "failed to read symlink target"))?;
			let target = std::str::from_utf8(&buffer)
				.map_err(|source| tg::error!(!source, "symlink target not valid UTF-8"))?;
			tokio::fs::symlink(target, &path)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
		} else {
			let parent = path.parent().expect("expected the entry to have a parent");
			tokio::fs::create_dir_all(parent)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			let mut file = tokio::fs::OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the file"))?;
			tokio::io::copy(&mut entry_reader.compat(), &mut file)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the file"))?;
			if is_executable {
				let permissions = std::fs::Permissions::from_mode(0o755);
				tokio::fs::set_permissions(&path, permissions)
					.await
					.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
			}
		}

		// Advance the reader.
		let value = entry
			.done()
			.await
			.map_err(|source| tg::error!(!source, "failed to advance the reader"))?;
		reader.replace(value);
	}

	Ok(())
}
