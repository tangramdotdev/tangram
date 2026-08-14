use {
	async_zip::{base::read::stream::ZipFileReader, error::ZipError},
	futures::StreamExt as _,
	std::{
		os::unix::fs::PermissionsExt as _,
		path::{Component, Path, PathBuf},
	},
	tangram_client::prelude::*,
	tangram_futures::read::{Ext as _, shared_position_reader::SharedPositionReader},
	tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _},
	tokio_util::compat::FuturesAsyncReadCompatExt as _,
};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long = "input", conflicts_with = "input_positional")]
	pub input_named: Option<PathBuf>,

	#[arg(index = 1, conflicts_with = "input_named")]
	pub input_positional: Option<PathBuf>,

	#[arg(long = "output", conflicts_with = "output_positional")]
	pub output_named: Option<PathBuf>,

	#[arg(index = 2, conflicts_with = "output_named")]
	pub output_positional: Option<PathBuf>,

	/// Allow an entry to replace one that was already extracted, as tar does.
	#[arg(long)]
	pub overwrite: bool,
}

pub async fn run(args: Args) -> tg::Result<()> {
	let input = super::util::resolve_path(args.input_named, args.input_positional);
	let output = super::util::resolve_path(args.output_named, args.output_positional)
		.ok_or_else(|| tg::error!("expected an output path"))?;
	let total = super::util::input_length(input.as_deref()).await;
	let input = super::util::open_input(input.as_deref()).await?;
	let input = SharedPositionReader::with_reader_and_position(input, 0)
		.await
		.map_err(|error| tg::error!(!error, "failed to track the input position"))?;
	let mut input = tokio::io::BufReader::new(input);
	let buffer = input
		.fill_buf()
		.await
		.map_err(|error| tg::error!(!error, "failed to read the input"))?;
	let (format, compression) = super::util::detect_archive_format(buffer)?
		.ok_or_else(|| tg::error!("invalid archive format"))?;
	let progress = super::progress::Progress::with_position(
		"extracting",
		total,
		input.get_ref().shared_position(),
	)?;
	tokio::fs::create_dir(&output).await.map_err(
		|error| tg::error!(!error, path = %output.display(), "failed to create the output"),
	)?;
	match format {
		tg::ArchiveFormat::Tar => {
			extract_tar(&output, &mut input, compression, args.overwrite).await?;
		},
		tg::ArchiveFormat::Zip => extract_zip(&output, &mut input, args.overwrite).await?,
	}
	progress.finish("finished extracting")?;

	Ok(())
}

pub(crate) async fn extract_tar(
	output: &Path,
	reader: &mut (impl tokio::io::AsyncBufRead + Send + Unpin + 'static),
	compression: Option<tg::CompressionFormat>,
	overwrite: bool,
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
		Some(tg::CompressionFormat::Zst) => {
			async_compression::tokio::bufread::ZstdDecoder::new(reader).boxed()
		},
		None => reader.boxed(),
	};
	let mut archive = tokio_tar::ArchiveBuilder::new(reader).build();
	let mut entries = archive
		.entries()
		.map_err(|error| tg::error!(!error, "failed to read the archive"))?;
	while let Some(entry) = entries.next().await {
		let mut entry =
			entry.map_err(|error| tg::error!(!error, "failed to read the archive entry"))?;
		if entry.header().entry_type().is_pax_global_extensions() {
			continue;
		}
		let path = entry
			.path()
			.map_err(|error| tg::error!(!error, "failed to read the archive entry path"))?;
		let path = resolve_entry_path(output, &path)?;
		let kind = entry.header().entry_type();
		if kind.is_dir() {
			tokio::fs::create_dir_all(&path)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the directory"))?;
			continue;
		}
		let parent = path.parent().expect("expected the entry to have a parent");
		tokio::fs::create_dir_all(parent)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the directory"))?;
		if overwrite {
			remove_entry(&path).await?;
		}
		if kind.is_symlink() {
			let target = entry
				.link_name()
				.map_err(|error| tg::error!(!error, "failed to read the symlink target"))?
				.ok_or_else(|| tg::error!("expected a symlink target"))?;
			tokio::fs::symlink(&target, &path)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the symlink"))?;
			continue;
		}
		if kind.is_hard_link() {
			let target = entry
				.link_name()
				.map_err(|error| tg::error!(!error, "failed to read the hard link target"))?
				.ok_or_else(|| tg::error!("expected a hard link target"))?;
			let target = resolve_entry_path(output, &target)?;
			tokio::fs::hard_link(&target, &path)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the hard link"))?;
			continue;
		}
		let mode = entry.header().mode().unwrap_or(0o644) & 0o777;
		let mut file = create_entry_file(&path).await?;
		tokio::io::copy(&mut entry, &mut file)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the file"))?;
		let permissions = std::fs::Permissions::from_mode(mode);
		tokio::fs::set_permissions(&path, permissions)
			.await
			.map_err(|error| tg::error!(!error, "failed to set the permissions"))?;
	}

	Ok(())
}

pub(crate) async fn extract_zip(
	output: &Path,
	reader: &mut (impl tokio::io::AsyncBufRead + Send + Unpin + 'static),
	overwrite: bool,
) -> tg::Result<()> {
	let mut entries = Vec::new();
	let mut central_directory = Vec::new();
	let mut zip_reader = Some(ZipFileReader::with_tokio(&mut *reader));
	loop {
		let Some(mut entry) = (match zip_reader.take().unwrap().next_with_entry().await {
			Ok(entry) => entry,
			Err(ZipError::UnexpectedHeaderError(0x0605_4b50, _)) if entries.is_empty() => {
				central_directory.extend_from_slice(&0x0605_4b50_u32.to_le_bytes());
				break;
			},
			Err(error) => return Err(tg::error!(!error, "failed to read the zip entry")),
		}) else {
			central_directory.extend_from_slice(&0x0201_4b50_u32.to_le_bytes());
			break;
		};
		let entry_reader = entry.reader_mut();
		let filename = entry_reader
			.entry()
			.filename()
			.as_str()
			.map_err(|error| tg::error!(!error, "failed to read the entry filename"))?;
		let path = resolve_entry_path(output, Path::new(filename))?;
		let is_directory = entry_reader
			.entry()
			.dir()
			.map_err(|error| tg::error!(!error, "failed to read the entry type"))?;
		if is_directory {
			tokio::fs::create_dir_all(&path)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the directory"))?;
		} else {
			let parent = path.parent().expect("expected the entry to have a parent");
			tokio::fs::create_dir_all(parent)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the directory"))?;
			if overwrite {
				remove_entry(&path).await?;
			}
			let mut file = create_entry_file(&path).await?;
			tokio::io::copy(&mut entry_reader.compat(), &mut file)
				.await
				.map_err(|error| tg::error!(!error, "failed to write the file"))?;
		}
		entries.push(path);
		let reader = entry
			.done()
			.await
			.map_err(|error| tg::error!(!error, "failed to advance the zip reader"))?;
		zip_reader.replace(reader);
	}
	reader
		.read_to_end(&mut central_directory)
		.await
		.map_err(|error| tg::error!(!error, "failed to read the zip central directory"))?;
	let mut executables = Vec::new();
	let mut symlinks = Vec::new();
	let mut position = 0;
	while position + 4 <= central_directory.len() {
		let signature = u32::from_le_bytes(
			central_directory[position..position + 4]
				.try_into()
				.expect("expected a four byte signature"),
		);
		if signature != 0x0201_4b50 {
			break;
		}
		if position + 46 > central_directory.len() {
			return Err(tg::error!("invalid zip central directory"));
		}
		let filename_length = u16::from_le_bytes(
			central_directory[position + 28..position + 30]
				.try_into()
				.expect("expected a two byte filename length"),
		) as usize;
		let extra_field_length = u16::from_le_bytes(
			central_directory[position + 30..position + 32]
				.try_into()
				.expect("expected a two byte extra field length"),
		) as usize;
		let comment_length = u16::from_le_bytes(
			central_directory[position + 32..position + 34]
				.try_into()
				.expect("expected a two byte comment length"),
		) as usize;
		let attributes = u32::from_le_bytes(
			central_directory[position + 38..position + 42]
				.try_into()
				.expect("expected four byte attributes"),
		);
		let permissions = (attributes >> 16) as u16;
		executables.push(permissions & 0o000_111 != 0);
		symlinks.push(permissions & 0o120_000 == 0o120_000);
		position = position
			.checked_add(46 + filename_length + extra_field_length + comment_length)
			.ok_or_else(|| tg::error!("invalid zip central directory"))?;
	}
	if symlinks.len() != entries.len() || executables.len() != entries.len() {
		return Err(tg::error!("invalid zip central directory"));
	}
	for ((path, is_symlink), is_executable) in entries.iter().zip(symlinks).zip(executables) {
		if is_symlink {
			let target = tokio::fs::read(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to read the symlink target"))?;
			let target = std::str::from_utf8(&target)
				.map_err(|error| tg::error!(!error, "the symlink target is not UTF-8"))?;
			tokio::fs::remove_file(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to remove the symlink placeholder"))?;
			tokio::fs::symlink(target, path)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the symlink"))?;
		} else if is_executable {
			let metadata = tokio::fs::symlink_metadata(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to read the entry metadata"))?;
			if metadata.is_file() {
				let permissions = std::fs::Permissions::from_mode(0o755);
				tokio::fs::set_permissions(path, permissions)
					.await
					.map_err(|error| tg::error!(!error, "failed to set the permissions"))?;
			}
		}
	}

	Ok(())
}

// Remove an already extracted entry so that a later entry with the same path can replace it, as tar does.
async fn remove_entry(path: &Path) -> tg::Result<()> {
	let metadata = match tokio::fs::symlink_metadata(path).await {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
		Err(error) => {
			return Err(tg::error!(
				!error,
				?path,
				"failed to read the entry metadata"
			));
		},
	};
	if metadata.is_dir() {
		return Err(tg::error!(?path, "an entry cannot replace a directory"));
	}
	tokio::fs::remove_file(path)
		.await
		.map_err(|error| tg::error!(!error, ?path, "failed to remove the entry"))
}

// Create the file for an archive entry. The file is created exclusively so that an archive cannot overwrite an entry it has already extracted.
async fn create_entry_file(path: &Path) -> tg::Result<tokio::fs::File> {
	let result = tokio::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(path)
		.await;
	let error = match result {
		Ok(file) => return Ok(file),
		Err(error) => error,
	};
	if error.kind() != std::io::ErrorKind::AlreadyExists {
		return Err(tg::error!(!error, ?path, "failed to create the file"));
	}

	// A case insensitive file system reports that an entry exists when a previously extracted entry has a name differing only in case. Report that cause, because it cannot be inferred from the path alone.
	let Some(existing) = find_case_conflict(path).await else {
		return Err(tg::error!(!error, ?path, "failed to create the file"));
	};
	let source = tg::error!(
		!error,
		%existing,
		"an entry whose name differs only in case already exists, and the file system is case insensitive"
	);

	Err(tg::error!(
		!source,
		?path,
		"failed to extract the archive entry"
	))
}

// Find an entry in the path's parent directory whose name differs from the path's name only in case.
async fn find_case_conflict(path: &Path) -> Option<String> {
	let parent = path.parent()?;
	let name = path.file_name()?.to_str()?;
	let mut entries = tokio::fs::read_dir(parent).await.ok()?;
	while let Ok(Some(entry)) = entries.next_entry().await {
		let existing = entry.file_name();
		let Some(existing) = existing.to_str() else {
			continue;
		};
		if existing != name && existing.eq_ignore_ascii_case(name) {
			return Some(existing.to_owned());
		}
	}
	None
}

fn resolve_entry_path(root: &Path, path: &Path) -> tg::Result<PathBuf> {
	let mut output = root.to_owned();
	for component in path.components() {
		match component {
			Component::CurDir => (),
			Component::Normal(component) => output.push(component),
			_ => return Err(tg::error!(path = %path.display(), "invalid archive entry path")),
		}
	}
	Ok(output)
}
