use super::Runtime;
use crate::Server;
use futures::{AsyncReadExt as _, StreamExt as _};
use std::{
	path::{Path, PathBuf},
	pin::Pin,
	time::Duration,
};
use tangram_client as tg;
use tangram_futures::read::shared_position_reader::SharedPositionReader;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncSeek};
use tokio_util::compat::{FuturesAsyncReadCompatExt as _, TokioAsyncReadCompatExt as _};

impl Runtime {
	pub async fn extract(&self, process: &tg::Process) -> tg::Result<tg::Value> {
		let server = &self.server;
		let command = process.command(server).await?;

		// Get the args.
		let args = command.args(server).await?;

		// Get the blob.
		let blob: tg::Blob = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("expected a blob"))?;

		// Detect archive format.
		let (format, compression) = detect_archive_format(server, &blob).await?;

		// Create the reader.
		let reader = crate::blob::Reader::new(&self.server, blob.clone()).await?;
		let reader = SharedPositionReader::with_reader_and_position(reader, 0)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the shared position reader"))?;

		// Create the log task.
		let position = reader.shared_position();
		let size = blob.size(server).await?;
		let log_task = tokio::spawn({
			let server = server.clone();
			let process = process.clone();
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
					let message = format!("{indicator}\n");
					let arg = tg::process::log::post::Arg {
						bytes: message.into(),
						remote: process.remote().cloned(),
					};
					if !server
						.try_post_process_log(process.id(), arg)
						.await
						.map_or(true, |ok| ok.added)
					{
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

		// Extract the artifact.
		let artifact = match format {
			tg::artifact::archive::Format::Tar => {
				// If there is a compression format, wrap the reader.
				let reader: Pin<Box<dyn AsyncRead + Send + 'static>> = match compression {
					Some(tg::blob::compress::Format::Bz2) => {
						Box::pin(async_compression::tokio::bufread::BzDecoder::new(reader))
					},
					Some(tg::blob::compress::Format::Gz) => {
						Box::pin(async_compression::tokio::bufread::GzipDecoder::new(reader))
					},
					Some(tg::blob::compress::Format::Xz) => {
						Box::pin(async_compression::tokio::bufread::XzDecoder::new(reader))
					},
					Some(tg::blob::compress::Format::Zstd) => {
						Box::pin(async_compression::tokio::bufread::ZstdDecoder::new(reader))
					},
					None => Box::pin(reader),
				};
				tar(server, reader).await?
			},
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
		let arg = tg::process::log::post::Arg {
			bytes: message.into(),
			remote: process.remote().cloned(),
		};
		server.try_post_process_log(process.id(), arg).await.ok();

		Ok(artifact.into())
	}
}

async fn tar<R>(server: &Server, reader: R) -> tg::Result<tg::Artifact>
where
	R: AsyncRead + Unpin + Send + 'static,
{
	// Create the reader.
	let mut reader = tokio_tar::Archive::new(reader);

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
			tokio_tar::EntryType::Directory => {
				let directory = tg::Directory::with_entries([].into());
				let artifact = tg::Artifact::Directory(directory);
				entries.push((path, artifact));
			},
			tokio_tar::EntryType::Symlink => {
				let target = header
					.link_name()
					.map_err(|source| tg::error!(!source, "failed to read the symlink target"))?
					.ok_or_else(|| tg::error!("no symlink target stored in the archive"))?;
				let symlink = tg::Symlink::with_target(target.as_ref().into());
				let artifact = tg::Artifact::Symlink(symlink);
				entries.push((path, artifact));
			},
			tokio_tar::EntryType::Link => {
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
			tokio_tar::EntryType::XGlobalHeader
			| tokio_tar::EntryType::XHeader
			| tokio_tar::EntryType::GNULongName
			| tokio_tar::EntryType::GNULongLink => (),
			_ => {
				let mode = header
					.mode()
					.map_err(|source| tg::error!(!source, "failed to read the entry mode"))?;
				let executable = mode & 0o111 != 0;
				let blob = tg::Blob::with_reader(server, entry).await?;
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
	R: AsyncBufRead + AsyncSeek + Unpin + Send + 'static,
{
	// Create the reader.
	let mut reader = async_zip::base::read::seek::ZipFileReader::new(reader.compat())
		.await
		.map_err(|source| tg::error!(!source, "failed to create the zip reader"))?;

	let mut entries: Vec<(PathBuf, tg::Artifact)> = Vec::new();
	for index in 0..reader.file().entries().len() {
		// Get the reader.
		let mut reader = reader
			.reader_with_entry(index)
			.await
			.map_err(|source| tg::error!(!source, "unable to get the entry"))?;

		// Get the path.
		let path = PathBuf::from(
			reader
				.entry()
				.filename()
				.as_str()
				.map_err(|source| tg::error!(!source, "failed to get the entry filename"))?,
		);

		// Check if the entry is a directory.
		let is_dir = reader
			.entry()
			.dir()
			.map_err(|source| tg::error!(!source, "failed to get type of entry"))?;

		// Check if the entry is a symlink.
		let is_symlink = reader
			.entry()
			.unix_permissions()
			.is_some_and(|permissions| permissions & 0o120_000 == 0o120_000);

		// Check if the entry is executable.
		let is_executable = reader
			.entry()
			.unix_permissions()
			.is_some_and(|permissions| permissions & 0o000_111 != 0);

		// Create the artifacts.
		if is_dir {
			let directory = tg::Directory::with_entries([].into());
			let artifact = tg::Artifact::Directory(directory);
			entries.push((path, artifact));
		} else if is_symlink {
			let mut buffer = Vec::new();
			reader
				.read_to_end(&mut buffer)
				.await
				.map_err(|source| tg::error!(!source, "failed to read symlink target"))?;
			let target = core::str::from_utf8(&buffer)
				.map_err(|source| tg::error!(!source, "symlink target not valid UTF-8"))?;
			let symlink = tg::Symlink::with_target(target.into());
			let artifact = tg::Artifact::Symlink(symlink);
			entries.push((path, artifact));
		} else {
			let output = server.create_blob_with_reader(reader.compat()).await?;
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

async fn detect_archive_format(
	server: &Server,
	blob: &tg::Blob,
) -> tg::Result<(
	tg::artifact::archive::Format,
	Option<tg::blob::compress::Format>,
)> {
	// Read first 6 bytes.
	let mut magic_reader = blob
		.read(
			server,
			tg::blob::read::Arg {
				length: Some(6),
				..Default::default()
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create magic bytes reader"))?;
	let mut magic_bytes = [0u8; 6];
	let bytes_read = magic_reader
		.read(&mut magic_bytes)
		.await
		.map_err(|source| tg::error!(!source, "failed to read magic bytes"))?;

	if bytes_read < 2 {
		return Err(tg::error!("blob too small to be an archive"));
	}

	// .zip
	if magic_bytes[0] == 0x50 && magic_bytes[1] == 0x4B {
		return Ok((tg::artifact::archive::Format::Zip, None));
	}

	// If we can detect a compression format from the magic bytes, assume tar.
	if let Some(compression) = tg::blob::compress::Format::from_magic(&magic_bytes) {
		return Ok((tg::artifact::archive::Format::Tar, Some(compression)));
	}

	// Otherwise, check for uncompressed ustar.
	let mut ustar_reader = blob
		.read(
			server,
			tg::blob::read::Arg {
				position: Some(std::io::SeekFrom::Start(257)),
				length: Some(5),
				..Default::default()
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create ustar reader"))?;
	let mut ustar_bytes = [0u8; 5];
	let ustar_bytes_read_result = ustar_reader.read(&mut ustar_bytes).await.ok();

	// If we successfully read the bytes "ustar" from position 257, it's a tar archive.
	if let Some(bytes_read) = ustar_bytes_read_result {
		if bytes_read == 5 && &ustar_bytes == b"ustar" {
			return Ok((tg::artifact::archive::Format::Tar, None));
		}
	}

	// If not ustar, check for a valid tar checksum.
	if valid_tar_checksum(server, blob).await? {
		return Ok((tg::artifact::archive::Format::Tar, None));
	}

	Err(tg::error!(?magic_bytes, "unrecognized archive format"))
}

async fn valid_tar_checksum(server: &Server, blob: &tg::Blob) -> tg::Result<bool> {
	let mut reader = blob
		.read(
			server,
			tg::blob::read::Arg {
				length: Some(512),
				..Default::default()
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create the tar header reader"))?;

	let mut header = [0u8; 512];
	let Some(bytes_read) = reader.read(&mut header).await.ok() else {
		// We couldn't read bytes, not a tar archive.
		return Ok(false);
	};
	// We expected to be able to read the full 512 bytes. If not, it's not a tar archive.
	if bytes_read != 512 {
		return Ok(false);
	}

	// Parse the checksum for the header record. This is an 8-byte field at offset 148.
	// See <https://en.wikipedia.org/wiki/Tar_(computing)#File_format>
	// "The checksum is calculated by taking the sum of the unsigned byte values of the header record with the eight checksum bytes taken to be ASCII spaces (decimal value 32). It is stored as a six digit octal number with leading zeroes followed by a NUL and then a space."
	let offset = 148;
	let field_size = 8;
	let recorded_checksum = parse_octal_checksum(&header[offset..offset + field_size])?;

	let mut checksum_calc = 0u32;
	for (i, item) in header.iter().enumerate() {
		// If we're in the checksum field, add ASCII space value.
		if i >= offset && i < offset + field_size {
			checksum_calc += 32;
		} else {
			checksum_calc += u32::from(*item);
		}
	}

	let result = checksum_calc == recorded_checksum;
	Ok(result)
}

fn parse_octal_checksum(bytes: &[u8]) -> tg::Result<u32> {
	// Checksums are stored as octal ASCII digits terminated by a NUL or space.
	let mut checksum_str = String::new();

	for &byte in bytes {
		if byte == 0 || byte == b' ' {
			break;
		}
		checksum_str.push(byte as char);
	}

	// Convert octal string to u32
	match u32::from_str_radix(checksum_str.trim(), 8) {
		Ok(value) => Ok(value),
		Err(_) => Err(tg::error!("Invalid tar checksum format")),
	}
}
