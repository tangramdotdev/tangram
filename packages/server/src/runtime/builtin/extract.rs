use super::Runtime;
use crate::{Server, temp::Temp};
use async_zip::base::read::stream::ZipFileReader;
use futures::AsyncReadExt as _;
use std::{
	os::unix::fs::PermissionsExt as _,
	pin::{Pin, pin},
	time::Duration,
};
use tangram_client as tg;
use tangram_futures::read::shared_position_reader::SharedPositionReader;
use tangram_futures::stream::TryExt as _;
use tokio::io::{AsyncRead, AsyncReadExt as _};
use tokio_util::compat::FuturesAsyncReadCompatExt as _;

impl Runtime {
	pub async fn extract(&self, process: &tg::Process) -> tg::Result<tg::Value> {
		let server = &self.server;
		let command = process.command(server).await?;

		// Get the args.
		let args = command.args(server).await?;

		// Get the blob.
		let blob = match args
			.first()
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
		{
			tg::Value::Object(tg::Object::Branch(branch)) => tg::Blob::Branch(branch.clone()),
			tg::Value::Object(tg::Object::Leaf(leaf)) => tg::Blob::Leaf(leaf.clone()),
			tg::Value::Object(tg::Object::File(file)) => file.contents(server).await?,
			_ => return Err(tg::error!("expected a file or blob")),
		};

		// Detect archive format.
		let (format, compression) = detect_archive_format(server, &blob).await?;

		// Create the reader.
		let reader = crate::blob::Reader::new(&self.server, blob.clone()).await?;
		let reader = SharedPositionReader::with_reader_and_position(reader, 0)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the shared position reader"))?;

		// Create the log task.
		let position = reader.shared_position();
		let size = blob.length(server).await?;
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
						stream: tg::process::log::Stream::Stderr,
					};
					let result = server.try_post_process_log(process.id(), arg).await;
					if let Err(error) = result {
						tracing::error!(?error, "failed to post process log");
					}
					tokio::time::sleep(Duration::from_secs(1)).await;
				}
			}
		});
		let log_task_abort_handle = log_task.abort_handle();
		scopeguard::defer! {
			log_task_abort_handle.abort();
		};

		// Create a temp.
		let temp = Temp::new(&self.server);

		// Extract to the temp.
		match format {
			tg::artifact::archive::Format::Tar => {
				self.extract_tar(&temp, reader, compression).await?;
			},
			tg::artifact::archive::Format::Zip => {
				self.extract_zip(&temp, reader).await?;
			},
		}

		// Check in the temp.
		let stream = self
			.server
			.check_in_artifact(tg::artifact::checkin::Arg {
				cache: false,
				deterministic: false,
				ignore: false,
				locked: false,
				lockfile: false,
				destructive: true,
				path: temp.path().to_owned(),
			})
			.await?;
		let output = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;
		let artifact = tg::Artifact::with_id(output.artifact);

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
			stream: tg::process::log::Stream::Stderr,
		};
		server.try_post_process_log(process.id(), arg).await.ok();

		Ok(artifact.into())
	}

	pub(super) async fn extract_tar(
		&self,
		temp: &Temp,
		reader: impl tokio::io::AsyncBufRead + Send + 'static,
		compression: Option<tangram_client::blob::compress::Format>,
	) -> tg::Result<()> {
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
		tokio_tar::ArchiveBuilder::new(reader)
			.set_preserve_permissions(true)
			.build()
			.unpack(&temp)
			.await
			.map_err(|source| tg::error!(!source, "failed to extract the archive"))?;
		Ok(())
	}

	pub(super) async fn extract_zip(
		&self,
		temp: &Temp,
		reader: impl tokio::io::AsyncBufRead + Send + Unpin + 'static,
	) -> tg::Result<()> {
		// Create the reader.
		let mut ready = Some(ZipFileReader::with_tokio(reader));

		// Extract.
		loop {
			let Some(mut reading) = ready
				.take()
				.unwrap()
				.next_with_entry()
				.await
				.map_err(|source| tg::error!(!source, "failed to read first entry"))?
			else {
				break;
			};

			// Get the reader
			let reader = reading.reader_mut();

			// Get the path.
			let filename = reader
				.entry()
				.filename()
				.as_str()
				.map_err(|source| tg::error!(!source, "failed to get the entry filename"))?;
			let path = temp.path().join(filename);

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

			if is_dir {
				tokio::fs::create_dir_all(&path)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			} else if is_symlink {
				let mut buffer = Vec::new();
				reader
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
				tokio::io::copy(&mut reader.compat(), &mut file)
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
			ready.replace(
				reading
					.done()
					.await
					.map_err(|source| tg::error!(!source, "failed to advance the reader"))?,
			);
		}
		Ok(())
	}
}

async fn detect_archive_format(
	server: &Server,
	blob: &tg::Blob,
) -> tg::Result<(
	tg::artifact::archive::Format,
	Option<tg::blob::compress::Format>,
)> {
	// Read the first 6 bytes.
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

	// Detect zip.
	if magic_bytes[0] == 0x50 && magic_bytes[1] == 0x4B {
		return Ok((tg::artifact::archive::Format::Zip, None));
	}

	// If we can detect a compression format from the magic bytes, assume tar.
	if let Some(compression) = tg::blob::compress::Format::with_magic_number(&magic_bytes) {
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
