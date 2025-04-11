use super::Runtime;
use crate::temp::Temp;
use futures::TryStreamExt as _;
use num::ToPrimitive as _;
use std::{
	sync::{Arc, atomic::AtomicU64},
	time::Duration,
};
use tangram_client as tg;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio_util::io::StreamReader;
use url::Url;

impl Runtime {
	pub async fn download(&self, process: &tg::Process) -> tg::Result<tg::Value> {
		let server = &self.server;
		let command = process.command(server).await?;

		// Ensure the process has a checksum.
		if process.load(server).await?.checksum.is_none() {
			return Err(tg::error!("a download must have a checksum"));
		}

		// Get the args.
		let args = command.args(server).await?;

		// Get the URL.
		let url = args
			.first()
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse::<Url>()
			.map_err(|source| tg::error!(!source, "invalid url"))?;

		// Get the unpack arg.
		let unpack = *args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_bool_ref()
			.map_err(|_| tg::error!("expected a boolean"))?;

		// Send the request.
		let response = reqwest::get(url.clone())
			.await
			.map_err(|source| tg::error!(!source, %url, "failed to perform the request"))?
			.error_for_status()
			.map_err(|source| tg::error!(!source, %url, "expected a success status"))?;

		// Log that the download started.
		let message = format!("downloading from \"{url}\"\n");
		let arg = tg::process::log::post::Arg {
			bytes: message.into(),
			remote: process.remote().cloned(),
			stream: tg::process::log::Stream::Stderr,
		};
		server.try_post_process_log(process.id(), arg).await.ok();

		// Create the log task.
		let downloaded = Arc::new(AtomicU64::new(0));
		let content_length = response.content_length();
		let log_task = tokio::spawn({
			let server = server.clone();
			let process = process.clone();
			let downloaded = downloaded.clone();
			async move {
				loop {
					let downloaded = downloaded.load(std::sync::atomic::Ordering::Relaxed);
					let indicator = tg::progress::Indicator {
						current: Some(downloaded),
						format: tg::progress::IndicatorFormat::Bytes,
						name: String::new(),
						title: "downloading".to_owned(),
						total: content_length,
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

		// Create the reader.
		let reader = StreamReader::new(
			response
				.bytes_stream()
				.map_err(std::io::Error::other)
				.inspect_ok({
					let n = downloaded.clone();
					move |bytes| {
						n.fetch_add(
							bytes.len().to_u64().unwrap(),
							std::sync::atomic::Ordering::Relaxed,
						);
					}
				}),
		);

		// Make sure the reader is buffered.
		let mut reader = BufReader::with_capacity(512, reader);

		let temp = Temp::new(server);
		if unpack {
			// Pre-fill the buffer
			let header = reader
				.fill_buf()
				.await
				.map_err(|source| tg::error!(!source, "failed to fill he buffer"))?;

			// Detect the archive format.
			let (format, compression) = detect_archive_format(header)?;

			// Exract to the temp.
			match format {
				tg::artifact::archive::Format::Tar => {
					self.extract_tar(&temp, reader, compression).await?
				},
				tg::artifact::archive::Format::Zip => {
					self.extract_zip(&temp, reader).await?;
				},
			};
		} else {
			// Download the temp file.
			let mut file = tokio::fs::File::create(temp.path())
				.await
				.map_err(|source| tg::error!(!source, "failed create the temp file"))?;
			tokio::io::copy(&mut reader, &mut file)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to the temp file"))?;
		}

		// Check in the temp file.
		let arg = tg::artifact::checkin::Arg {
			cache: false,
			destructive: true,
			deterministic: true,
			ignore: false,
			locked: false,
			lockfile: false,
			path: temp.path().to_owned(),
		};

		let artifact = tg::Artifact::check_in(server, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the downloaded file"))?;

		let object: tg::Object = if unpack {
			artifact.into()
		} else {
			artifact
				.try_unwrap_file()
				.map_err(|_| tg::error!("expected a file"))?
				.contents(server)
				.await?
				.into()
		};

		// Abort and await the log task.
		log_task.abort();
		match log_task.await {
			Ok(()) => Ok(()),
			Err(error) if error.is_cancelled() => Ok(()),
			Err(error) => Err(error),
		}
		.unwrap();

		// Log that the download finished.
		let message = format!("finished download from \"{url}\"\n");
		let arg = tg::process::log::post::Arg {
			bytes: message.into(),
			remote: process.remote().cloned(),
			stream: tg::process::log::Stream::Stderr,
		};
		server.try_post_process_log(process.id(), arg).await.ok();

		Ok(object.into())
	}
}

fn detect_archive_format(
	buffer: &[u8],
) -> tg::Result<(
	tg::artifact::archive::Format,
	Option<tg::blob::compress::Format>,
)> {
	if buffer.len() < 2 {
		return Err(tg::error!("download too small to be an archive"));
	}

	// Detect zip.
	if buffer.split_at(2).0 == &[0x50, 0x4b] {
		return Ok((tg::artifact::archive::Format::Zip, None));
	}

	// If we can detect a compression format from the magic bytes, assume tar.
	if let Some(compression) = tg::blob::compress::Format::with_magic_number(buffer) {
		return Ok((tg::artifact::archive::Format::Tar, Some(compression)));
	}

	// Otherwise, check for uncompressed ustar.
	let ustar = buffer.split_at(257).0.split_at(5).0;
	if ustar == b"ustar" {
		return Ok((tg::artifact::archive::Format::Tar, None));
	}

	// If not ustar, check for a valid tar checksum.
	if valid_tar_checksum(buffer)? {
		return Ok((tg::artifact::archive::Format::Tar, None));
	}

	Err(tg::error!("unrecognized archive format"))
}

fn valid_tar_checksum(header: &[u8]) -> tg::Result<bool> {
	if header.len() < 512 {
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
