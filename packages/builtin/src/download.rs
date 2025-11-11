use {
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::sync::{Arc, Mutex, atomic::AtomicU64},
	tangram_client::prelude::*,
	tangram_futures::read::Ext as _,
	tangram_uri::Uri,
	tokio::io::AsyncBufReadExt as _,
	tokio_util::io::StreamReader,
};

enum Mode {
	Raw,
	Decompress(tg::CompressionFormat),
	Extract(tg::ArchiveFormat, Option<tg::CompressionFormat>),
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn download<H>(
	handle: &H,
	args: tg::value::data::Array,
	_cwd: std::path::PathBuf,
	_env: tg::value::data::Map,
	_executable: tg::command::data::Executable,
	logger: crate::Logger,
	checksum: Option<tg::checksum::Algorithm>,
	temp_path: &std::path::Path,
) -> tg::Result<crate::Output>
where
	H: tg::Handle,
{
	let url: Uri = match args.first() {
		Some(tg::value::Data::String(s)) => s
			.parse()
			.map_err(|source| tg::error!(!source, "invalid url"))?,
		_ => return Err(tg::error!("expected a string")),
	};
	let options: Option<tg::DownloadOptions> = match args.get(1) {
		Some(tg::value::Data::Null) | None => None,
		Some(tg::value::Data::Map(map)) => {
			let mode = match map.get("mode") {
				Some(tg::value::Data::String(s)) => Some(
					s.parse::<tg::DownloadMode>()
						.map_err(|source| tg::error!(!source, "invalid download mode"))?,
				),
				Some(tg::value::Data::Null) | None => None,
				_ => return Err(tg::error!("invalid mode")),
			};
			Some(tg::DownloadOptions { mode })
		},
		_ => return Err(tg::error!("invalid options")),
	};

	// Send the request.
	let response = reqwest::get(url.as_str())
		.await
		.map_err(|source| tg::error!(!source, %url, "failed to perform the request"))?
		.error_for_status()
		.map_err(|source| tg::error!(!source, %url, "expected a success status"))?;

	// Log that the download started.
	let message = format!("downloading from \"{url}\"\n");
	logger(tg::process::log::Stream::Stderr, message).await?;

	// Spawn the progress task.
	let downloaded = Arc::new(AtomicU64::new(0));
	let _content_length = response.content_length();

	// Create the checksum writer.
	let checksum = checksum.map(|algorithm| {
		let writer = tg::checksum::Writer::new(algorithm);
		Arc::new(Mutex::new(writer))
	});

	// Create the reader.
	let stream = response
		.bytes_stream()
		.map_err(std::io::Error::other)
		.inspect_ok({
			let checksum = checksum.clone();
			let n = downloaded.clone();
			move |bytes| {
				// Update the checksum if necessary.
				if let Some(checksum) = &checksum {
					checksum.lock().unwrap().update(bytes);
				}

				// Update the progress.
				n.fetch_add(
					bytes.len().to_u64().unwrap(),
					std::sync::atomic::Ordering::Relaxed,
				);
			}
		});
	let mut reader = StreamReader::new(stream);

	// Fill the buffer.
	let header = reader
		.fill_buf()
		.await
		.map_err(|source| tg::error!(!source, "failed to fill the buffer"))?;

	// Determine the mode.
	let mode = options.and_then(|options| options.mode).unwrap_or_default();
	let mode = match mode {
		tg::DownloadMode::Raw => Mode::Raw,
		tg::DownloadMode::Decompress => {
			let format = super::util::detect_compression_format(header)?
				.ok_or_else(|| tg::error!("failed to determine the compression format"))?;
			Mode::Decompress(format)
		},
		tg::DownloadMode::Extract => {
			let (format, compression) = super::util::detect_archive_format(header)?
				.ok_or_else(|| tg::error!("failed to determine the archive format"))?;
			Mode::Extract(format, compression)
		},
	};

	// Download.
	let temp = tangram_temp::Temp::new_in(temp_path);
	match mode {
		Mode::Raw => {
			let mut file = tokio::fs::File::create(temp.path())
				.await
				.map_err(|source| tg::error!(!source, "failed create the temp file"))?;
			tokio::io::copy(&mut reader, &mut file)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to the temp file"))?;
		},
		Mode::Decompress(format) => {
			let mut reader = match format {
				tg::CompressionFormat::Bz2 => {
					async_compression::tokio::bufread::BzDecoder::new(&mut reader).boxed()
				},
				tg::CompressionFormat::Gz => {
					async_compression::tokio::bufread::GzipDecoder::new(&mut reader).boxed()
				},
				tg::CompressionFormat::Xz => {
					async_compression::tokio::bufread::XzDecoder::new(&mut reader).boxed()
				},
				tg::CompressionFormat::Zstd => {
					async_compression::tokio::bufread::ZstdDecoder::new(&mut reader).boxed()
				},
			};
			let mut file = tokio::fs::File::create(temp.path())
				.await
				.map_err(|source| tg::error!(!source, "failed create the temp file"))?;
			tokio::io::copy(&mut reader, &mut file)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to the temp file"))?;
		},
		Mode::Extract(format, compression) => match format {
			tg::ArchiveFormat::Tar => {
				super::extract::extract_tar(&temp, &mut reader, compression).await?;
			},
			tg::ArchiveFormat::Zip => {
				super::extract::extract_zip(&temp, &mut reader).await?;
			},
		},
	}

	// Drain and drop the reader.
	tokio::io::copy(&mut reader, &mut tokio::io::sink())
		.await
		.map_err(|source| tg::error!(!source, "failed to drain the reader"))?;
	drop(reader);

	// Finalize the checksum if necessary.
	let checksum = checksum.map(|checksum| {
		Arc::try_unwrap(checksum)
			.unwrap()
			.into_inner()
			.unwrap()
			.finalize()
	});

	// Check in the temp.
	let arg = tg::checkin::Arg {
		options: tg::checkin::Options {
			destructive: true,
			deterministic: true,
			ignore: false,
			lock: false,
			..Default::default()
		},
		path: temp.path().to_owned(),
		updates: Vec::new(),
	};
	let artifact = tg::checkin(handle, arg)
		.await
		.map_err(|source| tg::error!(!source, "failed to check in the downloaded file"))?;

	// Log that the download finished.
	let message = format!("finished download from \"{url}\"\n");
	logger(tg::process::log::Stream::Stderr, message).await?;

	let output = match mode {
		Mode::Raw => artifact
			.try_unwrap_file()
			.map_err(|_| tg::error!("expected a file"))?
			.contents(handle)
			.await?
			.into(),

		_ => artifact.into(),
	};

	let output = crate::Output {
		checksum,
		error: None,
		exit: 0,
		output: Some(output),
	};

	Ok(output)
}
