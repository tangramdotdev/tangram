use {
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::{
		path::PathBuf,
		sync::{Arc, Mutex, atomic::AtomicU64},
	},
	tangram_client::prelude::*,
	tangram_futures::read::Ext as _,
	tangram_uri::Uri,
	tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _},
	tokio_util::io::StreamReader,
};

enum Mode {
	Decompress(tg::CompressionFormat),
	Extract(tg::ArchiveFormat, Option<tg::CompressionFormat>),
	Raw,
}

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub checksum: Option<tg::checksum::Algorithm>,

	#[arg(long, default_value_t)]
	pub mode: tg::DownloadMode,

	#[arg(long = "output", conflicts_with = "output_positional")]
	pub output_named: Option<PathBuf>,

	#[arg(index = 2, conflicts_with = "output_named")]
	pub output_positional: Option<PathBuf>,

	#[arg(index = 1)]
	pub url: Uri,
}

pub async fn run(args: Args) -> tg::Result<()> {
	let output = super::util::resolve_path(args.output_named, args.output_positional);
	let response = reqwest::get(args.url.as_str())
		.await
		.map_err(|error| tg::error!(!error, url = %args.url, "failed to perform the request"))?
		.error_for_status()
		.map_err(|error| tg::error!(!error, url = %args.url, "expected a success status"))?;
	super::progress::write_message(&format!("downloading from \"{}\"", args.url))?;
	let content_length = response.content_length();
	let downloaded = Arc::new(AtomicU64::new(0));
	let progress = super::progress::Progress::with_position(
		"downloading",
		content_length,
		downloaded.clone(),
	)?;
	let checksum = args.checksum.map(|algorithm| {
		let writer = tg::checksum::Writer::new(algorithm);
		Arc::new(Mutex::new(writer))
	});
	let stream = response
		.bytes_stream()
		.map_err(std::io::Error::other)
		.inspect_ok({
			let checksum = checksum.clone();
			let downloaded = downloaded.clone();
			move |bytes| {
				if let Some(checksum) = &checksum {
					checksum.lock().unwrap().update(bytes);
				}
				downloaded.fetch_add(
					bytes.len().to_u64().unwrap(),
					std::sync::atomic::Ordering::Relaxed,
				);
			}
		});
	let mut reader = StreamReader::new(stream);
	let header = reader
		.fill_buf()
		.await
		.map_err(|error| tg::error!(!error, "failed to read the response"))?;
	let mode = match args.mode {
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
		tg::DownloadMode::Raw => Mode::Raw,
	};
	match mode {
		Mode::Raw => {
			let mut writer = super::util::open_output(output.as_deref()).await?;
			tokio::io::copy(&mut reader, &mut writer)
				.await
				.map_err(|error| tg::error!(!error, "failed to write the output"))?;
			writer
				.shutdown()
				.await
				.map_err(|error| tg::error!(!error, "failed to finish the output"))?;
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
				tg::CompressionFormat::Zst => {
					async_compression::tokio::bufread::ZstdDecoder::new(&mut reader).boxed()
				},
			};
			let mut writer = super::util::open_output(output.as_deref()).await?;
			tokio::io::copy(&mut reader, &mut writer)
				.await
				.map_err(|error| tg::error!(!error, "failed to write the output"))?;
			writer
				.shutdown()
				.await
				.map_err(|error| tg::error!(!error, "failed to finish the output"))?;
		},
		Mode::Extract(format, compression) => {
			let output = output
				.as_deref()
				.ok_or_else(|| tg::error!("expected an output path"))?;
			tokio::fs::create_dir(output).await.map_err(
				|error| tg::error!(!error, path = %output.display(), "failed to create the output"),
			)?;
			match format {
				tg::ArchiveFormat::Tar => {
					super::extract::extract_tar(output, &mut reader, compression).await?;
				},
				tg::ArchiveFormat::Zip => {
					super::extract::extract_zip(output, &mut reader).await?;
				},
			}
		},
	}
	tokio::io::copy(&mut reader, &mut tokio::io::sink())
		.await
		.map_err(|error| tg::error!(!error, "failed to finish reading the response"))?;
	drop(reader);
	if let Some(checksum) = checksum {
		let checksum = Arc::try_unwrap(checksum)
			.unwrap()
			.into_inner()
			.unwrap()
			.finalize()
			.to_string();
		if let Some(output) = output {
			xattr::set(&output, "user.tangram.checksum", checksum.as_bytes()).map_err(
				|error| tg::error!(!error, path = %output.display(), "failed to write the checksum xattr"),
			)?;
		}
	}
	progress.finish(&format!("finished download from \"{}\"", args.url))?;

	Ok(())
}
