use {
	futures::future,
	std::{
		os::unix::fs::PermissionsExt as _,
		path::{Path, PathBuf},
		sync::{Arc, atomic::AtomicU64},
	},
	tangram_client::prelude::*,
	tangram_futures::read::Ext as _,
	tokio_util::compat::{FuturesAsyncWriteCompatExt as _, TokioAsyncWriteCompatExt as _},
};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub compression: Option<tg::CompressionFormat>,

	#[arg(long)]
	pub format: tg::ArchiveFormat,

	#[arg(long = "input", conflicts_with = "input_positional")]
	pub input_named: Option<PathBuf>,

	#[arg(index = 1, conflicts_with = "input_named")]
	pub input_positional: Option<PathBuf>,

	#[arg(long = "output", conflicts_with = "output_positional")]
	pub output_named: Option<PathBuf>,

	#[arg(index = 2, conflicts_with = "output_named")]
	pub output_positional: Option<PathBuf>,
}

pub async fn run(args: Args) -> tg::Result<()> {
	let input = super::util::resolve_path(args.input_named, args.input_positional)
		.ok_or_else(|| tg::error!("expected an input path"))?;
	let output = super::util::resolve_path(args.output_named, args.output_positional);
	let metadata = tokio::fs::symlink_metadata(&input).await.map_err(
		|error| tg::error!(!error, path = %input.display(), "failed to read the input metadata"),
	)?;
	if !metadata.is_dir() {
		return Err(tg::error!(path = %input.display(), "the input is not a directory"));
	}
	if args.compression.is_some() && matches!(args.format, tg::ArchiveFormat::Zip) {
		return Err(tg::error!("compression is not supported for zip archives"));
	}
	let progress = super::progress::Progress::new("archiving", None)?;
	let position = progress.position();
	match args.format {
		tg::ArchiveFormat::Tar => {
			tar(&input, output.as_deref(), args.compression, &position).await?;
		},
		tg::ArchiveFormat::Zip => zip(&input, output.as_deref(), &position).await?,
	}
	progress.finish("finished archiving")?;

	Ok(())
}

async fn tar(
	input: &Path,
	output: Option<&Path>,
	compression: Option<tg::CompressionFormat>,
	position: &Arc<AtomicU64>,
) -> tg::Result<()> {
	let (reader, writer) = tokio::io::duplex(8192);
	let archive_future = async {
		let mut builder = tokio_tar::Builder::new(writer);
		for (name, path) in read_directory(input).await? {
			tar_inner(&mut builder, &path, Path::new(&name), position).await?;
		}
		builder
			.finish()
			.await
			.map_err(|error| tg::error!(!error, "failed to finish the archive"))
	};
	let output_future = async {
		let mut reader = match compression {
			Some(tg::CompressionFormat::Bz2) => {
				async_compression::tokio::bufread::BzEncoder::new(tokio::io::BufReader::new(reader))
					.boxed()
			},
			Some(tg::CompressionFormat::Gz) => async_compression::tokio::bufread::GzipEncoder::new(
				tokio::io::BufReader::new(reader),
			)
			.boxed(),
			Some(tg::CompressionFormat::Xz) => {
				async_compression::tokio::bufread::XzEncoder::new(tokio::io::BufReader::new(reader))
					.boxed()
			},
			Some(tg::CompressionFormat::Zst) => {
				async_compression::tokio::bufread::ZstdEncoder::new(tokio::io::BufReader::new(
					reader,
				))
				.boxed()
			},
			None => reader.boxed(),
		};
		let mut output = super::util::open_output(output).await?;
		tokio::io::copy(&mut reader, &mut output)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the archive"))?;
		tokio::io::AsyncWriteExt::shutdown(&mut output)
			.await
			.map_err(|error| tg::error!(!error, "failed to finish the output"))?;
		Ok::<_, tg::Error>(())
	};
	let (archive_result, output_result) = future::join(archive_future, output_future).await;
	archive_result?;
	output_result?;

	Ok(())
}

async fn tar_inner<W>(
	builder: &mut tokio_tar::Builder<W>,
	source: &Path,
	path: &Path,
	position: &Arc<AtomicU64>,
) -> tg::Result<()>
where
	W: tokio::io::AsyncWrite + Send + Unpin,
{
	let metadata = tokio::fs::symlink_metadata(source).await.map_err(
		|error| tg::error!(!error, path = %source.display(), "failed to read the entry metadata"),
	)?;
	let file_type = metadata.file_type();
	if file_type.is_dir() {
		let mut header = tokio_tar::Header::new_gnu();
		header.set_entry_type(tokio_tar::EntryType::Directory);
		header.set_mode(metadata.permissions().mode() & 0o777);
		header.set_size(0);
		builder
			.append_data(&mut header, path, &[][..])
			.await
			.map_err(|error| tg::error!(!error, "failed to append the directory"))?;
		for (name, source) in read_directory(source).await? {
			Box::pin(tar_inner(builder, &source, &path.join(name), position)).await?;
		}
	} else if file_type.is_file() {
		let mut header = tokio_tar::Header::new_gnu();
		header.set_entry_type(tokio_tar::EntryType::Regular);
		header.set_mode(metadata.permissions().mode() & 0o777);
		header.set_size(metadata.len());
		let file = tokio::fs::File::open(source).await.map_err(
			|error| tg::error!(!error, path = %source.display(), "failed to open the file"),
		)?;
		builder
			.append_data(&mut header, path, file)
			.await
			.map_err(|error| tg::error!(!error, "failed to append the file"))?;
		position.fetch_add(metadata.len(), std::sync::atomic::Ordering::Relaxed);
	} else if file_type.is_symlink() {
		let target = tokio::fs::read_link(source).await.map_err(
			|error| tg::error!(!error, path = %source.display(), "failed to read the symlink"),
		)?;
		let mut header = tokio_tar::Header::new_gnu();
		header.set_entry_type(tokio_tar::EntryType::Symlink);
		header.set_mode(0o777);
		header.set_size(0);
		header
			.set_link_name(target)
			.map_err(|error| tg::error!(!error, "failed to set the symlink target"))?;
		builder
			.append_data(&mut header, path, &[][..])
			.await
			.map_err(|error| tg::error!(!error, "failed to append the symlink"))?;
	} else {
		return Err(tg::error!(path = %source.display(), "unsupported file type"));
	}

	Ok(())
}

async fn zip(input: &Path, output: Option<&Path>, position: &Arc<AtomicU64>) -> tg::Result<()> {
	let output = super::util::open_output(output).await?;
	let mut builder = async_zip::base::write::ZipFileWriter::new(output.compat_write());
	for (name, path) in read_directory(input).await? {
		zip_inner(&mut builder, &path, Path::new(&name), position).await?;
	}
	builder
		.close()
		.await
		.map_err(|error| tg::error!(!error, "failed to finish the archive"))?;

	Ok(())
}

async fn zip_inner<W>(
	builder: &mut async_zip::base::write::ZipFileWriter<W>,
	source: &Path,
	path: &Path,
	position: &Arc<AtomicU64>,
) -> tg::Result<()>
where
	W: futures::io::AsyncWrite + Send + Sync + Unpin,
{
	let metadata = tokio::fs::symlink_metadata(source).await.map_err(
		|error| tg::error!(!error, path = %source.display(), "failed to read the entry metadata"),
	)?;
	let file_type = metadata.file_type();
	if file_type.is_dir() {
		let filename = format!("{}/", path.to_string_lossy());
		let entry =
			async_zip::ZipEntryBuilder::new(filename.into(), async_zip::Compression::Deflate)
				.unix_permissions((metadata.permissions().mode() & 0o777) as u16);
		builder
			.write_entry_whole(entry.build(), &[])
			.await
			.map_err(|error| tg::error!(!error, "failed to write the directory"))?;
		for (name, source) in read_directory(source).await? {
			Box::pin(zip_inner(builder, &source, &path.join(name), position)).await?;
		}
	} else if file_type.is_file() {
		let entry = async_zip::ZipEntryBuilder::new(
			path.to_string_lossy().as_ref().into(),
			async_zip::Compression::Deflate,
		)
		.unix_permissions((metadata.permissions().mode() & 0o777) as u16);
		let mut writer = builder
			.write_entry_stream(entry)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the zip entry"))?
			.compat_write();
		let mut file = tokio::fs::File::open(source).await.map_err(
			|error| tg::error!(!error, path = %source.display(), "failed to open the file"),
		)?;
		tokio::io::copy(&mut file, &mut writer)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the file"))?;
		writer
			.into_inner()
			.close()
			.await
			.map_err(|error| tg::error!(!error, "failed to finish the zip entry"))?;
		position.fetch_add(metadata.len(), std::sync::atomic::Ordering::Relaxed);
	} else if file_type.is_symlink() {
		let target = tokio::fs::read_link(source).await.map_err(
			|error| tg::error!(!error, path = %source.display(), "failed to read the symlink"),
		)?;
		let entry = async_zip::ZipEntryBuilder::new(
			path.to_string_lossy().as_ref().into(),
			async_zip::Compression::Deflate,
		)
		.unix_permissions(0o120_777);
		builder
			.write_entry_whole(entry.build(), target.to_string_lossy().as_bytes())
			.await
			.map_err(|error| tg::error!(!error, "failed to write the symlink"))?;
	} else {
		return Err(tg::error!(path = %source.display(), "unsupported file type"));
	}

	Ok(())
}

async fn read_directory(path: &Path) -> tg::Result<Vec<(String, PathBuf)>> {
	let mut directory = tokio::fs::read_dir(path).await.map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to read the directory"),
	)?;
	let mut entries = Vec::new();
	while let Some(entry) = directory.next_entry().await.map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to read the directory entry"),
	)? {
		let name = entry.file_name().into_string().map_err(
			|_| tg::error!(path = %entry.path().display(), "the entry name is not UTF-8"),
		)?;
		entries.push((name, entry.path()));
	}
	entries.sort_by(|a, b| a.0.cmp(&b.0));

	Ok(entries)
}
