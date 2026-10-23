use {
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
	tokio::io::{AsyncRead, AsyncWrite},
};

pub(crate) type Reader = Box<dyn AsyncRead + Send + Unpin>;
pub(crate) type Writer = Box<dyn AsyncWrite + Send + Sync + Unpin>;

pub(crate) fn resolve_path(named: Option<PathBuf>, positional: Option<PathBuf>) -> Option<PathBuf> {
	named.or(positional).filter(|path| path != Path::new("-"))
}

pub(crate) async fn input_length(path: Option<&Path>) -> Option<u64> {
	let path = path?;
	let metadata = tokio::fs::metadata(path).await.ok()?;

	Some(metadata.len())
}

pub(crate) async fn open_input(path: Option<&Path>) -> tg::Result<Reader> {
	let reader: Reader = if let Some(path) = path {
		let file = tokio::fs::File::open(path).await.map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to open the input"),
		)?;
		Box::new(file)
	} else {
		Box::new(tokio::io::stdin())
	};

	Ok(reader)
}

pub(crate) async fn open_output(path: Option<&Path>) -> tg::Result<Writer> {
	let writer: Writer = if let Some(path) = path {
		let file = tokio::fs::File::create(path).await.map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the output"),
		)?;
		Box::new(file)
	} else {
		Box::new(tokio::io::stdout())
	};

	Ok(writer)
}

pub(crate) fn detect_archive_format(
	bytes: &[u8],
) -> tg::Result<Option<(tg::ArchiveFormat, Option<tg::CompressionFormat>)>> {
	if bytes
		.get(0..2)
		.ok_or_else(|| tg::error!("buffer is too small"))?
		== [0x50, 0x4b]
	{
		return Ok(Some((tg::ArchiveFormat::Zip, None)));
	}
	if let Some(compression) = detect_compression_format(bytes)? {
		return Ok(Some((tg::ArchiveFormat::Tar, Some(compression))));
	}
	let position = 257;
	let length = 5;
	if bytes
		.get(position..position + length)
		.ok_or_else(|| tg::error!("buffer is too small"))?
		== b"ustar"
	{
		return Ok(Some((tg::ArchiveFormat::Tar, None)));
	}
	if let Some(block) = bytes.get(0..512)
		&& block.iter().all(|&byte| byte == 0)
	{
		return Ok(Some((tg::ArchiveFormat::Tar, None)));
	}
	let position = 148;
	let length = 8;
	let header = bytes
		.get(position..position + length)
		.ok_or_else(|| tg::error!("buffer is too small"))?;
	let mut checksum = String::new();
	for byte in header {
		if *byte == 0 || *byte == b' ' {
			break;
		}
		checksum.push(*byte as char);
	}
	let expected_checksum =
		u32::from_str_radix(checksum.trim(), 8).map_err(|_| tg::error!("invalid tar checksum"))?;
	let mut actual_checksum = 0u32;
	for (index, item) in bytes.iter().take(512).enumerate() {
		if index >= position && index < position + length {
			actual_checksum += 32;
		} else {
			actual_checksum += u32::from(*item);
		}
	}
	if actual_checksum == expected_checksum {
		return Ok(Some((tg::ArchiveFormat::Tar, None)));
	}

	Ok(None)
}

pub(crate) fn detect_compression_format(input: &[u8]) -> tg::Result<Option<tg::CompressionFormat>> {
	let bytes = input
		.get(..2)
		.ok_or_else(|| tg::error!("buffer is too small"))?;
	if bytes == [0x1f, 0x8b] {
		return Ok(Some(tg::CompressionFormat::Gz));
	}

	let bytes = input
		.get(..4)
		.ok_or_else(|| tg::error!("buffer is too small"))?;
	if bytes == [0x28, 0xb5, 0x2f, 0xfd] {
		return Ok(Some(tg::CompressionFormat::Zst));
	}

	let bytes = input
		.get(..3)
		.ok_or_else(|| tg::error!("buffer is too small"))?;
	if bytes == [0x42, 0x5a, 0x68] {
		return Ok(Some(tg::CompressionFormat::Bz2));
	}

	let bytes = input
		.get(..6)
		.ok_or_else(|| tg::error!("buffer is too small"))?;
	if bytes == [0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00] {
		return Ok(Some(tg::CompressionFormat::Xz));
	}

	Ok(None)
}
