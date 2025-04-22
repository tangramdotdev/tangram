use tangram_client as tg;

pub fn detect_archive_format(
	bytes: &[u8],
) -> tg::Result<Option<(tg::ArchiveFormat, Option<tg::CompressionFormat>)>> {
	// Detect zip.
	if bytes
		.get(0..2)
		.ok_or_else(|| tg::error!("buffer is too small"))?
		== [0x50, 0x4b]
	{
		return Ok(Some((tg::ArchiveFormat::Zip, None)));
	}

	// If a compression magic number is found, then assume the archive format is tar.
	if let Some(compression) = detect_compression_format(bytes)? {
		return Ok(Some((tg::ArchiveFormat::Tar, Some(compression))));
	}

	// Otherwise, check for ustar.
	let position = 257;
	let length = 6;
	if bytes
		.get(position..position + length)
		.ok_or_else(|| tg::error!("buffer is too small"))?
		== b"ustar\0"
	{
		return Ok(Some((tg::ArchiveFormat::Tar, None)));
	}

	// Otherwise, check for a valid tar checksum by parsing the header record. This is an 8-byte field at offset 148. See <https://en.wikipedia.org/wiki/Tar_(computing)#File_format>: "The checksum is calculated by taking the sum of the unsigned byte values of the header record with the eight checksum bytes taken to be ASCII spaces (decimal value 32). It is stored as a six digit octal number with leading zeroes followed by a NUL and then a space".
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
	for (i, item) in bytes.iter().enumerate() {
		if i >= position && i < position + length {
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

pub fn detect_compression_format(bytes: &[u8]) -> tg::Result<Option<tg::CompressionFormat>> {
	// Gz
	let n = bytes
		.get(..2)
		.ok_or_else(|| tg::error!("buffer is too small"))?;
	if n == [0x1F, 0x8B] {
		return Ok(Some(tg::CompressionFormat::Gz));
	}

	// Zstd
	let n = bytes
		.get(..4)
		.ok_or_else(|| tg::error!("buffer is too small"))?;
	if n == [0x28, 0xB5, 0x2F, 0xFD] {
		return Ok(Some(tg::CompressionFormat::Zstd));
	}

	// Bz2
	let n = bytes
		.get(..3)
		.ok_or_else(|| tg::error!("buffer is too small"))?;
	if n == [0x42, 0x5A, 0x68] {
		return Ok(Some(tg::CompressionFormat::Bz2));
	}

	// Xz
	let n = bytes
		.get(..6)
		.ok_or_else(|| tg::error!("buffer is too small"))?;
	if n == [0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00] {
		return Ok(Some(tg::CompressionFormat::Xz));
	}

	Ok(None)
}
