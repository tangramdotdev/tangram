use num::ToPrimitive;
use tangram_client as tg;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite};

pub struct Reader<R> {
	inner: R,
}

impl<R> Reader<R>
where
	R: AsyncRead + Unpin,
{
	pub fn new(reader: R) -> Self {
		Self { inner: reader }
	}

	pub async fn read_header(&mut self) -> tg::Result<u64> {
		let mut magic = [0u8; 4];
		self.inner
			.read_exact(&mut magic)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the magic number"))?;
		if &magic != b"tgar" {
			return Err(tg::error!("invalid magic number"));
		}
		let version = self
			.read_varint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the archive version"))?;
		Ok(version)
	}

	pub async fn read_artifact_kind(&mut self) -> tg::Result<tg::artifact::Kind> {
		let kind = self
			.read_varint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the artifact kind"))?;
		match kind {
			0 => Ok(tg::artifact::Kind::Directory),
			1 => Ok(tg::artifact::Kind::File),
			2 => Ok(tg::artifact::Kind::Symlink),
			_ => Err(tg::error!("invalid artifact kind")),
		}
	}

	pub async fn read_directory(&mut self) -> tg::Result<u64> {
		self.read_varint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the number of directory entries"))
	}

	pub async fn read_directory_entry_name(&mut self) -> tg::Result<String> {
		let len = self
			.read_varint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the entry name length"))?
			.to_usize()
			.unwrap();
		let mut buf = vec![0; len];
		self.inner
			.read_exact(&mut buf)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the directory entry"))?;
		String::from_utf8(buf)
			.map_err(|source| tg::error!(!source, "directory entry name is not valid UTF-8"))
	}

	pub async fn read_file<W>(&mut self, mut writer: W) -> tg::Result<bool>
	where
		W: AsyncWrite + Unpin,
	{
		let executable = self
			.read_varint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the executable bit"))?
			!= 0;
		let length = self
			.read_varint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the file size"))?;
		let mut reader = (&mut self.inner).take(length);
		let read = tokio::io::copy(&mut reader, &mut writer)
			.await
			.map_err(|source| tg::error!(!source, "failed to write file contents"))?;
		if read == length {
			Ok(executable)
		} else {
			Err(tg::error!("file length mismatch"))
		}
	}

	pub async fn read_symlink(&mut self) -> tg::Result<String> {
		let len = self
			.read_varint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the target path length"))?
			.to_usize()
			.unwrap();
		let mut buf = vec![0; len];
		self.inner
			.read_exact(&mut buf)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the target path"))?;
		String::from_utf8(buf)
			.map_err(|source| tg::error!(!source, "target path is not valid UTF-8"))
	}

	pub async fn read_varint(&mut self) -> tg::Result<u64> {
		let mut result: u64 = 0;
		let mut shift = 0;
		loop {
			let mut byte: u8 = 0;
			let read = self
				.inner
				.read_exact(std::slice::from_mut(&mut byte))
				.await
				.map_err(|source| tg::error!(!source, "failed to read the value"))?;
			if read == 0 && shift == 0 {
				return Err(tg::error!("failed to read the value"));
			}
			if read == 0 {
				break;
			}
			result |= u64::from(byte & 0x7F) << shift;
			if byte & 0x80 == 0 {
				break;
			}
			shift += 7;
		}
		Ok(result)
	}
}
