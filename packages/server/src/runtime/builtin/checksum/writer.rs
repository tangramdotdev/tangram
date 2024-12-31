use num::ToPrimitive as _;
use tangram_client as tg;
use tokio::io::{AsyncRead, AsyncWriteExt as _};

pub struct Writer {
	inner: tg::checksum::Writer,
}

impl Writer
{
	pub fn new(algorithm: tg::checksum::Algorithm) -> Self {
		let inner = tg::checksum::Writer::new(algorithm);
		Self { inner }
	}

	pub async fn write_header(&mut self) -> tg::Result<()> {
		self.inner
			.write_all(b"tgar")
			.await
			.map_err(|source| tg::error!(!source, "failed to write the magic number"))?;
		self.write_varint(0)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the archive version"))
	}

	pub async fn write_directory(&mut self, num_entries: u64) -> tg::Result<()> {
		self.write_varint(0)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the artifact kind"))?;
		self.write_varint(num_entries).await.map_err(|source| {
			tg::error!(!source, "failed to write the number of directory entries")
		})
	}

	pub async fn write_directory_entry(&mut self, name: &str) -> tg::Result<()> {
		self.write_varint(name.len().to_u64().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the entry name length"))?;
		self.inner
			.write_all(name.as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to write directory entry"))
	}

	pub async fn write_file<R>(
		&mut self,
		executable: bool,
		length: u64,
		reader: &mut R,
	) -> tg::Result<()>
	where
		R: AsyncRead + Unpin,
	{
		self.write_varint(1)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the artifact kind"))?;
		self.write_varint(u64::from(executable))
			.await
			.map_err(|source| tg::error!(!source, "failed to write the executable bit"))?;
		self.write_varint(length)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the file size"))?;
		let written = tokio::io::copy(reader, &mut self.inner)
			.await
			.map_err(|source| tg::error!(!source, "failed to write file contents"))?;
		if written == length {
			Ok(())
		} else {
			Err(tg::error!("file length mismatch"))
		}
	}

	pub async fn write_symlink(&mut self, target: &str) -> tg::Result<()> {
		self.write_varint(2)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the artifact kind"))?;
		self.write_varint(target.len().to_u64().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the target path length"))?;
		self.inner
			.write_all(target.as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the target path"))
	}

	pub async fn write_blob(&mut self, mut reader: impl AsyncRead) -> tg::Result<()> {
		let mut source = std::pin::pin!(reader);
		tokio::io::copy(&mut source, &mut self.inner).await.map_err(|source| tg::error!(!source, "failed to write blob"))?;
		Ok(())
	}

	pub async fn finish(mut self) -> tg::Result<tg::Checksum> {
		self.inner
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush the writer"))?;
		Ok(self.inner.finalize())
	}

	async fn write_varint(&mut self, mut src: u64) -> tg::Result<()> {
		loop {
			let mut byte = (src & 0x7F) as u8;
			src >>= 7;
			if src != 0 {
				byte |= 0x80;
			}
			self.inner
				.write_all(&[byte])
				.await
				.map_err(|source| tg::error!(!source, "failed to write the value"))?;
			if src == 0 {
				break;
			}
		}
		Ok(())
	}
}
