use num::ToPrimitive as _;
use tangram_client as tg;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};

pub struct Writer<W> {
	inner: W,
}

impl<W> Writer<W>
where
	W: AsyncWrite + Unpin + Send + Sync,
{
	pub fn new(writer: W) -> Self {
		Self { inner: writer }
	}

	pub async fn append_archive_header(&mut self) -> tg::Result<()> {
		self.inner
			.write_all(b"tgar")
			.await
			.map_err(|source| tg::error!(!source, "could not write the archive magic"))?;
		self.write_varint(1)
			.await
			.map_err(|source| tg::error!(!source, "could not write the archive version"))
	}

	pub async fn append_directory(&mut self, num_entries: u64) -> tg::Result<()> {
		self.write_varint(0)
			.await
			.map_err(|source| tg::error!(!source, "could not write the artifact type"))?;
		self.write_varint(num_entries).await.map_err(|source| {
			tg::error!(!source, "could not write the number of directory entries")
		})
	}

	pub async fn append_directory_entry(&mut self, name: &str) -> tg::Result<()> {
		self.write_varint(name.len().to_u64().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "could not write the entry name length"))?;
		self.inner
			.write_all(name.as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "could not write directory entry"))
	}

	pub async fn append_file<R>(
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
			.map_err(|source| tg::error!(!source, "could not write the artifact type"))?;
		self.write_varint(u64::from(executable))
			.await
			.map_err(|source| tg::error!(!source, "could not write the executable bit"))?;
		self.write_varint(length)
			.await
			.map_err(|source| tg::error!(!source, "could not write the file size"))?;
		let written = tokio::io::copy(reader, &mut self.inner)
			.await
			.map_err(|source| tg::error!(!source, "could not write file contents"))?;
		if written == length {
			Ok(())
		} else {
			Err(tg::error!("file length mismatch"))
		}
	}

	pub async fn append_symlink(&mut self, target: &str) -> tg::Result<()> {
		self.write_varint(2)
			.await
			.map_err(|source| tg::error!(!source, "could not write the artifact type"))?;
		self.write_varint(target.len().to_u64().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "could not write the target path length"))?;
		self.inner
			.write_all(target.as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "could not write the target path"))
	}

	pub async fn finish(&mut self) -> tg::Result<()> {
		self.inner
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "could not flush the writer"))
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
				.map_err(|source| tg::error!(!source, "could not write the value"))?;
			if src == 0 {
				break;
			}
		}
		Ok(())
	}
}
