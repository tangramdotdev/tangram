use integer_encoding::VarInt;
use num::ToPrimitive as _;
use tangram_client as tg;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};

pub struct Builder<W> {
	writer: W,
}

impl<W> Builder<W>
where
	W: AsyncWrite + Unpin + Send + Sync,
{
	pub fn new(writer: W) -> Self {
		Self { writer }
	}

	pub async fn append_archive_header(&mut self) -> tg::Result<()> {
		self.writer
			.write_all(b"tgar")
			.await
			.map_err(|source| tg::error!(!source, "could not write the archive magic"))?;
		self.write_varint(1, "archive version").await
	}

	pub async fn append_directory(&mut self, num_entries: u64) -> tg::Result<()> {
		self.write_varint(0, "artifact type").await?;
		self.write_varint(num_entries, "number of directory entries")
			.await
	}

	pub async fn append_directory_entry(&mut self, name: &str) -> tg::Result<()> {
		self.write_varint(name.len().to_u64().unwrap(), "entry name length")
			.await?;
		self.writer
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
		self.write_varint(1, "artifact type").await?;
		self.write_varint(u64::from(executable), "executable bit")
			.await?;
		self.write_varint(length, "file size").await?;
		let written = tokio::io::copy(reader, &mut self.writer)
			.await
			.map_err(|source| tg::error!(!source, "could not write file contents"))?;
		if written == length {
			Ok(())
		} else {
			Err(tg::error!("file length mismatch"))
		}
	}

	pub async fn append_symlink(&mut self, target: &str) -> tg::Result<()> {
		self.write_varint(2, "artifact type").await?;
		self.write_varint(target.len().to_u64().unwrap(), "target path length")
			.await?;
		self.writer
			.write_all(target.as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "could not write the target path"))
	}

	pub async fn finish(&mut self) -> tg::Result<()> {
		self.writer
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "could not flush the writer"))
	}

	async fn write_varint(&mut self, src: u64, description: &str) -> tg::Result<()> {
		//TODO: verify that the biggest varint from a u64 is 10-bytes long
		let mut bytes = [0u8; 10];
		let len = src.encode_var(&mut bytes);
		self.writer
			.write_all(&bytes[..len])
			.await
			.map_err(|source| tg::error!(!source, "could not write the {description}"))
	}
}
