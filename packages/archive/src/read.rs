use num::ToPrimitive;
use tangram_client as tg;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite};

pub struct Reader<R> {
	inner: R,
}

impl<R> Reader<R>
where
	R: AsyncRead + Unpin + Send + Sync,
{
	pub fn new(reader: R) -> Self {
		Self { inner: reader }
	}

	pub async fn read_header(&mut self) -> tg::Result<u64> {
		let mut magic = [0u8; 4];
		let read = self
			.inner
			.read_exact(&mut magic)
			.await
			.map_err(|source| tg::error!(!source, "could not read archive magic"))?;
		if read != 4 || b"tgar" != &magic {
			return Err(tg::error!("could not find archive magic"));
		}
		let version = self.read_varint("archive version").await?;
		Ok(version)
	}

	pub async fn read_artifact_type(&mut self) -> tg::Result<tg::artifact::Kind> {
		let atype = self.read_varint("artifact type").await?;
		match atype {
			0 => Ok(tg::artifact::Kind::Directory),
			1 => Ok(tg::artifact::Kind::File),
			2 => Ok(tg::artifact::Kind::Symlink),
			_ => Err(tg::error!("invalid artifact type")),
		}
	}

	pub async fn read_directory(&mut self) -> tg::Result<u64> {
		self.read_varint("number of directory entries").await
	}

	pub async fn read_directory_entry_name(&mut self) -> tg::Result<String> {
		let len = self
			.read_varint("entry name length")
			.await?
			.to_usize()
			.unwrap();
		let mut buf = vec![0; len];
		self.inner
			.read_exact(&mut buf)
			.await
			.map_err(|source| tg::error!(!source, "could not read directory entry"))?;
		String::from_utf8(buf)
			.map_err(|source| tg::error!(!source, "directory entry name is not valid UTF-8"))
	}

	pub async fn read_file<W>(&mut self, mut writer: W) -> tg::Result<()>
	where
		W: AsyncWrite + Unpin,
	{
		let _executable = self.read_varint("executable bit").await? != 0;
		let length = self.read_varint("file size").await?;
		let mut reader = (&mut self.inner).take(length);
		let read = tokio::io::copy(&mut reader, &mut writer)
			.await
			.map_err(|source| tg::error!(!source, "could not write file contents"))?;
		if read == length {
			Ok(())
		} else {
			Err(tg::error!("file length mismatch"))
		}
	}

	pub async fn read_symlink(&mut self) -> tg::Result<String> {
		let len = self
			.read_varint("target path length")
			.await?
			.to_usize()
			.unwrap();
		let mut buf = vec![0; len];
		self.inner
			.read_exact(&mut buf)
			.await
			.map_err(|source| tg::error!(!source, "could not read the target path"))?;
		String::from_utf8(buf)
			.map_err(|source| tg::error!(!source, "target path is not valid UTF-8"))
	}

	pub async fn read_varint(&mut self, description: &str) -> tg::Result<u64> {
		let mut result: u64 = 0;
		let mut shift = 0;
		loop {
			let mut buf = [0_u8; 1];
			let read = self
				.inner
				.read_exact(&mut buf)
				.await
				.map_err(|source| tg::error!(!source, "unable to read {description}"))?;
			if read == 0 && shift == 0 {
				return Err(tg::error!("unable to read {description}"));
			}
			if read == 0 {
				break;
			}
			result += u64::from(buf[0] & 0x7F) << shift;
			if buf[0] & 0x80 == 0 {
				break;
			}
			shift += 7;
		}
		Ok(result)
	}
}
