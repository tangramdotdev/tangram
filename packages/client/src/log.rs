use bytes::Bytes;
use num::ToPrimitive;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Entry {
	pub pos: u64,
	pub bytes: Bytes,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Params {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub pos: Option<u64>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub len: Option<i64>,
}

impl Entry {
	pub async fn read<R>(mut reader: R) -> std::io::Result<Self>
	where
		R: AsyncRead + Unpin,
	{
		let pos = reader.read_u64().await?;
		let len = reader.read_u64().await?.to_usize().unwrap();
		let mut bytes = vec![0u8; len];
		reader.read_exact(&mut bytes).await?;
		let bytes = bytes.into();
		Ok(Self { pos, bytes })
	}

	pub fn to_bytes(&self) -> Bytes {
		let pos = self.pos.to_be_bytes();
		let len = self.bytes.len().to_u64().unwrap().to_be_bytes();
		pos.into_iter()
			.chain(len.into_iter())
			.chain(self.bytes.iter().copied())
			.collect()
	}
}
