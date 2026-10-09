use {
	crate::{Error, Result, body},
	futures::{StreamExt as _, stream},
	hyper::body::Frame,
	num::ToPrimitive as _,
	serde::de::DeserializeOwned,
	tangram_futures::{read::Ext as _, write::Ext as _},
	tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _},
};

pub const MAX_LENGTH: u64 = 1_048_576;

pub async fn get<T, R>(
	mut reader: &mut R,
	max_len: u64,
	encoding: crate::body::encoding::Encoding,
) -> Result<T>
where
	T: DeserializeOwned + for<'de> tangram_serialize::Deserialize<'de>,
	R: AsyncRead + Unpin + Send + ?Sized,
{
	let len = reader.read_uvarint().await?;
	if len > max_len {
		return Err(std::io::Error::other("header too large").into());
	}
	let len = len
		.try_into()
		.map_err(|_| std::io::Error::other("invalid header length"))?;
	let mut bytes = vec![0; len];
	reader.read_exact(&mut bytes).await?;
	let header = encoding.deserialize(&bytes)?;
	Ok(header)
}

pub fn set<T>(
	body: body::Boxed,
	header: &T,
	encoding: crate::body::encoding::Encoding,
) -> Result<body::Boxed>
where
	T: serde::Serialize + tangram_serialize::Serialize,
{
	let header = encoding.serialize(header)?;
	let stream = stream::once(async move {
		let mut bytes = Vec::with_capacity(9 + header.len());
		bytes.write_uvarint(header.len().to_u64().unwrap()).await?;
		bytes.write_all(&header).await?;
		Ok::<_, Error>(Frame::data(bytes.into()))
	})
	.chain(body.into_stream());
	Ok(body::Boxed::with_stream(stream))
}
