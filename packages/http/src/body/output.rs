use {
	crate::{Error, Result, body},
	futures::{StreamExt as _, stream},
	hyper::body::Frame,
	num::ToPrimitive as _,
	serde::de::DeserializeOwned,
	tangram_futures::{read::Ext as _, write::Ext as _},
	tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _},
};

pub const HEADER: &str = "x-tg-output-in-body";
pub const MAX_LENGTH: u64 = 1_048_576;

pub fn get_header(headers: &http::HeaderMap) -> Result<bool> {
	let Some(value) = headers.get(HEADER) else {
		return Ok(false);
	};
	let value = value.to_str()?;
	if value == "true" {
		Ok(true)
	} else {
		Err(std::io::Error::other("invalid x-tg-output-in-body header").into())
	}
}

pub async fn get<T, R>(mut reader: &mut R, max_len: u64) -> Result<T>
where
	T: DeserializeOwned,
	R: AsyncRead + Unpin + Send + ?Sized,
{
	let len = reader.read_uvarint().await?;
	if len > max_len {
		return Err(std::io::Error::other("output too large").into());
	}
	let len = len
		.try_into()
		.map_err(|_| std::io::Error::other("invalid output length"))?;
	let mut bytes = vec![0; len];
	reader.read_exact(&mut bytes).await?;
	let output = serde_json::from_slice(&bytes)?;
	Ok(output)
}

pub fn set<T>(body: body::Boxed, output: &T) -> Result<body::Boxed>
where
	T: serde::Serialize,
{
	let output = serde_json::to_vec(output)?;
	let stream = stream::once(async move {
		let mut bytes = Vec::with_capacity(9 + output.len());
		bytes.write_uvarint(output.len().to_u64().unwrap()).await?;
		bytes.write_all(&output).await?;
		Ok::<_, Error>(Frame::data(bytes.into()))
	})
	.chain(body.into_stream());
	Ok(body::Boxed::with_stream(stream))
}
