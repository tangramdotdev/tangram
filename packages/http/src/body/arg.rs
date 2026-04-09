use {
	crate::{Error, Result, body},
	futures::{StreamExt as _, stream},
	hyper::body::Frame,
	num::ToPrimitive as _,
	serde::de::DeserializeOwned,
	tangram_futures::{read::Ext as _, write::Ext as _},
	tangram_uri::builder::QUERY_PARAMS_LENGTH_THRESHOLD,
	tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _},
};

pub const HEADER: &str = "x-tg-arg-in-body";
pub const THRESHOLD: usize = QUERY_PARAMS_LENGTH_THRESHOLD;

pub fn get_header(headers: &http::HeaderMap) -> Result<bool> {
	let Some(value) = headers.get(HEADER) else {
		return Ok(false);
	};
	let value = value.to_str()?;
	if value == "true" {
		Ok(true)
	} else {
		Err(std::io::Error::other("invalid x-tg-arg-in-body header").into())
	}
}

pub async fn get<T, R>(mut reader: &mut R) -> Result<T>
where
	T: DeserializeOwned,
	R: AsyncRead + Unpin + Send + ?Sized,
{
	let len = reader.read_uvarint().await?;
	let len = len
		.try_into()
		.map_err(|_| std::io::Error::other("invalid arg length"))?;
	let mut bytes = vec![0; len];
	reader.read_exact(&mut bytes).await?;
	let arg = serde_json::from_slice(&bytes)?;
	Ok(arg)
}

pub fn set<T>(body: body::Boxed, arg: &T) -> Result<body::Boxed>
where
	T: serde::Serialize,
{
	let arg = serde_json::to_vec(arg)?;
	let stream = stream::once(async move {
		let mut bytes = Vec::with_capacity(9 + arg.len());
		bytes.write_uvarint(arg.len().to_u64().unwrap()).await?;
		bytes.write_all(&arg).await?;
		Ok::<_, Error>(Frame::data(bytes.into()))
	})
	.chain(body.into_stream());
	Ok(body::Boxed::with_stream(stream))
}
