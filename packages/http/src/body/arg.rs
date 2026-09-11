use {
	crate::Result,
	bytes::Bytes,
	http_body::{Frame, SizeHint},
	num::ToPrimitive as _,
	pin_project::pin_project,
	serde::de::DeserializeOwned,
	std::{
		pin::Pin,
		task::{Context, Poll},
	},
	tangram_futures::read::Ext as _,
	tangram_uri::builder::QUERY_PARAMS_LENGTH_THRESHOLD,
	tokio::io::{AsyncRead, AsyncReadExt as _},
};

pub const HEADER: &str = "x-tg-arg-in-body";
pub const THRESHOLD: usize = QUERY_PARAMS_LENGTH_THRESHOLD;

#[pin_project]
#[derive(Clone)]
pub struct Body<B> {
	arg: Option<Bytes>,
	#[pin]
	body: B,
}

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

pub async fn get<T, R>(mut reader: &mut R, max_len: u64) -> Result<T>
where
	T: DeserializeOwned,
	R: AsyncRead + Unpin + Send + ?Sized,
{
	let len = reader.read_uvarint().await?;
	if len > max_len {
		return Err(std::io::Error::other("arg too large").into());
	}
	let len = len
		.try_into()
		.map_err(|_| std::io::Error::other("invalid arg length"))?;
	let mut bytes = vec![0; len];
	reader.read_exact(&mut bytes).await?;
	let arg = serde_json::from_slice(&bytes)?;
	Ok(arg)
}

impl<B> Body<B> {
	#[must_use]
	pub fn new(body: B) -> Self {
		Self { arg: None, body }
	}

	pub fn with_arg<T>(body: B, arg: &T) -> Result<Self>
	where
		T: serde::Serialize,
	{
		let arg = serde_json::to_vec(arg)?;
		let mut bytes = Vec::with_capacity(10 + arg.len());
		let mut length = arg.len();
		while length >= 0x80 {
			bytes.push(u8::try_from(length & 0x7f).unwrap() | 0x80);
			length >>= 7;
		}
		bytes.push(u8::try_from(length).unwrap());
		bytes.extend_from_slice(&arg);
		let arg = Some(bytes.into());
		Ok(Self { arg, body })
	}
}

impl<B> http_body::Body for Body<B>
where
	B: http_body::Body<Data = Bytes>,
{
	type Data = Bytes;
	type Error = B::Error;

	fn poll_frame(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<std::result::Result<Frame<Bytes>, Self::Error>>> {
		let this = self.project();
		if let Some(arg) = this.arg.take() {
			return Poll::Ready(Some(Ok(Frame::data(arg))));
		}
		this.body.poll_frame(cx)
	}

	fn is_end_stream(&self) -> bool {
		self.arg.is_none() && self.body.is_end_stream()
	}

	fn size_hint(&self) -> SizeHint {
		let length = self
			.arg
			.as_ref()
			.map_or(0, |arg| arg.len().to_u64().unwrap());
		let body = self.body.size_hint();
		let mut hint = SizeHint::new();
		hint.set_lower(body.lower().saturating_add(length));
		if let Some(upper) = body.upper().and_then(|upper| upper.checked_add(length)) {
			hint.set_upper(upper);
		}
		hint
	}
}
