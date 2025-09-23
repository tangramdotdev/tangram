use {
	self::and_then_frame::AndThenFrame,
	crate::Error,
	bytes::Bytes,
	futures::{Stream, TryStreamExt as _, future},
	http_body_util::{BodyExt as _, BodyStream, StreamBody},
	std::pin::{Pin, pin},
	tokio::io::{AsyncBufRead, AsyncRead},
	tokio_util::io::{ReaderStream, StreamReader},
};

pub use http_body_util::{Empty, Full};

pub mod and_then_frame;
pub mod compression;

pub trait Ext: hyper::body::Body {
	fn and_then_frame<F, B>(self, f: F) -> AndThenFrame<Self, F>
	where
		Self: Sized,
		F: FnMut(hyper::body::Frame<Self::Data>) -> Result<hyper::body::Frame<B>, Self::Error>,
		B: bytes::Buf,
	{
		AndThenFrame::new(self, f)
	}
}

#[derive(Default)]
pub enum Body {
	#[default]
	Empty,
	Bytes(Option<Bytes>),
	Body(Pin<Box<dyn http_body::Body<Data = Bytes, Error = Error> + Send + 'static>>),
}

impl Body {
	pub fn new<B>(body: B) -> Self
	where
		B: http_body::Body<Data = Bytes> + Send + 'static,
		<B as http_body::Body>::Error: Into<Error>,
	{
		Self::Body(Box::pin(body.map_err(Into::into)))
	}

	pub fn try_clone(&self) -> Option<Self> {
		match self {
			Self::Empty => Some(Self::Empty),
			Self::Bytes(bytes) => Some(Self::Bytes(bytes.clone())),
			Self::Body(_) => None,
		}
	}

	#[must_use]
	pub fn empty() -> Self {
		Self::Empty
	}

	pub fn with_bytes<T>(bytes: T) -> Self
	where
		T: Into<Bytes>,
	{
		Self::Bytes(Some(bytes.into()))
	}

	pub fn with_stream<S, T, E>(stream: S) -> Self
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<hyper::body::Frame<Bytes>> + 'static,
		E: Into<Error> + 'static,
	{
		Self::new(StreamBody::new(
			stream.map_ok(Into::into).map_err(Into::into),
		))
	}

	pub fn with_data_stream<S, T, E>(stream: S) -> Self
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		Self::new(StreamBody::new(
			stream
				.map_ok(|bytes| hyper::body::Frame::data(bytes.into()))
				.map_err(Into::into),
		))
	}

	pub fn with_reader<R>(reader: R) -> Self
	where
		R: AsyncRead + Send + 'static,
	{
		Self::with_data_stream(ReaderStream::new(reader))
	}

	pub fn with_sse_stream<S, E>(value: S) -> Self
	where
		S: Stream<Item = Result<crate::sse::Event, E>> + Send + 'static,
		E: Into<Error> + 'static,
	{
		Self::with_data_stream(value.map_ok(|event| event.to_string()))
	}

	pub fn into_stream(self) -> impl Stream<Item = Result<hyper::body::Frame<Bytes>, Error>> {
		BodyStream::new(self)
	}

	pub fn into_data_stream(self) -> impl Stream<Item = Result<Bytes, Error>> {
		self.into_stream()
			.try_filter_map(|frame| future::ok(frame.into_data().ok()))
	}

	pub fn into_reader(self) -> impl AsyncBufRead {
		StreamReader::new(self.into_data_stream().map_err(std::io::Error::other))
	}
}

impl hyper::body::Body for Body {
	type Data = Bytes;

	type Error = Error;

	fn poll_frame(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
		match self.get_mut() {
			Body::Empty => std::task::Poll::Ready(None),
			Body::Bytes(option) => {
				std::task::Poll::Ready(option.take().map(hyper::body::Frame::data).map(Ok))
			},
			Body::Body(body) => pin!(body).poll_frame(cx),
		}
	}
}
