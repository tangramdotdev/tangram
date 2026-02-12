use {
	self::and_then_frame::AndThenFrame,
	crate::Error,
	futures::{Stream, TryStreamExt as _, future},
	http_body_util::StreamBody,
	num::ToPrimitive as _,
	std::{convert::Infallible, pin::Pin},
	tokio::io::{AsyncBufRead, AsyncRead},
	tokio_util::io::{ReaderStream, StreamReader},
};

pub mod and_then_frame;
pub mod compression;

pub use http_body_util::BodyStream;

pub trait Ext: hyper::body::Body {
	fn and_then_frame<F, B>(self, f: F) -> AndThenFrame<Self, F>
	where
		Self: Sized,
		F: FnMut(hyper::body::Frame<Self::Data>) -> Result<hyper::body::Frame<B>, Self::Error>,
		B: bytes::Buf,
	{
		AndThenFrame::new(self, f)
	}

	fn boxed(self) -> Boxed
	where
		Self: http_body::Body<Data = bytes::Bytes> + Sized + Send + 'static,
		Self::Error: Into<Error>,
	{
		Boxed::new(self)
	}

	fn collect(self) -> http_body_util::combinators::Collect<Self>
	where
		Self: Sized,
	{
		http_body_util::BodyExt::collect(self)
	}

	fn map_err<F, E>(self, f: F) -> http_body_util::combinators::MapErr<Self, F>
	where
		Self: Sized,
		F: FnMut(Self::Error) -> E,
	{
		http_body_util::BodyExt::map_err(self, f)
	}
}

impl<B: hyper::body::Body + ?Sized> Ext for B {}

#[derive(Clone, Default)]
pub struct Empty;

#[derive(Clone)]
pub struct Bytes(Option<bytes::Bytes>);

pub struct Boxed(
	Pin<Box<dyn http_body::Body<Data = bytes::Bytes, Error = Error> + Send + 'static>>,
);

impl Empty {
	#[must_use]
	pub fn new() -> Self {
		Self
	}
}

impl Bytes {
	pub fn new(bytes: impl Into<bytes::Bytes>) -> Self {
		Self(Some(bytes.into()))
	}
}

impl Boxed {
	pub fn new<B>(body: B) -> Self
	where
		B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
		B::Error: Into<Error>,
	{
		Self(Box::pin(body.map_err(Into::into)))
	}

	#[must_use]
	pub fn empty() -> Self {
		Self::new(Empty::new())
	}

	pub fn with_bytes(bytes: impl Into<bytes::Bytes>) -> Self {
		Self::new(Bytes::new(bytes))
	}

	pub fn with_stream<S, T, E>(stream: S) -> Self
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<hyper::body::Frame<bytes::Bytes>> + 'static,
		E: Into<Error> + 'static,
	{
		Self::new(StreamBody::new(
			stream.map_ok(Into::into).map_err(Into::into),
		))
	}

	pub fn with_data_stream<S, T, E>(stream: S) -> Self
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<bytes::Bytes> + 'static,
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

	pub fn into_stream(
		self,
	) -> impl Stream<Item = Result<hyper::body::Frame<bytes::Bytes>, Error>> {
		BodyStream::new(self)
	}

	pub fn into_data_stream(self) -> impl Stream<Item = Result<bytes::Bytes, Error>> {
		self.into_stream()
			.try_filter_map(|frame| future::ok(frame.into_data().ok()))
	}

	pub fn into_reader(self) -> impl AsyncBufRead {
		StreamReader::new(self.into_data_stream().map_err(std::io::Error::other))
	}
}

impl hyper::body::Body for Empty {
	type Data = bytes::Bytes;
	type Error = Infallible;

	fn poll_frame(
		self: Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
		std::task::Poll::Ready(None)
	}

	fn is_end_stream(&self) -> bool {
		true
	}

	fn size_hint(&self) -> http_body::SizeHint {
		http_body::SizeHint::with_exact(0)
	}
}

impl hyper::body::Body for Bytes {
	type Data = bytes::Bytes;
	type Error = Infallible;

	fn poll_frame(
		self: Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
		let Some(bytes) = self.get_mut().0.take() else {
			return std::task::Poll::Ready(None);
		};
		let frame = hyper::body::Frame::data(bytes);
		std::task::Poll::Ready(Some(Ok(frame)))
	}

	fn is_end_stream(&self) -> bool {
		self.0.is_none()
	}

	fn size_hint(&self) -> http_body::SizeHint {
		let value = self
			.0
			.as_ref()
			.map_or(0, |bytes| bytes.len().to_u64().unwrap());
		http_body::SizeHint::with_exact(value)
	}
}

impl hyper::body::Body for Boxed {
	type Data = bytes::Bytes;
	type Error = Error;

	fn poll_frame(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
		self.get_mut().0.as_mut().poll_frame(cx)
	}

	fn is_end_stream(&self) -> bool {
		self.0.is_end_stream()
	}

	fn size_hint(&self) -> http_body::SizeHint {
		self.0.size_hint()
	}
}

impl Default for Boxed {
	fn default() -> Self {
		Self::empty()
	}
}
