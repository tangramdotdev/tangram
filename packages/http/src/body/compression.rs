use {
	bytes::Bytes,
	num::ToPrimitive as _,
	pin_project::pin_project,
	std::{io::Write as _, pin::Pin, task::Poll},
};

#[pin_project]
pub struct Compression<B> {
	#[pin]
	inner: B,
	compressor: Option<Compressor>,
}

#[pin_project]
pub struct Decompression<B> {
	#[pin]
	inner: B,
	decompressor: Option<Decompressor>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, derive_more::Display, derive_more::FromStr)]
#[display(rename_all = "lowercase")]
#[from_str(rename_all = "lowercase")]
pub enum Algorithm {
	Gzip,
	Zstd,
}

enum Compressor {
	Gzip(flate2::write::GzEncoder<Vec<u8>>),
	Zstd(zstd::stream::write::Encoder<'static, Vec<u8>>),
}

enum Decompressor {
	Gzip(flate2::write::GzDecoder<Vec<u8>>),
	Zstd(zstd::stream::write::Decoder<'static, Vec<u8>>),
}

impl Compressor {
	fn new(algorithm: Algorithm, level: u32) -> Self {
		match algorithm {
			Algorithm::Gzip => {
				let encoder =
					flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(level));
				Self::Gzip(encoder)
			},
			Algorithm::Zstd => {
				let encoder = zstd::Encoder::new(Vec::new(), level.to_i32().unwrap()).unwrap();
				Self::Zstd(encoder)
			},
		}
	}

	fn update(&mut self, data: &[u8]) -> std::io::Result<Bytes> {
		match self {
			Self::Gzip(s) => {
				s.write_all(data)?;
				let buffer = s.get_mut();
				let output = Bytes::copy_from_slice(buffer);
				buffer.clear();
				Ok(output)
			},
			Self::Zstd(s) => {
				s.write_all(data)?;
				let buffer = s.get_mut();
				let output = Bytes::copy_from_slice(buffer);
				buffer.clear();
				Ok(output)
			},
		}
	}

	fn finish(self) -> std::io::Result<Bytes> {
		match self {
			Self::Gzip(mut s) => {
				s.flush()?;
				let bytes = s.finish()?;
				Ok(bytes.into())
			},
			Self::Zstd(mut s) => {
				s.flush()?;
				let bytes = s.finish()?;
				Ok(bytes.into())
			},
		}
	}
}

impl Decompressor {
	fn new(algorithm: Algorithm) -> Self {
		match algorithm {
			Algorithm::Gzip => {
				let decoder = flate2::write::GzDecoder::new(Vec::new());
				Decompressor::Gzip(decoder)
			},
			Algorithm::Zstd => {
				let decoder = zstd::stream::write::Decoder::new(Vec::new()).unwrap();
				Decompressor::Zstd(decoder)
			},
		}
	}

	fn update(&mut self, data: &[u8]) -> std::io::Result<Bytes> {
		match self {
			Self::Gzip(s) => {
				s.write_all(data)?;
				let buffer = s.get_mut();
				let output = Bytes::copy_from_slice(buffer);
				buffer.clear();
				Ok(output)
			},
			Self::Zstd(s) => {
				s.write_all(data)?;
				let buffer = s.get_mut();
				let output = Bytes::copy_from_slice(buffer);
				buffer.clear();
				Ok(output)
			},
		}
	}

	fn finish(self) -> std::io::Result<Bytes> {
		match self {
			Self::Gzip(mut s) => {
				s.flush()?;
				let bytes = s.finish()?;
				Ok(bytes.into())
			},
			Self::Zstd(mut s) => {
				s.flush()?;
				let bytes = s.into_inner();
				Ok(bytes.into())
			},
		}
	}
}

impl<B> Compression<B>
where
	B: hyper::body::Body,
{
	pub fn new(body: B, algorithm: Algorithm, level: u32) -> Self {
		let compressor = Compressor::new(algorithm, level);
		Self {
			inner: body,
			compressor: Some(compressor),
		}
	}
}

impl<B> hyper::body::Body for Compression<B>
where
	B: hyper::body::Body<Data = Bytes>,
	B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
	type Data = Bytes;
	type Error = Box<dyn std::error::Error + Send + Sync>;

	fn poll_frame(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
		let mut this = self.project();
		if this.compressor.is_none() {
			return Poll::Ready(None);
		}
		match this.inner.as_mut().poll_frame(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error.into()))),
			Poll::Ready(Some(Ok(frame))) => {
				if frame.is_data() {
					let data = frame.into_data().unwrap();
					let result = this.compressor.as_mut().unwrap().update(&data);
					match result {
						Ok(data) => Poll::Ready(Some(Ok(hyper::body::Frame::data(data)))),
						Err(error) => Poll::Ready(Some(Err(error.into()))),
					}
				} else if frame.is_trailers() {
					let trailers = frame.into_trailers().unwrap();
					Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))))
				} else {
					unreachable!()
				}
			},
			Poll::Ready(None) => {
				let result = this.compressor.take().unwrap().finish();
				match result {
					Ok(data) => Poll::Ready(Some(Ok(hyper::body::Frame::data(data)))),
					Err(error) => Poll::Ready(Some(Err(error.into()))),
				}
			},
		}
	}

	fn is_end_stream(&self) -> bool {
		self.compressor.is_none()
	}

	fn size_hint(&self) -> hyper::body::SizeHint {
		hyper::body::SizeHint::default()
	}
}

impl<B> Decompression<B>
where
	B: hyper::body::Body,
{
	pub fn new(body: B, algorithm: Algorithm) -> Self {
		let decompressor = Decompressor::new(algorithm);
		Self {
			inner: body,
			decompressor: Some(decompressor),
		}
	}
}

impl<B> hyper::body::Body for Decompression<B>
where
	B: hyper::body::Body<Data = Bytes>,
	B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
	type Data = Bytes;
	type Error = Box<dyn std::error::Error + Send + Sync>;

	fn poll_frame(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
		let mut this = self.project();
		if this.decompressor.is_none() {
			return Poll::Ready(None);
		}
		match this.inner.as_mut().poll_frame(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error.into()))),
			Poll::Ready(Some(Ok(frame))) => {
				if frame.is_data() {
					let data = frame.into_data().unwrap();
					let result = this.decompressor.as_mut().unwrap().update(&data);
					match result {
						Ok(data) => Poll::Ready(Some(Ok(hyper::body::Frame::data(data)))),
						Err(error) => Poll::Ready(Some(Err(error.into()))),
					}
				} else if frame.is_trailers() {
					let trailers = frame.into_trailers().unwrap();
					Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))))
				} else {
					unreachable!()
				}
			},
			Poll::Ready(None) => {
				let result = this.decompressor.take().unwrap().finish();
				match result {
					Ok(data) => Poll::Ready(Some(Ok(hyper::body::Frame::data(data)))),
					Err(error) => Poll::Ready(Some(Err(error.into()))),
				}
			},
		}
	}

	fn is_end_stream(&self) -> bool {
		self.decompressor.is_none()
	}

	fn size_hint(&self) -> hyper::body::SizeHint {
		hyper::body::SizeHint::default()
	}
}
