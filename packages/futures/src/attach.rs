use {
	futures::Stream,
	std::{pin::Pin, task::Poll},
	tokio::io::AsyncRead,
};

pub struct Attach<I, T> {
	inner: I,
	#[expect(dead_code)]
	value: T,
}

impl<I, T> Attach<I, T> {
	pub fn new(inner: I, value: T) -> Self {
		Self { inner, value }
	}
}

impl<I, T> Future for Attach<I, T>
where
	I: Future,
{
	type Output = I::Output;

	fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
		let future = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.inner) };
		future.poll(cx)
	}
}

impl<I, T> Stream for Attach<I, T>
where
	I: Stream,
{
	type Item = I::Item;

	fn poll_next(
		mut self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<Option<Self::Item>> {
		let stream = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.inner) };
		stream.poll_next(cx)
	}
}

impl<I, T> AsyncRead for Attach<I, T>
where
	I: AsyncRead,
{
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let reader = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.inner) };
		reader.poll_read(cx, buf)
	}
}
