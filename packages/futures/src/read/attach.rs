use std::{pin::Pin, task::Poll};
use tokio::io::AsyncRead;

pub struct Attach<R, T> {
	reader: R,
	#[allow(dead_code)]
	value: T,
}

impl<R, T> Attach<R, T> {
	pub fn new(reader: R, value: T) -> Self {
		Self { reader, value }
	}
}

impl<R, T> AsyncRead for Attach<R, T>
where
	R: AsyncRead,
{
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let reader = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.reader) };
		reader.poll_read(cx, buf)
	}
}
