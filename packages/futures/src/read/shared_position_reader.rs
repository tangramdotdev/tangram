use {
	pin_project::pin_project,
	std::{
		pin::{Pin, pin},
		sync::{
			Arc,
			atomic::{AtomicU64, Ordering},
		},
		task::Poll,
	},
	tokio::io::{AsyncBufRead, AsyncRead, AsyncSeek},
};

#[pin_project]
pub struct SharedPositionReader<R> {
	#[pin]
	inner: R,
	position: Arc<AtomicU64>,
}

impl<R> SharedPositionReader<R> {
	pub async fn with_reader_and_position(inner: R, position: u64) -> std::io::Result<Self> {
		let position = Arc::new(AtomicU64::new(position));
		Ok(SharedPositionReader { inner, position })
	}

	pub fn shared_position(&self) -> Arc<AtomicU64> {
		self.position.clone()
	}
}

impl<R> AsyncRead for SharedPositionReader<R>
where
	R: AsyncRead,
{
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.project();
		let poll = this.inner.poll_read(cx, buf);
		if let Poll::Ready(Ok(())) = &poll {
			let read_bytes = buf.filled().len() as u64;
			this.position.fetch_add(read_bytes, Ordering::Relaxed);
		}
		poll
	}
}

impl<R> AsyncBufRead for SharedPositionReader<R>
where
	R: AsyncBufRead,
{
	fn poll_fill_buf(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<&[u8]>> {
		let this = self.project();
		this.inner.poll_fill_buf(cx)
	}

	fn consume(self: Pin<&mut Self>, amt: usize) {
		let this = self.project();
		this.inner.consume(amt);
		this.position.fetch_add(amt as u64, Ordering::Relaxed);
	}
}

impl<R> AsyncSeek for SharedPositionReader<R>
where
	R: AsyncSeek,
{
	fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
		let this = self.project();
		this.inner.start_seek(position)
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<u64>> {
		let this = self.project();
		this.inner.poll_complete(cx)
	}
}
