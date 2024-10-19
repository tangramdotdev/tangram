use std::sync::atomic::AtomicU64;

use crate as tg;
use crate::blob::Reader;
use std::pin::Pin;
use std::sync::{atomic::Ordering, Arc};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncSeek};

pub struct ProgressReader<H> {
	inner: Reader<H>,
	position: Arc<AtomicU64>,
}

impl<H> ProgressReader<H>
where
	H: tg::Handle,
{
	pub fn new(inner: Reader<H>) -> tg::Result<ProgressReader<H>> {
		Ok(ProgressReader {
			inner,
			position: Arc::new(AtomicU64::new(0)),
		})
	}

	pub fn position(&self) -> Arc<AtomicU64> {
		self.position.clone()
	}

	pub fn size(&self) -> u64 {
		self.inner.size()
	}
}

impl<H> AsyncRead for ProgressReader<H>
where
	H: tg::Handle,
{
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		let this = self.get_mut();
		let poll_result = Pin::new(&mut this.inner).poll_read(cx, buf);
		if let std::task::Poll::Ready(Ok(())) = &poll_result {
			let read_bytes = buf.filled().len() as u64;
			this.position.fetch_add(read_bytes, Ordering::Relaxed);
		}
		poll_result
	}
}

impl<H> AsyncSeek for ProgressReader<H>
where
	H: tg::Handle,
{
	fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
		Pin::new(&mut self.get_mut().inner).start_seek(position)
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		Pin::new(&mut self.get_mut().inner).poll_complete(cx)
	}
}

impl<H> AsyncBufRead for ProgressReader<H>
where
	H: tg::Handle,
{
	fn poll_fill_buf(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<&[u8]>> {
		Pin::new(&mut self.get_mut().inner).poll_fill_buf(cx)
	}

	fn consume(self: Pin<&mut Self>, amt: usize) {
		let this = self.get_mut();
		Pin::new(&mut this.inner).consume(amt);
		this.position.fetch_add(amt as u64, Ordering::Relaxed);
	}
}
