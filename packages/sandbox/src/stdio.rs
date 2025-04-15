use futures::{FutureExt, future::BoxFuture};
use num::ToPrimitive;
use std::{
	collections::VecDeque,
	future,
	os::fd::{AsRawFd, IntoRawFd, OwnedFd},
	pin::Pin,
};
use tokio::io::{AsyncRead, AsyncWrite};

pub(crate) struct Writer {
	pub(crate) file: OwnedFd,
	pub(crate) isatty: bool,
	pub(crate) future: Option<BoxFuture<'static, std::io::Result<usize>>>,
}

pub(crate) struct Reader {
	pub(crate) file: OwnedFd,
	pub(crate) buffer: VecDeque<u8>,
	pub(crate) future: Option<BoxFuture<'static, std::io::Result<Vec<u8>>>>,
}

impl IntoRawFd for Writer {
	fn into_raw_fd(self) -> std::os::unix::prelude::RawFd {
		self.file.into_raw_fd()
	}
}

impl IntoRawFd for Reader {
	fn into_raw_fd(self) -> std::os::unix::prelude::RawFd {
		self.file.into_raw_fd()
	}
}

impl Writer {
	pub(crate) fn change_window_size(&self, tty: crate::Tty) -> std::io::Result<()> {
		if !self.isatty {
			return Ok(());
		}
		let fd = self.file.as_raw_fd();
		unsafe {
			let mut winsize = libc::winsize {
				ws_col: tty.cols,
				ws_row: tty.rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			if libc::ioctl(fd, libc::TIOCSWINSZ, std::ptr::addr_of_mut!(winsize)) != 0 {
				return Err(std::io::Error::last_os_error());
			}
		}
		Ok(())
	}
}

impl AsyncWrite for Writer {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> std::task::Poll<Result<usize, std::io::Error>> {
		let this = self.get_mut();
		if this.future.is_none() {
			// Create a new future to write to the output.
			let fd = this.file.as_raw_fd();
			let bytes = buf.to_vec();
			let future = tokio::task::spawn_blocking(move || unsafe {
				let n = libc::write(fd, bytes.as_ptr().cast(), bytes.len());
				if n < 0 {
					return Err(std::io::Error::last_os_error());
				}
				Ok(n.to_usize().unwrap())
			})
			.then(|result| future::ready(result.unwrap()))
			.boxed();
			this.future.replace(future);
		}

		let mut future = this.future.take().unwrap();
		match std::pin::pin!(&mut future).poll(cx) {
			std::task::Poll::Pending => {
				this.future.replace(future);
				std::task::Poll::Pending
			},
			std::task::Poll::Ready(ready) => std::task::Poll::Ready(ready),
		}
	}

	fn is_write_vectored(&self) -> bool {
		false
	}

	fn poll_flush(
		self: Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		std::task::Poll::Ready(Ok(()))
	}

	fn poll_shutdown(
		self: Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		std::task::Poll::Ready(Ok(()))
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		let this = self.get_mut();

		// Create the future.
		if this.future.is_none() {
			let fd = this.file.as_raw_fd();
			let future = tokio::task::spawn_blocking(move || unsafe {
				let mut buf = vec![0u8; 4096];
				let n = libc::read(fd, buf.as_mut_ptr().cast(), buf.len());
				if n < 0 {
					let error = std::io::Error::last_os_error();
					if error.raw_os_error() == Some(libc::EIO) {
						buf.truncate(0);
						return Ok(buf);
					}
					return Err(error);
				}
				buf.truncate(n.to_usize().unwrap());
				Ok(buf)
			})
			.then(|result| future::ready(result.unwrap()))
			.boxed();
			this.future.replace(future);
		}

		// Poll the internal future.
		if this.buffer.is_empty() {
			let mut future = this.future.take().unwrap();
			this.buffer = match std::pin::pin!(&mut future).poll(cx) {
				std::task::Poll::Pending => {
					this.future.replace(future);
					return std::task::Poll::Pending;
				},
				std::task::Poll::Ready(Ok(ready)) => ready.into(),
				std::task::Poll::Ready(Err(error)) => {
					return std::task::Poll::Ready(Err(error));
				},
			}
		};

		// Read from the internal ring buffer.
		let count = buf.remaining().min(this.buffer.len());

		let slice = this.buffer.make_contiguous();
		buf.put_slice(&slice[0..count]);
		this.buffer.drain(0..count);

		// Return ready.
		std::task::Poll::Ready(Ok(()))
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::VecDeque,
		os::fd::{FromRawFd, OwnedFd},
	};

	use tokio::io::{AsyncReadExt, AsyncWriteExt};

	#[tokio::test]
	async fn pipe() {
		let (reader, writer) = unsafe {
			let mut fds = [0; 2];
			assert_eq!(libc::pipe(fds.as_mut_ptr()), 0);
			(OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1]))
		};
		let mut reader = super::Reader {
			file: reader,
			buffer: VecDeque::new(),
			future: None,
		};
		let mut writer = super::Writer {
			file: writer,
			isatty: false,
			future: None,
		};

		writer.write_all(b"hello, pipe!").await.unwrap();
		drop(writer);

		let mut output = Vec::new();
		let n = reader.read_to_end(&mut output).await.unwrap();
		output.truncate(n);
		assert_eq!(&output, b"hello, pipe!");
	}
}
