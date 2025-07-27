use bytes::Bytes;
use futures::{Stream, TryStreamExt as _};
use std::io::Read as _;
use tangram_client as tg;
use tokio::io::AsyncBufRead;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;

pub(crate) fn stdin() -> impl AsyncBufRead + Send + 'static {
	StreamReader::new(stdin_stream().map_err(std::io::Error::other))
}

pub(crate) fn stdin_stream() -> impl Stream<Item = tg::Result<Bytes>> + Send + 'static {
	let (send, recv) = tokio::sync::mpsc::channel(1);
	std::thread::spawn(move || {
		let mut stdin = std::io::stdin();
		loop {
			let mut buf = vec![0u8; 4096];
			let result = match stdin.read(&mut buf) {
				Ok(0) => break,
				Ok(n) => Ok(Bytes::copy_from_slice(&buf[0..n])),
				Err(source) => Err(tg::error!(!source, "failed to read stdin")),
			};
			let result = send.blocking_send(result);
			if result.is_err() {
				break;
			}
		}
	});
	ReceiverStream::new(recv)
}
