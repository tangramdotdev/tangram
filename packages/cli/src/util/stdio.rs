use {bytes::Bytes, futures::Stream, std::io::Read as _, tokio_stream::wrappers::ReceiverStream};

pub(crate) fn stdin() -> impl Stream<Item = std::io::Result<Bytes>> + Send + 'static {
	let (send, recv) = tokio::sync::mpsc::channel(1);
	std::thread::spawn(move || {
		let mut stdin = std::io::stdin();
		loop {
			let mut buf = vec![0u8; 4096];
			let result = match stdin.read(&mut buf) {
				Ok(0) => break,
				Ok(n) => Ok(Bytes::copy_from_slice(&buf[0..n])),
				Err(error) => Err(error),
			};
			let result = send.blocking_send(result);
			if result.is_err() {
				break;
			}
		}
	});
	ReceiverStream::new(recv)
}
