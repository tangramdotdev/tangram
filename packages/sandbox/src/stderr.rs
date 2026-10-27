use {
	std::{collections::VecDeque, sync::Arc},
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
};

/// The number of trailing bytes of the sandbox process's stderr to retain.
const CAPACITY: usize = 4096;

pub(crate) struct Stderr {
	ended: tokio::sync::watch::Receiver<bool>,
	tail: Arc<std::sync::Mutex<VecDeque<u8>>>,
}

impl Stderr {
	/// Forward the sandbox process's stderr to the server's stderr, retaining its tail.
	pub(crate) fn new(mut stderr: tokio::process::ChildStderr) -> Self {
		let (ended_sender, ended) = tokio::sync::watch::channel(false);
		let tail = Arc::new(std::sync::Mutex::new(VecDeque::with_capacity(CAPACITY)));
		tokio::spawn({
			let tail = tail.clone();
			async move {
				let mut buffer = vec![0; CAPACITY];
				let mut output = tokio::io::stderr();
				while let Ok(n) = stderr.read(&mut buffer).await
					&& n > 0
				{
					let bytes = &buffer[..n];
					output.write_all(bytes).await.ok();
					let mut tail = tail.lock().unwrap();
					tail.extend(bytes);
					let excess = tail.len().saturating_sub(CAPACITY);
					tail.drain(..excess);
				}
				ended_sender.send_replace(true);
			}
		});
		Self { ended, tail }
	}

	/// Get the tail of the sandbox process's stderr after it ends.
	pub(crate) async fn tail(&self) -> String {
		let mut ended = self.ended.clone();
		ended.wait_for(|ended| *ended).await.ok();
		let tail = self.tail.lock().unwrap();
		let (front, back) = tail.as_slices();
		let bytes = [front, back].concat();
		String::from_utf8_lossy(&bytes).trim().to_owned()
	}
}
