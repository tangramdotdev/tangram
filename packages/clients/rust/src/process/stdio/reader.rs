use {
	super::Stream,
	crate::prelude::*,
	bytes::Bytes,
	futures::{StreamExt as _, stream::BoxStream},
	std::sync::{Arc, Weak},
	tokio::{io::AsyncReadExt as _, sync::Mutex},
};

#[derive(Clone)]
pub struct Reader(Arc<Mutex<State>>);

struct State {
	fd: Option<Fd>,
	input: Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>,
	process: Option<Weak<tg::process::Inner>>,
	stream: Stream,
}

enum Fd {
	Stderr(tokio::process::ChildStderr),
	Stdout(tokio::process::ChildStdout),
}

impl Reader {
	fn new(
		fd: Option<Fd>,
		input: Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>,
		stream: Stream,
	) -> Self {
		Self(Arc::new(Mutex::new(State {
			fd,
			input,
			process: None,
			stream,
		})))
	}

	pub(crate) fn from_process(stream: Stream) -> Self {
		Self::new(None, None, stream)
	}

	pub(crate) fn from_stderr(stderr: tokio::process::ChildStderr) -> Self {
		Self::new(Some(Fd::Stderr(stderr)), None, Stream::Stderr)
	}

	pub(crate) fn from_stdout(stdout: tokio::process::ChildStdout) -> Self {
		Self::new(Some(Fd::Stdout(stdout)), None, Stream::Stdout)
	}

	pub(crate) fn unavailable(stream: Stream) -> Self {
		Self::new(None, None, stream)
	}

	pub(crate) fn set_process(&self, process: Weak<tg::process::Inner>) {
		self.0.try_lock().unwrap().process = Some(process);
	}

	pub async fn close(&mut self) -> tg::Result<()> {
		let mut state = self.0.lock().await;
		state.fd = None;
		state.input = None;
		state.process = None;
		Ok(())
	}

	pub async fn read(&mut self) -> tg::Result<Option<Bytes>> {
		let handle = tg::handle()?;
		self.read_with_handle(handle).await
	}

	pub async fn read_with_handle<H>(&mut self, handle: &H) -> tg::Result<Option<Bytes>>
	where
		H: tg::Handle,
	{
		let mut state = self.0.lock().await;
		if let Some(fd) = state.fd.as_mut() {
			let mut buffer = [0; 4096];
			let count = match fd {
				Fd::Stderr(stderr) => stderr
					.read(&mut buffer)
					.await
					.map_err(|source| tg::error!(!source, "failed to read stderr"))?,
				Fd::Stdout(stdout) => stdout
					.read(&mut buffer)
					.await
					.map_err(|source| tg::error!(!source, "failed to read stdout"))?,
			};
			if count == 0 {
				state.fd = None;
				state.process = None;
				return Ok(None);
			}
			return Ok(Some(Bytes::copy_from_slice(&buffer[..count])));
		}
		if state.process.is_none() {
			return Err(tg::error!("{} is not available", state.stream));
		}
		if state.input.is_none() {
			let process = state.process.clone();
			let stream = state.stream;
			drop(state);
			let process = process
				.and_then(|process| process.upgrade())
				.ok_or_else(|| tg::error!("the process is not available"))?;
			let location = process.location.read().unwrap().clone();
			let process = process
				.id
				.as_ref()
				.right()
				.cloned()
				.ok_or_else(|| tg::error!("the process is not available"))?;
			state = self.0.lock().await;
			let arg = tg::process::stdio::read::Arg {
				location,
				streams: vec![stream],
				..Default::default()
			};
			let Some(input) = handle.try_read_process_stdio(&process, arg).await? else {
				return Err(tg::error!("{} is not available", stream));
			};
			state.input = Some(input.boxed());
		}
		loop {
			let result = state.input.as_mut().unwrap().next().await;
			match result {
				Some(Ok(tg::process::stdio::read::Event::Chunk(chunk))) => {
					if chunk.stream != state.stream {
						return Err(tg::error!("invalid process stdio stream"));
					}
					if !chunk.bytes.is_empty() {
						return Ok(Some(chunk.bytes));
					}
				},
				Some(Ok(tg::process::stdio::read::Event::End)) | None => {
					state.input = None;
					state.process = None;
					return Ok(None);
				},
				Some(Err(error)) => return Err(error),
			}
		}
	}

	pub async fn read_all(&mut self) -> tg::Result<Bytes> {
		let handle = tg::handle()?;
		self.read_all_with_handle(handle).await
	}

	pub async fn read_all_with_handle<H>(&mut self, handle: &H) -> tg::Result<Bytes>
	where
		H: tg::Handle,
	{
		let mut chunks = Vec::new();
		let mut length = 0;
		while let Some(bytes) = self.read_with_handle(handle).await? {
			length += bytes.len();
			chunks.push(bytes);
		}
		let mut output = Vec::with_capacity(length);
		for chunk in chunks {
			output.extend_from_slice(&chunk);
		}
		Ok(Bytes::from(output))
	}

	pub async fn text(&mut self) -> tg::Result<String> {
		let handle = tg::handle()?;
		self.text_with_handle(handle).await
	}

	pub async fn text_with_handle<H>(&mut self, handle: &H) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		String::from_utf8(self.read_all_with_handle(handle).await?.to_vec())
			.map_err(|source| tg::error!(!source, "failed to decode the output as UTF-8"))
	}
}

impl std::fmt::Debug for Reader {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Reader").finish_non_exhaustive()
	}
}
