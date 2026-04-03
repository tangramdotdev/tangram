use {
	super::Stream,
	crate::prelude::*,
	bytes::Bytes,
	futures::{StreamExt as _, stream::BoxStream},
	std::sync::Arc,
	tokio::{io::AsyncReadExt as _, sync::Mutex},
};

#[derive(Clone)]
pub struct Reader(Arc<Mutex<State>>);

struct State {
	fd: Option<Fd>,
	input: Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>,
	process: Option<tg::process::Id>,
	remote: Option<String>,
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
		process: Option<tg::process::Id>,
		remote: Option<String>,
		stream: Stream,
	) -> Self {
		Self(Arc::new(Mutex::new(State {
			fd,
			input,
			process,
			remote,
			stream,
		})))
	}

	pub(crate) fn from_process(
		process: tg::process::Id,
		remote: Option<String>,
		stream: Stream,
	) -> Self {
		Self::new(None, None, Some(process), remote, stream)
	}

	pub(crate) fn from_stderr(stderr: tokio::process::ChildStderr) -> Self {
		Self::new(Some(Fd::Stderr(stderr)), None, None, None, Stream::Stderr)
	}

	pub(crate) fn from_stdout(stdout: tokio::process::ChildStdout) -> Self {
		Self::new(Some(Fd::Stdout(stdout)), None, None, None, Stream::Stdout)
	}

	pub(crate) fn unavailable(stream: Stream) -> Self {
		Self::new(None, None, None, None, stream)
	}

	pub async fn close(&mut self) -> tg::Result<()> {
		let mut state = self.0.lock().await;
		state.fd = None;
		state.input = None;
		state.process = None;
		state.remote = None;
		Ok(())
	}

	pub async fn read<H>(&mut self, handle: &H) -> tg::Result<Option<Bytes>>
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
				state.remote = None;
				return Ok(None);
			}
			return Ok(Some(Bytes::copy_from_slice(&buffer[..count])));
		}
		if state.process.is_none() {
			return Err(tg::error!("{} is not available", state.stream));
		}
		if state.input.is_none() {
			let arg = tg::process::stdio::read::Arg {
				local: None,
				remotes: state.remote.clone().map(|remote| vec![remote]),
				streams: vec![state.stream],
				..Default::default()
			};
			let process = state.process.clone().unwrap();
			let Some(input) = handle.try_read_process_stdio(&process, arg).await? else {
				return Err(tg::error!("{} is not available", state.stream));
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
					state.remote = None;
					return Ok(None);
				},
				Some(Err(error)) => return Err(error),
			}
		}
	}

	pub async fn read_all<H>(&mut self, handle: &H) -> tg::Result<Bytes>
	where
		H: tg::Handle,
	{
		let mut chunks = Vec::new();
		let mut length = 0;
		while let Some(bytes) = self.read(handle).await? {
			length += bytes.len();
			chunks.push(bytes);
		}
		let mut output = Vec::with_capacity(length);
		for chunk in chunks {
			output.extend_from_slice(&chunk);
		}
		Ok(Bytes::from(output))
	}

	pub async fn text<H>(&mut self, handle: &H) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		String::from_utf8(self.read_all(handle).await?.to_vec())
			.map_err(|source| tg::error!(!source, "failed to decode the output as UTF-8"))
	}
}

impl std::fmt::Debug for Reader {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Reader").finish_non_exhaustive()
	}
}
