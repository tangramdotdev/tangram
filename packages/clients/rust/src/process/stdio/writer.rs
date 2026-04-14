use {
	super::Stream,
	crate::prelude::*,
	bytes::Bytes,
	futures::{
		StreamExt as _, future,
		stream::{self, BoxStream},
	},
	std::sync::Arc,
	tokio::{io::AsyncWriteExt as _, sync::Mutex},
};

#[derive(Clone)]
pub struct Writer(Arc<Mutex<State>>);

struct State {
	fd: Option<Fd>,
	process: Option<tg::process::Id>,
	remote: Option<String>,
	stream: Stream,
}

enum Fd {
	Stdin(tokio::process::ChildStdin),
}

impl Writer {
	fn new(
		fd: Option<Fd>,
		process: Option<tg::process::Id>,
		remote: Option<String>,
		stream: Stream,
	) -> Self {
		Self(Arc::new(Mutex::new(State {
			fd,
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
		Self::new(None, Some(process), remote, stream)
	}

	pub(crate) fn from_stdin(stdin: tokio::process::ChildStdin) -> Self {
		Self::new(Some(Fd::Stdin(stdin)), None, None, Stream::Stdin)
	}

	pub(crate) fn unavailable(stream: Stream) -> Self {
		Self::new(None, None, None, stream)
	}

	pub async fn close(&mut self) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.close_with_handle(handle).await
	}

	pub async fn close_with_handle<H>(&mut self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let mut state = self.0.lock().await;
		let fd = state.fd.take();
		let process = state.process.take();
		let remote = state.remote.take();
		let stream = state.stream;
		drop(fd);
		if let Some(process) = process {
			let arg = tg::process::stdio::write::Arg {
				local: None,
				remotes: remote.map(|remote| vec![remote]),
				streams: vec![stream],
			};
			let input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>> =
				stream::once(future::ok(tg::process::stdio::read::Event::End)).boxed();
			handle.write_process_stdio_all(&process, arg, input).await?;
		}
		Ok(())
	}

	pub async fn write(&mut self, input: &[u8]) -> tg::Result<usize> {
		let handle = tg::handle()?;
		self.write_with_handle(handle, input).await
	}

	pub async fn write_with_handle<H>(&mut self, handle: &H, input: &[u8]) -> tg::Result<usize>
	where
		H: tg::Handle,
	{
		let mut state = self.0.lock().await;
		if input.is_empty() {
			return Ok(0);
		}
		if let Some(Fd::Stdin(stdin)) = state.fd.as_mut() {
			stdin
				.write_all(input)
				.await
				.map_err(|source| tg::error!(!source, "failed to write stdin"))?;
			return Ok(input.len());
		}
		let Some(process) = state.process.clone() else {
			return Err(tg::error!("{} is not available", state.stream));
		};
		let stream = state.stream;
		let arg = tg::process::stdio::write::Arg {
			local: None,
			remotes: state.remote.clone().map(|remote| vec![remote]),
			streams: vec![stream],
		};
		let event = tg::process::stdio::read::Event::Chunk(tg::process::stdio::Chunk {
			bytes: Bytes::copy_from_slice(input),
			position: None,
			stream,
		});
		let stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>> =
			stream::once(future::ok(event)).boxed();
		handle
			.write_process_stdio_all(&process, arg, stream)
			.await?;
		Ok(input.len())
	}

	pub async fn write_all(&mut self, input: &[u8]) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.write_all_with_handle(handle, input).await
	}

	pub async fn write_all_with_handle<H>(&mut self, handle: &H, input: &[u8]) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let mut position = 0;
		while position < input.len() {
			let count = self.write_with_handle(handle, &input[position..]).await?;
			if count == 0 {
				return Err(tg::error!("failed to write stdin"));
			}
			position += count;
		}
		self.close_with_handle(handle).await
	}
}

impl std::fmt::Debug for Writer {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Writer").finish_non_exhaustive()
	}
}
