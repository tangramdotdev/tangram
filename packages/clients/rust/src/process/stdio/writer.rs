use {
	super::Stream,
	crate::prelude::*,
	bytes::Bytes,
	futures::{
		StreamExt as _, future,
		stream::{self, BoxStream},
	},
	std::{
		marker::PhantomData,
		sync::{Arc, Weak},
	},
	tokio::{io::AsyncWriteExt as _, sync::Mutex},
};

#[derive(Clone)]
pub struct Writer(Arc<Mutex<State>>);

struct State {
	fd: Option<Fd>,
	process: Option<Weak<tg::process::Inner>>,
	stream: Stream,
}

enum Fd {
	Stdin(tokio::process::ChildStdin),
}

impl Writer {
	fn new(fd: Option<Fd>, stream: Stream) -> Self {
		Self(Arc::new(Mutex::new(State {
			fd,
			process: None,
			stream,
		})))
	}

	pub(crate) fn from_process(stream: Stream) -> Self {
		Self::new(None, stream)
	}

	pub(crate) fn from_stdin(stdin: tokio::process::ChildStdin) -> Self {
		Self::new(Some(Fd::Stdin(stdin)), Stream::Stdin)
	}

	pub(crate) fn unavailable(stream: Stream) -> Self {
		Self::new(None, stream)
	}

	pub(crate) fn set_process(&self, process: Weak<tg::process::Inner>) {
		self.0.try_lock().unwrap().process = Some(process);
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
		let stream = state.stream;
		if matches!(fd, Some(Fd::Stdin(_))) {
			drop(fd);
			return Ok(());
		}
		drop(fd);
		if let Some(process) = process {
			let (location, process) = ensure_process_with_handle(Some(process), handle).await?;
			let arg = tg::process::stdio::write::Arg {
				location,
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
		drop(state);
		let (location, process) = ensure_process_with_handle(Some(process), handle).await?;
		let arg = tg::process::stdio::write::Arg {
			location,
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

async fn ensure_process_with_handle<H>(
	process: Option<Weak<tg::process::Inner>>,
	handle: &H,
) -> tg::Result<(Option<tg::location::Arg>, tg::process::Id)>
where
	H: tg::Handle,
{
	let process = process
		.and_then(|process| process.upgrade())
		.ok_or_else(|| tg::error!("the process is not available"))?;
	let handle_process = crate::process::Process::<tg::Value>(process.clone(), PhantomData);
	handle_process.ensure_location_with_handle(handle).await?;
	let location = process.location.read().unwrap().clone();
	let id = process
		.id
		.as_ref()
		.right()
		.cloned()
		.ok_or_else(|| tg::error!("the process is not available"))?;
	Ok((location, id))
}
