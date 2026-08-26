use {
	super::Stream,
	crate::prelude::*,
	futures::{StreamExt as _, stream},
	num::ToPrimitive as _,
	std::{
		marker::PhantomData,
		sync::{Arc, Weak},
	},
	tangram_futures::task::Task,
	tokio::{io::AsyncWriteExt as _, sync::Mutex},
};

#[derive(Clone)]
pub struct Writer(Arc<Mutex<State>>);

struct State {
	fd: Option<Fd>,
	input: Option<async_channel::Sender<Input>>,
	position: u64,
	process: Option<Weak<tg::process::Inner>>,
	stream: Stream,
	task: Option<Task<tg::Result<()>>>,
}

struct Input {
	acknowledgment: tokio::sync::oneshot::Sender<()>,
	chunk: tg::process::stdio::Chunk,
}

enum Fd {
	Stdin(tokio::process::ChildStdin),
}

impl Writer {
	fn new(fd: Option<Fd>, stream: Stream) -> Self {
		Self(Arc::new(Mutex::new(State {
			fd,
			input: None,
			position: 0,
			process: None,
			stream,
			task: None,
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
		if matches!(fd, Some(Fd::Stdin(_))) {
			drop(fd);

			return Ok(());
		}
		drop(fd);
		if state.input.is_none() && state.process.is_some() {
			Self::start_with_handle(&mut state, handle).await?;
		}
		state.input.take();
		let Some(task) = state.task.take() else {
			state.process.take();

			return Ok(());
		};
		drop(state);
		let result = task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the stdin task panicked"))?;
		let mut state = self.0.lock().await;
		state.process.take();
		result
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
				.map_err(|error| tg::error!(!error, "failed to write stdin"))?;

			return Ok(input.len());
		}
		if state.input.is_none() {
			Self::start_with_handle(&mut state, handle).await?;
		}
		let input_length = input.len();
		let length = input_length.to_u64().unwrap();
		let chunk = tg::process::stdio::Chunk {
			bytes: bytes::Bytes::copy_from_slice(input),
			combined_position: state.position,
			stream: state.stream,
			stream_position: state.position,
			timestamp: None,
		};
		let (acknowledgment, receiver) = tokio::sync::oneshot::channel();
		let input = Input {
			acknowledgment,
			chunk,
		};
		if state.input.as_ref().unwrap().send(input).await.is_err() {
			return Self::wait_for_task_error(&mut state).await;
		}
		if receiver.await.is_err() {
			return Self::wait_for_task_error(&mut state).await;
		}
		state.position = state
			.position
			.checked_add(length)
			.ok_or_else(|| tg::error!("the stdin position is too large"))?;

		Ok(input_length)
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

	async fn start_with_handle<H>(state: &mut State, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let (location, process, tokens) =
			ensure_process_with_handle(state.process.clone(), handle).await?;
		let arg = tg::process::stdio::write::Arg {
			location,
			streams: vec![state.stream],
			tokens,
		};
		let (sender, receiver) = async_channel::bounded::<Input>(1);
		let input = stream::unfold(
			(receiver, None),
			|(receiver, acknowledgment): (_, Option<tokio::sync::oneshot::Sender<()>>)| async move {
				if let Some(acknowledgment) = acknowledgment {
					acknowledgment.send(()).ok();
				}
				let input = receiver.recv().await.ok()?;
				let chunk = input.chunk;
				let acknowledgment = Some(input.acknowledgment);

				Some((Ok(chunk), (receiver, acknowledgment)))
			},
		)
		.boxed();
		let handle = handle.clone();
		let task = Task::spawn(move |_| async move {
			handle.write_process_stdio_all(&process, arg, input).await
		});
		state.input = Some(sender);
		state.task = Some(task);

		Ok(())
	}

	async fn wait_for_task_error(state: &mut State) -> tg::Result<usize> {
		state.input.take();
		let task = state
			.task
			.take()
			.ok_or_else(|| tg::error!("the stdin task ended unexpectedly"))?;
		let result = task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the stdin task panicked"))?;
		match result {
			Ok(()) => Err(tg::error!("the stdin task ended unexpectedly")),
			Err(error) => Err(error),
		}
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
) -> tg::Result<(
	Option<tg::location::Arg>,
	tg::process::Id,
	tg::authorization::Tokens,
)>
where
	H: tg::Handle,
{
	let process = process
		.and_then(|process| process.upgrade())
		.ok_or_else(|| tg::error!("the process is not available"))?;
	let handle_process = crate::process::Process::<tg::Value>(process.clone(), PhantomData);
	handle_process.ensure_location_with_handle(handle).await?;
	let location = process.location.read().unwrap().clone();
	let tokens = process.tokens.read().unwrap().clone();
	let id = process
		.id
		.as_ref()
		.right()
		.cloned()
		.ok_or_else(|| tg::error!("the process is not available"))?;

	Ok((location, id, tokens))
}
