use {
	super::ProcessControlSender,
	crate::session::Session,
	bytes::Bytes,
	futures::{
		StreamExt as _, TryFutureExt as _, future,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	std::{
		collections::{BTreeMap, BTreeSet, VecDeque},
		sync::Arc,
		task::Poll,
	},
	tangram_client::{
		prelude::*,
		process::stdio::{
			flow,
			read::{Event, Output, ServerMessage},
		},
	},
	tangram_futures::task::Task,
};

#[cfg(test)]
mod tests;

const BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

pub(super) struct RunProcessControlOutputTaskArg {
	pub(super) receiver: tokio::sync::mpsc::Receiver<Message>,
	pub(super) sandbox: tangram_sandbox::Sandbox,
	pub(super) sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub(super) sender: ProcessControlSender,
	pub(super) stderr: tg::process::Stdio,
	pub(super) stderr_buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
	pub(super) stderr_progress: Option<BoxStream<'static, tg::Result<Bytes>>>,
	pub(super) stdout: tg::process::Stdio,
	pub(super) stdout_buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

pub(super) enum Message {
	Close(String),
	Progress(tg::process::control::ReadServerNotification),
	Read {
		arg: tg::process::stdio::read::Arg,
		id: String,
	},
	Reconnect,
}

struct Read {
	arg: tg::process::stdio::read::Arg,
	deadline: Option<tokio::time::Instant>,
	position: u64,
	window: tg::process::stdio::flow::Sender,
}

struct Reader {
	buffered: BTreeMap<tg::process::stdio::Stream, tokio::sync::oneshot::Sender<tg::Result<()>>>,
	chunks: VecDeque<tg::process::stdio::Chunk>,
	combined_position: u64,
	eof: BTreeSet<tg::process::stdio::Stream>,
	error: Option<tg::Error>,
	input_ended: bool,
	inputs: Vec<Input>,
	next_input: usize,
	progress_stream: tg::process::stdio::Stream,
	sources: BTreeMap<tg::process::stdio::Stream, usize>,
	stderr_position: u64,
	stdout_position: u64,
	streams: BTreeSet<tg::process::stdio::Stream>,
}

struct Input {
	buffered_chunks: usize,
	buffered_length: usize,
	ended: bool,
	inner: BoxStream<'static, InputEvent>,
	stream: tg::process::stdio::Stream,
}

enum InputEvent {
	Progress(Option<tg::Result<Bytes>>),
	Sandbox {
		event: Option<tg::Result<tangram_sandbox::stdio::read::Event>>,
		stream: tg::process::stdio::Stream,
	},
}

impl Session {
	pub(super) fn spawn_process_control_output_task(
		&self,
		arg: RunProcessControlOutputTaskArg,
	) -> Task<tg::Result<()>> {
		let session = self.clone();
		Task::spawn(move |_| {
			async move { session.run_process_control_output_task(arg).await }.inspect_err(
				|error| tracing::error!(error = %error.trace(), "the process control output task failed"),
			)
		})
	}

	async fn run_process_control_output_task(
		&self,
		arg: RunProcessControlOutputTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlOutputTaskArg {
			receiver,
			sandbox,
			mut sandbox_process,
			sender,
			stderr,
			stderr_buffered,
			stderr_progress,
			stdout,
			stdout_buffered,
		} = arg;
		let sandbox_process = sandbox_process
			.wait_for(Option::is_some)
			.await
			.ok()
			.and_then(|sandbox_process| sandbox_process.as_ref().cloned());
		let shared_tty =
			matches!(stderr, tg::process::Stdio::Tty) && matches!(stdout, tg::process::Stdio::Tty);
		let mut buffered = BTreeMap::new();
		let mut eof = BTreeSet::new();
		let mut inputs = BTreeMap::<_, Vec<BoxStream<'static, InputEvent>>>::new();
		let mut sources = BTreeMap::new();
		let mut streams = BTreeSet::new();
		let stdio = [
			(tg::process::stdio::Stream::Stderr, stderr, stderr_buffered),
			(tg::process::stdio::Stream::Stdout, stdout, stdout_buffered),
		];
		for (stream_name, stdio, buffered_sender) in stdio {
			if shared_tty && stream_name == tg::process::stdio::Stream::Stderr
				|| !matches!(stdio, tg::process::Stdio::Pipe | tg::process::Stdio::Tty)
			{
				buffered_sender.send(Ok(())).ok();
				eof.insert(stream_name);
				continue;
			}
			streams.insert(stream_name);
			crate::checkpoint!(
				self.server,
				"runner.process.control.reader.create",
				stream = %stream_name,
			)
			.await;
			let Some(sandbox_process) = &sandbox_process else {
				buffered_sender.send(Ok(())).ok();
				eof.insert(stream_name);
				continue;
			};
			let input = sandbox
				.read_stdio(sandbox_process, vec![stream_name])
				.await
				.map_err(|error| tg::error!(!error, "failed to create the stdio stream"))?;
			let input = input
				.map(move |event| InputEvent::Sandbox {
					event: Some(event),
					stream: stream_name,
				})
				.chain(stream::once(future::ready(InputEvent::Sandbox {
					event: None,
					stream: stream_name,
				})))
				.boxed();
			buffered.insert(stream_name, buffered_sender);
			eof.remove(&stream_name);
			*sources.entry(stream_name).or_default() += 1;
			inputs.entry(stream_name).or_default().push(input);
		}
		// When stdout and stderr share a tty, the pty merges the streams and reads request only stdout, so tag progress as stdout.
		let progress_stream = if shared_tty {
			tg::process::stdio::Stream::Stdout
		} else {
			tg::process::stdio::Stream::Stderr
		};
		if let Some(progress) = stderr_progress {
			let input = progress
				.map(|result| InputEvent::Progress(Some(result)))
				.chain(stream::once(future::ready(InputEvent::Progress(None))))
				.boxed();
			eof.remove(&progress_stream);
			*sources.entry(progress_stream).or_default() += 1;
			inputs.entry(progress_stream).or_default().push(input);
		}
		let inputs = inputs
			.into_iter()
			.map(|(stream, inputs)| Input {
				buffered_chunks: 0,
				buffered_length: 0,
				ended: false,
				inner: stream::select_all(inputs).boxed(),
				stream,
			})
			.collect();
		let reader = Reader {
			buffered,
			chunks: VecDeque::new(),
			combined_position: 0,
			eof,
			error: None,
			input_ended: false,
			inputs,
			next_input: 0,
			progress_stream,
			sources,
			stderr_position: 0,
			stdout_position: 0,
			streams,
		};
		Self::run_process_control_output_reader_task(reader, receiver, sender).await?;

		Ok(())
	}

	async fn run_process_control_output_reader_task(
		mut reader: Reader,
		mut receiver: tokio::sync::mpsc::Receiver<Message>,
		sender: ProcessControlSender,
	) -> tg::Result<()> {
		let mut reads = BTreeMap::<String, Read>::new();
		let mut drained = BTreeSet::new();
		loop {
			// Give each read a turn without waiting for another read to consume its output.
			let mut ready = false;
			let mut finished = Vec::new();
			for (id, read) in &mut reads {
				let message = reader.read(read);
				let response = match message {
					Ok(Some(tg::process::stdio::read::ServerMessage::Notification(event))) => {
						let notification = tg::process::control::ReadClientNotification {
							event,
							id: id.clone(),
						};
						let message = tg::process::control::ClientMessage::Notification(
							tg::process::control::ClientNotification::Read(notification),
						);
						sender.send_low(message).await?;
						ready = true;
						continue;
					},
					Ok(Some(tg::process::stdio::read::ServerMessage::Response(output))) => {
						Ok(tg::process::control::ClientResponseOutput::Read(output))
					},
					Ok(None) => continue,
					Err(error) => Err(error),
				};
				if matches!(
					&response,
					Ok(tg::process::control::ClientResponseOutput::Read(
						tg::process::stdio::read::Output::End(_)
					))
				) {
					drained.extend(read.arg.streams.iter().copied());
				}
				let response = Self::process_control_response(id.clone(), response);
				sender.send_low(response).await?;
				finished.push(id.clone());
			}
			for id in finished {
				reads.remove(&id);
			}
			// Keep empty streams available until their readers have received EOF too.
			if reader.streams.is_subset(&drained)
				&& reads.is_empty()
				&& reader.sources.is_empty()
				&& reader.chunks.is_empty()
				&& reader.error.is_none()
			{
				break;
			}
			let deadline = reads.values().filter_map(|read| read.deadline).min();
			tokio::select! {
				() = reader.fill(), if !reader.input_ended && reader.error.is_none() => {},
				message = receiver.recv() => {
					let Some(message) = message else { break; };
					Self::handle_process_control_output_message(&mut reads, message, &sender).await?;
				},
				() = async { tokio::time::sleep_until(deadline.unwrap()).await }, if deadline.is_some() => {},
				() = tokio::task::yield_now(), if ready => {},
			}
		}
		Ok(())
	}

	async fn handle_process_control_output_message(
		reads: &mut BTreeMap<String, Read>,
		message: Message,
		sender: &ProcessControlSender,
	) -> tg::Result<()> {
		match message {
			Message::Close(id) => {
				if reads.remove(&id).is_some() {
					let error = tg::error!("the process read was canceled");
					let response = Self::process_control_response(id, Err(error));
					sender.send_low(response).await?;
				}
			},
			Message::Progress(notification) => {
				if let Some(read) = reads.get_mut(&notification.id)
					&& let Err(error) = read.window.update(notification.progress)
				{
					reads.remove(&notification.id);
					let response = Self::process_control_response(notification.id, Err(error));
					sender.send_low(response).await?;
				}
			},
			Message::Read { arg, id } => {
				let result = if reads.len() >= 64 {
					Err(tg::error!("too many process reads"))
				} else {
					Read::new(arg)
				};
				match result {
					Ok(read) => {
						reads.insert(id, read);
					},
					Err(error) => {
						let response = Self::process_control_response(id, Err(error));
						sender.send_low(response).await?;
					},
				}
			},
			Message::Reconnect => {
				// The previous transport may have lost chunks or consumption progress, so its reads cannot safely continue.
				for (id, _) in std::mem::take(reads) {
					let error = tg::error!("the process control connection was interrupted");
					let response = Self::process_control_response(id, Err(error));
					sender.send_low(response).await?;
				}
			},
		}
		Ok(())
	}
}

impl Read {
	fn new(mut arg: tg::process::stdio::read::Arg) -> tg::Result<Self> {
		arg.streams.sort();
		arg.streams.dedup();
		if arg.streams.is_empty()
			|| arg.streams.len() > 2
			|| arg.streams.contains(&tg::process::stdio::Stream::Stdin)
		{
			return Err(tg::error!("invalid process stdio streams"));
		}
		if arg.length.is_some_and(|length| length < 0) || arg.size == Some(0) {
			return Err(tg::error!("invalid piped stdio read length"));
		}
		let position = match arg.position {
			None => 0,
			Some(std::io::SeekFrom::Start(position)) => position,
			_ => return Err(tg::error!("piped stdio only supports an absolute position")),
		};
		let deadline = arg
			.timeout
			.and_then(|timeout| tokio::time::Instant::now().checked_add(timeout));
		let window = tg::process::stdio::flow::Sender::default();
		Ok(Self {
			arg,
			deadline,
			position,
			window,
		})
	}
}

impl Reader {
	fn read(
		&mut self,
		read: &mut Read,
	) -> tg::Result<Option<tg::process::stdio::read::ServerMessage>> {
		if read.arg.length == Some(0) {
			return Ok(Some(ServerMessage::Response(Output::Limit {
				position: read.position,
			})));
		}

		while let Some(index) = self
			.chunks
			.iter()
			.position(|chunk| read.arg.streams.contains(&chunk.stream))
		{
			let chunk = &self.chunks[index];
			let start = if read.arg.streams.len() > 1 {
				chunk.combined_position
			} else {
				chunk.stream_position
			};
			let end = start
				.checked_add(chunk.bytes.len() as u64)
				.ok_or_else(|| tg::error!("the stdio position is too large"))?;
			if end <= read.position {
				let chunk = self.chunks.remove(index).unwrap();
				let input = self
					.inputs
					.iter_mut()
					.find(|input| input.stream == chunk.stream)
					.unwrap();
				input.buffered_chunks -= 1;
				input.buffered_length -= chunk.bytes.len();
				continue;
			}
			if start > read.position {
				return Err(
					tg::error!(expected = %read.position, actual = %start, "encountered a gap in the process stdio stream"),
				);
			}
			let offset = (read.position - start).to_usize().unwrap();
			let length = (chunk.bytes.len() - offset)
				.min(flow::CHUNK_SIZE)
				.min(
					read.arg
						.size
						.unwrap_or(u64::MAX)
						.try_into()
						.unwrap_or(usize::MAX),
				)
				.min(
					read.arg
						.length
						.map_or(usize::MAX, |length| length.try_into().unwrap_or(usize::MAX)),
				);
			if !read.window.available(length) {
				if read
					.deadline
					.is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
				{
					return Ok(Some(ServerMessage::Response(Output::Timeout {
						position: read.position,
					})));
				}
				return Ok(None);
			}
			read.window.send(length)?;
			let mut chunk = chunk.clone();
			chunk.bytes = chunk.bytes.slice(offset..offset + length);
			chunk.combined_position += offset as u64;
			chunk.stream_position += offset as u64;
			read.position += length as u64;
			if let Some(remaining) = &mut read.arg.length {
				*remaining -= i64::try_from(length).unwrap();
			}
			return Ok(Some(ServerMessage::Notification(Event::Chunk(chunk))));
		}
		if let Some(error) = &self.error {
			return Err(error.clone());
		}
		if read
			.arg
			.streams
			.iter()
			.all(|stream| self.eof.contains(stream))
		{
			let position = if read.arg.streams.len() > 1 {
				self.combined_position
			} else {
				match read.arg.streams[0] {
					tg::process::stdio::Stream::Stderr => self.stderr_position,
					tg::process::stdio::Stream::Stdin => unreachable!(),
					tg::process::stdio::Stream::Stdout => self.stdout_position,
				}
			};
			if read.position != position {
				return Err(
					tg::error!(expected = %read.position, actual = %position, "encountered a gap in the process stdio stream"),
				);
			}

			let stream_positions = [
				(tg::process::stdio::Stream::Stderr, self.stderr_position),
				(tg::process::stdio::Stream::Stdout, self.stdout_position),
			]
			.into_iter()
			.filter(|(stream, _)| read.arg.streams.contains(stream))
			.collect();
			let end = tg::process::stdio::End {
				combined_position: self.combined_position,
				stream_positions,
			};
			return Ok(Some(ServerMessage::Response(Output::End(end))));
		}
		if read
			.deadline
			.is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
		{
			return Ok(Some(ServerMessage::Response(Output::Timeout {
				position: read.position,
			})));
		}
		Ok(None)
	}

	async fn fill(&mut self) {
		let event = future::poll_fn(|cx| {
			// A full stdout buffer must not prevent polling stderr, or vice versa.
			for offset in 0..self.inputs.len() {
				let index = (self.next_input + offset) % self.inputs.len();
				let input = &mut self.inputs[index];
				if input.ended
					|| input.buffered_length >= BUFFER_CAPACITY
					|| input.buffered_chunks >= BUFFER_CAPACITY / flow::CHUNK_SIZE
				{
					continue;
				}
				match input.inner.poll_next_unpin(cx) {
					Poll::Pending => {},
					Poll::Ready(Some(event)) => {
						self.next_input = (index + 1) % self.inputs.len();
						return Poll::Ready(Some(event));
					},
					Poll::Ready(None) => input.ended = true,
				}
			}
			if self.inputs.iter().all(|input| input.ended) {
				Poll::Ready(None)
			} else {
				Poll::Pending
			}
		})
		.await;
		let Some(event) = event else {
			self.input_ended = true;
			if !self.sources.is_empty() {
				self.fail(tg::error!("the sandbox stdio stream ended unexpectedly"));
			}

			return;
		};
		match event {
			InputEvent::Sandbox {
				event: None,
				stream,
			} if self.buffered.contains_key(&stream) => {
				self.fail(tg::error!("the sandbox stdio stream ended unexpectedly"));
			},
			InputEvent::Progress(None) => {
				self.end_source(self.progress_stream);
			},
			InputEvent::Sandbox { event: None, .. } => (),
			InputEvent::Progress(Some(Err(error)))
			| InputEvent::Sandbox {
				event: Some(Err(error)),
				..
			} => self.fail(error),
			InputEvent::Progress(Some(Ok(bytes))) => {
				self.push(bytes, self.progress_stream);
			},
			InputEvent::Sandbox {
				event: Some(Ok(tangram_sandbox::stdio::read::Event::Chunk(chunk))),
				..
			} => {
				self.push(chunk.bytes, chunk.stream);
			},
			InputEvent::Sandbox {
				event: Some(Ok(tangram_sandbox::stdio::read::Event::End)),
				stream,
			} => {
				if let Some(buffered) = self.buffered.remove(&stream) {
					buffered.send(Ok(())).ok();
				}
				self.end_source(stream);
			},
		}
	}

	fn end_source(&mut self, stream: tg::process::stdio::Stream) {
		let Some(count) = self.sources.get_mut(&stream) else {
			return;
		};
		*count -= 1;
		if *count == 0 {
			self.sources.remove(&stream);
			self.eof.insert(stream);
		}
	}

	fn push(&mut self, bytes: Bytes, stream: tg::process::stdio::Stream) {
		if bytes.is_empty() {
			return;
		}
		let length = bytes.len().to_u64().unwrap();
		let stream_position = match stream {
			tg::process::stdio::Stream::Stderr => self.stderr_position,
			tg::process::stdio::Stream::Stdin => return,
			tg::process::stdio::Stream::Stdout => self.stdout_position,
		};
		let chunk = tg::process::stdio::Chunk {
			bytes,
			combined_position: self.combined_position,
			stream,
			stream_position,
			timestamp: None,
		};
		let input = self
			.inputs
			.iter_mut()
			.find(|input| input.stream == chunk.stream)
			.unwrap();
		input.buffered_chunks += 1;
		input.buffered_length += chunk.bytes.len();
		self.chunks.push_back(chunk);
		self.combined_position += length;
		match stream {
			tg::process::stdio::Stream::Stderr => self.stderr_position += length,
			tg::process::stdio::Stream::Stdin => unreachable!(),
			tg::process::stdio::Stream::Stdout => self.stdout_position += length,
		}
	}

	fn fail(&mut self, error: tg::Error) {
		for (_, buffered) in std::mem::take(&mut self.buffered) {
			buffered.send(Err(error.clone())).ok();
		}
		self.error = Some(error);
	}
}
