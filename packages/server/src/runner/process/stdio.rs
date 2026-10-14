use {
	crate::session::Session,
	bytes::Bytes,
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _, future,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	std::{
		collections::{BTreeMap, BTreeSet, VecDeque},
		sync::Arc,
		task::Poll,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tg::process::stdio::read::{Event, Output, ServerMessage},
	tracing::Instrument as _,
};

#[cfg(test)]
mod tests;

pub(super) type Sender = tokio::sync::mpsc::Sender<tg::Result<ServerMessage>>;

pub(super) struct RunProcessStdioTaskArg {
	pub(super) receiver: tokio::sync::mpsc::Receiver<Message>,
	pub(super) sandbox: tangram_sandbox::Sandbox,
	pub(super) sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub(super) stderr: tg::process::Stdio,
	pub(super) stderr_buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
	pub(super) stderr_progress: Option<BoxStream<'static, tg::Result<Bytes>>>,
	pub(super) stdout: tg::process::Stdio,
	pub(super) stdout_buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

pub(super) enum Message {
	/// Release the kept chunks through the position, in the coordinates of a read of the streams.
	Ack {
		position: u64,
		streams: Vec<tg::process::stdio::Stream>,
	},
	Read {
		arg: tg::process::stdio::read::Arg,
		sender: Sender,
	},
}

struct Read {
	arg: tg::process::stdio::read::Arg,
	deadline: Option<tokio::time::Instant>,
	position: u64,
	sender: Sender,
}

struct Pipe {
	buffer_chunks: usize,
	buffer_size: usize,
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
	pub(super) fn spawn_process_stdio_task(
		&self,
		arg: RunProcessStdioTaskArg,
	) -> Task<tg::Result<()>> {
		let session = self.clone();
		Task::spawn(move |_| {
			async move { session.run_process_stdio_task(arg).boxed().await }
				.inspect_err(
					|error| tracing::error!(error = %error.trace(), "the process stdio task failed"),
				)
				.in_current_span()
		})
	}

	async fn run_process_stdio_task(&self, arg: RunProcessStdioTaskArg) -> tg::Result<()> {
		let RunProcessStdioTaskArg {
			receiver,
			sandbox,
			mut sandbox_process,
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
		let pipe = Pipe {
			buffer_chunks: self.server.config.runner.stdio_buffer_chunks,
			buffer_size: self.server.config.runner.stdio_buffer_size,
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
		let drained_ttl = self.server.config.runner.stdio_drained_ttl;
		Self::run_process_stdio_pipe_task(pipe, receiver, drained_ttl)
			.boxed()
			.await?;

		Ok(())
	}

	async fn run_process_stdio_pipe_task(
		mut pipe: Pipe,
		mut receiver: tokio::sync::mpsc::Receiver<Message>,
		drained_ttl: std::time::Duration,
	) -> tg::Result<()> {
		let mut reads = Vec::<Read>::new();
		let mut drained = BTreeSet::new();
		let mut drained_deadline = None;
		let mut receiving = true;
		loop {
			// Send to each read whose sink has room, and collect the deadlines of the reads that are not waiting for room.
			let mut deadline = None::<tokio::time::Instant>;
			let mut ready = false;
			let mut waiting = Vec::new();
			let mut index = 0;
			while index < reads.len() {
				let sender = reads[index].sender.clone();
				let permit = match sender.clone().try_reserve_owned() {
					Ok(permit) => permit,
					Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
						waiting.push(sender);
						index += 1;
						continue;
					},
					Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
						reads.remove(index);
						continue;
					},
				};
				let read = &mut reads[index];
				let finished = match pipe.read(read) {
					Ok(Some(message @ ServerMessage::Notification(_))) => {
						permit.send(Ok(message));
						ready = true;
						false
					},
					Ok(Some(message @ ServerMessage::Response(_))) => {
						if matches!(message, ServerMessage::Response(Output::End(_))) {
							drained.extend(read.arg.streams.iter().copied());
						}
						permit.send(Ok(message));
						true
					},
					Ok(None) => false,
					Err(error) => {
						permit.send(Err(error));
						true
					},
				};
				if finished {
					reads.remove(index);
				} else {
					if let Some(read_deadline) = reads[index].deadline {
						deadline = Some(
							deadline.map_or(read_deadline, |deadline| deadline.min(read_deadline)),
						);
					}
					index += 1;
				}
			}

			// Exit once no more reads are accepted and the queued reads are answered.
			if !receiving && reads.is_empty() {
				break;
			}

			// Keep the streams for a while after their readers have received EOF, so that a reader that reconnects can resume from its unacknowledged chunks or receive EOF again.
			let finished = pipe.streams.is_subset(&drained)
				&& reads.is_empty()
				&& pipe.sources.is_empty()
				&& pipe.error.is_none();
			if !finished {
				drained_deadline = None;
			} else if drained_deadline.is_none() {
				drained_deadline = Some(tokio::time::Instant::now() + drained_ttl);
			}

			// Wait for input, a new read, a deadline, or room in a sink.
			let deadline = deadline.into_iter().chain(drained_deadline).min();
			let room = async {
				let futures = waiting
					.iter()
					.map(|sender| sender.reserve().map(drop).boxed());
				futures::future::select_all(futures).await;
			};
			tokio::select! {
				() = pipe.fill(), if !pipe.input_ended && pipe.error.is_none() => {},
				message = receiver.recv(), if receiving => {
					let Some(message) = message else {
						receiving = false;
						continue;
					};
					Self::handle_process_stdio_message(&mut pipe, &mut reads, message);
				},
				() = async { tokio::time::sleep_until(deadline.unwrap()).await }, if deadline.is_some() => {
					// Stop accepting reads once the streams have been kept long enough, and answer the reads that are already queued before exiting.
					if drained_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
						receiver.close();
						drained_deadline = None;
					}
				},
				() = room, if !waiting.is_empty() => {},
				() = tokio::task::yield_now(), if ready => {},
				else => break,
			}
		}
		Ok(())
	}

	fn handle_process_stdio_message(pipe: &mut Pipe, reads: &mut Vec<Read>, message: Message) {
		match message {
			Message::Ack { position, streams } => pipe.acknowledge(position, &streams),
			Message::Read { arg, sender } => {
				let read = match Read::new(arg, sender) {
					Ok(read) => read,
					Err((error, sender)) => {
						sender.try_send(Err(error)).ok();
						return;
					},
				};

				// Acknowledge the bytes before the read's position, which its reader already has.
				pipe.acknowledge(read.position, &read.arg.streams);

				// A pipe or tty stream has one reader, so a new read supersedes any read that shares a stream.
				reads.retain(|existing| {
					let superseded = existing
						.arg
						.streams
						.iter()
						.any(|stream| read.arg.streams.contains(stream));
					if superseded {
						// Deliver the cancellation even when the read's channel is full, so the reader does not reconnect and take the stream back.
						let error = tg::error!(
							code = tg::error::Code::Cancellation,
							"the process read was superseded"
						);
						if let Err(tokio::sync::mpsc::error::TrySendError::Full(message)) =
							existing.sender.try_send(Err(error))
						{
							let sender = existing.sender.clone();
							tokio::spawn(async move {
								sender.send(message).await.ok();
							});
						}
					}
					!superseded
				});
				reads.push(read);
			},
		}
	}
}

impl Read {
	fn new(
		arg: tg::process::stdio::read::Arg,
		sender: Sender,
	) -> Result<Self, (tg::Error, Sender)> {
		match Self::validate(arg) {
			Ok((arg, deadline, position)) => Ok(Self {
				arg,
				deadline,
				position,
				sender,
			}),
			Err(error) => Err((error, sender)),
		}
	}

	fn validate(
		mut arg: tg::process::stdio::read::Arg,
	) -> tg::Result<(
		tg::process::stdio::read::Arg,
		Option<tokio::time::Instant>,
		u64,
	)> {
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
		Ok((arg, deadline, position))
	}
}

impl Pipe {
	fn read(
		&mut self,
		read: &mut Read,
	) -> tg::Result<Option<tg::process::stdio::read::ServerMessage>> {
		if read.arg.length == Some(0) {
			return Ok(Some(ServerMessage::Response(Output::Limit {
				position: read.position,
			})));
		}

		// Find the next kept chunk of the read's streams, including chunks sent to a previous read that were not acknowledged.
		let mut skipped = 0;
		while let Some(index) = self
			.chunks
			.iter()
			.skip(skipped)
			.position(|chunk| read.arg.streams.contains(&chunk.stream))
			.map(|index| index + skipped)
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
				skipped = index + 1;
				continue;
			}
			if start > read.position {
				return Err(
					tg::error!(expected = %read.position, actual = %start, "encountered a gap in the process stdio stream"),
				);
			}
			let offset = (read.position - start).to_usize().unwrap();
			let length = (chunk.bytes.len() - offset)
				.min(tg::process::stdio::MAX_CHUNK_SIZE)
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
			// Poll the inputs in turn, skipping an input whose buffer is full, so that one stream cannot block the other.
			for offset in 0..self.inputs.len() {
				let index = (self.next_input + offset) % self.inputs.len();
				let input = &mut self.inputs[index];
				if input.ended
					|| input.buffered_length >= self.buffer_size
					|| input.buffered_chunks >= self.buffer_chunks
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

	fn acknowledge(&mut self, position: u64, streams: &[tg::process::stdio::Stream]) {
		let combined = streams.len() > 1;
		let inputs = &mut self.inputs;
		self.chunks.retain(|chunk| {
			let start = if combined {
				chunk.combined_position
			} else {
				chunk.stream_position
			};
			let acknowledged =
				streams.contains(&chunk.stream) && start + chunk.bytes.len() as u64 <= position;
			if acknowledged {
				let input = inputs
					.iter_mut()
					.find(|input| input.stream == chunk.stream)
					.unwrap();
				input.buffered_chunks -= 1;
				input.buffered_length -= chunk.bytes.len();
			}
			!acknowledged
		});
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
