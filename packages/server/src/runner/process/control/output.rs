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
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

const BUFFER_CAPACITY: usize = 16 * 1024 * 1024;

pub(super) struct RunProcessControlOutputTaskArg {
	pub(super) sandbox: tangram_sandbox::Sandbox,
	pub(super) sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub(super) sender: ProcessControlSender,
	pub(super) stderr: tg::process::Stdio,
	pub(super) stderr_buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
	pub(super) stderr_progress: Option<BoxStream<'static, tg::Result<Bytes>>>,
	pub(super) stdout: tg::process::Stdio,
	pub(super) stdout_buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
	pub(super) receiver:
		tokio::sync::mpsc::Receiver<(String, tg::process::control::ReadServerRequestArg)>,
}

struct Reader {
	buffered: BTreeMap<tg::process::stdio::Stream, tokio::sync::oneshot::Sender<tg::Result<()>>>,
	buffered_length: usize,
	chunks: VecDeque<tg::process::stdio::Chunk>,
	combined_position: u64,
	eof: BTreeSet<tg::process::stdio::Stream>,
	error: Option<tg::Error>,
	input: BoxStream<'static, InputEvent>,
	input_ended: bool,
	progress_stream: tg::process::stdio::Stream,
	sources: BTreeMap<tg::process::stdio::Stream, usize>,
	stderr_position: u64,
	stdout_position: u64,
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
		let mut inputs = Vec::new();
		let mut sources = BTreeMap::new();
		let streams = [
			(tg::process::stdio::Stream::Stderr, stderr, stderr_buffered),
			(tg::process::stdio::Stream::Stdout, stdout, stdout_buffered),
		];
		for (stream_name, stdio, buffered_sender) in streams {
			if shared_tty && stream_name == tg::process::stdio::Stream::Stderr
				|| !matches!(stdio, tg::process::Stdio::Pipe | tg::process::Stdio::Tty)
			{
				buffered_sender.send(Ok(())).ok();
				eof.insert(stream_name);
				continue;
			}
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
			inputs.push(input);
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
			inputs.push(input);
		}
		let input = stream::select_all(inputs).boxed();
		let reader = Reader {
			buffered,
			buffered_length: 0,
			chunks: VecDeque::new(),
			combined_position: 0,
			eof,
			error: None,
			input,
			input_ended: false,
			progress_stream,
			sources,
			stderr_position: 0,
			stdout_position: 0,
		};
		Self::run_process_control_output_reader_task(reader, receiver, sender).await
	}

	async fn run_process_control_output_reader_task(
		mut reader: Reader,
		mut receiver: tokio::sync::mpsc::Receiver<(
			String,
			tg::process::control::ReadServerRequestArg,
		)>,
		sender: ProcessControlSender,
	) -> tg::Result<()> {
		loop {
			tokio::select! {
				() = reader.fill(), if !reader.input_ended && reader.error.is_none() && reader.buffered_length < BUFFER_CAPACITY => {},
				request = receiver.recv() => {
					let Some((id, request)) = request else {
						break;
					};
					let response = reader.read(request).await;
					let eof = response.as_ref().is_ok_and(|response| response.chunk.is_none())
						&& reader.error.is_none()
						&& reader.sources.is_empty()
						&& reader.chunks.is_empty();
					let response = response.map(tg::process::control::ClientResponseOutput::Read);
					let response = Self::process_control_response(id, response);
					sender.send(response).await?;
					if eof {
						break;
					}
				},
			}
		}

		Ok(())
	}
}

impl Reader {
	async fn read(
		&mut self,
		request: tg::process::control::ReadServerRequestArg,
	) -> tg::Result<tg::process::control::ReadClientResponseOutput> {
		let streams = request.streams.into_iter().collect::<BTreeSet<_>>();
		if streams.is_empty()
			|| streams.len() > 2
			|| streams.contains(&tg::process::stdio::Stream::Stdin)
		{
			return Err(tg::error!("invalid process stdio streams"));
		}
		if request.length == 0 {
			return Err(tg::error!("expected a nonzero stdio read length"));
		}
		loop {
			while let Some(index) = self
				.chunks
				.iter()
				.position(|chunk| streams.contains(&chunk.stream))
			{
				let chunk = &self.chunks[index];
				let start = if streams.len() > 1 {
					chunk.combined_position
				} else {
					chunk.stream_position
				};
				let end = start
					.checked_add(chunk.bytes.len().to_u64().unwrap())
					.ok_or_else(|| tg::error!("the stdio position is too large"))?;
				if end <= request.position {
					let chunk = self.chunks.remove(index).unwrap();
					self.buffered_length -= chunk.bytes.len();
					continue;
				}
				if start > request.position {
					return Err(tg::error!(
						expected = %request.position,
						actual = %start,
						"encountered a gap in the process stdio stream"
					));
				}
				let mut chunk = chunk.clone();
				let offset = (request.position - start).to_usize().unwrap();
				let length = request.length.min(chunk.bytes.len() - offset);
				chunk.bytes = chunk.bytes.slice(offset..offset + length);
				chunk.combined_position += offset.to_u64().unwrap();
				chunk.stream_position += offset.to_u64().unwrap();
				let output = tg::process::control::ReadClientResponseOutput { chunk: Some(chunk) };

				return Ok(output);
			}
			if let Some(error) = self.error.take() {
				return Err(error);
			}
			if streams.iter().all(|stream| self.eof.contains(stream)) {
				return Ok(tg::process::control::ReadClientResponseOutput { chunk: None });
			}
			self.fill().await;
		}
	}

	async fn fill(&mut self) {
		let Some(event) = self.input.next().await else {
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
		self.buffered_length += chunk.bytes.len();
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
