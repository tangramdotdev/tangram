use crate::{ProcessPermit, Server, runtime};
use crossterm::style::Stylize as _;
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future,
};
use indexmap::IndexMap;
use num::ToPrimitive;
use std::{fmt::Write as _, pin::pin, sync::Arc, time::Duration};
use tangram_client::{self as tg, prelude::*};
use tangram_either::Either;
use tangram_futures::{stream::Ext as _, task::Task};
use tokio_util::task::AbortOnDropHandle;
use unicode_width::UnicodeWidthChar as _;

struct ProgressPtyState {
	server: crate::Server,
	pty: tg::pty::Id,
	indicators: IndexMap<String, tg::progress::Indicator>,
	lines: Option<u16>,
	send: async_channel::Sender<tg::Result<tg::pty::Event>>,
}

impl Server {
	pub(crate) async fn runner_task(&self) {
		loop {
			// Wait for a permit.
			let permit = self
				.process_semaphore
				.clone()
				.acquire_owned()
				.await
				.unwrap();
			let permit = ProcessPermit(Either::Left(permit));

			// Try to dequeue a process locally or from one of the remotes.
			let arg = tg::process::dequeue::Arg::default();
			let futures = std::iter::once(
				self.dequeue_process(arg)
					.map_ok(|output| tg::Process::new(output.process, None, None, None, None))
					.boxed(),
			)
			.chain(self.config.runner.iter().flat_map(|config| {
				config.remotes.iter().map(|name| {
					let server = self.clone();
					let remote = name.to_owned();
					async move {
						let client = server.get_remote_client(remote).await?;
						let arg = tg::process::dequeue::Arg::default();
						let output = client.dequeue_process(arg).await?;
						let process =
							tg::Process::new(output.process, Some(name.clone()), None, None, None);
						Ok::<_, tg::Error>(process)
					}
					.boxed()
				})
			}));
			let process = match future::select_ok(futures).await {
				Ok((process, _)) => process,
				Err(error) => {
					tracing::error!(?error, "failed to dequeue a process");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			// Attempt to start the process.
			let arg = tg::process::start::Arg {
				remote: process.remote().cloned(),
			};
			let result = self.start_process(process.id(), arg.clone()).await;
			if let Err(error) = result {
				tracing::trace!(?error, "failed to start the process");
				continue;
			}

			// Spawn the process task.
			self.spawn_process_task(&process, permit);
		}
	}

	pub(crate) fn spawn_process_task(&self, process: &tg::Process, permit: ProcessPermit) {
		// Spawn the process task.
		self.process_task_map.spawn(
			process.id().clone(),
			Task::spawn(|_| {
				let server = self.clone();
				let process = process.clone();
				async move { server.process_task(&process, permit).await }
					.inspect_err(|error| {
						tracing::error!(?error, "the process task failed");
					})
					.map(|_| ())
			}),
		);

		// Spawn the heartbeat task.
		tokio::spawn({
			let server = self.clone();
			let process = process.clone();
			async move { server.heartbeat_task(&process).await }
				.inspect_err(|error| {
					tracing::error!(?error, "the heartbeat task failed");
				})
				.map(|_| ())
		});
	}

	async fn process_task(&self, process: &tg::Process, permit: ProcessPermit) -> tg::Result<()> {
		// Set the process's permit.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.process_permits.insert(process.id().clone(), permit);
		scopeguard::defer! {
			self.process_permits.remove(process.id());
		}

		// Run.
		let wait = self.process_task_inner(process).await?;

		// Store the output.
		let output = if let Some(output) = &wait.output {
			output.store(self).await?;
			let data = output.to_data();
			Some(data)
		} else {
			None
		};

		// If the process is remote, then push the output.
		if let Some(remote) = process.remote() {
			if let Some(output) = &output {
				let objects = output.children();
				let arg = tg::push::Arg {
					items: objects.into_iter().map(Either::Right).collect(),
					remote: Some(remote.to_owned()),
					..Default::default()
				};
				let stream = self.push(arg).await?;
				self.log_progress_stream(process, stream).await?;
			}
		}

		// Finish the process.
		let arg = tg::process::finish::Arg {
			checksum: wait.checksum,
			error: wait.error.as_ref().map(tg::Error::to_data),
			exit: wait.exit,
			force: false,
			output,
			remote: process.remote().cloned(),
		};
		self.finish_process(process.id(), arg).await?;

		Ok::<_, tg::Error>(())
	}

	async fn process_task_inner(&self, process: &tg::Process) -> tg::Result<runtime::Output> {
		// Get the host.
		let command = process.command(self).await?;
		let host = command.host(self).await?;

		// Get the runtime.
		let runtime = self
			.runtimes
			.read()
			.unwrap()
			.get(&*host)
			.ok_or_else(
				|| tg::error!(?id = process, ?host = &*host, "failed to find a runtime for the process"),
			)?
			.clone();

		// Run the process.
		let output = runtime.run(process).await;

		Ok(output)
	}

	async fn heartbeat_task(&self, process: &tg::Process) -> tg::Result<()> {
		let config = self.config.runner.clone().unwrap_or_default();
		loop {
			let arg = tg::process::heartbeat::Arg {
				remote: process.remote().cloned(),
			};
			let result = self.heartbeat_process(process.id(), arg).await;
			if let Ok(output) = result {
				if output.status.is_finished() {
					self.process_task_map.abort(process.id());
					break;
				}
			}
			tokio::time::sleep(config.heartbeat_interval).await;
		}
		Ok(())
	}

	pub(crate) async fn log_progress_stream<T: Send + std::fmt::Debug>(
		&self,
		process: &tg::Process,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<()> {
		// Get the remote/stderr output.
		let stderr = process.load(self).await?.stderr.clone();
		match stderr {
			Some(tg::process::Stdio::Pipe(_)) => (),
			Some(tg::process::Stdio::Pty(pty)) => {
				self.write_progress_to_pty(process.remote().cloned(), &pty, stream)
					.await?;
			},
			None => self.write_progress_to_log(process, stream).await?,
		}
		Ok(())
	}

	async fn write_progress_to_log<T: std::fmt::Debug>(
		&self,
		process: &tg::Process,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<()> {
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			let indicator = match event {
				tg::progress::Event::Start(indicator)
				| tg::progress::Event::Finish(indicator)
				| tg::progress::Event::Update(indicator) => indicator,
				_ => continue,
			};
			let message = format!("{indicator}\n");
			let arg = tg::process::log::post::Arg {
				bytes: message.into(),
				stream: tg::process::log::Stream::Stderr,
				remote: process.remote().cloned(),
			};
			self.post_process_log(process.id(), arg).await?;
		}
		Ok(())
	}

	async fn write_progress_to_pty<T: Send>(
		&self,
		remote: Option<String>,
		pty: &tg::pty::Id,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<()> {
		let (send, recv) = async_channel::bounded(1024);
		let mut state = ProgressPtyState {
			server: self.clone(),
			pty: pty.clone(),
			indicators: IndexMap::new(),
			lines: None,
			send,
		};
		let task = tokio::spawn(async move {
			let mut interval = tokio::time::interval(std::time::Duration::from_millis(16));
			let mut stream = pin!(stream);
			loop {
				let next = stream.next();
				let tick = interval.tick().boxed();
				let either = future::select(next, tick).await;
				match either {
					future::Either::Left((Some(Ok(event)), _)) => {
						let is_update = event.is_update();
						state.update(event).await;
						if is_update {
							continue;
						}
					},
					future::Either::Left((Some(Err(error)), _)) => {
						state.clear().await;
						return Err(error);
					},
					future::Either::Left((None, _)) => {
						state.clear().await;
						break;
					},
					future::Either::Right(_) => (),
				}
				state.clear().await;
				state.print().await?;
			}

			Ok::<_, tg::Error>(())
		});
		let stream = recv.attach(AbortOnDropHandle::new(task));
		let arg = tg::pty::write::Arg {
			remote,
			master: false,
		};
		self.write_pty(pty, arg, stream).await?;
		Ok(())
	}
}

impl ProgressPtyState {
	async fn update<T>(&mut self, event: tg::progress::Event<T>) {
		match event {
			tg::progress::Event::Log(log) => {
				if let Some(level) = log.level {
					let output = match level {
						tg::progress::Level::Success => {
							format!("{} ", "success".green().bold())
						},
						tg::progress::Level::Info => {
							format!("{} ", "info".blue().bold())
						},
						tg::progress::Level::Warning => {
							format!("{} ", "warning".yellow().bold())
						},
						tg::progress::Level::Error => {
							format!("{} ", "error".red().bold())
						},
					};
					self.send
						.send(Ok(tg::pty::Event::Chunk(output.into())))
						.await
						.ok();
				}
			},

			tg::progress::Event::Diagnostic(diagnostic) => {
				let output = diagnostic.to_string();
				self.send
					.send(Ok(tg::pty::Event::Chunk(output.into())))
					.await
					.ok();
			},

			tg::progress::Event::Start(indicator) | tg::progress::Event::Update(indicator) => {
				self.indicators.insert(indicator.name.clone(), indicator);
			},

			tg::progress::Event::Finish(indicator) => {
				self.indicators.shift_remove(&indicator.name);
			},

			tg::progress::Event::Output(_) => (),
		}
	}

	async fn clear(&mut self) {
		match self.lines.take() {
			Some(n) if n > 0 => {
				let mut message = Vec::new();
				crossterm::queue!(
					&mut message,
					crossterm::cursor::MoveToPreviousLine(n),
					crossterm::terminal::Clear(crossterm::terminal::ClearType::FromCursorDown),
				)
				.unwrap();
				self.send
					.send(Ok(tg::pty::Event::Chunk(message.into())))
					.await
					.ok();
			},
			_ => (),
		}
	}

	async fn print(&mut self) -> tg::Result<()> {
		// Get the size of the tty.
		let size = self
			.server
			.get_pty_size(&self.pty, tg::pty::read::Arg::default())
			.await?
			.map_or((128, 128), |size| (size.rows, size.cols));

		// Render the indicators.
		let title_length = self
			.indicators
			.values()
			.map(|indicator| indicator.title.len())
			.max();
		let now = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis();
		let mut buffer = Vec::new();
		for indicator in self.indicators.values() {
			let mut line = String::new();
			const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
			let position = (now / (1000 / 10)) % 10;
			let position = position.to_usize().unwrap();
			let spinner = crossterm::style::Stylize::blue(SPINNER[position]);
			write!(line, "{spinner}").unwrap();
			write!(
				line,
				" {:title_length$}",
				indicator.title,
				title_length = title_length.unwrap(),
			)
			.unwrap();
			const LENGTH: u64 = 20;
			if let (Some(current), Some(total)) = (indicator.current, indicator.total) {
				write!(line, " [").unwrap();
				let n = if total > 0 {
					(current * LENGTH / total).min(LENGTH)
				} else {
					LENGTH
				};
				for _ in 0..n {
					write!(line, "=").unwrap();
				}
				if current < total {
					write!(line, ">").unwrap();
				} else {
					write!(line, "=").unwrap();
				}
				for _ in n..LENGTH {
					write!(line, " ").unwrap();
				}
				write!(line, "]").unwrap();
			}
			if let Some(current) = indicator.current {
				match indicator.format {
					tg::progress::IndicatorFormat::Normal => {
						write!(line, " {current}").unwrap();
					},
					tg::progress::IndicatorFormat::Bytes => {
						let current = byte_unit::Byte::from_u64(current)
							.get_appropriate_unit(byte_unit::UnitType::Decimal);
						write!(line, " {current:#.1}").unwrap();
					},
				}
				if let Some(total) = indicator.total {
					match indicator.format {
						tg::progress::IndicatorFormat::Normal => {
							write!(line, " of {total}").unwrap();
						},
						tg::progress::IndicatorFormat::Bytes => {
							let total = byte_unit::Byte::from_u64(total)
								.get_appropriate_unit(byte_unit::UnitType::Decimal);
							write!(line, " of {total:#.1}").unwrap();
						},
					}
					let percent = 100.0 * current.to_f64().unwrap() / total.to_f64().unwrap();
					write!(line, " {percent:.2}%").unwrap();
				}
			}
			buffer.extend_from_slice(clip(&line, size.0.into()).as_bytes());
			buffer.extend_from_slice(b"\r\n");
		}

		// Send the chunk.
		self.send
			.send(Ok(tg::pty::Event::Chunk(buffer.into())))
			.await
			.ok();

		// Update the number of lines.
		self.lines.replace(self.indicators.len().to_u16().unwrap());
		Ok(())
	}
}

fn clip(string: &str, mut width: usize) -> &str {
	let mut len = 0;
	let mut chars = string.chars();
	while width > 0 {
		let Some(char) = chars.next() else {
			break;
		};
		len += char.len_utf8();
		width = width.saturating_sub(char.width().unwrap_or(0));
	}
	&string[0..len]
}
