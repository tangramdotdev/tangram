use {
	crate::Server,
	bytes::Bytes,
	crossterm::style::Stylize as _,
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future},
	indexmap::IndexMap,
	num::ToPrimitive as _,
	std::{fmt::Write as _, pin::pin},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tokio_util::io::StreamReader,
	unicode_width::UnicodeWidthChar as _,
};

struct State {
	indicators: IndexMap<String, tg::progress::Indicator>,
	lines: Option<u16>,
	sender: async_channel::Sender<tg::Result<Bytes>>,
}

impl Server {
	pub(crate) async fn write_progress_stream<T: Send + std::fmt::Debug + 'static>(
		&self,
		process: &tg::Process,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<T> {
		let stderr = process.load(self).await?.stderr;
		match stderr {
			Some(tg::process::Stdio::Log) => {
				self.write_progress_stream_to_log(process, stream).await
			},
			Some(tg::process::Stdio::Pipe | tg::process::Stdio::Pty) => {
				let remote = process.remote().cloned();
				self.write_progress_stream_to_pty(process.id(), remote, stream)
					.await
			},
			Some(tg::process::Stdio::Null) | None => {
				self.write_progress_stream_to_null(stream).await
			},
		}
	}

	async fn write_progress_stream_to_null<T>(
		&self,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<T> {
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			if let tg::progress::Event::Output(output) = event {
				return Ok(output);
			}
		}
		Err(tg::error!("expected an output"))
	}

	async fn write_progress_stream_to_log<T: std::fmt::Debug>(
		&self,
		process: &tg::Process,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<T> {
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			match event {
				tg::progress::Event::Indicators(indicators) => {
					for indicator in indicators {
						let message = format!("{indicator}\n");
						let arg = tg::process::log::post::Arg {
							bytes: message.into(),
							local: None,
							remotes: process.remote().cloned().map(|r| vec![r]),
							stream: tg::process::log::Stream::Stderr,
						};
						self.post_process_log(process.id(), arg).await?;
					}
				},
				tg::progress::Event::Output(output) => return Ok(output),
				_ => (),
			}
		}
		Err(tg::error!("expected an output"))
	}

	async fn write_progress_stream_to_pty<T: Send + 'static>(
		&self,
		id: &tg::process::Id,
		remote: Option<String>,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<T> {
		let (sender, receiver) = async_channel::bounded(1024);
		let mut state = State {
			indicators: IndexMap::new(),
			lines: None,
			sender,
		};
		let task = Task::spawn(|_| async move {
			let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
			let mut stream = pin!(stream);
			let mut output = None;
			loop {
				let next = stream.next();
				let tick = interval.tick().boxed();
				let either = future::select(next, tick).await;
				match either {
					future::Either::Left((Some(Ok(event)), _)) => {
						let is_indicators = event.is_indicators();
						match event {
							tg::progress::Event::Output(value) => {
								output.replace(value);
							},
							event => {
								state.update(event).await;
							},
						}
						if is_indicators {
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
			output.ok_or_else(|| tg::error!("expected an output"))
		});
		let stream = receiver.map_err(std::io::Error::other);
		let reader = StreamReader::new(stream);
		let arg = tg::process::stdio::Arg {
			local: None,
			remotes: remote.map(|remote| vec![remote]),
		};
		self.write_process_stderr(id, arg, reader)
			.await
			.map_err(|source| tg::error!(!source, "failed to write process stderr"))?;
		task.wait()
			.await
			.map_err(|source| tg::error!(!source, "the progress task panicked"))?
	}
}

impl State {
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
					self.sender.send(Ok(output.into())).await.ok();
				}
			},

			tg::progress::Event::Diagnostic(diagnostic) => {
				let output = diagnostic.to_string();
				self.sender.send(Ok(output.into())).await.ok();
			},

			tg::progress::Event::Indicators(indicators) => {
				self.indicators = indicators
					.into_iter()
					.map(|i| (i.name.clone(), i))
					.collect();
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
				self.sender.send(Ok(message.into())).await.ok();
			},
			_ => (),
		}
	}

	async fn print(&mut self) -> tg::Result<()> {
		let size = (64usize, 64usize);

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
			write!(line, " {indicator}").unwrap();
			buffer.extend_from_slice(clip(&line, size.0).as_bytes());
			buffer.extend_from_slice(b"\r\n");
		}

		// Send the event.
		self.sender.send(Ok(buffer.into())).await.ok();

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
