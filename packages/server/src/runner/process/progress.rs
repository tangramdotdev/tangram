use {
	crate::Session,
	bytes::Bytes,
	crossterm::style::Stylize as _,
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future},
	indexmap::IndexMap,
	num::ToPrimitive as _,
	std::{fmt::Write as _, pin::pin},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tokio_stream::wrappers::ReceiverStream,
	unicode_width::UnicodeWidthChar as _,
};

struct State {
	indicators: IndexMap<String, tg::progress::Indicator>,
	lines: Option<u16>,
	sender: tokio::sync::mpsc::Sender<Bytes>,
}

impl Session {
	pub(crate) async fn write_progress_stream<T: Send + std::fmt::Debug + 'static>(
		&self,
		progress: tokio::sync::mpsc::UnboundedSender<Bytes>,
		stderr: &tg::process::Stdio,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<T> {
		let quiet = std::env::var("TANGRAM_QUIET")
			.ok()
			.and_then(|value| value.parse().ok())
			.unwrap_or(false);
		if quiet {
			return self.write_progress_stream_to_null(stream).await;
		}
		let output = match stderr {
			tg::process::Stdio::Log => self.write_progress_stream_to_log(progress, stream).await?,
			tg::process::Stdio::Null => self.write_progress_stream_to_null(stream).await?,
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => {
				self.write_progress_stream_to_tty(progress, stream).await?
			},
			tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit => {
				return Err(tg::error!("invalid stdio"));
			},
		};
		Ok(output)
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
		progress: tokio::sync::mpsc::UnboundedSender<Bytes>,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<T> {
		const INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
		// Log nothing until the delay has elapsed so that short operations stay quiet.
		let start = std::time::Instant::now() + self.server.config.runner.progress_log_delay;
		let mut stream = pin!(stream);
		let mut latest: IndexMap<String, String> = IndexMap::new();
		let mut written: IndexMap<String, (String, std::time::Instant)> = IndexMap::new();
		while let Some(event) = stream.try_next().await? {
			match event {
				tg::progress::Event::Indicators(indicators) => {
					latest.clear();
					for indicator in indicators {
						let current = indicator.current.unwrap_or(0);
						let total = indicator.total.unwrap_or(0);
						if current == 0 && total == 0 {
							continue;
						}
						let line = format!("{} {indicator}", indicator.title);
						latest.insert(indicator.name.clone(), line);
					}
					// Write each indicator at most once per interval, and only when its line changed.
					let now = std::time::Instant::now();
					if now < start {
						continue;
					}
					for (name, line) in &latest {
						let skip = written
							.get(name)
							.is_some_and(|(previous, at)| previous == line || now < *at + INTERVAL);
						if skip {
							continue;
						}
						progress.send(format!("{line}\n").into()).ok();
						written.insert(name.clone(), (line.clone(), now));
					}
				},
				tg::progress::Event::Output(output) => {
					// Flush the final state of any indicator whose last written line is stale.
					if std::time::Instant::now() < start {
						return Ok(output);
					}
					for (name, line) in &latest {
						if written.get(name).is_some_and(|(previous, _)| previous == line) {
							continue;
						}
						progress.send(format!("{line}\n").into()).ok();
					}
					return Ok(output);
				},
				_ => (),
			}
		}
		Err(tg::error!("expected an output"))
	}

	async fn write_progress_stream_to_tty<T: Send + 'static>(
		&self,
		progress: tokio::sync::mpsc::UnboundedSender<Bytes>,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<T> {
		let (sender, receiver) = tokio::sync::mpsc::channel(16);
		let mut state = State {
			indicators: IndexMap::new(),
			lines: None,
			sender,
		};
		let progress_task = Task::spawn(|_| async move {
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
		let stderr_task = Task::spawn(|_| async move {
			let mut receiver = pin!(ReceiverStream::new(receiver));
			while let Some(bytes) = receiver.next().await {
				progress.send(bytes).ok();
			}
			Ok::<_, tg::Error>(())
		});
		let (result1, result2) = future::join(progress_task.wait(), stderr_task.wait()).await;
		result2.map_err(|error| tg::error!(!error, "the stderr task panicked"))??;
		result1.map_err(|error| tg::error!(!error, "the progress task panicked"))?
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
					self.sender.send(output.into()).await.ok();
				}
			},

			tg::progress::Event::Diagnostic(diagnostic) => {
				let output = diagnostic.to_string();
				self.sender.send(output.into()).await.ok();
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
				self.sender.send(message.into()).await.ok();
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
		self.sender.send(buffer.into()).await.ok();

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
