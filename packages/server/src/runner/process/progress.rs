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

const LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

struct State {
	indicators: IndexMap<String, tg::progress::Indicator>,
	lines: Option<u16>,
	rendered: Option<(Bytes, u16)>,
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
		// Log nothing until the delay has elapsed so that short operations stay quiet.
		let start = tokio::time::Instant::now() + self.server.config.runner.progress_log_delay;
		let mut interval = tokio::time::interval_at(start, LOG_INTERVAL);
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut stream = pin!(stream);
		let mut latest: IndexMap<String, String> = IndexMap::new();
		let mut written: IndexMap<String, String> = IndexMap::new();
		loop {
			tokio::select! {
				result = stream.try_next() => {
					let Some(event) = result? else {
						break;
					};
					match event {
						tg::progress::Event::Diagnostic(diagnostic) => {
							let message = format!("{diagnostic}\n");
							progress.send(message.into()).ok();
							reset_log_interval(&mut interval, start);
						},
						tg::progress::Event::Indicators(indicators) => {
							latest.clear();
							for indicator in indicators {
								let value = indicator.to_string();
								let line = if value.is_empty() {
									format!("↻ {}", indicator.title)
								} else {
									format!("↻ {} {value}", indicator.title)
								};
								latest.insert(indicator.name.clone(), line);
							}
						},
						tg::progress::Event::Log(log) => {
							let message = match log.level {
								Some(level) => format!("{level} {}\n", log.message),
								None => format!("{}\n", log.message),
							};
							progress.send(message.into()).ok();
							reset_log_interval(&mut interval, start);
						},
						tg::progress::Event::Output(output) => {
							let now = tokio::time::Instant::now();
							if now >= start {
								write_progress_log(&progress, &latest, &mut written);
							}
							return Ok(output);
						},
					}
				},
				_ = interval.tick() => {
					write_progress_log(&progress, &latest, &mut written);
				},
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
			rendered: None,
			sender,
		};
		let progress_task = Task::spawn(|_| async move {
			let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
			let mut stream = pin!(stream);
			let mut output = None;
			loop {
				let reset_interval = {
					let next = stream.next();
					let tick = interval.tick().boxed();
					let either = future::select(next, tick).await;
					match either {
						future::Either::Left((Some(Ok(event)), _)) => {
							let is_message = event.is_diagnostic() || event.is_log();
							if is_message {
								state.clear().await;
							}
							match event {
								tg::progress::Event::Output(value) => {
									state.clear().await;
									output.replace(value);
								},
								event => {
									state.update(event).await;
								},
							}
							if is_message {
								state.restore().await;
							}
							is_message
						},
						future::Either::Left((Some(Err(error)), _)) => {
							state.clear().await;
							return Err(error);
						},
						future::Either::Left((None, _)) => {
							state.clear().await;
							break;
						},
						future::Either::Right(_) => {
							state.clear().await;
							state.print().await?;
							false
						},
					}
				};
				if reset_interval {
					interval.reset();
				}
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
			tg::progress::Event::Diagnostic(diagnostic) => {
				let output = format!("{diagnostic}\n");
				self.sender.send(output.into()).await.ok();
			},

			tg::progress::Event::Indicators(indicators) => {
				self.indicators = indicators
					.into_iter()
					.map(|i| (i.name.clone(), i))
					.collect();
			},

			tg::progress::Event::Log(log) => {
				let output = match log.level {
					Some(level) => {
						let level = match level {
							tg::progress::Level::Error => "error".red().bold(),
							tg::progress::Level::Info => "info".blue().bold(),
							tg::progress::Level::Success => "success".green().bold(),
							tg::progress::Level::Warning => "warning".yellow().bold(),
						};
						format!("{level} {}\n", log.message)
					},
					None => format!("{}\n", log.message),
				};
				self.sender.send(output.into()).await.ok();
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
		let buffer = Bytes::from(buffer);
		self.sender.send(buffer.clone()).await.ok();

		// Update the number of lines.
		let lines = self.indicators.len().to_u16().unwrap();
		self.lines.replace(lines);
		self.rendered.replace((buffer, lines));

		Ok(())
	}

	async fn restore(&mut self) {
		let Some((buffer, lines)) = self.rendered.clone() else {
			return;
		};
		self.sender.send(buffer).await.ok();
		self.lines.replace(lines);
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

fn reset_log_interval(interval: &mut tokio::time::Interval, start: tokio::time::Instant) {
	let deadline = (tokio::time::Instant::now() + LOG_INTERVAL).max(start);
	interval.reset_at(deadline);
}

fn write_progress_log(
	progress: &tokio::sync::mpsc::UnboundedSender<Bytes>,
	latest: &IndexMap<String, String>,
	written: &mut IndexMap<String, String>,
) {
	for (name, line) in latest {
		if written.get(name).is_some_and(|previous| previous == line) {
			continue;
		}
		progress.send(format!("{line}\n").into()).ok();
		written.insert(name.clone(), line.clone());
	}
}
