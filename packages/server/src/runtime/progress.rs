use {
	crate::Server,
	crossterm::style::Stylize as _,
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future},
	indexmap::IndexMap,
	num::ToPrimitive as _,
	std::{fmt::Write as _, pin::pin},
	tangram_client as tg,
	tangram_futures::stream::Ext as _,
	tokio_util::task::AbortOnDropHandle,
	unicode_width::UnicodeWidthChar as _,
};

struct State {
	server: Server,
	pty: tg::pty::Id,
	indicators: IndexMap<String, tg::progress::Indicator>,
	lines: Option<u16>,
	sender: async_channel::Sender<tg::Result<tg::pty::Event>>,
}

impl Server {
	pub(crate) async fn log_progress_stream<T: Send + std::fmt::Debug>(
		&self,
		process: &tg::Process,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<()> {
		let stderr = process.load(self).await?.stderr.clone();
		match stderr {
			None => {
				self.write_progress_stream_to_log(process, stream).await?;
			},
			Some(tg::process::Stdio::Pipe(_)) => (),
			Some(tg::process::Stdio::Pty(pty)) => {
				let remote = process.remote().cloned();
				self.write_progress_stream_to_pty(remote, &pty, stream)
					.await?;
			},
		}
		Ok(())
	}

	async fn write_progress_stream_to_log<T: std::fmt::Debug>(
		&self,
		process: &tg::Process,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<()> {
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			let (tg::progress::Event::Start(indicator)
			| tg::progress::Event::Finish(indicator)
			| tg::progress::Event::Update(indicator)) = event
			else {
				continue;
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

	async fn write_progress_stream_to_pty<T: Send>(
		&self,
		remote: Option<String>,
		pty: &tg::pty::Id,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
	) -> tg::Result<()> {
		let (sender, receiver) = async_channel::bounded(1024);
		let mut state = State {
			server: self.clone(),
			pty: pty.clone(),
			indicators: IndexMap::new(),
			lines: None,
			sender,
		};
		let task = AbortOnDropHandle::new(tokio::spawn(async move {
			let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
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
		}));
		let stream = receiver.attach(task);
		let arg = tg::pty::write::Arg {
			master: false,
			remote,
		};
		self.write_pty(pty, arg, stream).await?;
		Ok(())
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
					let event = tg::pty::Event::Chunk(output.into());
					self.sender.send(Ok(event)).await.ok();
				}
			},

			tg::progress::Event::Diagnostic(diagnostic) => {
				let output = diagnostic.to_string();
				let event = tg::pty::Event::Chunk(output.into());
				self.sender.send(Ok(event)).await.ok();
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
				let event = tg::pty::Event::Chunk(message.into());
				self.sender.send(Ok(event)).await.ok();
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

		// Send the event.
		let event = tg::pty::Event::Chunk(buffer.into());
		self.sender.send(Ok(event)).await.ok();

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
