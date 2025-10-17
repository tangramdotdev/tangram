use {
	crate::{Cli, viewer::clip},
	crossterm::{self as ct, style::Stylize as _},
	futures::{FutureExt as _, Stream, StreamExt as _, future},
	indexmap::IndexMap,
	num::ToPrimitive as _,
	std::{
		fmt::Write as _,
		io::{IsTerminal as _, Write as _},
		pin::pin,
		time::Duration,
	},
	tangram_client as tg,
	tangram_futures::stream::TryExt as _,
};

struct State<T> {
	indicators: IndexMap<String, tg::progress::Indicator>,
	lines: Option<u16>,
	output: Option<T>,
	tty: std::io::Stderr,
}

impl Cli {
	pub async fn render_progress_stream<T>(
		&mut self,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>>,
	) -> tg::Result<T> {
		if self.args.quiet {
			let output = pin!(stream)
				.try_last()
				.await?
				.ok_or_else(|| tg::error!("expected an event"))?
				.try_unwrap_output()
				.ok()
				.ok_or_else(|| tg::error!("expected the output"))?;
			return Ok(output);
		}

		let tty = std::io::stderr();

		if !tty.is_terminal() {
			let stream = pin!(stream);
			let output = stream
				.try_last()
				.await?
				.and_then(|event| event.try_unwrap_output().ok())
				.ok_or_else(|| tg::error!("stream ended without output"))?;
			return Ok(output);
		}

		let mut state = State {
			indicators: IndexMap::new(),
			lines: None,
			tty,
			output: None,
		};

		let interval = Duration::from_millis(20);
		let mut interval = tokio::time::interval(interval);
		let mut stream = pin!(stream);
		loop {
			let next = stream.next();
			let tick = interval.tick().boxed();
			let either = future::select(next, tick).await;
			match either {
				future::Either::Left((Some(Ok(event)), _)) => {
					let is_update = event.is_update();
					self.render_progress_stream_update(&mut state, event).await;
					if is_update {
						continue;
					}
				},
				future::Either::Left((Some(Err(error)), _)) => {
					state.clear();
					return Err(error);
				},
				future::Either::Left((None, _)) => {
					state.clear();
					break;
				},
				future::Either::Right(_) => (),
			}
			state.clear();
			state.print();
		}

		let output = state
			.output
			.ok_or_else(|| tg::error!("expected an output"))?;

		Ok(output)
	}

	async fn render_progress_stream_update<T>(
		&mut self,
		state: &mut State<T>,
		event: tg::progress::Event<T>,
	) {
		match event {
			tg::progress::Event::Log(log) => {
				if let Some(level) = log.level {
					match level {
						tg::progress::Level::Success => {
							write!(state.tty, "{} ", "success".green().bold()).unwrap();
						},
						tg::progress::Level::Info => {
							write!(state.tty, "{} ", "info".blue().bold()).unwrap();
						},
						tg::progress::Level::Warning => {
							write!(state.tty, "{} ", "warning".yellow().bold()).unwrap();
						},
						tg::progress::Level::Error => {
							write!(state.tty, "{} ", "error".red().bold()).unwrap();
						},
					}
				}
				writeln!(state.tty, "{}", log.message).unwrap();
			},

			tg::progress::Event::Diagnostic(diagnostic) => {
				match diagnostic.try_into() {
					Ok(diagnostic) => {
						self.print_diagnostic(tg::Referent::with_item(diagnostic))
							.await;
					},
					Err(error) => {
						eprintln!("Failed to convert diagnostic: {error}");
					},
				}
			},

			tg::progress::Event::Start(indicator) | tg::progress::Event::Update(indicator) => {
				state.indicators.insert(indicator.name.clone(), indicator);
			},

			tg::progress::Event::Finish(indicator) => {
				state.indicators.shift_remove(&indicator.name);
			},

			tg::progress::Event::Output(output) => {
				state.output.replace(output);
			},
		}
	}
}

impl<T> State<T> {
	fn clear(&mut self) {
		match self.lines.take() {
			Some(n) if n > 0 => {
				ct::queue!(
					self.tty,
					ct::cursor::MoveToPreviousLine(n),
					ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown),
				)
				.unwrap();
			},
			_ => (),
		}
	}

	fn print(&mut self) {
		// Get the size of the tty.
		let size = ct::terminal::size().map_or((64, 64), |(columns, rows)| {
			(columns.to_usize().unwrap(), rows.to_usize().unwrap())
		});

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
		for indicator in self.indicators.values() {
			const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
			let mut line = String::new();
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
			let line = clip(&line, size.0);
			writeln!(self.tty, "{line}").unwrap();
		}

		// Flush the tty.
		self.tty.flush().unwrap();

		// Set the lines.
		self.lines.replace(self.indicators.len().to_u16().unwrap());
	}
}
