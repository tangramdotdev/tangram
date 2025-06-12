use crate::{Cli, viewer::clip};
use crossterm::{self as ct, style::Stylize as _};
use futures::{FutureExt as _, Stream, StreamExt as _, future};
use indexmap::IndexMap;
use num::ToPrimitive as _;
use std::{
	fmt::Write as _,
	io::{IsTerminal as _, Write as _},
	pin::pin,
	time::Duration,
};
use tangram_client as tg;
use tangram_futures::stream::TryExt as _;

struct State<T> {
	indicators: IndexMap<String, tg::progress::Indicator>,
	lines: Option<u16>,
	tty: std::io::Stderr,
	output: Option<T>,
}

impl Cli {
	pub async fn render_progress_stream<T>(
		&self,
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
					state.update(event);
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
}

impl<T> State<T> {
	fn update(&mut self, event: tg::progress::Event<T>) {
		match event {
			tg::progress::Event::Log(log) => {
				if let Some(level) = log.level {
					match level {
						tg::progress::Level::Success => {
							write!(self.tty, "{} ", "success".green().bold()).unwrap();
						},
						tg::progress::Level::Info => {
							write!(self.tty, "{} ", "info".blue().bold()).unwrap();
						},
						tg::progress::Level::Warning => {
							write!(self.tty, "{} ", "warning".yellow().bold()).unwrap();
						},
						tg::progress::Level::Error => {
							write!(self.tty, "{} ", "error".red().bold()).unwrap();
						},
					}
				}
				writeln!(self.tty, "{}", log.message).unwrap();
			},

			tg::progress::Event::Diagnostic(diagnostic) => {
				Cli::print_diagnostic(&diagnostic);
			},

			tg::progress::Event::Start(indicator) | tg::progress::Event::Update(indicator) => {
				self.indicators.insert(indicator.name.clone(), indicator);
			},

			tg::progress::Event::Finish(indicator) => {
				self.indicators.shift_remove(&indicator.name);
			},

			tg::progress::Event::Output(output) => {
				self.output.replace(output);
			},
		}
	}

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
				let n = if total > 0 { current / total } else { 1 } * LENGTH;
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
