use crate::Cli;
use byte_unit::Byte;
use crossterm::{self as ct, style::Stylize as _};
use futures::{stream::TryStreamExt as _, Stream};
use indexmap::IndexMap;
use std::{io::IsTerminal as _, pin::pin};
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;

impl Cli {
	pub async fn render_progress_stream<T>(
		&self,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>>,
	) -> tg::Result<T> {
		let mut indicators = IndexMap::new();
		let mut tty = std::io::stderr();
		let mut stream = pin!(stream);

		if !tty.is_terminal() {
			return stream
				.try_last()
				.await?
				.and_then(|event| event.try_unwrap_output().ok())
				.ok_or_else(|| tg::error!("stream ended without output"));
		}

		while let Some(event) = stream.try_next().await? {
			// Clear.
			let action = ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown);
			ct::execute!(tty, action).unwrap();

			match event {
				tg::progress::Event::Log(log) => {
					if let Some(level) = log.level {
						match level {
							tg::progress::Level::Success => {
								eprint!("{} ", "success".green().bold());
							},
							tg::progress::Level::Info => {
								eprint!("{} ", "info".blue().bold());
							},
							tg::progress::Level::Warning => {
								eprint!("{} ", "warning".yellow().bold());
							},
							tg::progress::Level::Error => {
								eprint!("{} ", "error".red().bold());
							},
						}
					}
					eprintln!("{}", log.message);
				},

				tg::progress::Event::Diagnostic(diagnostic) => {
					Self::print_diagnostic(&diagnostic);
				},

				tg::progress::Event::Start(indicator) | tg::progress::Event::Update(indicator) => {
					indicators.insert(indicator.name.clone(), indicator);
				},

				tg::progress::Event::Finish(indicator) => {
					indicators.shift_remove(&indicator.name);
				},

				tg::progress::Event::Output(value) => {
					return Ok(value);
				},
			}

			// Save the cursor position.
			let action = ct::cursor::SavePosition;
			ct::execute!(tty, action).unwrap();

			// Render the indicators.
			let bar_length = 20;
			for indicator in indicators.values() {
				eprint!("{}", indicator.title);
				if let (Some(current), Some(total)) = (indicator.current, indicator.total) {
					eprint!(" [");
					let last = current * bar_length / total;
					for _ in 0..last {
						eprint!("=");
					}
					if current < total {
						eprint!(">");
					} else {
						eprint!("=");
					}

					for _ in last..bar_length {
						eprint!(" ");
					}
					eprint!("]");
					if indicator.title == "bytes_off" {
						let current = Byte::from_u64(current);
						let total = Byte::from_u64(total);
						eprint!(" {current:#} of {total:#}");
					} else {
						eprint!(" {current} of {total}");
					}
				}
				eprintln!();
			}

			// Restore the cursor position.
			let action = ct::cursor::RestorePosition;
			ct::execute!(tty, action).unwrap();
		}

		Err(tg::error!("stream ended without output"))
	}
}
