use crate::Cli;
use crossterm::{self as ct, style::Stylize as _};
use futures::{stream::TryStreamExt as _, Stream};
use indexmap::IndexMap;
use std::{
	io::{IsTerminal as _, Write as _},
	pin::pin,
};
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

		let mut indicators_count = None;
		while let Some(event) = stream.try_next().await? {
			// Clear the indicators.
			if let Some(indicators_count) = indicators_count {
				for _ in 0..indicators_count {
					ct::queue!(
						tty,
						ct::cursor::MoveToPreviousLine(1),
						ct::terminal::Clear(ct::terminal::ClearType::CurrentLine),
					)
					.unwrap();
				}
			}

			match event {
				tg::progress::Event::Log(log) => {
					if let Some(level) = log.level {
						match level {
							tg::progress::Level::Success => {
								write!(tty, "{} ", "success".green().bold()).unwrap();
							},
							tg::progress::Level::Info => {
								write!(tty, "{} ", "info".blue().bold()).unwrap();
							},
							tg::progress::Level::Warning => {
								write!(tty, "{} ", "warning".yellow().bold()).unwrap();
							},
							tg::progress::Level::Error => {
								write!(tty, "{} ", "error".red().bold()).unwrap();
							},
						}
					}
					writeln!(tty, "{}", log.message).unwrap();
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

			// Render the indicators.
			for indicator in indicators.values() {
				writeln!(tty, "{indicator}").unwrap();
			}
			indicators_count = Some(indicators.len());

			// Flush the tty.
			tty.flush().unwrap();
		}

		Err(tg::error!("stream ended without output"))
	}
}
