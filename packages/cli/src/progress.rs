use crate::Cli;
use crossterm::{self as ct, style::Stylize as _};
use futures::{stream::TryStreamExt as _, Stream};
use indexmap::IndexMap;
use std::pin::pin;
use tangram_client as tg;

impl Cli {
	pub async fn render_progress_stream<T>(
		&self,
		stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>>,
	) -> tg::Result<T> {
		let mut indicators = IndexMap::new();
		let mut stdout = std::io::stdout();
		let mut stream = pin!(stream);

		while let Some(event) = stream.try_next().await? {
			// Clear.
			ct::execute!(
				stdout,
				ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown),
			)
			.unwrap();

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
					self.print_diagnostic(&diagnostic).await;
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
			ct::execute!(stdout, ct::cursor::SavePosition).unwrap();

			// Render the indicators.
			for indicator in indicators.values() {
				eprint!("{}", indicator.title);
				if let Some(current) = indicator.current {
					eprint!(" {current}");
				}
				if let Some(total) = indicator.total {
					eprint!(" / {total}");
				}
				eprintln!();
			}

			// Restore the cursor position.
			ct::execute!(stdout, ct::cursor::RestorePosition).unwrap();
		}

		Err(tg::error!("stream ended without output"))
	}
}
