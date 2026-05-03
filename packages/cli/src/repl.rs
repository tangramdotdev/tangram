use {
	crate::js::Engine,
	crossterm::style::Color,
	std::{borrow::Cow, path::PathBuf, thread},
	tangram_client::prelude::*,
};

const HISTORY_SIZE: usize = 10_000;

/// Run a REPL.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The JS engine to use.
	#[arg(long, default_value = "auto")]
	pub engine: Engine,
}

struct Highlighter {
	syntax_set: syntect::parsing::SyntaxSet,
	theme: syntect::highlighting::Theme,
}

#[derive(Clone)]
struct Prompt;

impl crate::Cli {
	pub async fn command_repl(&mut self, args: Args) -> tg::Result<()> {
		let engine = args.engine;

		// Start the runtime thread.
		let args = Vec::new();
		let cwd = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get the current directory"))?;
		let env = tg::process::env()?
			.into_iter()
			.map(|(key, value)| (key, value.to_data()))
			.collect();
		let executable = tg::command::data::Executable::Path(tg::command::data::PathExecutable {
			path: PathBuf::from("<repl>"),
		});
		let client = tg::handle::dynamic::Handle::new(self.client().await?);
		let host = tg::host::current().to_owned();
		let main_runtime_handle = tokio::runtime::Handle::current();
		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let arg = tangram_js::Arg {
			args,
			cwd,
			env,
			executable,
			handle: client,
			host: Some(host),
			main_runtime_handle,
			inspect: None,
			repl: Some(receiver),
		};
		let thread = thread::spawn(move || {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.map_err(|source| tg::error!(!source, "failed to create the tokio runtime"))?;
			runtime.block_on(async move {
				match engine {
					#[allow(unreachable_patterns)]
					#[cfg(feature = "v8")]
					Engine::Auto | Engine::V8 => {
						let mut runtime = tangram_js::v8::Runtime::new(arg)?;
						runtime.run().await?;
					},
					#[allow(unreachable_patterns)]
					#[cfg(feature = "quickjs")]
					Engine::Auto | Engine::QuickJs => {
						let mut runtime = tangram_js::quickjs::Runtime::new(arg).await?;
						runtime.run().await?;
					},
					#[allow(unreachable_patterns)]
					_ => {
						drop(arg);
						return Err(tg::error!("the requested JS engine is not available"));
					},
				}
				Ok(())
			})
		});

		// Set up reedline.
		let history_path = self.directory_path().join("history");
		let history = reedline::FileBackedHistory::with_file(HISTORY_SIZE, history_path)
			.map_err(|source| tg::error!(!source, "failed to initialize the repl history"))?;
		let history_exclusion_prefix = " ".to_owned();
		let hinter_style = nu_ansi_term::Style::new()
			.italic()
			.fg(nu_ansi_term::Color::DarkGray);
		let hinter = reedline::DefaultHinter::default().with_style(hinter_style);
		let highlighter = Highlighter::new();
		let mut editor = reedline::Reedline::create()
			.with_history(Box::new(history))
			.with_history_exclusion_prefix(Some(history_exclusion_prefix))
			.with_hinter(Box::new(hinter))
			.with_highlighter(Box::new(highlighter));
		let prompt = Prompt;

		// REPL it.
		loop {
			let signal = editor
				.read_line(&prompt)
				.map_err(|source| tg::error!(!source, "failed to read from the repl"))?;
			match signal {
				reedline::Signal::Success(source) if !source.trim().is_empty() => {
					if source.trim() == ".exit" {
						break;
					}
					let (response, receiver) = tokio::sync::oneshot::channel();
					let command = tangram_js::repl::Command { source, response };
					if sender.send(command).is_err() {
						return Err(tg::error!("the repl runtime stopped"));
					}
					match receiver.await {
						Ok(Ok(())) => (),
						Ok(Err(error)) => {
							self.print_error(tg::Referent::with_item(error)).await;
						},
						Err(source) => {
							return Err(tg::error!(!source, "failed to receive the repl response"));
						},
					}
				},
				reedline::Signal::CtrlC | reedline::Signal::CtrlD => {
					break;
				},
				_ => (),
			}
		}

		drop(sender);
		thread
			.join()
			.map_err(|_| tg::error!("the REPL runtime panicked"))??;

		Ok(())
	}
}

impl Highlighter {
	fn new() -> Self {
		let syntax_set = syntect::parsing::SyntaxSet::load_defaults_newlines();
		let theme = crate::theme::tangram();
		Self { syntax_set, theme }
	}

	fn syntax(&self) -> &syntect::parsing::SyntaxReference {
		self.syntax_set
			.find_syntax_by_extension("ts")
			.or_else(|| self.syntax_set.find_syntax_by_extension("tsx"))
			.or_else(|| self.syntax_set.find_syntax_by_name("TypeScript"))
			.or_else(|| self.syntax_set.find_syntax_by_extension("js"))
			.unwrap_or_else(|| self.syntax_set.find_syntax_plain_text())
	}
}

impl reedline::Highlighter for Highlighter {
	fn highlight(&self, line: &str, _cursor: usize) -> reedline::StyledText {
		let mut styled_text = reedline::StyledText::new();

		let mut highlighter = syntect::easy::HighlightLines::new(self.syntax(), &self.theme);
		for source_line in syntect::util::LinesWithEndings::from(line) {
			let Ok(ranges) = highlighter.highlight_line(source_line, &self.syntax_set) else {
				styled_text.push((nu_ansi_term::Style::new(), source_line.to_owned()));
				continue;
			};
			for (style, text) in ranges {
				styled_text.push((syntect_style_to_ansi(style), text.to_owned()));
			}
		}

		styled_text
	}
}

impl reedline::Prompt for Prompt {
	fn render_prompt_left(&self) -> Cow<'_, str> {
		Cow::Borrowed("")
	}

	fn render_prompt_right(&self) -> Cow<'_, str> {
		Cow::Borrowed("")
	}

	fn render_prompt_indicator(&self, _edit_mode: reedline::PromptEditMode) -> Cow<'_, str> {
		Cow::Borrowed("> ")
	}

	fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
		Cow::Borrowed("  ")
	}

	fn render_prompt_history_search_indicator(
		&self,
		history_search: reedline::PromptHistorySearch,
	) -> Cow<'_, str> {
		let prefix = match history_search.status {
			reedline::PromptHistorySearchStatus::Passing => "",
			reedline::PromptHistorySearchStatus::Failing => "failing ",
		};
		Cow::Owned(format!(
			"({prefix}reverse-search: {}) ",
			history_search.term
		))
	}

	fn get_indicator_color(&self) -> Color {
		Color::AnsiValue(244)
	}
}

fn syntect_style_to_ansi(style: syntect::highlighting::Style) -> nu_ansi_term::Style {
	let mut ansi = nu_ansi_term::Style::new().fg(nu_ansi_term::Color::Rgb(
		style.foreground.r,
		style.foreground.g,
		style.foreground.b,
	));

	if style
		.font_style
		.contains(syntect::highlighting::FontStyle::BOLD)
	{
		ansi = ansi.bold();
	}
	if style
		.font_style
		.contains(syntect::highlighting::FontStyle::ITALIC)
	{
		ansi = ansi.italic();
	}
	if style
		.font_style
		.contains(syntect::highlighting::FontStyle::UNDERLINE)
	{
		ansi = ansi.underline();
	}

	ansi
}
