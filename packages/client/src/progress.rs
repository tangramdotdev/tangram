use {
	crate::prelude::*,
	crossterm::{self as ct, style::Stylize as _},
	futures::{FutureExt as _, Stream, StreamExt as _, future},
	indexmap::IndexMap,
	num::ToPrimitive as _,
	std::{fmt::Write as _, path::Path, pin::pin, time::Duration},
	tokio::io::AsyncReadExt as _,
};

#[derive(
	Debug,
	Clone,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Event<T> {
	Diagnostic(tg::diagnostic::Data),
	Indicators(Vec<Indicator>),
	Log(Log),
	Output(T),
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Indicator {
	pub current: Option<u64>,
	pub format: IndicatorFormat,
	pub name: String,
	pub title: String,
	pub total: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndicatorFormat {
	Normal,
	Bytes,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Log {
	pub level: Option<Level>,
	pub message: String,
}

#[derive(Clone, Debug, serde_with::SerializeDisplay, serde_with::DeserializeFromStr)]
pub enum Level {
	Success,
	Info,
	Warning,
	Error,
}

impl<T> Event<T> {
	pub fn map_output<U>(self, f: impl FnOnce(T) -> U) -> Event<U> {
		match self {
			Self::Log(log) => Event::Log(log),
			Self::Diagnostic(diagnostic) => Event::Diagnostic(diagnostic),
			Self::Indicators(indicators) => Event::Indicators(indicators),
			Self::Output(value) => Event::Output(f(value)),
		}
	}

	pub fn try_map_output<U, E>(
		self,
		f: impl FnOnce(T) -> std::result::Result<U, E>,
	) -> std::result::Result<Event<U>, E> {
		match self {
			Self::Log(log) => Ok(Event::Log(log)),
			Self::Diagnostic(diagnostic) => Ok(Event::Diagnostic(diagnostic)),
			Self::Indicators(indicators) => Ok(Event::Indicators(indicators)),
			Self::Output(value) => f(value).map(Event::Output),
		}
	}
}

impl std::fmt::Display for Indicator {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		const LENGTH: u64 = 20;
		if let (Some(current), Some(total)) = (self.current, self.total) {
			write!(f, "[")?;
			let n = if total > 0 {
				(current * LENGTH / total).min(LENGTH)
			} else {
				LENGTH
			};
			for _ in 0..n {
				write!(f, "=")?;
			}
			if current < total {
				write!(f, ">")?;
			} else {
				write!(f, "=")?;
			}
			for _ in n..LENGTH {
				write!(f, " ")?;
			}
			write!(f, "]")?;
		}
		if let Some(current) = self.current {
			match self.format {
				tg::progress::IndicatorFormat::Normal => {
					write!(f, " {current}")?;
				},
				tg::progress::IndicatorFormat::Bytes => {
					let current = byte_unit::Byte::from_u64(current)
						.get_appropriate_unit(byte_unit::UnitType::Decimal);
					write!(f, " {current:#.1}")?;
				},
			}
			if let Some(total) = self.total
				&& total > 0
			{
				match self.format {
					tg::progress::IndicatorFormat::Normal => {
						write!(f, " of {total}")?;
					},
					tg::progress::IndicatorFormat::Bytes => {
						let total = byte_unit::Byte::from_u64(total)
							.get_appropriate_unit(byte_unit::UnitType::Decimal);
						write!(f, " of {total:#.1}")?;
					},
				}
				let percent = 100.0 * current.to_f64().unwrap() / total.to_f64().unwrap();
				write!(f, " {percent:.2}%")?;
			}
		}

		Ok(())
	}
}

impl<T> TryFrom<Event<T>> for tangram_http::sse::Event
where
	T: serde::Serialize,
{
	type Error = tg::Error;

	fn try_from(value: Event<T>) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Diagnostic(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("diagnostic".to_owned()),
					data,
					..Default::default()
				}
			},
			Event::Indicators(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("indicators".to_owned()),
					data,
					..Default::default()
				}
			},
			Event::Log(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("log".to_owned()),
					data,
					..Default::default()
				}
			},
			Event::Output(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("output".to_owned()),
					data,
					..Default::default()
				}
			},
		};
		Ok(event)
	}
}

impl<T> TryFrom<tangram_http::sse::Event> for Event<T>
where
	T: serde::de::DeserializeOwned,
{
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("diagnostic") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Diagnostic(data))
			},
			Some("indicators") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Indicators(data))
			},
			Some("log") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Log(data))
			},
			Some("output") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Output(data))
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}

impl std::fmt::Display for Level {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Success => write!(f, "success"),
			Self::Info => write!(f, "info"),
			Self::Warning => write!(f, "warning"),
			Self::Error => write!(f, "error"),
		}
	}
}

impl std::str::FromStr for Level {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"success" => Ok(Self::Success),
			"info" => Ok(Self::Info),
			"warning" => Ok(Self::Warning),
			"error" => Ok(Self::Error),
			_ => Err(tg::error!(kind = %s, "invalid value")),
		}
	}
}

struct State<H, T, W> {
	handle: H,
	indicators: IndexMap<String, tg::progress::Indicator>,
	is_tty: bool,
	lines: Option<u16>,
	output: Option<T>,
	writer: W,
}

pub async fn write_progress_stream<H, T>(
	handle: &H,
	stream: impl Stream<Item = tg::Result<tg::progress::Event<T>>>,
	writer: impl std::io::Write,
	is_tty: bool,
) -> tg::Result<T>
where
	H: tg::Handle,
{
	let mut state = State {
		handle: handle.clone(),
		indicators: IndexMap::new(),
		is_tty,
		lines: None,
		writer,
		output: None,
	};

	let interval = Duration::from_millis(100);
	let mut interval = tokio::time::interval(interval);
	let mut stream = pin!(stream);
	loop {
		let next = stream.next();
		let tick = interval.tick().boxed();
		let either = future::select(next, tick).await;
		match either {
			future::Either::Left((Some(Ok(event)), _)) => {
				let is_indicators = event.is_indicators();
				state.render_progress_stream_update(event).await;
				if is_indicators {
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

impl<H, T, W> State<H, T, W>
where
	H: tg::Handle,
	W: std::io::Write,
{
	async fn render_progress_stream_update(&mut self, event: tg::progress::Event<T>) {
		match event {
			tg::progress::Event::Diagnostic(diagnostic) => {
				let item = diagnostic.try_into().unwrap();
				let referent = tg::Referent::with_item(item);
				self.print_diagnostic(referent).await;
			},

			tg::progress::Event::Indicators(indicators) => {
				self.indicators = indicators
					.into_iter()
					.map(|i| (i.name.clone(), i))
					.collect();
			},

			tg::progress::Event::Log(log) => {
				if let Some(level) = log.level {
					match level {
						tg::progress::Level::Success => {
							write!(self.writer, "{} ", "success".green().bold()).unwrap();
						},
						tg::progress::Level::Info => {
							write!(self.writer, "{} ", "info".blue().bold()).unwrap();
						},
						tg::progress::Level::Warning => {
							write!(self.writer, "{} ", "warning".yellow().bold()).unwrap();
						},
						tg::progress::Level::Error => {
							write!(self.writer, "{} ", "error".red().bold()).unwrap();
						},
					}
				}
				writeln!(self.writer, "{}", log.message).unwrap();
			},

			tg::progress::Event::Output(output) => {
				self.output.replace(output);
			},
		}
	}

	fn clear(&mut self) {
		let n = self.lines.take();
		if let Some(n) = n
			&& self.is_tty
		{
			ct::queue!(
				self.writer,
				ct::cursor::MoveToPreviousLine(n),
				ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown),
			)
			.unwrap();
		}
	}

	fn print(&mut self) {
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
			write!(line, " {indicator}").unwrap();
			writeln!(self.writer, "{line}").unwrap();
		}

		// Flush the tty.
		self.writer.flush().unwrap();

		// Set the lines.
		self.lines.replace(self.indicators.len().to_u16().unwrap());
	}

	pub(crate) async fn print_diagnostic(&mut self, referent: tg::Referent<tg::Diagnostic>) {
		let diagnostic = referent.item();
		let severity = match diagnostic.severity {
			tg::diagnostic::Severity::Error => "error".red().bold(),
			tg::diagnostic::Severity::Warning => "warning".yellow().bold(),
			tg::diagnostic::Severity::Info => "info".blue().bold(),
			tg::diagnostic::Severity::Hint => "hint".cyan().bold(),
		};
		writeln!(self.writer, "{severity} {}", diagnostic.message).unwrap();
		if let Some(location) = &diagnostic.location {
			Box::pin(self.print_location(location, &diagnostic.message)).await;
		}
	}

	async fn print_location(&mut self, location: &tg::Location, message: &str) {
		let tg::Location { module, range } = location;
		match &module.referent.item {
			tg::module::Item::Edge(edge) => {
				let mut title = String::new();
				if let Some(tag) = module.referent.tag() {
					write!(title, "{tag}").unwrap();
					if let Some(path) = module.referent.path() {
						write!(title, ":{}", path.display()).unwrap();
					}
				} else if let Some(path) = module.referent.path() {
					if path.components().next().is_some_and(|component| {
						matches!(component, std::path::Component::Normal(_))
					}) {
						write!(title, "./").unwrap();
					}
					write!(title, "{}", path.display()).unwrap();
				} else {
					write!(title, "<unknown>").unwrap();
				}
				if true {
					self.print_code_with_edge(&title, range, message, edge)
						.await;
				} else {
					writeln!(
						self.writer,
						"   {title}:{}:{}",
						location.range.start.line + 1,
						location.range.start.character + 1,
					)
					.unwrap();
				}
			},

			tg::module::Item::Path(path) => {
				if true {
					self.print_code_with_path(&path.display().to_string(), range, message, path)
						.await;
				} else {
					writeln!(
						self.writer,
						"   {}:{}:{}",
						path.display(),
						location.range.start.line + 1,
						location.range.start.character + 1,
					)
					.unwrap();
				}
			},
		}
	}

	async fn print_code_with_edge(
		&mut self,
		title: &str,
		range: &tg::Range,
		message: &str,
		edge: &tg::graph::Edge<tg::Object>,
	) {
		let file = match edge {
			tg::graph::Edge::Pointer(pointer) => {
				let Ok(artifact) = pointer.get(&self.handle).await else {
					return;
				};
				let Ok(file) = artifact.try_unwrap_file() else {
					return;
				};
				file
			},
			tg::graph::Edge::Object(object) => {
				let Ok(file) = object.clone().try_unwrap_file() else {
					return;
				};
				file
			},
		};
		let Ok(text) = file.text(&self.handle).await else {
			return;
		};
		self.print_code(title, range, message, text);
	}

	async fn print_code_with_path(
		&mut self,
		title: &str,
		range: &tg::Range,
		message: &str,
		path: &Path,
	) {
		let Ok(file) = tokio::fs::File::open(path).await else {
			return;
		};
		let mut reader = tokio::io::BufReader::new(file);
		let mut buffer = Vec::new();
		reader.read_to_end(&mut buffer).await.ok();
		let Ok(text) = String::from_utf8(buffer) else {
			return;
		};
		self.print_code(title, range, message, text);
	}

	fn print_code(&mut self, title: &str, range: &tg::Range, message: &str, text: String) {
		let range = range
			.try_to_byte_range_in_string(&text, tg::position::Encoding::Utf8)
			.unwrap_or(0..text.len());
		let label = miette::LabeledSpan::new_with_span(Some(message.to_owned()), range);
		let code = miette::NamedSource::new(title, text).with_language("JavaScript");
		let diagnostic = miette::diagnostic!(labels = vec![label], "hello world wow");
		let report = miette::Report::new(diagnostic).with_source_code(code);
		let mut string = String::new();
		write!(string, "{report:?}").unwrap();
		let string = &string[string.find('\n').unwrap() + 1..].trim_end();
		writeln!(self.writer, "{string}").unwrap();
	}
}
