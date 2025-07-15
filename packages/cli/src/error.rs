use crate::Cli;
use anstream::eprintln;
use crossterm::style::Stylize as _;
use std::{
	fmt::Write as _,
	path::{Path, PathBuf},
};
use tangram_client::{self as tg};
use tokio::io::AsyncReadExt;

impl Cli {
	pub(crate) fn print_error_basic(error: tg::Referent<tg::Error>) {
		let mut stack = vec![error];

		while let Some(error) = stack.pop() {
			let (referent, error) = error.replace(());

			// Print the message.
			let message = error.message.as_deref().unwrap_or("an error occurred");
			eprintln!("{} {message}", "->".red());

			// Print the values.
			for (key, value) in &error.values {
				let key = key.as_str();
				let value = value.as_str();
				eprintln!("   {key} = {value}");
			}

			// Print the location.
			if let Some(mut location) = error.location {
				if let tg::error::File::Module(module) = &mut location.file {
					module.referent.inherit(&referent);
				}
				Self::print_error_location_basic(&location, message);
			}

			// Print the stack.
			for mut location in error.stack.into_iter().flatten() {
				if let tg::error::File::Module(module) = &mut location.file {
					module.referent.inherit(&referent);
				}
				Self::print_error_location_basic(&location, message);
			}

			// Add the source to the stack.
			if let Some(source) = error.source {
				let mut source = source.map(|item| *item);
				source.inherit(&referent);
				stack.push(source);
			}
		}
	}

	fn print_error_location_basic(location: &tg::error::Location, _message: &str) {
		match &location.file {
			tg::error::File::Internal(path) => {
				eprintln!(
					"   internal:{}:{}:{}",
					path.display(),
					location.range.start.line + 1,
					location.range.start.character + 1
				);
			},
			tg::error::File::Module(module) => {
				Self::print_location_basic(module, &location.range);
			},
		}
	}

	fn print_location_basic(module: &tg::Module, range: &tg::Range) {
		match &module.referent.item {
			tg::module::Item::Path(path) => {
				eprintln!(
					"   {}:{}:{}",
					path.display(),
					range.start.line + 1,
					range.start.character + 1,
				);
			},
			tg::module::Item::Object(_) => {
				let mut name = String::new();
				if let Some(tag) = module.referent.tag() {
					write!(name, "{tag}").unwrap();
					if let Some(path) = module.referent.path() {
						write!(name, ":").unwrap();
						let path = crate::util::path::normalize(path);
						write!(name, "{}", path.display()).unwrap();
					}
				} else if let Some(path) = module.referent.path() {
					let path = std::env::current_dir()
						.ok()
						.and_then(|cwd| {
							let path = std::fs::canonicalize(cwd.join(path)).ok()?;
							let path = crate::util::path::diff(&cwd, &path).ok()?;
							if path.is_relative() && !path.starts_with("..") {
								return Some(PathBuf::from(".").join(path));
							}
							Some(path)
						})
						.unwrap_or_else(|| path.clone());
					write!(name, "{}", path.display()).unwrap();
				} else {
					write!(name, "<unknown>").unwrap();
				}
				eprintln!(
					"   {name}:{}:{}",
					range.start.line + 1,
					range.start.character + 1,
				);
			},
		}
	}

	pub(crate) async fn print_error(&mut self, error: tg::Referent<tg::Error>) {
		let internal = self
			.config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.is_some_and(|advanced| advanced.internal_error_locations);
		let mut stack = vec![error];
		while let Some(error) = stack.pop() {
			let (referent, error) = error.replace(());

			// Print the message.
			let message = error.message.as_deref().unwrap_or("an error occurred");
			eprintln!("{} {message}", "->".red());

			// Print the values.
			for (key, value) in &error.values {
				let key = key.as_str();
				let value = value.as_str();
				eprintln!("   {key} = {value}");
			}

			// Print the location.
			if let Some(mut location) = error.location {
				if let tg::error::File::Module(module) = &mut location.file {
					module.referent.inherit(&referent);
				}
				self.print_error_location(&location, message, internal)
					.await;
			}

			// Print the stack.
			for mut location in error.stack.into_iter().flatten() {
				if let tg::error::File::Module(module) = &mut location.file {
					module.referent.inherit(&referent);
				}
				self.print_error_location(&location, message, internal)
					.await;
			}

			// Add the source to the stack.
			if let Some(source) = error.source {
				let mut source = source.map(|item| *item);
				source.inherit(&referent);
				stack.push(source);
			}
		}
	}

	async fn print_error_location(
		&mut self,
		location: &tg::error::Location,
		message: &str,
		internal: bool,
	) {
		match &location.file {
			tg::error::File::Internal(path) => {
				if internal {
					eprintln!(
						"   internal:{}:{}:{}",
						path.display(),
						location.range.start.line + 1,
						location.range.start.character + 1
					);
				}
			},
			tg::error::File::Module(module) => {
				let location = tg::Location {
					module: module.to_data(),
					range: location.range,
				};
				self.print_location(&location, message).await;
			},
		}
	}

	pub(crate) async fn print_diagnostic(&mut self, referent: tg::Referent<tg::Diagnostic>) {
		let diagnostic = referent.item();
		let severity = match diagnostic.severity {
			tg::diagnostic::Severity::Error => "error".red().bold(),
			tg::diagnostic::Severity::Warning => "warning".yellow().bold(),
			tg::diagnostic::Severity::Info => "info".blue().bold(),
			tg::diagnostic::Severity::Hint => "hint".cyan().bold(),
		};
		eprintln!("{severity} {}", diagnostic.message);
		if let Some(location) = &diagnostic.location {
			Box::pin(self.print_location(location, &diagnostic.message)).await;
		}
	}

	async fn print_location(&mut self, location: &tg::Location, message: &str) {
		let tg::Location { module, range } = location;
		match &module.referent.item {
			tg::module::data::Item::Path(path) => {
				let mut name = String::new();
				write!(name, "{}", path.display()).unwrap();
				if true {
					Self::print_code_path(&name, range, message, path).await;
				} else {
					eprintln!(
						"   {name}:{}:{}",
						location.range.start.line + 1,
						location.range.start.character + 1,
					);
				}
			},
			tg::module::data::Item::Object(object) => {
				let mut name = String::new();
				if let Some(tag) = module.referent.tag() {
					write!(name, "{tag}").unwrap();
					if let Some(path) = module.referent.path() {
						write!(name, ":").unwrap();
						let path = crate::util::path::normalize(path);
						write!(name, "{}", path.display()).unwrap();
					}
				} else if let Some(path) = module.referent.path() {
					let path = if path.is_relative() {
						std::env::current_dir()
							.ok()
							.and_then(|cwd| {
								let path = std::fs::canonicalize(cwd.join(path)).ok()?;
								let path = crate::util::path::diff(&cwd, &path).ok()?;
								if path.is_relative() && !path.starts_with("..") {
									return Some(PathBuf::from(".").join(path));
								}
								Some(path)
							})
							.unwrap_or_else(|| path.clone())
					} else {
						crate::util::path::normalize(path)
					};
					write!(name, "{}", path.display()).unwrap();
				} else {
					write!(name, "<unknown>").unwrap();
				}
				if true {
					self.print_code_object(&name, range, message, object).await;
				} else {
					eprintln!(
						"   {name}:{}:{}",
						location.range.start.line + 1,
						location.range.start.character + 1,
					);
				}
			},
		}
	}

	async fn print_code_path(name: &str, range: &tg::Range, message: &str, path: &Path) {
		let Ok(file) = tokio::fs::File::open(path).await else {
			return;
		};
		let mut reader = tokio::io::BufReader::new(file);
		let mut buffer = Vec::new();
		reader.read_to_end(&mut buffer).await.ok();
		let Ok(text) = String::from_utf8(buffer) else {
			return;
		};
		Self::print_code(name, range, message, text);
	}

	async fn print_code_object(
		&mut self,
		name: &str,
		range: &tg::Range,
		message: &str,
		object: &tg::object::Id,
	) {
		let Ok(handle) = self.handle().await else {
			return;
		};
		let Ok(file) = object.clone().try_unwrap_file() else {
			return;
		};
		let Ok(text) = tg::File::with_id(file).text(&handle).await else {
			return;
		};
		Self::print_code(name, range, message, text);
	}

	fn print_code(name: &str, range: &tg::Range, message: &str, text: String) {
		let range = range.to_byte_range_in_string(&text);
		let label = miette::LabeledSpan::new_with_span(Some(message.to_owned()), range);
		let code = miette::NamedSource::new(name, text).with_language("JavaScript");
		let diagnostic = miette::diagnostic!(labels = vec![label], "");
		let report = miette::Report::new(diagnostic).with_source_code(code);
		let mut string = String::new();
		write!(string, "{report:?}").unwrap();
		let string = &string[string.find('\n').unwrap() + 1..].trim_end();
		eprintln!("{string}");
	}
}
