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
	pub(crate) async fn print_error(
		&mut self,
		error: &tg::Error,
		referent: Option<&tg::Referent<tg::object::Id>>,
	) {
		let internal = self
			.config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.error_trace_options.clone())
			.unwrap_or_default()
			.internal;
		let path = referent.and_then(|referent| referent.path.clone());
		let tag = referent.and_then(|referent| referent.tag.clone());
		let mut stack = vec![(path, tag, error)];
		while let Some((path, tag, error)) = stack.pop() {
			// Print the message.
			let message = error.message.as_deref().unwrap_or("an error occurred");
			eprintln!("{} {message}", "->".red());

			// Print the location.
			if let Some(location) = &error.location {
				self.print_error_location(
					&mut path.clone(),
					&mut tag.clone(),
					location,
					internal,
					message,
				)
				.await;
			}

			// Print the stack.
			let mut stack_path = path.clone();
			let mut stack_tag = tag.clone();
			for location in error.stack.iter().flatten() {
				self.print_error_location(
					&mut stack_path,
					&mut stack_tag,
					location,
					internal,
					message,
				)
				.await;
			}

			// Print the values.
			for (key, value) in &error.values {
				let key = key.as_str();
				let value = value.as_str();
				eprintln!("   {key} = {value}");
			}

			// Add the source to the stack.
			if let Some(source) = &error.source {
				let mut path = path;
				let mut tag = tag;
				match (&path, &source.path, &tag, &source.tag) {
					(Some(path_), Some(source), None, None) => {
						path.replace(path_.parent().unwrap().join(source));
					},
					(None, Some(source), None, None) => {
						path.replace(source.clone());
					},
					(_, path_, _, Some(tag_)) => {
						tag.replace(tag_.clone());
						path = path_.clone();
					},
					_ => (),
				}
				stack.push((path, tag, source.item.as_ref()));
			}
		}
	}

	async fn print_error_location(
		&mut self,
		path: &mut Option<PathBuf>,
		tag: &mut Option<tg::Tag>,
		location: &tg::error::Location,
		internal: bool,
		message: &str,
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
				self.print_location(path, tag, &location, message).await;
			},
		}
	}

	pub(crate) async fn print_diagnostic(&mut self, diagnostic: &tg::Diagnostic) {
		let severity = match diagnostic.severity {
			tg::diagnostic::Severity::Error => "error".red().bold(),
			tg::diagnostic::Severity::Warning => "warning".yellow().bold(),
			tg::diagnostic::Severity::Info => "info".blue().bold(),
			tg::diagnostic::Severity::Hint => "hint".cyan().bold(),
		};
		eprintln!("{severity} {}", diagnostic.message);
		if let Some(location) = &diagnostic.location {
			Box::pin(self.print_location(&mut None, &mut None, location, &diagnostic.message))
				.await;
		}
	}

	async fn print_location(
		&mut self,
		path: &mut Option<PathBuf>,
		tag: &mut Option<tg::Tag>,
		location: &tg::Location,
		message: &str,
	) {
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
				if let Some(tag_) = &module.referent.tag {
					tag.replace(tag_.clone());
					path.take();
				}
				if let Some(path_) = &module.referent.path {
					let path_ = path
						.take()
						.map_or_else(|| path_.clone(), |path| path.parent().unwrap().join(path_));
					path.replace(path_);
				}
				let mut name = String::new();
				if let Some(tag) = &tag {
					write!(name, "{tag}").unwrap();
					if let Some(path) = &path {
						write!(name, ":").unwrap();
						let path = crate::util::normalize_path(path);
						write!(name, "{}", path.display()).unwrap();
					}
				} else if let Some(path) = &path {
					let path = std::env::current_dir()
						.ok()
						.and_then(|cwd| {
							let path = std::fs::canonicalize(cwd.join(path)).ok()?;
							let path = crate::util::path_diff(&cwd, &path).ok()?;
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
