use {
	crate::Cli,
	anstream::eprintln,
	crossterm::style::Stylize as _,
	std::{fmt::Write as _, path::Path},
	tangram_client as tg,
	tokio::io::AsyncReadExt,
};

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

			// Print the diagnostics.
			for mut diagnostic in error.diagnostics.into_iter().flatten() {
				if let Some(location) = &mut diagnostic.location {
					location.module.referent.inherit(&referent);
				}
				let severity = match diagnostic.severity {
					tg::diagnostic::Severity::Error => "error",
					tg::diagnostic::Severity::Warning => "warning",
					tg::diagnostic::Severity::Info => "info",
					tg::diagnostic::Severity::Hint => "hint",
				};
				eprintln!("{} {}", severity, diagnostic.message);
				if let Some(location) = &diagnostic.location {
					Self::print_location_basic(&location.module, &location.range);
				}
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
				eprint!(
					"   {}:{}:{}",
					path.display(),
					range.start.line + 1,
					range.start.character + 1,
				);
			},
			tg::module::Item::Object(_) => {
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
				eprint!(
					"   {title}:{}:{}",
					range.start.line + 1,
					range.start.character + 1,
				);
				eprintln!();
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

			// Print the diagnostics.
			for mut diagnostic in error.diagnostics.into_iter().flatten() {
				if let Some(location) = &mut diagnostic.location {
					location.module.referent.inherit(&referent);
				}
				let diagnostic_referent = tg::Referent::with_item(diagnostic);
				self.print_diagnostic(diagnostic_referent).await;
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
					module: module.clone(),
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
			tg::module::Item::Path(path) => {
				if true {
					Self::print_code_path(&path.display().to_string(), range, message, path).await;
				} else {
					eprintln!(
						"   {}:{}:{}",
						path.display(),
						location.range.start.line + 1,
						location.range.start.character + 1,
					);
				}
			},
			tg::module::Item::Object(object) => {
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
					self.print_code_object(&title, range, message, &object.id())
						.await;
				} else {
					eprintln!(
						"   {title}:{}:{}",
						location.range.start.line + 1,
						location.range.start.character + 1,
					);
				}
			},
		}
	}

	async fn print_code_path(title: &str, range: &tg::Range, message: &str, path: &Path) {
		let Ok(file) = tokio::fs::File::open(path).await else {
			return;
		};
		let mut reader = tokio::io::BufReader::new(file);
		let mut buffer = Vec::new();
		reader.read_to_end(&mut buffer).await.ok();
		let Ok(text) = String::from_utf8(buffer) else {
			return;
		};
		Self::print_code(title, range, message, text);
	}

	async fn print_code_object(
		&mut self,
		title: &str,
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
		Self::print_code(title, range, message, text);
	}

	fn print_code(title: &str, range: &tg::Range, message: &str, text: String) {
		let range = range
			.try_to_byte_range_in_string(&text)
			.unwrap_or(0..text.len());
		let label = miette::LabeledSpan::new_with_span(Some(message.to_owned()), range);
		let code = miette::NamedSource::new(title, text).with_language("JavaScript");
		let diagnostic = miette::diagnostic!(labels = vec![label], "hello world wow");
		let report = miette::Report::new(diagnostic).with_source_code(code);
		let mut string = String::new();
		write!(string, "{report:?}").unwrap();
		let string = &string[string.find('\n').unwrap() + 1..].trim_end();
		eprintln!("{string}");
	}
}
