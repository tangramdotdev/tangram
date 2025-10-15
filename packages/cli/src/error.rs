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
				Self::print_error_location_basic(
					referent.name(),
					referent.process(),
					&location,
					message,
				);
			}

			// Print the stack.
			for mut location in error.stack.into_iter().flatten() {
				if let tg::error::File::Module(module) = &mut location.file {
					module.referent.inherit(&referent);
				}
				Self::print_error_location_basic(
					referent.name(),
					referent.process(),
					&location,
					message,
				);
			}

			// Add the source to the stack.
			if let Some(source) = error.source {
				let mut source = source.map(|item| *item);
				source.inherit(&referent);
				stack.push(source);
			}
		}
	}

	fn print_error_location_basic(
		name: Option<&str>,
		process: Option<&tg::process::Id>,
		location: &tg::error::Location,
		_message: &str,
	) {
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
				Self::print_location_basic(name, process, module, &location.range);
			},
		}
	}

	fn print_location_basic(
		name: Option<&str>,
		process: Option<&tg::process::Id>,
		module: &tg::Module,
		range: &tg::Range,
	) {
		let mut title = name.map(|name| format!("{name} ")).unwrap_or_default();
		let prefix = process
			.map(|process| format!("{process} "))
			.unwrap_or_default();
		match &module.referent.item {
			tg::module::Item::Path(path) => {
				eprint!(
					"   {prefix}{title}{}:{}:{}",
					path.display(),
					range.start.line + 1,
					range.start.character + 1,
				);
			},
			tg::module::Item::Object(_) => {
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
					"   {prefix}{title}:{}:{}",
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
				self.print_error_location(
					referent.name(),
					referent.process(),
					&location,
					message,
					internal,
				)
				.await;
			}

			// Print the stack.
			for mut location in error.stack.into_iter().flatten() {
				if let tg::error::File::Module(module) = &mut location.file {
					module.referent.inherit(&referent);
				}
				self.print_error_location(
					referent.name(),
					referent.process(),
					&location,
					message,
					internal,
				)
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
		name: Option<&str>,
		process: Option<&tg::process::Id>,
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
				self.print_location(name, process, &location, message).await;
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
			Box::pin(self.print_location(
				referent.name(),
				referent.process(),
				location,
				&diagnostic.message,
			))
			.await;
		}
	}

	async fn print_location(
		&mut self,
		name: Option<&str>,
		process: Option<&tg::process::Id>,
		location: &tg::Location,
		message: &str,
	) {
		let mut title = name.map(|name| format!("{name} ")).unwrap_or_default();
		let prefix = process
			.map(|process| format!("{process} "))
			.unwrap_or_default();

		let tg::Location { module, range } = location;
		match &module.referent.item {
			tg::module::data::Item::Path(path) => {
				write!(title, "{prefix}").unwrap();
				write!(title, "{}", path.display()).unwrap();
				if true {
					Self::print_code_path(&title, range, message, path).await;
				} else {
					eprintln!(
						"   {title}:{}:{}",
						location.range.start.line + 1,
						location.range.start.character + 1,
					);
				}
			},
			tg::module::data::Item::Object(object) => {
				write!(title, "{prefix}").unwrap();
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
					self.print_code_object(&title, range, message, object).await;
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
		let range = range.to_byte_range_in_string(&text);
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
