use {
	super::Data,
	crate::prelude::*,
	std::{collections::BTreeMap, fmt, path::PathBuf},
};

#[derive(Clone, Debug, Default)]
pub struct Error {
	pub code: Option<tg::error::Code>,
	pub diagnostics: Option<Vec<tg::Diagnostic>>,
	pub kind: Option<tg::error::Kind>,
	pub location: Option<tg::error::Location>,
	pub message: Option<String>,
	pub source: Option<tg::Referent<tg::Either<Box<tg::error::Object>, Box<tg::Error>>>>,
	pub stack: Option<Vec<tg::error::Location>>,
	pub values: BTreeMap<String, String>,
}

impl Error {
	/// Create an error at a location in the internal source, as the `tg::error!` macro does.
	#[doc(hidden)]
	#[must_use]
	pub fn with_internal_location(symbol: &str, file: &str, line: u32, column: u32) -> Self {
		let symbol = symbol.strip_suffix("::Marker").unwrap_or(symbol).to_owned();
		let position = tg::Position {
			line: line - 1,
			character: column - 1,
		};
		let range = tg::Range {
			start: position,
			end: position,
		};
		let location = tg::error::Location {
			symbol: Some(symbol),
			file: tg::error::File::Internal(PathBuf::from(file)),
			range,
		};
		Self {
			location: Some(location),
			message: Some(String::new()),
			..Default::default()
		}
	}

	/// Set a value with its display representation.
	#[doc(hidden)]
	pub fn set_display_value(&mut self, name: &str, value: &dyn fmt::Display) {
		self.values.insert(name.to_owned(), value.to_string());
	}

	/// Set a value with its debug representation.
	#[doc(hidden)]
	pub fn set_debug_value(&mut self, name: &str, value: &dyn fmt::Debug) {
		self.values.insert(name.to_owned(), format!("{value:?}"));
	}

	/// Set the message from format arguments.
	#[doc(hidden)]
	pub fn set_message(&mut self, arguments: fmt::Arguments<'_>) {
		let message = arguments
			.as_str()
			.map_or_else(|| fmt::format(arguments), ToOwned::to_owned);
		self.message = Some(message);
	}

	/// Set the source.
	#[doc(hidden)]
	pub fn set_source(
		&mut self,
		source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
	) {
		self.set_boxed_source(source.into());
	}

	fn set_boxed_source(&mut self, source: Box<dyn std::error::Error + Send + Sync + 'static>) {
		let source = tg::Error::from(source);
		let node = match source.state().object() {
			Some(object) => {
				let object = object.unwrap_error_ref().as_ref().clone();
				tg::Either::Left(Box::new(object))
			},
			None => tg::Either::Right(Box::new(source)),
		};
		let source = tg::Referent::with_node(node);
		self.source.replace(source);
	}

	/// Get the kind without loading source objects.
	#[must_use]
	pub fn kind(&self) -> Option<tg::error::Kind> {
		// Inherit the source's kind, except that only an error's own kind can make it unavailable.
		self.kind.or_else(|| {
			let kind = match &self.source.as_ref()?.node {
				tg::Either::Left(object) => object.kind(),
				tg::Either::Right(error) => error.kind(),
			};
			kind.filter(|kind| *kind != tg::error::Kind::Unavailable)
		})
	}

	pub fn to_data(&self) -> Data {
		let code = self.code;
		let diagnostics = self
			.diagnostics
			.as_ref()
			.map(|data| data.iter().map(tg::Diagnostic::to_data).collect());
		let kind = self.kind;
		let location = self.location.as_ref().map(tg::error::Location::to_data);
		let message = self.message.clone();
		let source = self.source.as_ref().map(|source| {
			source.clone().map(|node| match node {
				tg::Either::Left(object) => {
					let data = object.to_data();
					tg::Either::Left(Box::new(data))
				},
				tg::Either::Right(instance) => {
					let id = instance.id();
					tg::Either::Right(id)
				},
			})
		});
		let stack = self
			.stack
			.as_ref()
			.map(|stack| stack.iter().map(tg::error::Location::to_data).collect());
		let values = self.values.clone();
		Data {
			code,
			diagnostics,
			kind,
			location,
			message,
			source,
			stack,
			values,
		}
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let code = data.code;
		let diagnostics = data
			.diagnostics
			.map(|data| {
				data.into_iter()
					.map(tg::Diagnostic::try_from_data)
					.collect()
			})
			.transpose()?;
		let kind = data.kind;
		let location = data
			.location
			.map(tg::error::Location::try_from_data)
			.transpose()?;
		let message = data.message;
		let source = data
			.source
			.map(|source| {
				source.try_map(|node| {
					let error = match node {
						tg::Either::Left(data) => {
							let object = Error::try_from_data(*data)?;
							tg::Either::Left(Box::new(object))
						},
						tg::Either::Right(id) => {
							let error = tg::Error::with_id(id);
							tg::Either::Right(Box::new(error))
						},
					};
					Ok::<_, tg::Error>(error)
				})
			})
			.transpose()?;
		let stack = data
			.stack
			.map(|stack| {
				stack
					.into_iter()
					.map(tg::error::Location::try_from_data)
					.collect()
			})
			.transpose()?;
		let values = data.values;
		Ok(Self {
			code,
			diagnostics,
			kind,
			location,
			message,
			source,
			stack,
			values,
		})
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		let mut children = Vec::new();
		if let Some(diagnostics) = &self.diagnostics {
			for diagnostic in diagnostics {
				children.extend(diagnostic.children());
			}
		}
		if let Some(location) = &self.location {
			children.extend(location.children());
		}
		if let Some(stack) = &self.stack {
			for location in stack {
				children.extend(location.children());
			}
		}
		if let Some(source) = &self.source {
			let source_children = match &source.node {
				tg::Either::Left(object) => object.children(),
				tg::Either::Right(error) => vec![(*error.clone()).into()],
			};
			for child in &source_children {
				child.inherit_location(source.options.location.as_ref());
				child.inherit_tokens(&source.options.tokens);
			}
			children.extend(source_children);
		}
		children
	}
}
