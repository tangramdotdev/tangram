use crate::{self as tg, util::serde::is_false};
use serde_with::serde_as;
use std::{collections::BTreeMap, fmt::Debug, path::PathBuf, sync::Arc};

/// A result alias that defaults to `Error` as the error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An error.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Error {
	/// The error code.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub code: Option<Code>,

	/// The error's message.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message: Option<String>,

	/// The location where the error occurred.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	/// A stack trace associated with the error.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stack: Option<Vec<Location>>,

	/// The error's source.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub source: Option<Source>,

	/// Values associated with the error.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Code {
	Cancelation,
}

/// An error location.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Location {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub symbol: Option<String>,
	pub file: File,
	pub line: u32,
	pub column: u32,
}

/// An error location's source.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum File {
	Internal(PathBuf),
	Module(tg::module::Data),
}

pub struct Trace<'a> {
	pub error: &'a Error,
	pub options: &'a TraceOptions,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TraceOptions {
	#[serde(default, skip_serializing_if = "is_false")]
	pub internal: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub reverse: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Source {
	pub error: Arc<Error>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub referent: Option<tg::Referent<tg::object::Id>>,
}

impl Error {
	#[must_use]
	pub fn trace<'a>(&'a self, options: &'a TraceOptions) -> Trace<'a> {
		Trace {
			error: self,
			options,
		}
	}
}

impl File {
	#[must_use]
	pub fn is_internal(&self) -> bool {
		matches!(self, Self::Internal { .. })
	}

	#[must_use]
	pub fn is_module(&self) -> bool {
		matches!(self, Self::Module { .. })
	}
}

pub fn ok<T>(value: T) -> Result<T> {
	Ok(value)
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let message = self.message.as_deref().unwrap_or("an error occurred");
		write!(f, "{message}")
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		self.source
			.as_ref()
			.map(|source| source.error.as_ref() as &(dyn std::error::Error + 'static))
	}
}

impl From<Box<dyn std::error::Error + Send + Sync + 'static>> for Error {
	fn from(value: Box<dyn std::error::Error + Send + Sync + 'static>) -> Self {
		match value.downcast::<Error>() {
			Ok(error) => *error,
			Err(error) => Self {
				code: None,
				message: Some(error.to_string()),
				location: None,
				stack: None,
				source: error.source().map(Into::into).map(|error| Source {
					error: Arc::new(error),
					referent: None,
				}),
				values: BTreeMap::new(),
			},
		}
	}
}

impl From<&(dyn std::error::Error + 'static)> for Error {
	fn from(value: &(dyn std::error::Error + 'static)) -> Self {
		Self {
			code: None,
			message: Some(value.to_string()),
			location: None,
			stack: None,
			source: value.source().map(Into::into).map(|error| Source {
				error: Arc::new(error),
				referent: None,
			}),
			values: BTreeMap::new(),
		}
	}
}

impl<'a> From<&'a std::panic::Location<'a>> for Location {
	fn from(location: &'a std::panic::Location<'a>) -> Self {
		Self {
			symbol: None,
			file: File::Internal(location.file().parse().unwrap()),
			line: location.line() - 1,
			column: location.column() - 1,
		}
	}
}

impl std::fmt::Display for Trace<'_> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut errors = vec![self.error];
		while let Some(next) = errors.last().unwrap().source.as_ref() {
			errors.push(next.error.as_ref());
		}
		if self.options.reverse {
			errors.reverse();
		}

		for error in errors {
			let message = error.message.as_deref().unwrap_or("an error occurred");
			writeln!(f, "-> {message}")?;
			if let Some(location) = &error.location {
				if !location.file.is_internal() || self.options.internal {
					writeln!(f, "   {location}")?;
				}
			}

			for (name, value) in &error.values {
				let name = name.as_str();
				let value = value.as_str();
				writeln!(f, "   {name} = {value}")?;
			}

			let mut stack = error.stack.iter().flatten().collect::<Vec<_>>();
			if self.options.reverse {
				stack.reverse();
			}
			for location in stack {
				if !location.file.is_internal() || self.options.internal {
					writeln!(f, "   {location}")?;
				}
			}
		}

		Ok(())
	}
}

impl std::fmt::Display for Location {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}:{}:{}", self.file, self.line + 1, self.column + 1)?;
		if let Some(symbol) = &self.symbol {
			write!(f, " {symbol}")?;
		}
		Ok(())
	}
}

impl std::fmt::Display for File {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			File::Internal(path) => {
				write!(f, "(internal) {}", path.display())?;
			},
			File::Module(module) => {
				write!(f, "{module}")?;
			},
		}
		Ok(())
	}
}

impl TryFrom<Error> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Error) -> Result<Self, Self::Error> {
		let data = serde_json::to_string(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize the event"))?;
		let event = tangram_http::sse::Event {
			event: Some("error".to_owned()),
			data,
			..Default::default()
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for Error {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		if value.event.as_ref().is_none_or(|event| event != "error") {
			return Err(tg::error!("invalid event"));
		}
		let error = serde_json::from_str(&value.data)
			.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
		Ok(error)
	}
}

impl Default for TraceOptions {
	fn default() -> Self {
		Self {
			internal: true,
			reverse: false,
		}
	}
}

/// Create an [Error].
///
/// Usage:
/// ```rust
/// use tangram_client as tg;
/// tg::error!("error message");
/// tg::error!("error message with interpolation {}", 42);
///
/// let name = "value";
/// tg::error!(%name, "error message with a named value (pretty printed)");
/// tg::error!(?name, "error message with a named value (debug printed)");
///
/// let error = std::io::Error::last_os_error();
/// tg::error!(source = error, "an error that wraps an existing error");
///
/// let stack_trace = vec![
///     tg::error::Location {
///         symbol: Some("my_cool_function".into()),
///         source: tg::error::Source::Internal("foo.rs".parse().unwrap()),
///         line: 123,
///         column: 456,
///     }
/// ];
/// tg::error!(stack = stack_trace, "an error with a custom stack trace");
/// ```
#[macro_export]
macro_rules! error {
	({ $error:ident }, %$name:ident, $($arg:tt)*) => {
		$error.values.insert(stringify!($name).to_owned(), $name.to_string());
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, ?$name:ident, $($arg:tt)*) => {
		$error.values.insert(stringify!($name).to_owned(), format!("{:?}", $name));
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, %$name:ident = $value:expr, $($arg:tt)*) => {
		$error.values.insert(stringify!($name).to_owned(), $value.to_string());
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, ?$name:ident = $value:expr, $($arg:tt)*) => {
		$error.values.insert(stringify!($name).to_owned(), format!("{:?}", $value));
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, !$source:ident, $($arg:tt)*) => {
		let source = Box::<dyn std::error::Error + Send + Sync + 'static>::from($source);
		let source = $crate::Error::from(source);
		let source = $crate::error::Source { error: std::sync::Arc::new(source), 					referent: None, };
		$error.source.replace(source);
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, source = $source:expr, $($arg:tt)*) => {
		let source = Box::<dyn std::error::Error + Send + Sync + 'static>::from($source);
		let source = $crate::Error::from(source);
		let source = $crate::error::Source { error: std::sync::Arc::new(source), 					referent: None, };
		$error.source.replace(source);
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, code = $code:expr, $($arg:tt)*) => {
		$error.code.replace($code);
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, stack = $stack:expr, $($arg:tt)*) => {
		$error.stack.replace($stack);
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, $($arg:tt)*) => {
		$error.message = Some(format!($($arg)*));
	};
	($($arg:tt)*) => {{
		let mut error = $crate::Error {
			code: None,
			message: Some(String::new()),
			location: Some($crate::error::Location {
				symbol: Some($crate::function!().to_owned()),
				file: $crate::error::File::Internal(format!("{}", ::std::file!()).parse().unwrap()),
				line: line!() - 1,
				column: column!() - 1,
			}),
			source: None,
			stack: None,
			values: std::collections::BTreeMap::new(),
		};
		$crate::error!({ error }, $($arg)*);
		error
	}};
}

#[macro_export]
macro_rules! function {
	() => {{
		struct Marker;
		std::any::type_name::<Marker>()
			.strip_suffix("::Marker")
			.unwrap()
	}};
}

#[cfg(test)]
mod tests {
	use crate as tg;

	#[test]
	fn error_macro() {
		let options = tg::error::TraceOptions::default();

		let foo = "foo";
		let bar = "bar";
		let error = tg::error!(?foo, %bar, %baz = "baz", ?qux ="qux", "{}", "message");
		let trace = error.trace(&options).to_string();
		println!("{trace}");

		let source = std::io::Error::other("an io error");
		let error = tg::error!(source = source, "another error");
		let trace = error.trace(&options).to_string();
		println!("{trace}");

		let source = std::io::Error::other("an io error");
		let error = tg::error!(!source, "another error");
		let trace = error.trace(&options).to_string();
		println!("{trace}");

		let stack = vec![tg::error::Location {
			symbol: None,
			file: tg::error::File::Internal("foobar.rs".parse().unwrap()),
			line: 123,
			column: 456,
		}];
		let error = tg::error!(stack = stack, "an error occurred");
		let trace = error.trace(&options).to_string();
		println!("{trace}");
	}

	#[test]
	fn function_macro() {
		let f = function!();
		assert_eq!(f, "tangram_client::error::tests::function_macro");
	}

	#[test]
	fn serde() {
		let error = tg::error!("foo");
		let serialized = serde_json::to_string_pretty(&error).unwrap();
		let _e: tg::Error = serde_json::from_str(&serialized).expect("failed to deserialize error");
	}
}
