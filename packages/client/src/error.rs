use crate as tg;
use serde_with::serde_as;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

/// A result alias that defaults to `Error` as the error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An error.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Error {
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
	pub source: Option<Arc<Error>>,

	/// Values associated with the error.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub values: BTreeMap<String, String>,
}

/// An error location.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Location {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub symbol: Option<String>,
	pub source: Source,
	pub line: u32,
	pub column: u32,
}

/// An error location's source.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Source {
	Internal(tg::Path),
	Package(tg::package::Id),
}

pub struct Trace<'a> {
	pub error: &'a Error,
	pub options: &'a TraceOptions,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct TraceOptions {
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub internal: bool,
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub reverse: bool,
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

impl Source {
	#[must_use]
	pub fn is_internal(&self) -> bool {
		matches!(self, Self::Internal { .. })
	}

	#[must_use]
	pub fn is_external(&self) -> bool {
		matches!(self, Self::Package { .. })
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
			.map(|source| source.as_ref() as &(dyn std::error::Error + 'static))
	}
}

impl From<Box<dyn std::error::Error + Send + Sync + 'static>> for Error {
	fn from(value: Box<dyn std::error::Error + Send + Sync + 'static>) -> Self {
		match value.downcast::<Error>() {
			Ok(error) => *error,
			Err(error) => Self {
				message: Some(error.to_string()),
				location: None,
				stack: None,
				source: error.source().map(Into::into).map(Arc::new),
				values: BTreeMap::new(),
			},
		}
	}
}

impl From<&(dyn std::error::Error + 'static)> for Error {
	fn from(value: &(dyn std::error::Error + 'static)) -> Self {
		Self {
			message: Some(value.to_string()),
			location: None,
			stack: None,
			source: value.source().map(Into::into).map(Arc::new),
			values: BTreeMap::new(),
		}
	}
}

impl<'a> From<&'a std::panic::Location<'a>> for Location {
	fn from(location: &'a std::panic::Location<'a>) -> Self {
		Self {
			symbol: None,
			source: Source::Internal(location.file().parse().unwrap()),
			line: location.line() - 1,
			column: location.column() - 1,
		}
	}
}

impl<'a> std::fmt::Display for Trace<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut errors = vec![self.error];
		while let Some(next) = errors.last().unwrap().source.as_ref() {
			errors.push(next);
		}
		if self.options.reverse {
			errors.reverse();
		}

		for error in errors {
			let message = error.message.as_deref().unwrap_or("an error occurred");
			writeln!(f, "-> {message}")?;
			if let Some(location) = &error.location {
				if !location.source.is_internal() || self.options.internal {
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
				if !location.source.is_internal() || self.options.internal {
					writeln!(f, "   {location}")?;
				}
			}
		}

		Ok(())
	}
}

impl std::fmt::Display for Location {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}:{}:{}", self.source, self.line + 1, self.column + 1)?;
		if let Some(symbol) = &self.symbol {
			write!(f, " {symbol}")?;
		}
		Ok(())
	}
}

impl std::fmt::Display for Source {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Source::Internal(path) => {
				write!(f, "{path}")?;
			},
			Source::Package(package) => {
				write!(f, "{package}")?;
			},
		}
		Ok(())
	}
}

impl serde::ser::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: std::fmt::Display,
	{
		Self {
			message: Some(msg.to_string()),
			location: None,
			stack: None,
			source: None,
			values: BTreeMap::new(),
		}
	}
}

impl serde::de::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: std::fmt::Display,
	{
		Self {
			message: Some(msg.to_string()),
			location: None,
			stack: None,
			source: None,
			values: BTreeMap::new(),
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
///         source: tg::error::Source::Internal { path: "foo.rs".parse().unwrap() },
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
	({ $error:ident }, !$source:expr, $($arg:tt)*) => {
		let source = Box::<dyn std::error::Error + Send + Sync + 'static>::from($source);
		let source = $crate::Error::from(source);
		let source = std::sync::Arc::new(source);
		$error.source.replace(source);
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, source = $source:expr, $($arg:tt)*) => {
		let source = Box::<dyn std::error::Error + Send + Sync + 'static>::from($source);
		let source = $crate::Error::from(source);
		let source = std::sync::Arc::new(source);
		$error.source.replace(source);
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
		let mut __error = $crate::Error {
			message: Some(String::new()),
			location: Some($crate::error::Location {
				symbol: Some($crate::function!().to_owned()),
				source: $crate::error::Source::Internal(file!().parse().unwrap()),
				line: line!() - 1,
				column: column!() - 1,
			}),
			source: None,
			stack: None,
			values: std::collections::BTreeMap::new(),
		};
		$crate::error!({ __error }, $($arg)*);
		$crate::Error::from(__error)
	}};
}

#[macro_export]
macro_rules! function {
	() => {{
		struct __Dummy {}
		std::any::type_name::<__Dummy>()
			.strip_suffix("::__Dummy")
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
			source: tg::error::Source::Internal("foobar.rs".parse().unwrap()),
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
}
