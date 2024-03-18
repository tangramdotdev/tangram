pub use glob::Pattern;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};
use thiserror::Error;

/// A result alias that defaults to `Error` as the error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An error.
#[derive(Clone, Debug, Error, serde::Deserialize, serde::Serialize)]
#[error("{message}")]
pub struct Error {
	/// The error's message.
	pub message: String,

	/// The optional location of where the error occurred.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	/// An optional stack trace associated with the error.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stack: Option<Vec<Location>>,

	/// An optional error that this error wraps.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub source: Option<Arc<Error>>,

	/// A map of key/value pairs of context associated with the error.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub values: BTreeMap<String, String>,
}

/// An error location.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Location {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub symbol: Option<String>,
	pub source: String,
	pub line: u32,
	pub column: u32,
}

pub struct Trace<'e, 'o> {
	error: &'e Error,
	options: TraceOptions<'o>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TraceOptions<'o> {
	pub reverse: bool,
	pub exclude: &'o [Pattern],
	pub include: &'o [Pattern],
}

impl Error {
	/// Construct a [Trace] from an error, which can be used to display a helpful error trace.
	#[must_use]
	pub fn trace<'e, 'o>(&'e self, options: TraceOptions<'o>) -> Trace
	where
		'o: 'e,
	{
		Trace {
			error: self,
			options,
		}
	}
}

impl From<Box<dyn std::error::Error + Send + Sync + 'static>> for Error {
	fn from(value: Box<dyn std::error::Error + Send + Sync + 'static>) -> Self {
		match value.downcast::<Error>() {
			Ok(error) => *error,
			Err(error) => Self {
				message: error.to_string(),
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
			message: value.to_string(),
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
			source: location.file().to_owned(),
			line: location.line() - 1,
			column: location.column() - 1,
		}
	}
}

impl<'o> TraceOptions<'o> {
	#[must_use]
	pub fn location_included(&self, location: &Location) -> bool {
		let is_excluded = self.exclude.iter().any(|pat| pat.matches(&location.source));
		let is_included = self.include.iter().any(|pat| pat.matches(&location.source));
		is_included || !is_excluded
	}
}

impl<'e, 'o> std::fmt::Display for Trace<'e, 'o> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		// Collect the error trace.
		let mut errors = vec![self.error];
		while let Some(next) = errors.last().unwrap().source.as_ref() {
			errors.push(next);
		}

		if self.options.reverse {
			errors.reverse();
		}

		// Display each error in the desired order.
		for (n, error) in errors.iter().enumerate() {
			if n != 0 {
				writeln!(f)?;
			}
			let Error {
				message,
				location,
				stack,
				values,
				..
			} = error;
			write!(f, " {message}")?;

			if let Some(location) = &location {
				if self.options.location_included(location) {
					write!(f, " {location}")?;
				}
			}

			for (name, value) in values {
				writeln!(f)?;
				write!(f, "   {name} = {value}")?;
			}

			let mut stack = stack
				.iter()
				.flatten()
				.filter(|location| self.options.location_included(location))
				.collect::<Vec<_>>();
			if self.options.reverse {
				stack.reverse();
			}
			for location in stack {
				writeln!(f)?;
				write!(f, "   ")?;
				write!(f, "   {location}")?;
			}
		}

		Ok(())
	}
}

impl std::fmt::Display for Location {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "at")?;
		if let Some(symbol) = &self.symbol {
			write!(f, " {symbol}")?;
		}
		write!(
			f,
			" ({}:{}:{})",
			self.source,
			self.line + 1,
			self.column + 1
		)
	}
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

pub fn default_exclude() -> Vec<String> {
	let patterns = [
		"[[]global[]]:/**/*.ts",
		"packages/cli/**/*.rs",
		"packages/client/**/*.rs",
		"packages/error/**/*.rs",
		"packages/eslint/**/*.rs",
		"packages/fuse/**/*.rs",
		"packages/install/**/*.rs",
		"packages/language/**/*.rs",
		"packages/nfs/**/*.rs",
		"packages/runtime/**/*.rs",
		"packages/server/**/*.rs",
		"packages/util/**/*.rs",
	];
	patterns.into_iter().map(String::from).collect()
}

pub fn default_include() -> Vec<String> {
	Vec::new()
}

/// Generate an [Error].
///
/// Usage:
/// ```rust
/// use tangram_error::{error, Location};
/// error!("error message");
/// error!("error message with interpolation {}", 42);
///
/// let name = "value";
/// error!(%name, "error message with a named value (pretty printed)");
/// error!(?name, "error message with a named value (debug printed)");
///
/// let error = std::io::Error::last_os_error();
/// error!(source = error, "an error that wraps an existing error");
///
/// let stack_trace = vec![
///     Location {
///         symbol: Some("my_cool_function".into()),
///         source: "foo.rs".into(),
///         line: 123,
///         column: 456,
///     }
/// ];
/// error!(stack = stack_trace, "an error with a custom stack trace");
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
	({ $error:ident }, source=$source:expr, $($arg:tt)*) => {
		$error.source.replace(std::sync::Arc::new({
			let source: Box<dyn std::error::Error + Send + Sync + 'static> = Box::new($source);
			source.into()
		}));
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, stack = $stack:expr, $($arg:tt)*) => {
		$error.stack.replace($stack);
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, $($arg:tt)*) => {
		$error.message = format!($($arg)*);
	};
	($($arg:tt)*) => {{
		let mut __error = $crate::Error {
			message: String::new(),
			location: Some($crate::Location {
				symbol: Some($crate::function!().into()),
				source: file!().to_owned(),
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

#[cfg(test)]
mod tests {
	use glob::Pattern;

	use crate::{error, function, Location, TraceOptions};

	#[test]
	fn error_macro() {
		let options = TraceOptions {
			exclude: &[Pattern::new("./packages/error/src/lib.rs").unwrap()],
			..Default::default()
		};

		let foo = "foo";
		let bar = "bar";
		let baz = "baz";
		let error = error!(?foo, %bar, ?baz, "{} bar {baz}", foo);
		let trace = error.trace(options).to_string();
		println!("{trace}");

		let source = std::io::Error::other("unexpected error");
		let error = error!(source = source, "an error occurred");
		let trace = error.trace(options).to_string();
		println!("{trace}");

		let stack = vec![Location {
			symbol: None,
			source: "foobar.rs".to_owned(),
			line: 123,
			column: 456,
		}];
		let error = error!(stack = stack, "an error occurred");
		let trace = error.trace(options).to_string();
		println!("{trace}");
	}

	#[test]
	fn function_macro() {
		let f = function!();
		assert_eq!(f, "tangram_error::tests::function_macro");
	}
}
