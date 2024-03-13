use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;

/// A result alias that defaults to `Error` as the error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An error.
#[derive(Clone, Debug, Error, serde::Deserialize, serde::Serialize)]
#[error("{message}")]
pub struct Error {
	pub message: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stack: Option<Vec<Location>>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub source: Option<Arc<Error>>,
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub values: BTreeMap<String, String>,
}

/// An error location.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Location {
	pub source: String,
	pub line: u32,
	pub column: u32,
}

pub struct Trace<'a>(&'a Error);

/// An extension trait for wrapping an error.
pub trait Wrap<E>: Sized {
	#[must_use]
	#[track_caller]
	fn wrap<C>(self, message: C) -> Error
	where
		C: std::fmt::Display,
	{
		self.wrap_with(|| message)
	}

	#[must_use]
	#[track_caller]
	fn wrap_with<C, F>(self, f: F) -> Error
	where
		C: std::fmt::Display,
		F: FnOnce() -> C;
}

/// An extension trait for wrapping `Err` in a `Result` or `None` in an `Option`.
pub trait WrapErr<T, E>: Sized {
	#[track_caller]
	fn wrap_err<M>(self, message: M) -> Result<T, Error>
	where
		M: std::fmt::Display,
	{
		self.wrap_err_with(|| message)
	}

	#[track_caller]
	fn wrap_err_with<C, F>(self, f: F) -> Result<T, Error>
	where
		C: std::fmt::Display,
		F: FnOnce() -> C;
}

impl Error {
	/// Construct a [Trace] from an error, which can be used to display a helpful error trace.
	#[must_use]
	pub fn trace(&self) -> Trace {
		Trace(self)
	}
}

impl<'a> From<&'a std::panic::Location<'a>> for Location {
	fn from(location: &'a std::panic::Location<'a>) -> Self {
		Self {
			source: location.file().to_owned(),
			line: location.line() - 1,
			column: location.column() - 1,
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

impl<E> Wrap<E> for E
where
	E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
{
	#[must_use]
	#[track_caller]
	fn wrap_with<C, F>(self, f: F) -> Error
	where
		C: std::fmt::Display,
		F: FnOnce() -> C,
	{
		Error {
			message: f().to_string(),
			location: Some(std::panic::Location::caller().into()),
			stack: None,
			source: Some(Arc::new(self.into().into())),
			values: BTreeMap::new(),
		}
	}
}

impl<T, E> WrapErr<T, E> for Result<T, E>
where
	E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
{
	#[track_caller]
	fn wrap_err_with<C, F>(self, f: F) -> Result<T, Error>
	where
		C: std::fmt::Display,
		F: FnOnce() -> C,
	{
		match self {
			Ok(value) => Ok(value),
			Err(error) => Err(Error {
				message: f().to_string(),
				location: Some(std::panic::Location::caller().into()),
				stack: None,
				source: Some(Arc::new(error.into().into())),
				values: BTreeMap::new(),
			}),
		}
	}
}

impl<T> WrapErr<T, Error> for Option<T> {
	#[track_caller]
	fn wrap_err_with<C, F>(self, f: F) -> Result<T, Error>
	where
		C: std::fmt::Display,
		F: FnOnce() -> C,
	{
		match self {
			Some(value) => Ok(value),
			None => Err(Error {
				message: f().to_string(),
				location: Some(std::panic::Location::caller().into()),
				stack: None,
				source: None,
				values: BTreeMap::new(),
			}),
		}
	}
}

impl<'a> std::fmt::Display for Trace<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let none = "\x1b[0m";
		let red = "\x1b[31m";
		let green = "\x1b[32m";
		let blue = "\x1b[34m";
		let cyan = "\x1b[36m";
		writeln!(f, "{red}error:{none}")?;
		let mut error = self.0;
		let mut first = true;
		loop {
			if !first {
				writeln!(f)?;
			}
			first = false;
			write!(f, "{red}->{none}")?;
			let Error {
				message,
				location,
				stack,
				source,
				values,
			} = error;
			write!(f, " {message}")?;
			if let Some(location) = &location {
				write!(f, " {cyan}{location}{none}")?;
			}
			for (name, value) in values.iter() {
				writeln!(f)?;
				write!(f, "   {blue}{name}{none} = {green}{value}{none}")?;
			}
			for location in stack.iter().flatten() {
				writeln!(f)?;
				write!(f, "   {cyan}{location}{none}")?;
			}
			if let Some(source) = &source {
				error = source;
			} else {
				break;
			}
		}
		Ok(())
	}
}

impl std::fmt::Display for Location {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}:{}:{}", self.source, self.line + 1, self.column + 1)
	}
}

#[doc(hidden)]
pub mod support {
	use std::{collections::BTreeMap, sync::Arc};

	#[derive(Default)]
	pub struct Builder {
		pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
		pub stack: Option<Vec<crate::Location>>,
		pub values: BTreeMap<String, String>,
		pub message: Option<String>,
		pub location: Option<crate::Location>,
	}

	impl From<Builder> for crate::Error {
		fn from(value: Builder) -> Self {
			Self {
				message: value.message.unwrap(),
				source: value.source.map(|e| Arc::new(e.into())),
				stack: value.stack,
				values: value.values,
				location: value.location,
			}
		}
	}
}

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
	use crate::{error, Location};

	#[test]
	fn test_error_macro() {
		let foo = "foo";
		let bar = "bar";
		let baz = "baz";
		let error = error!(?foo, %bar, ?baz, "{} bar {baz}", foo);
		let trace = error.trace().to_string();
		eprintln!("{trace}");

		let source = std::io::Error::other("Unexpected error.");
		let error = error!(source = source, "An error occurred.");
		let trace = error.trace().to_string();
		eprintln!("{trace}");

		let stack = vec![Location {
			source: "foobar.rs".to_owned(),
			line: 123,
			column: 456,
		}];
		let error = error!(stack = stack, "An error occurred.");
		let trace = error.trace().to_string();
		eprintln!("{trace}");
	}
}
