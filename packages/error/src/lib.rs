use std::sync::Arc;
use thiserror::Error;

/// A result alias that defaults to `Error` as the error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An error.
#[derive(Clone, Debug, Error, serde::Deserialize, serde::Serialize)]
#[error("{message}")]
pub struct Error {
	pub message: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub stack: Option<Vec<Location>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub source: Option<Arc<Error>>,
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
	#[track_caller]
	pub fn with_message(message: impl std::fmt::Display) -> Self {
		Self {
			message: message.to_string(),
			location: Some(std::panic::Location::caller().into()),
			stack: None,
			source: None,
		}
	}

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
			}),
		}
	}
}

impl<'a> std::fmt::Display for Trace<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut first = true;
		let mut error = self.0;
		loop {
			if !first {
				writeln!(f)?;
			}
			first = false;
			let message = &error.message;
			write!(f, "{message}")?;
			if let Some(location) = &error.location {
				write!(f, " {location}")?;
			}
			for location in error.stack.iter().flatten() {
				writeln!(f)?;
				write!(f, "  {location}")?;
			}
			if let Some(source) = &error.source {
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

#[macro_export]
macro_rules! error {
	($($t:tt)*) => {{
		$crate::Error::with_message(format!($($t)*))
	}};
}
