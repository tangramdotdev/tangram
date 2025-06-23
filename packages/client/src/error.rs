use crate::{self as tg, util::serde::is_false};
use itertools::Itertools as _;
use serde_with::serde_as;
use std::{collections::BTreeMap, fmt::Debug, path::PathBuf};

pub use self::data::Error as Data;

pub mod data;

/// An alias for `std::result::Result` that defaults to `tg::Error` as the error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An error.
#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(try_from = "Data")]
pub struct Error {
	pub code: Option<tg::error::Code>,
	pub message: Option<String>,
	pub location: Option<tg::error::Location>,
	pub stack: Option<Vec<tg::error::Location>>,
	pub source: Option<tg::Referent<Box<tg::Error>>>,
	pub values: BTreeMap<String, String>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Code {
	#[display("cancellation")]
	Cancellation,
}

/// An error location.
#[derive(Clone, Debug)]
pub struct Location {
	pub symbol: Option<String>,
	pub file: File,
	pub range: tg::Range,
}

/// An error location's source.
#[derive(Clone, Debug, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum File {
	Internal(PathBuf),
	Module(tg::Module),
}

pub struct Trace<'a> {
	pub error: &'a Error,
	pub options: &'a TraceOptions,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct TraceOptions {
	#[serde(default, skip_serializing_if = "is_false")]
	pub internal: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub reverse: bool,
}

impl Error {
	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		data.try_into()
	}

	#[must_use]
	pub fn trace<'a>(&'a self, options: &'a TraceOptions) -> Trace<'a> {
		Trace {
			error: self,
			options,
		}
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		std::iter::empty()
			.chain(
				self.location
					.as_ref()
					.map(tg::error::Location::children)
					.into_iter()
					.flatten(),
			)
			.collect()
	}

	pub fn to_data(&self) -> Data {
		let code = self.code;
		let message = self.message.clone();
		let location = self.location.as_ref().map(Location::to_data);
		let source = self
			.source
			.as_ref()
			.map(|source| source.clone().map(|item| Box::new(item.to_data())));
		let stack = self
			.stack
			.as_ref()
			.map(|stack| stack.iter().map(Location::to_data).collect());
		let values = self.values.clone();
		Data {
			code,
			message,
			location,
			stack,
			source,
			values,
		}
	}
}

impl Location {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		self.file
			.try_unwrap_module_ref()
			.ok()
			.map(tg::Module::children)
			.into_iter()
			.flatten()
			.collect()
	}

	#[must_use]
	pub fn to_data(&self) -> data::Location {
		let symbol = self.symbol.clone();
		let file = self.file.to_data();
		let range = self.range;
		data::Location {
			symbol,
			file,
			range,
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

	#[must_use]
	pub fn to_data(&self) -> data::File {
		match self {
			File::Internal(path) => data::File::Internal(path.clone()),
			File::Module(module) => data::File::Module(module.to_data()),
		}
	}
}

impl TraceOptions {
	#[must_use]
	pub fn internal() -> Self {
		Self {
			internal: true,
			..Default::default()
		}
	}
}

pub fn ok<T>(value: T) -> Result<T> {
	Ok(value)
}

impl TryFrom<Data> for Error {
	type Error = tg::Error;

	fn try_from(value: Data) -> Result<Self, Self::Error> {
		let code = value.code;
		let message = value.message;
		let location = value.location.map(TryInto::try_into).transpose()?;
		let stack = value
			.stack
			.map(|stack| stack.into_iter().map(TryInto::try_into).try_collect())
			.transpose()?;
		let source = value
			.source
			.map(|referent| {
				let item = Box::new((*referent.item).try_into()?);
				let referent = tg::Referent {
					item,
					path: referent.path,
					tag: referent.tag,
				};
				Ok::<_, tg::Error>(referent)
			})
			.transpose()?;
		let values = value.values;
		let value = Self {
			code,
			message,
			location,
			stack,
			source,
			values,
		};
		Ok(value)
	}
}

impl TryFrom<data::Location> for Location {
	type Error = tg::Error;

	fn try_from(value: data::Location) -> Result<Self, Self::Error> {
		Ok(Self {
			symbol: value.symbol,
			file: value.file.try_into()?,
			range: value.range,
		})
	}
}

impl TryFrom<data::File> for File {
	type Error = tg::Error;

	fn try_from(value: data::File) -> Result<Self, Self::Error> {
		let value = match value {
			data::File::Internal(path) => File::Internal(path),
			data::File::Module(module) => File::Module(module.into()),
		};
		Ok(value)
	}
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
			.map(|source| source.item.as_ref() as &(dyn std::error::Error + 'static))
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
				source: error.source().map(Into::into).map(|error| tg::Referent {
					item: Box::new(error),
					path: None,
					tag: None,
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
			source: value.source().map(Into::into).map(|error| tg::Referent {
				item: Box::new(error),
				path: None,
				tag: None,
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
			range: tg::Range {
				start: tg::Position {
					line: location.line() - 1,
					character: location.column() - 1,
				},
				end: tg::Position {
					line: location.line() - 1,
					character: location.column() - 1,
				},
			},
		}
	}
}

impl std::fmt::Display for Trace<'_> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut errors = vec![self.error];
		while let Some(next) = errors.last().unwrap().source.as_ref() {
			errors.push(next.item.as_ref());
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
			for (key, value) in &error.values {
				let key = key.as_str();
				let value = value.as_str();
				writeln!(f, "   {key} = {value}")?;
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
		write!(
			f,
			"{}:{}:{}",
			self.file,
			self.range.start.line + 1,
			self.range.start.character + 1,
		)?;
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
				write!(f, "internal:{}", path.display())?;
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
		let data = serde_json::to_string(&value.to_data())
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
		let source = $crate::Referent { item: std::boxed::Box::new(source), path: None, tag: None};
		$error.source.replace(source);
		$crate::error!({ $error }, $($arg)*)
	};
	({ $error:ident }, source = $source:expr, $($arg:tt)*) => {
		let source = Box::<dyn std::error::Error + Send + Sync + 'static>::from($source);
		let source = $crate::Error::from(source);
		let source = $crate::Referent { item: std::boxed::Box::new(source), path: None, tag: None };
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
				range: tg::Range {
					start: tg::Position { line: line!() - 1, character: column!() - 1 },
					end: tg::Position { line: line!() - 1, character: column!() - 1 }
				}
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
