use {crate::prelude::*, std::path::PathBuf};

pub use self::{data::Error as Data, handle::Error, id::Id, object::Error as Object};

pub mod data;
pub mod handle;
pub mod id;
pub mod object;

/// An alias for `std::result::Result` that defaults to `tg::Error` as the error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(
	Clone,
	Copy,
	Debug,
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum Code {
	Cancellation,
	ChecksumMismatch,
	HeartbeatExpiration,
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

impl Location {
	pub fn try_from_data(value: data::Location) -> tg::Result<Self> {
		Ok(Self {
			symbol: value.symbol,
			file: value.file.try_into()?,
			range: value.range,
		})
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

pub fn ok<T>(value: T) -> Result<T> {
	Ok(value)
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
			data::File::Module(module) => {
				let module = module.try_into()?;
				File::Module(module)
			},
		};
		Ok(value)
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
		let json = serde_json::to_string(&value.to_data_or_id()).unwrap();
		let event = tangram_http::sse::Event {
			event: Some("error".to_owned()),
			data: json,
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
		let data: Data = serde_json::from_str(&value.data)
			.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
		let object = Object::try_from_data(data)?;
		Ok(Error::with_object(object))
	}
}

#[macro_export]
macro_rules! error {
	({ $object:ident }, %$name:ident, $($arg:tt)*) => {
		$object.values.insert(stringify!($name).to_owned(), $name.to_string());
		$crate::error!({ $object }, $($arg)*)
	};
	({ $object:ident }, ?$name:ident, $($arg:tt)*) => {
		$object.values.insert(stringify!($name).to_owned(), format!("{:?}", $name));
		$crate::error!({ $object }, $($arg)*)
	};
	({ $object:ident }, $name:ident = %$value:expr, $($arg:tt)*) => {
		$object.values.insert(stringify!($name).to_owned(), $value.to_string());
		$crate::error!({ $object }, $($arg)*)
	};
	({ $object:ident }, $name:ident = ?$value:expr, $($arg:tt)*) => {
		$object.values.insert(stringify!($name).to_owned(), format!("{:?}", $value));
		$crate::error!({ $object }, $($arg)*)
	};
	({ $object:ident }, !$source:ident, $($arg:tt)*) => {
		let source = Box::<dyn std::error::Error + Send + Sync + 'static>::from($source);
		let source_handle = $crate::Error::from(source);
		let source_arc = source_handle.state().object().unwrap();
		let source_obj = source_arc.unwrap_error_ref().as_ref().clone();
		let source = $crate::Referent::with_item($crate::Either::Left(std::boxed::Box::new(source_obj)));
		$object.source.replace(source);
		$crate::error!({ $object }, $($arg)*)
	};
	({ $object:ident }, source = $source:expr, $($arg:tt)*) => {
		let source = Box::<dyn std::error::Error + Send + Sync + 'static>::from($source);
		let source_handle = $crate::Error::from(source);
		let source_arc = source_handle.state().object().unwrap();
		let source_obj = source_arc.unwrap_error_ref().as_ref().clone();
		let source = $crate::Referent::with_item($crate::Either::Left(std::boxed::Box::new(source_obj)));
		$object.source.replace(source);
		$crate::error!({ $object }, $($arg)*)
	};
	({ $object:ident }, code = $code:expr, $($arg:tt)*) => {
		$object.code.replace($code);
		$crate::error!({ $object }, $($arg)*)
	};
	({ $object:ident }, stack = $stack:expr, $($arg:tt)*) => {
		$object.stack.replace($stack);
		$crate::error!({ $object }, $($arg)*)
	};
	({ $object:ident }, diagnostics = $diagnostics:expr, $($arg:tt)*) => {
		$object.diagnostics.replace($diagnostics);
		$crate::error!({ $object }, $($arg)*)
	};
	({ $object:ident }, $($arg:tt)*) => {
		$object.message = Some(format!($($arg)*));
	};
	($($arg:tt)*) => {{
		let mut object = $crate::error::Object {
			code: None,
			diagnostics: None,
			location: Some($crate::error::Location {
				symbol: Some($crate::function!().to_owned()),
				file: $crate::error::File::Internal(format!("{}", ::std::file!()).parse().unwrap()),
				range: $crate::Range {
					start: $crate::Position { line: line!() - 1, character: column!() - 1 },
					end: $crate::Position { line: line!() - 1, character: column!() - 1 }
				}
			}),
			message: Some(String::new()),
			source: None,
			stack: None,
			values: ::std::collections::BTreeMap::new(),
		};
		$crate::error!({ object }, $($arg)*);
		$crate::Error::with_object(object)
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
