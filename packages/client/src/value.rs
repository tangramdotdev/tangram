use self::{parse::parse, print::Printer};
use crate as tg;
use bytes::Bytes;
use futures::{StreamExt as _, stream};
use itertools::Itertools as _;
use std::{collections::BTreeMap, pin::pin};
use tangram_either::Either;
use tangram_futures::stream::TryExt as _;

pub use self::data::*;

pub mod data;
pub mod parse;
pub mod print;

/// A value.
#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
)]
#[serde(try_from = "Data")]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Value {
	/// A null value.
	Null,

	/// A bool value.
	Bool(bool),

	/// A number value.
	Number(f64),

	/// A string value.
	String(String),

	/// An array value.
	Array(Array),

	/// A map value.
	Map(Map),

	/// An object value.
	Object(tg::object::Handle),

	/// A bytes value.
	Bytes(Bytes),

	/// A mutation value.
	Mutation(tg::Mutation),

	/// A template value.
	Template(tg::Template),
}

pub type Array = Vec<Value>;

pub type Map = BTreeMap<String, Value>;

impl Value {
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		match self {
			Self::Array(array) => array.iter().flat_map(Self::objects).collect(),
			Self::Map(map) => map.values().flat_map(Self::objects).collect(),
			Self::Object(object) => vec![object.clone()],
			Self::Template(template) => template.children(),
			Self::Mutation(mutation) => mutation.children(),
			_ => vec![],
		}
	}

	pub async fn store<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		// Get the objects.
		let objects = self.objects();

		// Collect all unstored objects in reverse topological order.
		let mut unstored = Vec::new();
		let mut stack = objects
			.into_iter()
			.filter(|object| !object.state().stored())
			.collect::<Vec<_>>();
		while let Some(object) = stack.pop() {
			unstored.push(object.clone());
			if let Some(object) = object.state().object() {
				let children = object
					.children()
					.into_iter()
					.filter(|object| !object.state().stored());
				stack.extend(children);
			}
		}
		unstored.reverse();

		// Import.
		let mut items = Vec::new();
		for object in &unstored {
			if let Some(object_) = object.state().object() {
				let data = object_.to_data();
				let bytes = data
					.serialize()
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				let id = tg::object::Id::new(data.kind(), &bytes);
				object.state().set_id(id.clone());
				let item = tg::export::Item::Object(tg::export::ObjectItem { id, bytes });
				items.push(item);
			}
		}
		let arg = tg::import::Arg {
			items: Some(
				self.objects()
					.into_iter()
					.map(|object| Either::Right(object.id()))
					.collect(),
			),
			remote: None,
		};
		let stream = stream::iter(items.into_iter().map(Ok)).boxed();
		let stream = handle.import(arg, stream).await?;
		pin!(stream)
			.try_last()
			.await?
			.ok_or_else(|| tg::error!("expected an event"))?
			.try_unwrap_end()
			.ok()
			.ok_or_else(|| tg::error!("expected the end"))?;

		// Mark all objects stored.
		for object in &unstored {
			object.state().set_stored(true);
		}

		Ok(())
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Null => Data::Null,
			Self::Bool(bool) => Data::Bool(*bool),
			Self::Number(number) => Data::Number(*number),
			Self::String(string) => Data::String(string.clone()),
			Self::Array(array) => Data::Array(array.iter().map(Value::to_data).collect()),
			Self::Map(map) => Data::Map(
				map.iter()
					.map(|(key, value)| (key.clone(), value.to_data()))
					.collect(),
			),
			Self::Object(object) => Data::Object(object.id()),
			Self::Bytes(bytes) => Data::Bytes(bytes.clone()),
			Self::Mutation(mutation) => Data::Mutation(mutation.to_data()),
			Self::Template(template) => Data::Template(template.to_data()),
		}
	}

	pub fn print(&self, options: self::print::Options) -> String {
		let mut string = String::new();
		let mut printer = Printer::new(&mut string, options);
		printer.value(self).unwrap();
		string
	}

	pub fn is_blob(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_blob())
	}

	pub fn is_artifact(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_artifact())
	}

	pub fn is_directory(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_directory())
	}

	pub fn is_file(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_file())
	}

	pub fn is_symlink(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_symlink())
	}

	pub fn is_graph(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_graph())
	}

	pub fn is_command(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_command())
	}
}

impl std::fmt::Display for Value {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = Printer::new(f, tg::value::print::Options::default());
		printer.value(self)?;
		Ok(())
	}
}

impl std::str::FromStr for Value {
	type Err = tg::Error;

	fn from_str(input: &str) -> Result<Self, Self::Err> {
		parse(input)
	}
}

impl TryFrom<Data> for Value {
	type Error = tg::Error;

	fn try_from(data: Data) -> Result<Self, Self::Error> {
		Ok(match data {
			Data::Null => Self::Null,
			Data::Bool(bool) => Self::Bool(bool),
			Data::Number(number) => Self::Number(number),
			Data::String(string) => Self::String(string),
			Data::Array(array) => {
				Self::Array(array.into_iter().map(TryInto::try_into).try_collect()?)
			},
			Data::Map(map) => Self::Map(
				map.into_iter()
					.map(|(key, value)| Ok::<_, tg::Error>((key, value.try_into()?)))
					.try_collect()?,
			),
			Data::Object(id) => Self::Object(tg::object::Handle::with_id(id)),
			Data::Bytes(bytes) => Self::Bytes(bytes),
			Data::Mutation(mutation) => Self::Mutation(mutation.try_into()?),
			Data::Template(template) => Self::Template(template.try_into()?),
		})
	}
}

impl From<&str> for Value {
	fn from(value: &str) -> Self {
		value.to_owned().into()
	}
}

impl<T> From<Option<T>> for Value
where
	T: Into<Value>,
{
	fn from(value: Option<T>) -> Self {
		match value {
			Some(value) => value.into(),
			None => Self::Null,
		}
	}
}

impl From<tg::Blob> for Value {
	fn from(value: tg::Blob) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::Blob {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<tg::Directory> for Value {
	fn from(value: tg::Directory) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::Directory {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<tg::File> for Value {
	fn from(value: tg::File) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::File {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<tg::Symlink> for Value {
	fn from(value: tg::Symlink) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::Symlink {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<tg::Graph> for Value {
	fn from(value: tg::Graph) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::Graph {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<tg::Command> for Value {
	fn from(value: tg::Command) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::Command {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<serde_json::Value> for Value {
	fn from(value: serde_json::Value) -> Self {
		match value {
			serde_json::Value::Null => Self::Null,
			serde_json::Value::Bool(value) => Self::Bool(value),
			serde_json::Value::Number(value) => Self::Number(value.as_f64().unwrap()),
			serde_json::Value::String(value) => Self::String(value),
			serde_json::Value::Array(value) => {
				Self::Array(value.into_iter().map(Into::into).collect())
			},
			serde_json::Value::Object(value) => Self::Map(
				value
					.into_iter()
					.map(|(key, value)| (key, value.into()))
					.collect(),
			),
		}
	}
}

impl TryFrom<Value> for serde_json::Value {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Null => Ok(Self::Null),
			Value::Bool(value) => Ok(Self::Bool(value)),
			Value::Number(value) => Ok(Self::Number(serde_json::Number::from_f64(value).unwrap())),
			Value::String(value) => Ok(Self::String(value)),
			Value::Array(value) => Ok(Self::Array(
				value
					.into_iter()
					.map(TryInto::try_into)
					.collect::<tg::Result<_>>()?,
			)),
			Value::Map(value) => Ok(Self::Object(
				value
					.into_iter()
					.map(|(key, value)| Ok((key, value.try_into()?)))
					.collect::<tg::Result<_>>()?,
			)),
			_ => Err(tg::error!("invalid value")),
		}
	}
}

impl<L, R> From<Either<L, R>> for Value
where
	L: Into<Value>,
	R: Into<Value>,
{
	fn from(value: Either<L, R>) -> Self {
		match value {
			Either::Left(value) => value.into(),
			Either::Right(value) => value.into(),
		}
	}
}
