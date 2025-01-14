use self::{parse::parse, print::Printer};
use crate as tg;
use bytes::Bytes;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt as _,
};
use itertools::Itertools as _;
use num::ToPrimitive as _;
use std::collections::{BTreeMap, BTreeSet};
use tangram_either::Either;

pub use self::data::*;

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

pub mod data {
	use crate as tg;
	use bytes::Bytes;
	use std::collections::BTreeMap;

	/// Value data.
	#[derive(Clone, Debug, PartialEq, derive_more::TryUnwrap, derive_more::Unwrap)]
	#[try_unwrap(ref)]
	#[unwrap(ref)]
	pub enum Data {
		Null,
		Bool(bool),
		Number(f64),
		String(String),
		Array(Vec<Data>),
		Map(BTreeMap<String, Data>),
		Object(tg::object::Id),
		Bytes(Bytes),
		Mutation(tg::mutation::Data),
		Template(tg::template::Data),
	}

	pub type Array = Vec<Data>;

	pub type Map = BTreeMap<String, Data>;
}

impl Value {
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		match self {
			Self::Array(array) => array.iter().flat_map(Self::objects).collect(),
			Self::Map(map) => map.values().flat_map(Self::objects).collect(),
			Self::Object(object) => vec![object.clone()],
			Self::Template(template) => template.objects(),
			Self::Mutation(mutation) => mutation.objects(),
			_ => vec![],
		}
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		let data = match self {
			Self::Null => Data::Null,
			Self::Bool(bool) => Data::Bool(*bool),
			Self::Number(number) => Data::Number(*number),
			Self::String(string) => Data::String(string.clone()),
			Self::Array(array) => Data::Array(
				array
					.iter()
					.map(|value| value.data(handle))
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await?,
			),
			Self::Map(map) => Data::Map(
				map.iter()
					.map(|(key, value)| async move {
						Ok::<_, tg::Error>((key.clone(), value.data(handle).await?))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?,
			),
			Self::Object(object) => Data::Object(object.id(handle).await?),
			Self::Bytes(bytes) => Data::Bytes(bytes.clone()),
			Self::Mutation(mutation) => Data::Mutation(mutation.data(handle).await?),
			Self::Template(template) => Data::Template(template.data(handle).await?),
		};
		Ok(data)
	}

	pub fn print(&self, options: self::print::Options) -> String {
		let mut string = String::new();
		let mut printer = Printer::new(&mut string, options);
		printer.value(self).unwrap();
		string
	}
}

impl Data {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		serde_json::to_vec(self)
			.map(Into::into)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))
	}

	pub fn deserialize(bytes: &Bytes) -> tg::Result<Self> {
		serde_json::from_reader(bytes.as_ref())
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			Self::Null | Self::Bool(_) | Self::Number(_) | Self::String(_) | Self::Bytes(_) => {
				[].into()
			},
			Self::Array(array) => array.iter().flat_map(Self::children).collect(),
			Self::Map(map) => map.values().flat_map(Self::children).collect(),
			Self::Object(object) => [object.clone()].into(),
			Self::Mutation(mutation) => mutation.children(),
			Self::Template(template) => template.children(),
		}
	}
}

impl std::fmt::Display for Value {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = Printer::new(f, tg::value::print::Options::default());
		printer.value(self)?;
		Ok(())
	}
}

impl std::str::FromStr for tg::Value {
	type Err = tg::Error;

	fn from_str(input: &str) -> Result<Self, Self::Err> {
		parse(input)
	}
}

impl TryFrom<Data> for Value {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
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

impl serde::Serialize for Data {
	fn serialize<S>(&self, serializer: S) -> tg::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		use serde::ser::{SerializeMap, SerializeSeq};
		match self {
			Self::Null => serializer.serialize_unit(),
			Self::Bool(value) => serializer.serialize_bool(*value),
			Self::Number(value) => serializer.serialize_f64(*value),
			Self::String(value) => serializer.serialize_str(value),
			Self::Array(value) => {
				let mut seq = serializer.serialize_seq(Some(value.len()))?;
				for value in value {
					seq.serialize_element(value)?;
				}
				seq.end()
			},
			Self::Map(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "map")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
			Self::Bytes(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "bytes")?;
				map.serialize_entry("value", &data_encoding::BASE64.encode(value))?;
				map.end()
			},
			Self::Mutation(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "mutation")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
			Self::Template(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "template")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
			Self::Object(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "object")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
		}
	}
}

impl<'de> serde::Deserialize<'de> for Data {
	fn deserialize<D>(deserializer: D) -> tg::Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		struct Visitor;
		impl<'de> serde::de::Visitor<'de> for Visitor {
			type Value = Data;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("a valid value")
			}

			fn visit_unit<E>(self) -> tg::Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Null)
			}

			fn visit_bool<E>(self, value: bool) -> tg::Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Bool(value))
			}

			fn visit_i64<E>(self, value: i64) -> tg::Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Number(value.to_f64().ok_or_else(|| {
					serde::de::Error::custom("invalid number")
				})?))
			}

			fn visit_u64<E>(self, value: u64) -> tg::Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Number(value.to_f64().ok_or_else(|| {
					serde::de::Error::custom("invalid number")
				})?))
			}

			fn visit_f64<E>(self, value: f64) -> tg::Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Number(value))
			}

			fn visit_str<E>(self, value: &str) -> tg::Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::String(value.to_owned()))
			}

			fn visit_string<E>(self, value: String) -> tg::Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::String(value))
			}

			fn visit_seq<A>(self, mut seq: A) -> tg::Result<Self::Value, A::Error>
			where
				A: serde::de::SeqAccess<'de>,
			{
				let mut value = Vec::with_capacity(seq.size_hint().unwrap_or(0));
				while let Some(element) = seq.next_element()? {
					value.push(element);
				}
				Ok(Data::Array(value))
			}

			fn visit_map<A>(self, mut map: A) -> tg::Result<Self::Value, A::Error>
			where
				A: serde::de::MapAccess<'de>,
			{
				#[derive(serde::Deserialize)]
				#[serde(field_identifier, rename_all = "snake_case")]
				enum Field {
					Kind,
					Value,
				}
				let mut kind: Option<String> = None;
				let mut value = None;
				while let Some(key) = map.next_key()? {
					match key {
						Field::Kind => kind = Some(map.next_value()?),
						Field::Value => {
							let Some(kind) = kind.as_deref() else {
								return Err(serde::de::Error::missing_field("kind"));
							};
							value = Some(match kind {
								"map" => Data::Map(map.next_value()?),
								"object" => Data::Object(map.next_value()?),
								"bytes" => Data::Bytes(
									data_encoding::BASE64
										.decode(map.next_value::<String>()?.as_bytes())
										.map_err(serde::de::Error::custom)?
										.into(),
								),
								"mutation" => Data::Mutation(map.next_value()?),
								"template" => Data::Template(map.next_value()?),
								_ => {
									return Err(serde::de::Error::unknown_variant(kind, &["kind"]));
								},
							});
						},
					}
				}
				let Some(value) = value else {
					return Err(serde::de::Error::missing_field("value"));
				};
				Ok(value)
			}
		}
		deserializer.deserialize_any(Visitor)
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

impl From<tg::Leaf> for Value {
	fn from(value: tg::Leaf) -> Self {
		tg::Object::from(value).into()
	}
}

impl From<tg::Branch> for Value {
	fn from(value: tg::Branch) -> Self {
		tg::Object::from(value).into()
	}
}

impl From<tg::Directory> for Value {
	fn from(value: tg::Directory) -> Self {
		tg::Object::from(value).into()
	}
}
impl From<tg::File> for Value {
	fn from(value: tg::File) -> Self {
		tg::Object::from(value).into()
	}
}

impl From<tg::Symlink> for Value {
	fn from(value: tg::Symlink) -> Self {
		tg::Object::from(value).into()
	}
}

impl From<tg::Graph> for Value {
	fn from(value: tg::Graph) -> Self {
		tg::Object::from(value).into()
	}
}

impl From<tg::Command> for Value {
	fn from(value: tg::Command) -> Self {
		tg::Object::from(value).into()
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
