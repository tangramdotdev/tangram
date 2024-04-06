use crate as tg;
use bytes::Bytes;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt as _,
};
use itertools::Itertools as _;
use num::ToPrimitive;
use std::collections::BTreeMap;

/// A value.
#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	serde::Deserialize,
)]
#[serde(try_from = "Data")]
#[try_unwrap(ref)]
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
	Array(Vec<Value>),

	/// A map value.
	Map(BTreeMap<String, Value>),

	/// An object value.
	Object(tg::object::Handle),

	/// A bytes value.
	Bytes(Bytes),

	/// A path value.
	Path(tg::Path),

	/// A mutation value.
	Mutation(tg::Mutation),

	/// A template value.
	Template(tg::Template),
}

/// Value data.
#[derive(Clone, Debug)]
pub enum Data {
	Null,
	Bool(bool),
	Number(f64),
	String(String),
	Array(Vec<Data>),
	Map(BTreeMap<String, Data>),
	Object(tg::object::Id),
	Bytes(Bytes),
	Path(tg::Path),
	Mutation(tg::mutation::Data),
	Template(tg::template::Data),
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

	pub async fn push<H1, H2>(
		&self,
		tg: &H1,
		remote: &H2,
		transaction: Option<&H2::Transaction<'_>>,
	) -> tg::Result<()>
	where
		H1: tg::Handle,
		H2: tg::Handle,
	{
		self.objects()
			.iter()
			.map(|object| object.push(tg, remote, transaction))
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(())
	}

	pub async fn pull<H1, H2>(
		&self,
		tg: &H1,
		remote: &H2,
		transaction: Option<&H1::Transaction<'_>>,
	) -> tg::Result<()>
	where
		H1: tg::Handle,
		H2: tg::Handle,
	{
		self.objects()
			.iter()
			.map(|object| object.pull(tg, remote, transaction))
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(())
	}

	pub async fn data<H>(
		&self,
		tg: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Data>
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
					.map(|value| value.data(tg, transaction))
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await?,
			),
			Self::Map(map) => Data::Map(
				map.iter()
					.map(|(key, value)| async move {
						Ok::<_, tg::Error>((key.clone(), value.data(tg, transaction).await?))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?,
			),
			Self::Object(object) => Data::Object(object.id(tg, transaction).await?),
			Self::Bytes(bytes) => Data::Bytes(bytes.clone()),
			Self::Path(path) => Data::Path(path.clone()),
			Self::Mutation(mutation) => Data::Mutation(mutation.data(tg, transaction).await?),
			Self::Template(template) => Data::Template(template.data(tg, transaction).await?),
		};
		Ok(data)
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
	pub fn children(&self) -> Vec<tg::object::Id> {
		match self {
			Self::Null
			| Self::Bool(_)
			| Self::Number(_)
			| Self::String(_)
			| Self::Bytes(_)
			| Self::Path(_) => {
				vec![]
			},
			Self::Array(array) => array.iter().flat_map(Self::children).collect(),
			Self::Map(map) => map.values().flat_map(Self::children).collect(),
			Self::Mutation(mutation) => mutation.children(),
			Self::Template(template) => template.children(),
			Self::Object(id) => vec![id.clone()],
		}
	}
}

impl std::fmt::Display for Value {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Null => {
				write!(f, "null")?;
			},
			Self::Bool(bool) => {
				write!(f, "{bool}")?;
			},
			Self::Number(number) => {
				write!(f, "{number}")?;
			},
			Self::String(string) => {
				write!(f, "\"{string}\"")?;
			},
			Self::Array(array) => {
				write!(f, "[")?;
				for (i, value) in array.iter().enumerate() {
					write!(f, "{value}")?;
					if i < array.len() - 1 {
						write!(f, ", ")?;
					}
				}
				write!(f, "]")?;
			},
			Self::Map(map) => {
				write!(f, "{{")?;
				if !map.is_empty() {
					write!(f, " ")?;
				}
				for (i, (key, value)) in map.iter().enumerate() {
					write!(f, "{key}: {value}")?;
					if i < map.len() - 1 {
						write!(f, ", ")?;
					}
				}
				if !map.is_empty() {
					write!(f, " ")?;
				}
				write!(f, "}}")?;
			},
			Self::Object(object) => {
				write!(f, "{object}")?;
			},
			Self::Bytes(_) => {
				write!(f, "(bytes)")?;
			},
			Self::Path(path) => {
				write!(f, "(path \"{path}\")")?;
			},
			Self::Mutation(mutation) => {
				write!(f, "{mutation}")?;
			},
			Self::Template(template) => {
				write!(f, "{template}")?;
			},
		}
		Ok(())
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
			Data::Path(path) => Self::Path(path),
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
			Self::Path(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "path")?;
				map.serialize_entry("value", value)?;
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
								"path" => Data::Path(map.next_value()?),
								"mutation" => Data::Mutation(map.next_value()?),
								"template" => Data::Template(map.next_value()?),
								_ => {
									return Err(serde::de::Error::unknown_variant(kind, &["kind"]))
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
