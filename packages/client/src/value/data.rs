use crate as tg;
use byteorder::ReadBytesExt as _;
use bytes::Bytes;
use num::ToPrimitive as _;
use std::collections::BTreeMap;
use tangram_itertools::IteratorExt as _;

/// Value data.
#[derive(
	Clone,
	Debug,
	PartialEq,
	derive_more::From,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Data {
	#[tangram_serialize(id = 0)]
	Null,

	#[tangram_serialize(id = 1)]
	Bool(bool),

	#[tangram_serialize(id = 2)]
	Number(f64),

	#[tangram_serialize(id = 3)]
	String(String),

	#[tangram_serialize(id = 4)]
	Array(Vec<Data>),

	#[tangram_serialize(id = 5)]
	Map(BTreeMap<String, Data>),

	#[tangram_serialize(id = 6)]
	Object(tg::object::Id),

	#[tangram_serialize(id = 7)]
	Bytes(Bytes),

	#[tangram_serialize(id = 8)]
	Mutation(tg::mutation::Data),

	#[tangram_serialize(id = 9)]
	Template(tg::template::Data),
}

pub type Array = Vec<Data>;

pub type Map = BTreeMap<String, Data>;

impl Data {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn serialize_json(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		serde_json::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let mut reader = std::io::Cursor::new(bytes.as_ref());
		let format = reader
			.read_u8()
			.map_err(|source| tg::error!(!source, "failed to read the format"))?;
		match format {
			0 => tangram_serialize::from_reader(&mut reader)
				.map_err(|source| tg::error!(!source, "failed to deserialize the data")),
			b'{' => serde_json::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the data")),
			_ => Err(tg::error!("invalid format")),
		}
	}

	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		match self {
			Self::Null | Self::Bool(_) | Self::Number(_) | Self::String(_) | Self::Bytes(_) => {
				std::iter::empty().boxed()
			},
			Self::Array(array) => array.iter().flat_map(Self::children).boxed(),
			Self::Map(map) => map.values().flat_map(Self::children).boxed(),
			Self::Object(object) => std::iter::once(object.clone()).boxed(),
			Self::Mutation(mutation) => mutation.children().boxed(),
			Self::Template(template) => template.children().boxed(),
		}
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
			Self::Object(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "object")?;
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
