use {
	crate::prelude::*,
	bytes::Bytes,
	num::ToPrimitive as _,
	serde::ser::{SerializeMap, SerializeSeq},
	std::collections::{BTreeMap, BTreeSet},
};

const BYTES_VARIANT_ID: u8 = 7;
const MAP_VARIANT_ID: u8 = 5;
const MODULE_VARIANT_ID: u8 = 11;
const MUTATION_VARIANT_ID: u8 = 8;
const OBJECT_VARIANT_ID: u8 = 6;
const PLACEHOLDER_VARIANT_ID: u8 = 10;
const TEMPLATE_VARIANT_ID: u8 = 9;

/// Value data.
#[derive(
	Clone,
	Debug,
	PartialEq,
	derive_more::From,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Data {
	Null,

	Bool(bool),

	Number(f64),

	String(String),

	Array(Vec<Data>),

	Map(BTreeMap<String, Data>),

	Object(tg::Referent<tg::object::Id>),

	Bytes(Bytes),

	Mutation(tg::mutation::Data),

	Template(tg::template::Data),

	Placeholder(tg::placeholder::Data),

	Module(tg::module::Data),
}

pub type Array = Vec<Data>;

pub type Map = BTreeMap<String, Data>;

impl Data {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|error| tg::error!(!error, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn serialize_json(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		serde_json::to_writer(&mut bytes, self)
			.map_err(|error| tg::error!(!error, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let bytes = bytes.as_ref();
		if bytes.is_empty() {
			return Err(tg::error!("missing format byte"));
		}
		let format = bytes[0];
		if format == 0 {
			tangram_serialize::from_slice(&bytes[1..])
				.map_err(|error| tg::error!(!error, "failed to deserialize the data"))
		} else {
			serde_json::from_slice(bytes)
				.map_err(|error| tg::error!(!error, "failed to deserialize the data"))
		}
	}

	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		match self {
			Self::Null
			| Self::Bool(_)
			| Self::Number(_)
			| Self::String(_)
			| Self::Bytes(_)
			| Self::Placeholder(_) => (),
			Self::Array(array) => {
				for value in array {
					value.children(children);
				}
			},
			Self::Map(map) => {
				for value in map.values() {
					value.children(children);
				}
			},
			Self::Object(object) => {
				children.insert(object.node.clone());
			},
			Self::Mutation(mutation) => mutation.children(children),
			Self::Module(module) => module.children(children),
			Self::Template(template) => template.children(children),
		}
	}

	pub fn children_with_tokens(&self, children: &mut Vec<tg::Referent<tg::object::Id>>) {
		match self {
			Self::Null
			| Self::Bool(_)
			| Self::Number(_)
			| Self::String(_)
			| Self::Bytes(_)
			| Self::Placeholder(_) => (),
			Self::Array(array) => {
				for value in array {
					value.children_with_tokens(children);
				}
			},
			Self::Map(map) => {
				for value in map.values() {
					value.children_with_tokens(children);
				}
			},
			Self::Object(object) => children.push(object.clone()),
			Self::Mutation(mutation) => mutation.children_with_tokens(children),
			Self::Module(module) => module.children_with_tokens(children),
			Self::Template(template) => template.children_with_tokens(children),
		}
	}

	#[must_use]
	pub fn without_location_and_tokens(self) -> Self {
		match self {
			Self::Array(array) => Self::Array(
				array
					.into_iter()
					.map(Self::without_location_and_tokens)
					.collect(),
			),
			Self::Map(map) => Self::Map(
				map.into_iter()
					.map(|(key, value)| (key, value.without_location_and_tokens()))
					.collect(),
			),
			Self::Object(mut object) => {
				object.options.clear_location_and_tokens();
				Self::Object(object)
			},
			Self::Mutation(mutation) => Self::Mutation(mutation.without_location_and_tokens()),
			Self::Module(module) => Self::Module(module.without_location_and_tokens()),
			Self::Template(template) => Self::Template(template.without_location_and_tokens()),
			value @ (Self::Null
			| Self::Bool(_)
			| Self::Number(_)
			| Self::String(_)
			| Self::Bytes(_)
			| Self::Placeholder(_)) => value,
		}
	}

	pub fn to_serde<T>(self) -> tg::Result<T>
	where
		T: serde::de::DeserializeOwned,
	{
		let json = serde_json::to_value(&self)
			.map_err(|error| tg::error!(!error, "failed to convert to json"))?;
		let result = serde_json::from_value(json)
			.map_err(|error| tg::error!(!error, "failed to deserialize from json"))?;
		Ok(result)
	}

	pub fn from_serde<T>(value: T) -> tg::Result<Self>
	where
		T: serde::Serialize,
	{
		let json = serde_json::to_value(&value)
			.map_err(|error| tg::error!(!error, "failed to serialize to json"))?;
		let result = serde_json::from_value(json)
			.map_err(|error| tg::error!(!error, "failed to convert from json"))?;
		Ok(result)
	}
}

impl tangram_serialize::Serialize for Data {
	fn serialize(&self, serializer: &mut tangram_serialize::Serializer<'_>) -> std::io::Result<()> {
		match self {
			Self::Array(value) => serializer.serialize(value),
			Self::Bool(value) => serializer.serialize(value),
			Self::Bytes(value) => {
				serializer.write_kind(tangram_serialize::Kind::Enum)?;
				serializer.write_id(BYTES_VARIANT_ID)?;
				serializer.serialize(value)
			},
			Self::Map(value) => {
				serializer.write_kind(tangram_serialize::Kind::Enum)?;
				serializer.write_id(MAP_VARIANT_ID)?;
				serializer.serialize(value)
			},
			Self::Module(value) => {
				serializer.write_kind(tangram_serialize::Kind::Enum)?;
				serializer.write_id(MODULE_VARIANT_ID)?;
				serializer.serialize(value)
			},
			Self::Mutation(value) => {
				serializer.write_kind(tangram_serialize::Kind::Enum)?;
				serializer.write_id(MUTATION_VARIANT_ID)?;
				serializer.serialize(value)
			},
			Self::Null => serializer.serialize(&()),
			Self::Number(value) => {
				if !value.is_finite() {
					return Err(std::io::Error::other("invalid number"));
				}
				serializer.serialize(value)
			},
			Self::Object(value) => {
				serializer.write_kind(tangram_serialize::Kind::Enum)?;
				serializer.write_id(OBJECT_VARIANT_ID)?;
				serializer.serialize(value)
			},
			Self::Placeholder(value) => {
				serializer.write_kind(tangram_serialize::Kind::Enum)?;
				serializer.write_id(PLACEHOLDER_VARIANT_ID)?;
				serializer.serialize(value)
			},
			Self::String(value) => serializer.serialize(value),
			Self::Template(value) => {
				serializer.write_kind(tangram_serialize::Kind::Enum)?;
				serializer.write_id(TEMPLATE_VARIANT_ID)?;
				serializer.serialize(value)
			},
		}
	}
}

impl<'de> tangram_serialize::Deserialize<'de> for Data {
	fn deserialize(
		deserializer: &mut tangram_serialize::Deserializer<'de>,
	) -> std::io::Result<Self> {
		let kind = deserializer.read_kind()?;
		let data = match kind {
			tangram_serialize::Kind::Array => Self::Array(deserializer.read_array()?),
			tangram_serialize::Kind::Bool => Self::Bool(deserializer.read_bool()?),
			tangram_serialize::Kind::Enum => {
				let id = deserializer.read_id()?;
				match id {
					BYTES_VARIANT_ID => Self::Bytes(deserializer.deserialize()?),
					MAP_VARIANT_ID => Self::Map(deserializer.deserialize()?),
					MODULE_VARIANT_ID => Self::Module(deserializer.deserialize()?),
					MUTATION_VARIANT_ID => Self::Mutation(deserializer.deserialize()?),
					OBJECT_VARIANT_ID => Self::Object(deserializer.deserialize()?),
					PLACEHOLDER_VARIANT_ID => Self::Placeholder(deserializer.deserialize()?),
					TEMPLATE_VARIANT_ID => Self::Template(deserializer.deserialize()?),
					_ => return Err(std::io::Error::other("invalid data variant")),
				}
			},
			tangram_serialize::Kind::F32 => {
				let value = deserializer.read_f32()?;
				if !value.is_finite() {
					return Err(std::io::Error::other("invalid number"));
				}
				Self::Number(f64::from(value))
			},
			tangram_serialize::Kind::F64 => {
				let value = deserializer.read_f64()?;
				if !value.is_finite() {
					return Err(std::io::Error::other("invalid number"));
				}
				Self::Number(value)
			},
			tangram_serialize::Kind::IVarint => {
				let value = deserializer
					.read_ivarint()?
					.to_f64()
					.ok_or_else(|| std::io::Error::other("invalid number"))?;
				Self::Number(value)
			},
			tangram_serialize::Kind::Null => Self::Null,
			tangram_serialize::Kind::String => Self::String(deserializer.read_string()?),
			tangram_serialize::Kind::UVarint => {
				let value = deserializer
					.read_uvarint()?
					.to_f64()
					.ok_or_else(|| std::io::Error::other("invalid number"))?;
				Self::Number(value)
			},
			tangram_serialize::Kind::Bytes
			| tangram_serialize::Kind::Map
			| tangram_serialize::Kind::Struct => {
				return Err(std::io::Error::other("invalid data kind"));
			},
		};

		Ok(data)
	}
}

impl serde::Serialize for Data {
	fn serialize<S>(&self, serializer: S) -> tg::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		match self {
			Self::Null => serializer.serialize_none(),
			Self::Bool(value) => serializer.serialize_bool(*value),
			Self::Number(value) => {
				if !value.is_finite() {
					return Err(serde::ser::Error::custom("invalid number"));
				}
				serializer.serialize_f64(*value)
			},
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
				map.serialize_entry("value", &value.to_string())?;
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
			Self::Module(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "module")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
			Self::Template(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "template")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
			Self::Placeholder(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "placeholder")?;
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

			fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Bytes(Bytes::copy_from_slice(value)))
			}

			fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Bytes(value.into()))
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
								"object" => {
									let string = map.next_value::<String>()?;
									let referent =
										string.parse().map_err(serde::de::Error::custom)?;
									Data::Object(referent)
								},
								"bytes" => Data::Bytes(
									data_encoding::BASE64
										.decode(map.next_value::<String>()?.as_bytes())
										.map_err(serde::de::Error::custom)?
										.into(),
								),
								"mutation" => Data::Mutation(map.next_value()?),
								"module" => Data::Module(map.next_value()?),
								"template" => Data::Template(map.next_value()?),
								"placeholder" => Data::Placeholder(map.next_value()?),
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
