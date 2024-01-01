use crate::{
	branch, directory, file, leaf, lock, mutation, object, symlink, target, template, Branch,
	Directory, Error, File, Handle, Leaf, Lock, Mutation, Result, Symlink, Target, Template,
	WrapErr,
};
use async_recursion::async_recursion;
use bytes::Bytes;
use derive_more::{From, TryInto, TryUnwrap};
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt,
};
use itertools::Itertools;
use num::ToPrimitive;
use std::collections::BTreeMap;

/// A value.
#[derive(Clone, Debug, From, TryInto, serde::Deserialize, TryUnwrap)]
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

	/// A bytes value.
	Bytes(Bytes),

	/// A mutation value.
	Mutation(Mutation),

	/// A template value.
	Template(Template),

	/// A leaf value.
	Leaf(Leaf),

	/// A branch value.
	Branch(Branch),

	/// A directory value.
	Directory(Directory),

	/// A file value.
	File(File),

	/// A symlink value.
	Symlink(Symlink),

	/// A lock value.
	Lock(Lock),

	/// A target value.
	Target(Target),
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
	Bytes(Bytes),
	Mutation(mutation::Data),
	Template(template::Data),
	Leaf(leaf::Id),
	Branch(branch::Id),
	Directory(directory::Id),
	File(file::Id),
	Symlink(symlink::Id),
	Lock(lock::Id),
	Target(target::Id),
}

impl Value {
	pub fn objects(&self) -> Vec<object::Handle> {
		match self {
			Self::Array(array) => array.iter().flat_map(Self::objects).collect(),
			Self::Map(map) => map.values().flat_map(Self::objects).collect(),
			Self::Leaf(leaf) => vec![object::Handle::Leaf(leaf.clone())],
			Self::Branch(branch) => vec![object::Handle::Branch(branch.clone())],
			Self::Directory(directory) => vec![object::Handle::Directory(directory.clone())],
			Self::File(file) => vec![object::Handle::File(file.clone())],
			Self::Symlink(symlink) => vec![object::Handle::Symlink(symlink.clone())],
			Self::Lock(lock) => vec![object::Handle::Lock(lock.clone())],
			Self::Target(target) => vec![object::Handle::Target(target.clone())],
			_ => vec![],
		}
	}

	pub async fn push(&self, tg: &dyn crate::Handle, remote: &dyn crate::Handle) -> Result<()> {
		self.objects()
			.iter()
			.map(|object| object.push(tg, remote))
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(())
	}

	#[async_recursion]
	pub async fn data(&self, tg: &dyn Handle) -> Result<Data> {
		let data = match self {
			Self::Null => Data::Null,
			Self::Bool(bool) => Data::Bool(*bool),
			Self::Number(number) => Data::Number(*number),
			Self::String(string) => Data::String(string.clone()),
			Self::Array(array) => Data::Array(
				array
					.iter()
					.map(|value| value.data(tg))
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await?,
			),
			Self::Map(map) => Data::Map(
				map.iter()
					.map(|(key, value)| async move {
						Ok::<_, Error>((key.clone(), value.data(tg).await?))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?,
			),
			Self::Bytes(bytes) => Data::Bytes(bytes.clone()),
			Self::Mutation(mutation) => Data::Mutation(mutation.data(tg).await?.clone()),
			Self::Template(template) => Data::Template(template.data(tg).await?.clone()),
			Self::Leaf(leaf) => Data::Leaf(leaf.id(tg).await?.clone()),
			Self::Branch(branch) => Data::Branch(branch.id(tg).await?.clone()),
			Self::Directory(directory) => Data::Directory(directory.id(tg).await?.clone()),
			Self::File(file) => Data::File(file.id(tg).await?.clone()),
			Self::Symlink(symlink) => Data::Symlink(symlink.id(tg).await?.clone()),
			Self::Lock(lock) => Data::Lock(lock.id(tg).await?.clone()),
			Self::Target(target) => Data::Target(target.id(tg).await?.clone()),
		};
		Ok(data)
	}
}

impl Data {
	pub fn serialize(&self) -> Result<Bytes> {
		serde_json::to_vec(self)
			.map(Into::into)
			.wrap_err("Failed to serialize the data.")
	}

	pub fn deserialize(bytes: &Bytes) -> Result<Self> {
		serde_json::from_reader(bytes.as_ref()).wrap_err("Failed to deserialize the data.")
	}

	#[must_use]
	pub fn children(&self) -> Vec<object::Id> {
		match self {
			Self::Null | Self::Bool(_) | Self::Number(_) | Self::String(_) | Self::Bytes(_) => {
				vec![]
			},
			Self::Array(array) => array.iter().flat_map(Self::children).collect(),
			Self::Map(map) => map.values().flat_map(Self::children).collect(),
			Self::Mutation(mutation) => mutation.children(),
			Self::Template(template) => template.children(),
			Self::Leaf(id) => vec![id.clone().into()],
			Self::Branch(id) => vec![id.clone().into()],
			Self::Directory(id) => vec![id.clone().into()],
			Self::File(id) => vec![id.clone().into()],
			Self::Symlink(id) => vec![id.clone().into()],
			Self::Lock(id) => vec![id.clone().into()],
			Self::Target(id) => vec![id.clone().into()],
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
			Self::Bytes(bytes) => {
				write!(f, "{}", hex::encode(bytes))?;
			},
			Self::Mutation(mutation) => {
				write!(f, "{mutation}")?;
			},
			Self::Template(template) => {
				write!(f, "{template}")?;
			},
			Self::Leaf(leaf) => {
				write!(f, "{leaf}")?;
			},
			Self::Branch(branch) => {
				write!(f, "{branch}")?;
			},
			Self::Directory(directory) => {
				write!(f, "{directory}")?;
			},
			Self::File(file) => {
				write!(f, "{file}")?;
			},
			Self::Symlink(symlink) => {
				write!(f, "{symlink}")?;
			},
			Self::Lock(lock) => {
				write!(f, "{lock}")?;
			},
			Self::Target(target) => {
				write!(f, "{target}")?;
			},
		}
		Ok(())
	}
}

impl TryFrom<Data> for Value {
	type Error = Error;

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
					.map(|(key, value)| Ok::<_, Error>((key, value.try_into()?)))
					.try_collect()?,
			),
			Data::Bytes(bytes) => Self::Bytes(bytes),
			Data::Mutation(mutation) => Self::Mutation(mutation.try_into()?),
			Data::Template(template) => Self::Template(template.try_into()?),
			Data::Leaf(id) => Self::Leaf(Leaf::with_id(id)),
			Data::Branch(id) => Self::Branch(Branch::with_id(id)),
			Data::Directory(id) => Self::Directory(Directory::with_id(id)),
			Data::File(id) => Self::File(File::with_id(id)),
			Data::Symlink(id) => Self::Symlink(Symlink::with_id(id)),
			Data::Lock(id) => Self::Lock(Lock::with_id(id)),
			Data::Target(id) => Self::Target(Target::with_id(id)),
		})
	}
}

impl serde::Serialize for Data {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
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
			Self::Leaf(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "leaf")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
			Self::Branch(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "branch")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
			Self::Directory(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "directory")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
			Self::File(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "file")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
			Self::Symlink(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "symlink")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
			Self::Lock(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "lock")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
			Self::Target(value) => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("kind", "target")?;
				map.serialize_entry("value", value)?;
				map.end()
			},
		}
	}
}

impl<'de> serde::Deserialize<'de> for Data {
	#[allow(clippy::too_many_lines)]
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		struct Visitor;
		impl<'de> serde::de::Visitor<'de> for Visitor {
			type Value = Data;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("a valid value")
			}

			fn visit_unit<E>(self) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Null)
			}

			fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Bool(value))
			}

			fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Number(value.to_f64().ok_or_else(|| {
					serde::de::Error::custom("invalid number")
				})?))
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Number(value.to_f64().ok_or_else(|| {
					serde::de::Error::custom("invalid number")
				})?))
			}

			fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::Number(value))
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::String(value.to_owned()))
			}

			fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(Data::String(value))
			}

			fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
			where
				A: serde::de::SeqAccess<'de>,
			{
				let mut value = Vec::with_capacity(seq.size_hint().unwrap_or(0));
				while let Some(element) = seq.next_element()? {
					value.push(element);
				}
				Ok(Data::Array(value))
			}

			fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
			where
				A: serde::de::MapAccess<'de>,
			{
				let mut kind: Option<&str> = None;
				let mut value = None;
				while let Some(key) = map.next_key()? {
					match key {
						"kind" => kind = map.next_value()?,
						"value" => {
							let Some(kind) = kind else {
								return Err(serde::de::Error::missing_field("kind"));
							};
							value = Some(match kind {
								"map" => Data::Map(map.next_value()?),
								"bytes" => Data::Bytes(map.next_value()?),
								"mutation" => Data::Mutation(map.next_value()?),
								"template" => Data::Template(map.next_value()?),
								"leaf" => Data::Leaf(map.next_value()?),
								"branch" => Data::Branch(map.next_value()?),
								"directory" => Data::Directory(map.next_value()?),
								"file" => Data::File(map.next_value()?),
								"symlink" => Data::Symlink(map.next_value()?),
								"lock" => Data::Lock(map.next_value()?),
								"target" => Data::Target(map.next_value()?),
								_ => {
									return Err(serde::de::Error::unknown_variant(kind, &["kind"]))
								},
							});
						},
						_ => return Err(serde::de::Error::unknown_field(key, &["kind", "value"])),
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
