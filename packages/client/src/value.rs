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
use std::collections::BTreeMap;

/// A value.
#[derive(Clone, Debug, From, TryInto, serde::Deserialize, TryUnwrap)]
#[serde(try_from = "Data")]
#[try_unwrap(ref)]
pub enum Value {
	/// A null value.
	Null(()),

	/// A bool value.
	Bool(bool),

	/// A number value.
	Number(f64),

	/// A string value.
	String(String),

	/// A bytes value.
	Bytes(Bytes),

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

	/// A mutation value.
	Mutation(Mutation),

	/// A template value.
	Template(Template),

	/// An array value.
	Array(Vec<Value>),

	/// A map value.
	Map(BTreeMap<String, Value>),
}

/// Value data.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum Data {
	Null(()),
	Bool(bool),
	Number(f64),
	String(String),
	Bytes(Bytes),
	Leaf(leaf::Id),
	Branch(branch::Id),
	Directory(directory::Id),
	File(file::Id),
	Symlink(symlink::Id),
	Template(template::Data),
	Mutation(mutation::Data),
	Lock(lock::Id),
	Target(target::Id),
	Array(Vec<Data>),
	Map(BTreeMap<String, Data>),
}

impl Value {
	pub fn object(&self) -> Option<object::Handle> {
		match self {
			Value::Leaf(leaf) => Some(object::Handle::Leaf(leaf.clone())),
			Value::Branch(branch) => Some(object::Handle::Branch(branch.clone())),
			Value::Directory(directory) => Some(object::Handle::Directory(directory.clone())),
			Value::File(file) => Some(object::Handle::File(file.clone())),
			Value::Symlink(symlink) => Some(object::Handle::Symlink(symlink.clone())),
			Value::Lock(lock) => Some(object::Handle::Lock(lock.clone())),
			Value::Target(target) => Some(object::Handle::Target(target.clone())),
			_ => None,
		}
	}

	#[async_recursion]
	pub async fn data(&self, tg: &dyn Handle) -> Result<Data> {
		let data = match self {
			Self::Null(()) => Data::Null(()),
			Self::Bool(bool) => Data::Bool(*bool),
			Self::Number(number) => Data::Number(*number),
			Self::String(string) => Data::String(string.clone()),
			Self::Bytes(bytes) => Data::Bytes(bytes.clone()),
			Self::Leaf(leaf) => Data::Leaf(leaf.id(tg).await?.clone()),
			Self::Branch(branch) => Data::Branch(branch.id(tg).await?.clone()),
			Self::Directory(directory) => Data::Directory(directory.id(tg).await?.clone()),
			Self::File(file) => Data::File(file.id(tg).await?.clone()),
			Self::Symlink(symlink) => Data::Symlink(symlink.id(tg).await?.clone()),
			Self::Lock(lock) => Data::Lock(lock.id(tg).await?.clone()),
			Self::Target(target) => Data::Target(target.id(tg).await?.clone()),
			Self::Mutation(mutation) => Data::Mutation(mutation.data(tg).await?.clone()),
			Self::Template(template) => Data::Template(template.data(tg).await?.clone()),
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
			Self::Null(()) | Self::Bool(_) | Self::Number(_) | Self::String(_) | Self::Bytes(_) => {
				vec![]
			},
			Self::Leaf(id) => vec![id.clone().into()],
			Self::Branch(id) => vec![id.clone().into()],
			Self::Directory(id) => vec![id.clone().into()],
			Self::File(id) => vec![id.clone().into()],
			Self::Symlink(id) => vec![id.clone().into()],
			Self::Lock(id) => vec![id.clone().into()],
			Self::Target(id) => vec![id.clone().into()],
			Self::Mutation(mutation) => mutation.children(),
			Self::Template(template) => template.children(),
			Self::Array(array) => array.iter().flat_map(Self::children).collect(),
			Self::Map(map) => map.values().flat_map(Self::children).collect(),
		}
	}
}

impl std::fmt::Display for Value {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Value::Null(()) => {
				write!(f, "null")?;
			},
			Value::Bool(bool) => {
				write!(f, "{bool}")?;
			},
			Value::Number(number) => {
				write!(f, "{number}")?;
			},
			Value::String(string) => {
				write!(f, "\"{string}\"")?;
			},
			Value::Bytes(bytes) => {
				write!(f, "{}", hex::encode(bytes))?;
			},
			Value::Leaf(leaf) => {
				write!(f, "{leaf}")?;
			},
			Value::Branch(branch) => {
				write!(f, "{branch}")?;
			},
			Value::Directory(directory) => {
				write!(f, "{directory}")?;
			},
			Value::File(file) => {
				write!(f, "{file}")?;
			},
			Value::Symlink(symlink) => {
				write!(f, "{symlink}")?;
			},
			Value::Lock(lock) => {
				write!(f, "{lock}")?;
			},
			Value::Target(target) => {
				write!(f, "{target}")?;
			},
			Value::Mutation(mutation) => {
				write!(f, "{mutation}")?;
			},
			Value::Template(template) => {
				write!(f, "{template}")?;
			},
			Value::Array(array) => {
				write!(f, "[")?;
				for (i, value) in array.iter().enumerate() {
					write!(f, "{value}")?;
					if i < array.len() - 1 {
						write!(f, ", ")?;
					}
				}
				write!(f, "]")?;
			},
			Value::Map(map) => {
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
		}
		Ok(())
	}
}

impl TryFrom<Data> for Value {
	type Error = Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		Ok(match data {
			Data::Null(()) => Self::Null(()),
			Data::Bool(bool) => Self::Bool(bool),
			Data::Number(number) => Self::Number(number),
			Data::String(string) => Self::String(string),
			Data::Bytes(bytes) => Self::Bytes(bytes),
			Data::Leaf(id) => Self::Leaf(Leaf::with_id(id)),
			Data::Branch(id) => Self::Branch(Branch::with_id(id)),
			Data::Directory(id) => Self::Directory(Directory::with_id(id)),
			Data::File(id) => Self::File(File::with_id(id)),
			Data::Symlink(id) => Self::Symlink(Symlink::with_id(id)),
			Data::Lock(id) => Self::Lock(Lock::with_id(id)),
			Data::Target(id) => Self::Target(Target::with_id(id)),
			Data::Mutation(mutation) => Self::Mutation(mutation.try_into()?),
			Data::Template(template) => Self::Template(template.try_into()?),
			Data::Array(array) => {
				Self::Array(array.into_iter().map(TryInto::try_into).try_collect()?)
			},
			Data::Map(map) => Self::Map(
				map.into_iter()
					.map(|(key, value)| Ok::<_, Error>((key, value.try_into()?)))
					.try_collect()?,
			),
		})
	}
}
