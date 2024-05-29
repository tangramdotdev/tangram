use crate as tg;
use futures::FutureExt as _;
use num::ToPrimitive as _;
use std::sync::Arc;
use tokio::io::AsyncRead;

pub use self::read::Reader;

pub mod checksum;
pub mod compress;
pub mod create;
pub mod decompress;
pub mod download;
pub mod read;

/// A blob kind.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
	Leaf,
	Branch,
}

/// A blob ID.
#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::From,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub enum Id {
	Leaf(tg::leaf::Id),
	Branch(tg::branch::Id),
}

#[derive(Clone, Debug, derive_more::From)]
pub enum Blob {
	Leaf(tg::Leaf),
	Branch(tg::Branch),
}

#[derive(Clone, Debug, derive_more::From, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Data {
	Leaf(tg::leaf::Data),
	Branch(tg::branch::Data),
}

#[derive(Clone, Debug, derive_more::From, derive_more::TryUnwrap)]
pub enum Object {
	Leaf(Arc<tg::leaf::Object>),
	Branch(Arc<tg::branch::Object>),
}

impl Blob {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		match id {
			Id::Leaf(id) => tg::Leaf::with_id(id).into(),
			Id::Branch(id) => tg::Branch::with_id(id).into(),
		}
	}

	pub async fn id<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		match self {
			Self::Leaf(leaf) => Ok(leaf.id(handle, transaction).await?.into()),
			Self::Branch(branch) => Ok(branch.id(handle, transaction).await?.into()),
		}
	}

	pub async fn data<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		match self {
			Self::Leaf(leaf) => Ok(leaf.data(handle, transaction).await?.into()),
			Self::Branch(branch) => Ok(branch.data(handle, transaction).await?.into()),
		}
	}

	pub async fn load<H>(&self, handle: &H) -> tg::Result<Object>
	where
		H: tg::Handle,
	{
		match self {
			Self::Leaf(leaf) => leaf.load(handle).await.map(Into::into),
			Self::Branch(branch) => branch.load(handle).await.map(Into::into),
		}
	}

	pub fn unload(&self) {
		match self {
			Self::Leaf(leaf) => leaf.unload(),
			Self::Branch(branch) => branch.unload(),
		}
	}

	pub async fn store<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		match self {
			Self::Leaf(leaf) => leaf.store(handle, transaction).await.map(Into::into),
			Self::Branch(branch) => branch.store(handle, transaction).await.map(Into::into),
		}
	}
}

impl Blob {
	/// Create a [`Blob`] from an `AsyncRead`.
	#[must_use]
	pub fn new(children: Vec<tg::branch::Child>) -> Self {
		match children.len() {
			0 => Self::default(),
			1 => children.into_iter().next().unwrap().blob,
			_ => tg::Branch::new(children).into(),
		}
	}

	pub async fn with_reader<H>(
		handle: &H,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let output = handle.create_blob(reader).boxed().await?;
		let blob = Self::with_id(output.blob);
		Ok(blob)
	}

	pub async fn size<H>(&self, handle: &H) -> tg::Result<u64>
	where
		H: tg::Handle,
	{
		match self {
			Self::Leaf(leaf) => {
				let bytes = &leaf.bytes(handle).await?;
				let size = bytes.len().to_u64().unwrap();
				Ok(size)
			},
			Self::Branch(branch) => {
				let children = &branch.children(handle).await?;
				let size = children.iter().map(|child| child.size).sum();
				Ok(size)
			},
		}
	}
}

impl std::fmt::Display for Id {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Leaf(id) => write!(f, "{id}"),
			Self::Branch(id) => write!(f, "{id}"),
		}
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Leaf(id) => id.into(),
			Id::Branch(id) => id.into(),
		}
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		match value.kind() {
			tg::id::Kind::Leaf => Ok(Self::Leaf(value.try_into()?)),
			tg::id::Kind::Branch => Ok(Self::Branch(value.try_into()?)),
			value => Err(tg::error!(%value, "expected a blob ID")),
		}
	}
}

impl From<Id> for tg::object::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Leaf(id) => id.into(),
			Id::Branch(id) => id.into(),
		}
	}
}

impl TryFrom<tg::object::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: tg::object::Id) -> tg::Result<Self, Self::Error> {
		match value {
			tg::object::Id::Leaf(value) => Ok(value.into()),
			tg::object::Id::Branch(value) => Ok(value.into()),
			value => Err(tg::error!(%value, "expected a blob ID")),
		}
	}
}

impl Default for Blob {
	fn default() -> Self {
		Self::Leaf(tg::Leaf::default())
	}
}

impl std::fmt::Display for Blob {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Leaf(leaf) => write!(f, "{leaf}"),
			Self::Branch(branch) => write!(f, "{branch}"),
		}
	}
}

impl From<Blob> for tg::object::Handle {
	fn from(value: Blob) -> Self {
		match value {
			Blob::Leaf(leaf) => Self::Leaf(leaf),
			Blob::Branch(branch) => Self::Branch(branch),
		}
	}
}

impl TryFrom<tg::object::Handle> for Blob {
	type Error = tg::Error;

	fn try_from(value: tg::object::Handle) -> tg::Result<Self, Self::Error> {
		match value {
			tg::object::Handle::Leaf(leaf) => Ok(Self::Leaf(leaf)),
			tg::object::Handle::Branch(branch) => Ok(Self::Branch(branch)),
			_ => Err(tg::error!("expected a blob")),
		}
	}
}

impl From<Blob> for tg::Value {
	fn from(value: Blob) -> Self {
		tg::object::Handle::from(value).into()
	}
}

impl TryFrom<tg::Value> for Blob {
	type Error = tg::Error;

	fn try_from(value: tg::Value) -> tg::Result<Self, Self::Error> {
		tg::object::Handle::try_from(value)
			.map_err(|source| tg::error!(!source, "invalid value"))?
			.try_into()
	}
}

impl From<String> for Blob {
	fn from(value: String) -> Self {
		tg::Leaf::from(value).into()
	}
}

impl From<&str> for Blob {
	fn from(value: &str) -> Self {
		tg::Leaf::from(value).into()
	}
}
