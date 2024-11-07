use super::{Data, Id, Object};
use crate as tg;
use futures::FutureExt as _;
use num::ToPrimitive as _;
use tokio::io::AsyncRead;

#[derive(Clone, Debug, derive_more::From, derive_more::TryInto)]
pub enum Blob {
	Leaf(tg::Leaf),
	Branch(tg::Branch),
}

impl Blob {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		match id {
			Id::Leaf(id) => tg::Leaf::with_id(id).into(),
			Id::Branch(id) => tg::Branch::with_id(id).into(),
		}
	}

	pub async fn id<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		match self {
			Self::Leaf(leaf) => Ok(leaf.id(handle).await?.into()),
			Self::Branch(branch) => Ok(branch.id(handle).await?.into()),
		}
	}

	pub async fn object<H>(&self, handle: &H) -> tg::Result<Object>
	where
		H: tg::Handle,
	{
		self.load(handle).await
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

	pub async fn store<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		match self {
			Self::Leaf(leaf) => leaf.store(handle).await.map(Into::into),
			Self::Branch(branch) => branch.store(handle).await.map(Into::into),
		}
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		match self {
			Self::Leaf(leaf) => Ok(leaf.data(handle).await?.into()),
			Self::Branch(branch) => Ok(branch.data(handle).await?.into()),
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

impl Default for Blob {
	fn default() -> Self {
		Self::Leaf(tg::Leaf::default())
	}
}

impl From<Blob> for tg::Object {
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
