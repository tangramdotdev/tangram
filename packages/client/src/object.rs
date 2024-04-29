use crate as tg;
use bytes::Bytes;
use futures::{stream::FuturesUnordered, FutureExt as _, TryStreamExt as _};
use std::sync::Arc;
use tangram_http::{incoming::ResponseExt as _, Outgoing};

pub mod get;
pub mod pull;
pub mod push;
pub mod put;

/// An object kind.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
	Leaf,
	Branch,
	Directory,
	File,
	Symlink,
	Lock,
	Target,
}

/// An object ID.
#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::From,
	derive_more::TryUnwrap,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
#[try_unwrap(ref)]
pub enum Id {
	Leaf(tg::leaf::Id),
	Branch(tg::branch::Id),
	Directory(tg::directory::Id),
	File(tg::file::Id),
	Symlink(tg::symlink::Id),
	Lock(tg::lock::Id),
	Target(tg::target::Id),
}

/// An object.
#[derive(Clone, Debug, derive_more::From, derive_more::TryInto, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Handle {
	Leaf(tg::Leaf),
	Branch(tg::Branch),
	Directory(tg::Directory),
	File(tg::File),
	Symlink(tg::Symlink),
	Lock(tg::Lock),
	Target(tg::Target),
}

#[derive(Debug)]
pub struct State<I, O> {
	pub id: Option<I>,
	pub object: Option<Arc<O>>,
}

/// An object.
#[derive(Clone, Debug, derive_more::From, derive_more::TryInto, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Object {
	Leaf(Arc<tg::leaf::Object>),
	Branch(Arc<tg::branch::Object>),
	Directory(Arc<tg::directory::Object>),
	File(Arc<tg::file::Object>),
	Symlink(Arc<tg::symlink::Object>),
	Lock(Arc<tg::lock::Object>),
	Target(Arc<tg::target::Object>),
}

/// Object data.
#[derive(Clone, Debug, derive_more::From, derive_more::TryInto, derive_more::TryUnwrap)]
pub enum Data {
	Leaf(tg::leaf::Data),
	Branch(tg::branch::Data),
	Directory(tg::directory::Data),
	File(tg::file::Data),
	Symlink(tg::symlink::Data),
	Lock(tg::lock::Data),
	Target(tg::target::Data),
}

impl Id {
	#[must_use]
	pub fn kind(&self) -> Kind {
		match self {
			Self::Leaf(_) => Kind::Leaf,
			Self::Branch(_) => Kind::Branch,
			Self::Directory(_) => Kind::Directory,
			Self::File(_) => Kind::File,
			Self::Symlink(_) => Kind::Symlink,
			Self::Lock(_) => Kind::Lock,
			Self::Target(_) => Kind::Target,
		}
	}
}

impl Handle {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		match id {
			Id::Leaf(id) => Self::Leaf(tg::Leaf::with_id(id)),
			Id::Branch(id) => Self::Branch(tg::Branch::with_id(id)),
			Id::Directory(id) => Self::Directory(tg::Directory::with_id(id)),
			Id::File(id) => Self::File(tg::File::with_id(id)),
			Id::Symlink(id) => Self::Symlink(tg::Symlink::with_id(id)),
			Id::Lock(id) => Self::Lock(tg::Lock::with_id(id)),
			Id::Target(id) => Self::Target(tg::Target::with_id(id)),
		}
	}

	#[must_use]
	pub fn with_object(object: Object) -> Self {
		match object {
			Object::Leaf(object) => Self::Leaf(tg::Leaf::with_object(object)),
			Object::Branch(object) => Self::Branch(tg::Branch::with_object(object)),
			Object::Directory(object) => Self::Directory(tg::Directory::with_object(object)),
			Object::File(object) => Self::File(tg::File::with_object(object)),
			Object::Symlink(object) => Self::Symlink(tg::Symlink::with_object(object)),
			Object::Lock(object) => Self::Lock(tg::Lock::with_object(object)),
			Object::Target(object) => Self::Target(tg::Target::with_object(object)),
		}
	}
	pub async fn id<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Id>
	where
		H: crate::Handle,
	{
		match self {
			Self::Leaf(object) => object.id(handle, transaction).await.map(Id::Leaf),
			Self::Branch(object) => object.id(handle, transaction).await.map(Id::Branch),
			Self::Directory(object) => object.id(handle, transaction).await.map(Id::Directory),
			Self::File(object) => object.id(handle, transaction).await.map(Id::File),
			Self::Symlink(object) => object.id(handle, transaction).await.map(Id::Symlink),
			Self::Lock(object) => object.id(handle, transaction).await.map(Id::Lock),
			Self::Target(object) => object.id(handle, transaction).await.map(Id::Target),
		}
	}

	pub async fn object<H>(&self, handle: &H) -> tg::Result<Object>
	where
		H: crate::Handle,
	{
		match self {
			Self::Leaf(object) => object.object(handle).await.map(Object::Leaf),
			Self::Branch(object) => object.object(handle).await.map(Object::Branch),
			Self::Directory(object) => object.object(handle).await.map(Object::Directory),
			Self::File(object) => object.object(handle).await.map(Object::File),
			Self::Symlink(object) => object.object(handle).await.map(Object::Symlink),
			Self::Lock(object) => object.object(handle).await.map(Object::Lock),
			Self::Target(object) => object.object(handle).await.map(Object::Target),
		}
	}

	pub async fn data<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Data>
	where
		H: crate::Handle,
	{
		match self {
			Self::Leaf(object) => object.data(handle, transaction).await.map(Data::Leaf),
			Self::Branch(object) => object.data(handle, transaction).await.map(Data::Branch),
			Self::Directory(object) => object.data(handle, transaction).await.map(Data::Directory),
			Self::File(object) => object.data(handle, transaction).await.map(Data::File),
			Self::Symlink(object) => object.data(handle, transaction).await.map(Data::Symlink),
			Self::Lock(object) => object.data(handle, transaction).await.map(Data::Lock),
			Self::Target(object) => object.data(handle, transaction).await.map(Data::Target),
		}
	}

	pub async fn push<H1, H2>(
		&self,
		handle: &H1,
		remote: &H2,
		transaction: Option<&H2::Transaction<'_>>,
	) -> tg::Result<()>
	where
		H1: crate::Handle,
		H2: crate::Handle,
	{
		let id = self.id(handle, None).await?;
		let data = self.data(handle, None).await?;
		let bytes = data.serialize()?;
		let arg = tg::object::put::Arg {
			bytes,
			count: None,
			weight: None,
		};
		let output = remote
			.put_object(&id.clone(), arg, transaction)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;
		output
			.incomplete
			.into_iter()
			.map(Self::with_id)
			.map(|object| async move { object.push(handle, remote, transaction).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(())
	}

	pub async fn pull<H1, H2>(
		&self,
		handle: &H1,
		remote: &H2,
		transaction: Option<&H1::Transaction<'_>>,
	) -> tg::Result<()>
	where
		H1: crate::Handle,
		H2: crate::Handle,
	{
		let id = self.id(handle, transaction).await?;
		let output = remote
			.get_object(&id)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;
		let arg = tg::object::put::Arg {
			bytes: output.bytes,
			count: None,
			weight: None,
		};
		let output = handle.put_object(&id, arg, transaction).boxed().await?;
		output
			.incomplete
			.into_iter()
			.map(Self::with_id)
			.map(|object| async move { object.pull(handle, remote, transaction).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(())
	}
}

impl Data {
	#[must_use]
	pub fn kind(&self) -> Kind {
		match self {
			Self::Leaf(_) => Kind::Leaf,
			Self::Branch(_) => Kind::Branch,
			Self::Directory(_) => Kind::Directory,
			Self::File(_) => Kind::File,
			Self::Symlink(_) => Kind::Symlink,
			Self::Lock(_) => Kind::Lock,
			Self::Target(_) => Kind::Target,
		}
	}

	#[must_use]
	pub fn children(&self) -> Vec<self::Id> {
		match self {
			Self::Leaf(data) => data.children(),
			Self::Branch(data) => data.children(),
			Self::Directory(data) => data.children(),
			Self::File(data) => data.children(),
			Self::Symlink(data) => data.children(),
			Self::Lock(data) => data.children(),
			Self::Target(data) => data.children(),
		}
	}

	#[allow(dead_code)]
	pub fn serialize(&self) -> tg::Result<Bytes> {
		match self {
			Self::Leaf(data) => Ok(data.serialize()?),
			Self::Branch(data) => Ok(data.serialize()?),
			Self::Directory(data) => Ok(data.serialize()?),
			Self::File(data) => Ok(data.serialize()?),
			Self::Symlink(data) => Ok(data.serialize()?),
			Self::Lock(data) => Ok(data.serialize()?),
			Self::Target(data) => Ok(data.serialize()?),
		}
	}

	pub fn deserialize(kind: Kind, bytes: &Bytes) -> tg::Result<Self> {
		match kind {
			Kind::Leaf => Ok(Self::Leaf(tg::leaf::Data::deserialize(bytes)?)),
			Kind::Branch => Ok(Self::Branch(tg::branch::Data::deserialize(bytes)?)),
			Kind::Directory => Ok(Self::Directory(tg::directory::Data::deserialize(bytes)?)),
			Kind::File => Ok(Self::File(tg::file::Data::deserialize(bytes)?)),
			Kind::Symlink => Ok(Self::Symlink(tg::symlink::Data::deserialize(bytes)?)),
			Kind::Lock => Ok(Self::Lock(tg::lock::Data::deserialize(bytes)?)),
			Kind::Target => Ok(Self::Target(tg::target::Data::deserialize(bytes)?)),
		}
	}
}

impl tg::Client {
	pub async fn push_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/objects/{id}/push");
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}

	pub async fn pull_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/objects/{id}/pull");
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}

impl<I, O> State<I, O> {
	#[must_use]
	pub fn new(id: Option<I>, object: Option<impl Into<Arc<O>>>) -> Self {
		assert!(id.is_some() || object.is_some());
		let object = object.map(Into::into);
		Self { id, object }
	}

	#[must_use]
	pub fn with_id(id: I) -> Self {
		Self {
			id: Some(id),
			object: None,
		}
	}

	#[must_use]
	pub fn with_object(object: impl Into<Arc<O>>) -> Self {
		Self {
			id: None,
			object: Some(object.into()),
		}
	}

	#[must_use]
	pub fn id(&self) -> Option<&I> {
		self.id.as_ref()
	}

	#[must_use]
	pub fn object(&self) -> Option<&Arc<O>> {
		self.object.as_ref()
	}
}

impl From<self::Id> for crate::Id {
	fn from(value: self::Id) -> Self {
		match value {
			self::Id::Leaf(id) => id.into(),
			self::Id::Branch(id) => id.into(),
			self::Id::Directory(id) => id.into(),
			self::Id::File(id) => id.into(),
			self::Id::Symlink(id) => id.into(),
			self::Id::Lock(id) => id.into(),
			self::Id::Target(id) => id.into(),
		}
	}
}

impl TryFrom<crate::Id> for self::Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		match value.kind() {
			crate::id::Kind::Leaf => Ok(Self::Leaf(value.try_into()?)),
			crate::id::Kind::Branch => Ok(Self::Branch(value.try_into()?)),
			crate::id::Kind::Directory => Ok(Self::Directory(value.try_into()?)),
			crate::id::Kind::File => Ok(Self::File(value.try_into()?)),
			crate::id::Kind::Symlink => Ok(Self::Symlink(value.try_into()?)),
			crate::id::Kind::Lock => Ok(Self::Lock(value.try_into()?)),
			crate::id::Kind::Target => Ok(Self::Target(value.try_into()?)),
			kind => Err(tg::error!(%kind, "expected an object ID")),
		}
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", tg::id::Kind::from(*self))
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::id::Kind::from_str(s)?.try_into()
	}
}

impl From<Kind> for tg::id::Kind {
	fn from(value: Kind) -> Self {
		match value {
			Kind::Leaf => Self::Leaf,
			Kind::Branch => Self::Branch,
			Kind::Directory => Self::Directory,
			Kind::File => Self::File,
			Kind::Symlink => Self::Symlink,
			Kind::Lock => Self::Lock,
			Kind::Target => Self::Target,
		}
	}
}

impl TryFrom<tg::id::Kind> for Kind {
	type Error = tg::Error;

	fn try_from(value: tg::id::Kind) -> tg::Result<Self, Self::Error> {
		match value {
			tg::id::Kind::Leaf => Ok(Self::Leaf),
			tg::id::Kind::Branch => Ok(Self::Branch),
			tg::id::Kind::Directory => Ok(Self::Directory),
			tg::id::Kind::File => Ok(Self::File),
			tg::id::Kind::Symlink => Ok(Self::Symlink),
			tg::id::Kind::Lock => Ok(Self::Lock),
			tg::id::Kind::Target => Ok(Self::Target),
			kind => Err(tg::error!(%kind, "invalid kind")),
		}
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		Ok(match data {
			Data::Leaf(data) => Self::Leaf(Arc::new(tg::leaf::Object::try_from(data)?)),
			Data::Branch(data) => Self::Branch(Arc::new(tg::branch::Object::try_from(data)?)),
			Data::Directory(data) => {
				Self::Directory(Arc::new(tg::directory::Object::try_from(data)?))
			},
			Data::File(data) => Self::File(Arc::new(tg::file::Object::try_from(data)?)),
			Data::Symlink(data) => Self::Symlink(Arc::new(tg::symlink::Object::try_from(data)?)),
			Data::Lock(data) => Self::Lock(Arc::new(tg::lock::Object::try_from(data)?)),
			Data::Target(data) => Self::Target(Arc::new(tg::target::Object::try_from(data)?)),
		})
	}
}

impl std::fmt::Display for Handle {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
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
