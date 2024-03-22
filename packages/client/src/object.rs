use crate as tg;
use crate::{
	branch, directory, file, id, leaf, lock, symlink, target, Branch, Client, Directory, Error,
	File, Leaf, Lock, Result, Symlink, Target,
};
use async_recursion::async_recursion;
use bytes::Bytes;
use derive_more::{Display, From, TryInto, TryUnwrap};
use futures::{stream::FuturesUnordered, TryStreamExt};
use http_body_util::BodyExt;
use tangram_error::error;
use tangram_util::http::{empty, full};

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
	Display,
	Eq,
	From,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	TryUnwrap,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
#[try_unwrap(ref)]
pub enum Id {
	Leaf(leaf::Id),
	Branch(branch::Id),
	Directory(directory::Id),
	File(file::Id),
	Symlink(symlink::Id),
	Lock(lock::Id),
	Target(target::Id),
}

/// An object.
#[derive(Clone, Debug, From, TryInto, TryUnwrap)]
#[try_unwrap(ref)]
pub enum Handle {
	Leaf(Leaf),
	Branch(Branch),
	Directory(Directory),
	File(File),
	Symlink(Symlink),
	Lock(Lock),
	Target(Target),
}

/// An object.
#[derive(Clone, Debug, From, TryInto, TryUnwrap)]
#[try_unwrap(ref)]
pub enum Object {
	Leaf(leaf::Object),
	Branch(branch::Object),
	Directory(directory::Object),
	File(file::Object),
	Symlink(symlink::Object),
	Lock(lock::Object),
	Target(target::Object),
}

/// Object data.
#[derive(Clone, Debug, From, TryInto)]
pub enum Data {
	Leaf(leaf::Data),
	Branch(branch::Data),
	Directory(directory::Data),
	File(file::Data),
	Symlink(symlink::Data),
	Lock(lock::Data),
	Target(target::Data),
}

#[derive(Debug)]
pub struct State<I, O> {
	pub id: Option<I>,
	pub object: Option<O>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetOutput {
	pub bytes: Bytes,
	pub count: Option<u64>,
	pub weight: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PutArg {
	pub bytes: Bytes,
	pub count: Option<u64>,
	pub weight: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PutOutput {
	pub incomplete: Vec<Id>,
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
			Id::Leaf(id) => Self::Leaf(Leaf::with_id(id)),
			Id::Branch(id) => Self::Branch(Branch::with_id(id)),
			Id::Directory(id) => Self::Directory(Directory::with_id(id)),
			Id::File(id) => Self::File(File::with_id(id)),
			Id::Symlink(id) => Self::Symlink(Symlink::with_id(id)),
			Id::Lock(id) => Self::Lock(Lock::with_id(id)),
			Id::Target(id) => Self::Target(Target::with_id(id)),
		}
	}

	#[must_use]
	pub fn with_object(object: Object) -> Self {
		match object {
			Object::Leaf(object) => Self::Leaf(Leaf::with_object(object)),
			Object::Branch(object) => Self::Branch(Branch::with_object(object)),
			Object::Directory(object) => Self::Directory(Directory::with_object(object)),
			Object::File(object) => Self::File(File::with_object(object)),
			Object::Symlink(object) => Self::Symlink(Symlink::with_object(object)),
			Object::Lock(object) => Self::Lock(Lock::with_object(object)),
			Object::Target(object) => Self::Target(Target::with_object(object)),
		}
	}

	pub async fn id(&self, tg: &dyn crate::Handle) -> Result<Id> {
		match self {
			Self::Leaf(object) => object.id(tg).await.cloned().map(Id::Leaf),
			Self::Branch(object) => object.id(tg).await.cloned().map(Id::Branch),
			Self::Directory(object) => object.id(tg).await.cloned().map(Id::Directory),
			Self::File(object) => object.id(tg).await.cloned().map(Id::File),
			Self::Symlink(object) => object.id(tg).await.cloned().map(Id::Symlink),
			Self::Lock(object) => object.id(tg).await.cloned().map(Id::Lock),
			Self::Target(object) => object.id(tg).await.cloned().map(Id::Target),
		}
	}

	pub async fn object(&self, tg: &dyn crate::Handle) -> Result<Object> {
		match self {
			Self::Leaf(object) => object.object(tg).await.cloned().map(Object::Leaf),
			Self::Branch(object) => object.object(tg).await.cloned().map(Object::Branch),
			Self::Directory(object) => object.object(tg).await.cloned().map(Object::Directory),
			Self::File(object) => object.object(tg).await.cloned().map(Object::File),
			Self::Symlink(object) => object.object(tg).await.cloned().map(Object::Symlink),
			Self::Lock(object) => object.object(tg).await.cloned().map(Object::Lock),
			Self::Target(object) => object.object(tg).await.cloned().map(Object::Target),
		}
	}

	pub async fn data(&self, tg: &dyn crate::Handle) -> Result<Data> {
		match self {
			Self::Leaf(object) => object.data(tg).await.map(Data::Leaf),
			Self::Branch(object) => object.data(tg).await.map(Data::Branch),
			Self::Directory(object) => object.data(tg).await.map(Data::Directory),
			Self::File(object) => object.data(tg).await.map(Data::File),
			Self::Symlink(object) => object.data(tg).await.map(Data::Symlink),
			Self::Lock(object) => object.data(tg).await.map(Data::Lock),
			Self::Target(object) => object.data(tg).await.map(Data::Target),
		}
	}

	#[async_recursion]
	pub async fn push(&self, tg: &dyn crate::Handle, remote: &dyn crate::Handle) -> Result<()> {
		let id = self.id(tg).await?;
		let data = self.data(tg).await?;
		let bytes = data.serialize()?;
		let arg = PutArg {
			bytes,
			count: None,
			weight: None,
		};
		let output = remote
			.put_object(&id.clone(), &arg)
			.await
			.map_err(|source| error!(!source, "failed to put the object"))?;
		output
			.incomplete
			.into_iter()
			.map(Self::with_id)
			.map(|object| async move { object.push(tg, remote).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(())
	}

	#[async_recursion]
	pub async fn pull(&self, tg: &dyn crate::Handle, remote: &dyn crate::Handle) -> Result<()> {
		let id = self.id(tg).await?;
		let output = remote
			.get_object(&id)
			.await
			.map_err(|source| error!(!source, "failed to put the object"))?;
		let arg = tg::object::PutArg {
			bytes: output.bytes,
			count: None,
			weight: None,
		};
		let output = tg.put_object(&id, &arg).await?;
		output
			.incomplete
			.into_iter()
			.map(Self::with_id)
			.map(|object| async move { object.pull(tg, remote).await })
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
	pub fn serialize(&self) -> Result<Bytes> {
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

	pub fn deserialize(kind: Kind, bytes: &Bytes) -> Result<Self> {
		match kind {
			Kind::Leaf => Ok(Self::Leaf(leaf::Data::deserialize(bytes)?)),
			Kind::Branch => Ok(Self::Branch(branch::Data::deserialize(bytes)?)),
			Kind::Directory => Ok(Self::Directory(directory::Data::deserialize(bytes)?)),
			Kind::File => Ok(Self::File(file::Data::deserialize(bytes)?)),
			Kind::Symlink => Ok(Self::Symlink(symlink::Data::deserialize(bytes)?)),
			Kind::Lock => Ok(Self::Lock(lock::Data::deserialize(bytes)?)),
			Kind::Target => Ok(Self::Target(target::Data::deserialize(bytes)?)),
		}
	}
}

impl Client {
	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<tg::object::GetOutput>> {
		let method = http::Method::GET;
		let uri = format!("/objects/{id}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = tg::object::GetOutput {
			bytes,
			count: None,
			weight: None,
		};
		Ok(Some(output))
	}

	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
	) -> Result<tg::object::PutOutput> {
		let method = http::Method::PUT;
		let uri = format!("/objects/{id}");
		let body = full(arg.bytes.clone());
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}

	pub async fn push_object(&self, id: &tg::object::Id) -> Result<()> {
		let method = http::Method::POST;
		let uri = format!("/objects/{id}/push");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
			return Err(error);
		}
		Ok(())
	}

	pub async fn pull_object(&self, id: &tg::object::Id) -> Result<()> {
		let method = http::Method::POST;
		let uri = format!("/objects/{id}/pull");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
			return Err(error);
		}
		Ok(())
	}
}

impl<I, O> State<I, O> {
	#[must_use]
	pub fn new(id: Option<I>, object: Option<O>) -> Self {
		assert!(id.is_some() || object.is_some());
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
	pub fn with_object(object: O) -> Self {
		Self {
			id: None,
			object: Some(object),
		}
	}

	#[must_use]
	pub fn id(&self) -> &Option<I> {
		&self.id
	}

	#[must_use]
	pub fn object(&self) -> &Option<O> {
		&self.object
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
	type Error = Error;

	fn try_from(value: crate::Id) -> Result<Self, Self::Error> {
		match value.kind() {
			crate::id::Kind::Leaf => Ok(Self::Leaf(value.try_into()?)),
			crate::id::Kind::Branch => Ok(Self::Branch(value.try_into()?)),
			crate::id::Kind::Directory => Ok(Self::Directory(value.try_into()?)),
			crate::id::Kind::File => Ok(Self::File(value.try_into()?)),
			crate::id::Kind::Symlink => Ok(Self::Symlink(value.try_into()?)),
			crate::id::Kind::Lock => Ok(Self::Lock(value.try_into()?)),
			crate::id::Kind::Target => Ok(Self::Target(value.try_into()?)),
			kind => Err(error!(%kind, "expected an object ID")),
		}
	}
}

impl std::str::FromStr for Id {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

impl Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", tg::id::Kind::from(*self))
	}
}

impl std::str::FromStr for Kind {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		tg::id::Kind::from_str(s)?.try_into()
	}
}

impl From<Kind> for id::Kind {
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

impl TryFrom<id::Kind> for Kind {
	type Error = Error;

	fn try_from(value: id::Kind) -> Result<Self, Self::Error> {
		match value {
			id::Kind::Leaf => Ok(Self::Leaf),
			id::Kind::Branch => Ok(Self::Branch),
			id::Kind::Directory => Ok(Self::Directory),
			id::Kind::File => Ok(Self::File),
			id::Kind::Symlink => Ok(Self::Symlink),
			id::Kind::Lock => Ok(Self::Lock),
			id::Kind::Target => Ok(Self::Target),
			kind => Err(error!(%kind, "invalid kind")),
		}
	}
}

impl TryFrom<Data> for Object {
	type Error = Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		Ok(match data {
			Data::Leaf(data) => Self::Leaf(data.try_into()?),
			Data::Branch(data) => Self::Branch(data.try_into()?),
			Data::Directory(data) => Self::Directory(data.try_into()?),
			Data::File(data) => Self::File(data.try_into()?),
			Data::Symlink(data) => Self::Symlink(data.try_into()?),
			Data::Lock(data) => Self::Lock(data.try_into()?),
			Data::Target(data) => Self::Target(data.try_into()?),
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
