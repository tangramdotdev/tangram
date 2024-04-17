pub use self::child::Child;
use crate::{self as tg, util::arc::Ext as _};
use bytes::Bytes;
use futures::{stream::FuturesOrdered, FutureExt as _, TryStreamExt as _};
use std::sync::Arc;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub struct Id(crate::Id);

#[derive(Clone, Debug)]
pub struct Branch {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	pub children: Vec<Child>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub children: Vec<child::Data>,
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::Branch, bytes))
	}
}

impl Branch {
	#[must_use]
	pub fn with_state(state: State) -> Self {
		let state = Arc::new(std::sync::RwLock::new(state));
		Self { state }
	}

	#[must_use]
	pub fn state(&self) -> &std::sync::RwLock<State> {
		&self.state
	}

	#[must_use]
	pub fn with_id(id: Id) -> Self {
		let state = State::with_id(id);
		let state = Arc::new(std::sync::RwLock::new(state));
		Self { state }
	}

	#[must_use]
	pub fn with_object(object: impl Into<Arc<Object>>) -> Self {
		let state = State::with_object(object);
		let state = Arc::new(std::sync::RwLock::new(state));
		Self { state }
	}

	pub async fn id<H>(&self, tg: &H, transaction: Option<&H::Transaction<'_>>) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		self.store(tg, transaction).await
	}

	pub async fn object(&self, tg: &impl tg::Handle) -> tg::Result<Arc<Object>> {
		self.load(tg).await
	}

	pub async fn load(&self, tg: &impl tg::Handle) -> tg::Result<Arc<Object>> {
		self.try_load(tg)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load(&self, tg: &impl tg::Handle) -> tg::Result<Option<Arc<Object>>> {
		if let Some(object) = self.state.read().unwrap().object.clone() {
			return Ok(Some(object));
		}
		let id = self.state.read().unwrap().id.clone().unwrap();
		let Some(output) = tg.try_get_object(&id.into()).await? else {
			return Ok(None);
		};
		let data = Data::deserialize(&output.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
		let object = Object::try_from(data)?;
		let object = Arc::new(object);
		self.state.write().unwrap().object.replace(object.clone());
		Ok(Some(object))
	}

	pub async fn store<H>(&self, tg: &H, transaction: Option<&H::Transaction<'_>>) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		if let Some(id) = self.state.read().unwrap().id.clone() {
			return Ok(id);
		}
		let data = self.data(tg, transaction).await?;
		let bytes = data.serialize()?;
		let id = Id::new(&bytes);
		let arg = tg::object::PutArg {
			bytes,
			count: None,
			weight: None,
		};
		tg.put_object(&id.clone().into(), arg, transaction)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;
		self.state.write().unwrap().id.replace(id.clone());
		Ok(id)
	}

	pub async fn data<H>(
		&self,
		tg: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		let object = self.object(tg).await?;
		let children = object
			.children
			.iter()
			.map(|child| async {
				Ok::<_, tg::Error>(child::Data {
					blob: child.blob.id(tg, transaction).await?,
					size: child.size,
				})
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		Ok(Data { children })
	}
}

impl Branch {
	#[must_use]
	pub fn new(children: Vec<Child>) -> Self {
		Self::with_object(Object { children })
	}

	pub async fn children(
		&self,
		tg: &impl tg::Handle,
	) -> tg::Result<impl std::ops::Deref<Target = Vec<Child>>> {
		Ok(self.object(tg).await?.map(|object| &object.children))
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
		self.children
			.iter()
			.map(|child| child.blob.clone().into())
			.collect()
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let children = data
			.children
			.into_iter()
			.map(|child| Child {
				blob: tg::Blob::with_id(child.blob),
				size: child.size,
			})
			.collect();
		Ok(Self { children })
	}
}

impl std::fmt::Display for Branch {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(id) = self.state.read().unwrap().id().as_ref() {
			write!(f, "{id}")?;
		} else {
			write!(f, "<unstored>")?;
		}

		Ok(())
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		value.0
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Branch {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

pub mod child {
	use crate as tg;

	#[derive(Clone, Debug)]
	pub struct Child {
		pub blob: tg::Blob,
		pub size: u64,
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Data {
		pub blob: tg::blob::Id,
		pub size: u64,
	}
}
