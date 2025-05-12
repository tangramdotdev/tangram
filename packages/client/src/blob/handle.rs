use super::{Data, Id, Object};
use crate as tg;
use bytes::Bytes;
use futures::FutureExt as _;
use num::ToPrimitive;
use std::{pin::pin, sync::Arc};
use tokio::io::{AsyncRead, AsyncReadExt as _};

#[derive(Clone, Debug)]
pub struct Blob {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

impl Blob {
	#[must_use]
	pub fn with_state(state: State) -> Self {
		let state = Arc::new(std::sync::RwLock::new(state));
		Self { state }
	}

	#[must_use]
	pub fn state(&self) -> &Arc<std::sync::RwLock<State>> {
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

	#[must_use]
	pub fn id(&self) -> Id {
		if let Some(id) = self.state.read().unwrap().id.clone() {
			return id;
		}
		let object = self.state.read().unwrap().object.clone().unwrap();
		let data = object.to_data();
		let bytes = data.serialize().unwrap();
		let id = Id::new(&bytes);
		self.state.write().unwrap().id.replace(id.clone());
		id
	}

	pub async fn object<H>(&self, handle: &H) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.load(handle).await
	}

	pub async fn load<H>(&self, handle: &H) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.try_load(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load<H>(&self, handle: &H) -> tg::Result<Option<Arc<Object>>>
	where
		H: tg::Handle,
	{
		if let Some(object) = self.state.read().unwrap().object.clone() {
			return Ok(Some(object));
		}
		let id = self.state.read().unwrap().id.clone().unwrap();
		let Some(output) = handle.try_get_object(&id.into()).await? else {
			return Ok(None);
		};
		let data = Data::deserialize(output.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
		let object = Object::try_from(data)?;
		let object = Arc::new(object);
		self.state.write().unwrap().object.replace(object.clone());
		Ok(Some(object))
	}

	pub fn unload(&self) {
		self.state.write().unwrap().object.take();
	}

	pub async fn store<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		tg::Value::from(self.clone()).store(handle).await?;
		Ok(self.id())
	}

	pub async fn children<H>(&self, handle: &H) -> tg::Result<Vec<tg::Object>>
	where
		H: tg::Handle,
	{
		let object = self.load(handle).await?;
		Ok(object.children())
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.to_data())
	}
}

impl Blob {
	#[must_use]
	pub fn new(children: Vec<tg::blob::Child>) -> Self {
		match children.len() {
			0 => Self::leaf(Bytes::new()),
			1 => children.into_iter().next().unwrap().blob,
			_ => Self::branch(children),
		}
	}

	#[must_use]
	pub fn leaf(bytes: impl Into<Bytes>) -> Self {
		let bytes = bytes.into();
		Self::with_object(Object::Leaf(tg::blob::object::Leaf { bytes }))
	}

	#[must_use]
	pub fn branch(children: Vec<tg::blob::Child>) -> Self {
		Self::with_object(Object::Branch(tg::blob::object::Branch { children }))
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

	pub async fn length<H>(&self, handle: &H) -> tg::Result<u64>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let length = match object.as_ref() {
			Object::Leaf(leaf) => leaf.bytes.len().to_u64().unwrap(),
			Object::Branch(branch) => branch.children.iter().map(|child| child.length).sum(),
		};
		Ok(length)
	}

	pub async fn bytes<H>(&self, handle: &H) -> tg::Result<Vec<u8>>
	where
		H: tg::Handle,
	{
		let mut bytes = Vec::new();
		let reader = self.read(handle, tg::blob::read::Arg::default()).await?;
		pin!(reader)
			.read_to_end(&mut bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read to the end"))?;
		Ok(bytes)
	}

	pub async fn text<H>(&self, handle: &H) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		let bytes = self.bytes(handle).await?;
		let string = String::from_utf8(bytes)
			.map_err(|source| tg::error!(!source, "failed to decode the blob's bytes as UTF-8"))?;
		Ok(string)
	}
}

impl std::fmt::Display for Blob {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.blob(self)?;
		Ok(())
	}
}

impl From<Bytes> for Blob {
	fn from(value: Bytes) -> Self {
		Self::with_object(Object::Leaf(tg::blob::object::Leaf { bytes: value }))
	}
}

impl From<String> for Blob {
	fn from(value: String) -> Self {
		Self::with_object(Object::Leaf(tg::blob::object::Leaf {
			bytes: value.into(),
		}))
	}
}

impl From<&str> for Blob {
	fn from(value: &str) -> Self {
		value.to_owned().into()
	}
}
