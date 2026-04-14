use {
	super::{Data, Id, Object},
	crate::prelude::*,
	bytes::Bytes,
	futures::FutureExt as _,
	num::ToPrimitive as _,
	std::{pin::pin, sync::Arc},
	tokio::io::{AsyncRead, AsyncReadExt as _},
};

#[derive(Clone, Debug)]
pub struct Blob {
	state: tg::object::State,
}

impl Blob {
	#[must_use]
	pub fn with_state(state: tg::object::State) -> Self {
		Self { state }
	}

	#[must_use]
	pub fn state(&self) -> &tg::object::State {
		&self.state
	}

	#[must_use]
	pub fn with_id(id: Id) -> Self {
		Self::with_state(tg::object::State::with_id(id))
	}

	#[must_use]
	pub fn with_object(object: impl Into<Arc<Object>>) -> Self {
		Self::with_state(tg::object::State::with_object(object.into()))
	}

	#[must_use]
	pub fn id(&self) -> Id {
		self.state.id().try_into().unwrap()
	}

	pub async fn object(&self) -> tg::Result<Arc<Object>> {
		let handle = tg::handle()?;
		self.object_with_handle(handle).await
	}

	pub async fn object_with_handle<H>(&self, handle: &H) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.load_with_handle(handle).await
	}

	pub async fn load(&self) -> tg::Result<Arc<Object>> {
		let handle = tg::handle()?;
		self.load_with_handle(handle).await
	}

	pub async fn load_with_handle<H>(&self, handle: &H) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.try_load_with_handle(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load(&self) -> tg::Result<Option<Arc<Object>>> {
		let handle = tg::handle()?;
		self.try_load_with_handle(handle).await
	}

	pub async fn try_load_with_handle<H>(&self, handle: &H) -> tg::Result<Option<Arc<Object>>>
	where
		H: tg::Handle,
	{
		self.try_load_with_arg_with_handle(handle, tg::object::get::Arg::default())
			.await
	}

	pub async fn load_with_arg(&self, arg: tg::object::get::Arg) -> tg::Result<Arc<Object>> {
		let handle = tg::handle()?;
		self.load_with_arg_with_handle(handle, arg).await
	}

	pub async fn load_with_arg_with_handle<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.try_load_with_arg_with_handle(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load_with_arg(
		&self,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<Arc<Object>>> {
		let handle = tg::handle()?;
		self.try_load_with_arg_with_handle(handle, arg).await
	}

	pub async fn try_load_with_arg_with_handle<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<Arc<Object>>>
	where
		H: tg::Handle,
	{
		let object = self
			.state
			.try_load_with_arg_with_handle(handle, arg)
			.await?;
		let Some(object) = object else {
			return Ok(None);
		};
		let object = object.unwrap_blob_ref().clone();
		Ok(Some(object))
	}

	pub fn unload(&self) {
		self.state.unload();
	}

	pub async fn store(&self) -> tg::Result<Id> {
		let handle = tg::handle()?;
		self.store_with_handle(handle).await
	}

	pub async fn store_with_handle<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		tg::Value::from(self.clone())
			.store_with_handle(handle)
			.await?;
		Ok(self.id())
	}

	pub async fn children(&self) -> tg::Result<Vec<tg::Object>> {
		let handle = tg::handle()?;
		self.children_with_handle(handle).await
	}

	pub async fn children_with_handle<H>(&self, handle: &H) -> tg::Result<Vec<tg::Object>>
	where
		H: tg::Handle,
	{
		self.state.children_with_handle(handle).await
	}

	pub async fn data(&self) -> tg::Result<Data> {
		let handle = tg::handle()?;
		self.data_with_handle(handle).await
	}

	pub async fn data_with_handle<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		Ok(self.object_with_handle(handle).await?.to_data())
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

	pub async fn with_reader(reader: impl AsyncRead + Send + 'static) -> tg::Result<Self> {
		let handle = tg::handle()?;
		Self::with_reader_with_handle(handle, reader).await
	}

	pub async fn with_reader_with_handle<H>(
		handle: &H,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let arg = tg::write::Arg::default();
		let output = handle.write(arg, reader).boxed().await?;
		let blob = Self::with_id(output.blob);
		Ok(blob)
	}

	pub async fn length(&self) -> tg::Result<u64> {
		let handle = tg::handle()?;
		self.length_with_handle(handle).await
	}

	pub async fn length_with_handle<H>(&self, handle: &H) -> tg::Result<u64>
	where
		H: tg::Handle,
	{
		let object = self.object_with_handle(handle).await?;
		let length = match object.as_ref() {
			Object::Leaf(leaf) => leaf.bytes.len().to_u64().unwrap(),
			Object::Branch(branch) => branch.children.iter().map(|child| child.length).sum(),
		};
		Ok(length)
	}

	pub async fn bytes(&self) -> tg::Result<Vec<u8>> {
		let handle = tg::handle()?;
		self.bytes_with_handle(handle).await
	}

	pub async fn bytes_with_handle<H>(&self, handle: &H) -> tg::Result<Vec<u8>>
	where
		H: tg::Handle,
	{
		let mut bytes = Vec::new();
		let reader = self
			.read_with_handle(handle, tg::read::Options::default())
			.await?;
		pin!(reader)
			.read_to_end(&mut bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read to the end"))?;
		Ok(bytes)
	}

	pub async fn text(&self) -> tg::Result<String> {
		let handle = tg::handle()?;
		self.text_with_handle(handle).await
	}

	pub async fn text_with_handle<H>(&self, handle: &H) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		let bytes = self.bytes_with_handle(handle).await?;
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
