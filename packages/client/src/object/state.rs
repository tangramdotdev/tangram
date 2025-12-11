use {
	crate as tg,
	std::sync::{Arc, RwLock},
};

#[derive(Clone, Debug)]
pub struct State(Arc<RwLock<Inner>>);

#[derive(Debug)]
struct Inner {
	id: Option<tg::object::Id>,
	object: Option<tg::object::Object>,
	stored: bool,
}

impl State {
	#[must_use]
	pub fn new(id: Option<tg::object::Id>, object: Option<impl Into<tg::object::Object>>) -> Self {
		assert!(id.is_some() || object.is_some());
		let object = object.map(Into::into);
		let stored = id.is_some();
		Self(Arc::new(RwLock::new(Inner { id, object, stored })))
	}

	#[must_use]
	pub fn with_id(id: impl Into<tg::object::Id>) -> Self {
		Self(Arc::new(RwLock::new(Inner {
			id: Some(id.into()),
			object: None,
			stored: true,
		})))
	}

	#[must_use]
	pub fn with_object(object: impl Into<tg::object::Object>) -> Self {
		Self(Arc::new(RwLock::new(Inner {
			id: None,
			object: Some(object.into()),
			stored: false,
		})))
	}

	#[must_use]
	pub fn kind(&self) -> tg::object::Kind {
		let inner = self.0.read().unwrap();
		if let Some(id) = &inner.id {
			id.kind()
		} else {
			inner.object.as_ref().unwrap().kind()
		}
	}

	#[must_use]
	pub fn id(&self) -> tg::object::Id {
		{
			let inner = self.0.read().unwrap();
			if let Some(id) = inner.id.clone() {
				return id;
			}
		}
		let inner = self.0.read().unwrap();
		let object = inner.object.as_ref().unwrap();
		let data = object.to_data();
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(data.kind(), &bytes);
		drop(inner);
		self.0.write().unwrap().id.replace(id.clone());
		id
	}

	#[must_use]
	pub fn object(&self) -> Option<tg::object::Object> {
		self.0.read().unwrap().object.clone()
	}

	#[must_use]
	pub fn stored(&self) -> bool {
		self.0.read().unwrap().stored
	}

	pub fn set_id(&self, id: tg::object::Id) {
		self.0.write().unwrap().id.replace(id);
	}

	pub fn set_stored(&self, stored: bool) {
		self.0.write().unwrap().stored = stored;
	}

	pub fn set_object(&self, object: impl Into<tg::object::Object>) {
		self.0.write().unwrap().object.replace(object.into());
	}

	#[must_use]
	pub fn try_get_id(&self) -> Option<tg::object::Id> {
		self.0.read().unwrap().id.clone()
	}

	pub fn unload(&self) {
		let mut inner = self.0.write().unwrap();
		if inner.stored {
			inner.object.take();
		}
	}

	pub async fn load<H>(&self, handle: &H) -> tg::Result<tg::object::Object>
	where
		H: tg::Handle,
	{
		self.load_with_arg(handle, tg::object::get::Arg::default())
			.await
	}

	pub async fn load_with_arg<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<tg::object::Object>
	where
		H: tg::Handle,
	{
		self.try_load_with_arg(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load<H>(&self, handle: &H) -> tg::Result<Option<tg::object::Object>>
	where
		H: tg::Handle,
	{
		self.try_load_with_arg(handle, tg::object::get::Arg::default())
			.await
	}

	pub async fn try_load_with_arg<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::Object>>
	where
		H: tg::Handle,
	{
		// Check if already loaded.
		if let Some(object) = self.0.read().unwrap().object.clone() {
			return Ok(Some(object));
		}

		// Get the id.
		let id = self.0.read().unwrap().id.clone().unwrap();

		// Load the object.
		let Some(output) = handle.try_get_object(&id, arg).await? else {
			return Ok(None);
		};

		// Deserialize.
		let data = tg::object::Data::deserialize(id.kind(), output.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
		let object = tg::object::Object::try_from_data(data)?;

		// Store and return.
		self.0.write().unwrap().object.replace(object.clone());
		Ok(Some(object))
	}

	pub async fn children<H>(&self, handle: &H) -> tg::Result<Vec<tg::Object>>
	where
		H: tg::Handle,
	{
		let object = self.load(handle).await?;
		Ok(object.children())
	}
}
