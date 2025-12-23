use {
	super::{Data, Id, Object},
	crate::prelude::*,
	std::{collections::BTreeMap, sync::Arc},
};

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(try_from = "Data")]
pub struct Error {
	state: tg::object::State,
	source: Option<Box<Error>>,
}

impl Error {
	#[must_use]
	pub fn with_state(state: tg::object::State) -> Self {
		let source = state.object().and_then(|object| {
			let object = object.try_unwrap_error_ref().ok()?;
			object.source.as_ref().map(|source| match &source.item {
				tg::Either::Left(object) => Box::new(Error::with_object(object.clone())),
				tg::Either::Right(handle) => handle.clone(),
			})
		});
		Self { state, source }
	}

	#[must_use]
	pub fn state(&self) -> &tg::object::State {
		&self.state
	}

	#[must_use]
	pub fn with_id(id: Id) -> Self {
		Self {
			state: tg::object::State::with_id(id),
			source: None,
		}
	}

	#[must_use]
	pub fn with_object(object: impl Into<Arc<Object>>) -> Self {
		let object: Arc<Object> = object.into();
		let source = object.source.as_ref().map(|s| match &s.item {
			tg::Either::Left(object) => Box::new(Error::with_object(object.clone())),
			tg::Either::Right(handle) => handle.clone(),
		});
		Self {
			state: tg::object::State::with_object(object),
			source,
		}
	}

	#[must_use]
	pub fn id(&self) -> Id {
		self.state.id().try_into().unwrap()
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

	pub async fn load_with_arg<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.try_load_with_arg(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load<H>(&self, handle: &H) -> tg::Result<Option<Arc<Object>>>
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
	) -> tg::Result<Option<Arc<Object>>>
	where
		H: tg::Handle,
	{
		let object = self.state.try_load_with_arg(handle, arg).await?;
		let Some(object) = object else {
			return Ok(None);
		};
		let object = object
			.try_unwrap_error()
			.map_err(|_| tg::error!("expected an error object"))?;
		Ok(Some(object))
	}

	pub fn unload(&self) {
		self.state.unload();
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
		self.state.children(handle).await
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.to_data())
	}

	#[must_use]
	pub fn message(&self) -> Option<String> {
		self.state.object().and_then(|object| {
			let object = object.try_unwrap_error_ref().ok()?;
			object.message.clone()
		})
	}

	#[must_use]
	pub fn to_data_or_id(&self) -> tg::Either<tg::error::Data, tg::error::Id> {
		if self.state().stored() {
			tg::Either::Right(self.id())
		} else {
			tg::Either::Left(self.state().object().unwrap().unwrap_error_ref().to_data())
		}
	}
}

impl Default for Error {
	fn default() -> Self {
		Self::with_object(Object::default())
	}
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(object) = self.state.object() {
			let message = object
				.unwrap_error_ref()
				.message
				.as_deref()
				.unwrap_or("an error occurred");
			write!(f, "{message}")?;
		} else {
			write!(f, "{}", self.id())?;
		}
		Ok(())
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		self.source
			.as_ref()
			.map(|source| source.as_ref() as &(dyn std::error::Error + 'static))
	}
}

impl TryFrom<Data> for Error {
	type Error = tg::Error;

	fn try_from(data: Data) -> tg::Result<Self> {
		let object = Object::try_from_data(data)?;
		Ok(Self::with_object(object))
	}
}

impl TryFrom<tg::Either<tg::error::Data, tg::error::Id>> for Error {
	type Error = tg::Error;
	fn try_from(value: tg::Either<tg::error::Data, tg::error::Id>) -> Result<Self, Self::Error> {
		match value {
			tg::Either::Left(data) => data.try_into(),
			tg::Either::Right(id) => Ok(Self::with_id(id)),
		}
	}
}

impl From<Box<dyn std::error::Error + Send + Sync + 'static>> for Error {
	fn from(value: Box<dyn std::error::Error + Send + Sync + 'static>) -> Self {
		match value.downcast::<Error>() {
			Ok(error) => *error,
			Err(error) => {
				let source = error.source().map(|s| {
					let error: Error = s.into();
					let item = error
						.to_data_or_id()
						.map_left(|data| {
							Box::new(tg::error::Object::try_from_data(data).unwrap_or_else(|_| {
								tg::error::Object {
									message: Some("invalid error".to_owned()),
									..Default::default()
								}
							}))
						})
						.map_right(|id| Box::new(tg::Error::with_id(id)));
					tg::Referent::with_item(item)
				});
				Self::with_object(Object {
					code: None,
					message: Some(error.to_string()),
					location: None,
					stack: None,
					source,
					values: BTreeMap::new(),
					diagnostics: None,
				})
			},
		}
	}
}

impl From<&(dyn std::error::Error + 'static)> for Error {
	fn from(value: &(dyn std::error::Error + 'static)) -> Self {
		let source = value.source().map(|s| {
			let error: Error = s.into();
			let item = error
				.to_data_or_id()
				.map_left(|data| {
					Box::new(tg::error::Object::try_from_data(data).unwrap_or_else(|_| {
						tg::error::Object {
							message: Some("invalid error".to_owned()),
							..Default::default()
						}
					}))
				})
				.map_right(|id| Box::new(tg::Error::with_id(id)));
			tg::Referent::with_item(item)
		});
		Self::with_object(Object {
			code: None,
			message: Some(value.to_string()),
			location: None,
			stack: None,
			source,
			values: BTreeMap::new(),
			diagnostics: None,
		})
	}
}
