use {
	super::{Data, Id, Object, Trace},
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
	pub fn try_with_referent<T>(referent: tg::Referent<T>) -> std::result::Result<Self, T::Error>
	where
		T: TryInto<Id>,
	{
		let referent = referent.try_map(TryInto::try_into)?;

		Ok(Self::with_referent(referent))
	}

	#[must_use]
	pub fn with_referent(referent: tg::Referent<Id>) -> Self {
		let error = Self::with_id(referent.node);
		error.state().set_location(referent.options.location);
		error.state().set_tokens(referent.options.tokens);

		error
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
		let source = object.source.as_ref().map(|s| match &s.node {
			tg::Either::Left(object) => Box::new(Error::with_object(object.clone())),
			tg::Either::Right(instance) => instance.clone(),
		});
		Self {
			state: tg::object::State::with_object(object),
			source,
		}
	}

	#[must_use]
	pub fn with_state(state: tg::object::State) -> Self {
		let source = state.object().and_then(|object| {
			let object = object.try_unwrap_error_ref().ok()?;
			object.source.as_ref().map(|source| match &source.node {
				tg::Either::Left(object) => Box::new(Error::with_object(object.clone())),
				tg::Either::Right(instance) => instance.clone(),
			})
		});
		Self { state, source }
	}

	#[must_use]
	pub fn state(&self) -> &tg::object::State {
		&self.state
	}

	#[must_use]
	pub fn id(&self) -> Id {
		self.state.id().try_into().unwrap()
	}

	#[must_use]
	pub fn to_referent(&self) -> tg::Referent<Id> {
		let options = tg::referent::Options {
			location: self.state.location(),
			tokens: self.state.collect_tokens(),
			..tg::referent::Options::default()
		};

		tg::Referent::new(self.id(), options)
	}

	pub async fn object(&self) -> tg::Result<Arc<Object>> {
		let instance = tg::instance()?;
		self.object_with_instance(instance).await
	}

	pub async fn object_with_instance<I>(&self, instance: &I) -> tg::Result<Arc<Object>>
	where
		I: tg::Instance,
	{
		self.load_with_instance(instance).await
	}

	pub async fn load(&self) -> tg::Result<Arc<Object>> {
		let instance = tg::instance()?;
		self.load_with_instance(instance).await
	}

	pub async fn load_with_instance<I>(&self, instance: &I) -> tg::Result<Arc<Object>>
	where
		I: tg::Instance,
	{
		self.try_load_with_instance(instance)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn load_with_arg(&self, arg: tg::object::get::Arg) -> tg::Result<Arc<Object>> {
		let instance = tg::instance()?;
		self.load_with_arg_with_instance(instance, arg).await
	}

	pub async fn load_with_arg_with_instance<I>(
		&self,
		instance: &I,
		arg: tg::object::get::Arg,
	) -> tg::Result<Arc<Object>>
	where
		I: tg::Instance,
	{
		self.try_load_with_arg_with_instance(instance, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load(&self) -> tg::Result<Option<Arc<Object>>> {
		let instance = tg::instance()?;
		self.try_load_with_instance(instance).await
	}

	pub async fn try_load_with_instance<I>(&self, instance: &I) -> tg::Result<Option<Arc<Object>>>
	where
		I: tg::Instance,
	{
		self.try_load_with_arg_with_instance(instance, tg::object::get::Arg::default())
			.await
	}

	pub async fn try_load_with_arg(
		&self,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<Arc<Object>>> {
		let instance = tg::instance()?;
		self.try_load_with_arg_with_instance(instance, arg).await
	}

	pub async fn try_load_with_arg_with_instance<I>(
		&self,
		instance: &I,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<Arc<Object>>>
	where
		I: tg::Instance,
	{
		let object = self
			.state
			.try_load_with_arg_with_instance(instance, arg)
			.await?;
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

	pub async fn store(&self) -> tg::Result<Id> {
		let instance = tg::instance()?;
		self.store_with_instance(instance).await
	}

	pub async fn store_with_instance<I>(&self, instance: &I) -> tg::Result<Id>
	where
		I: tg::Instance,
	{
		tg::Value::from(self.clone())
			.store_with_instance(instance)
			.await?;
		Ok(self.id())
	}

	pub async fn children(&self) -> tg::Result<Vec<tg::Object>> {
		let instance = tg::instance()?;
		self.children_with_instance(instance).await
	}

	pub async fn children_with_instance<I>(&self, instance: &I) -> tg::Result<Vec<tg::Object>>
	where
		I: tg::Instance,
	{
		self.state.children_with_instance(instance).await
	}

	pub async fn data(&self) -> tg::Result<Data> {
		let instance = tg::instance()?;
		self.data_with_instance(instance).await
	}

	pub async fn data_with_instance<I>(&self, instance: &I) -> tg::Result<Data>
	where
		I: tg::Instance,
	{
		Ok(self.object_with_instance(instance).await?.to_data())
	}

	/// Get the kind without loading the error or its sources.
	#[must_use]
	pub fn kind(&self) -> Option<tg::error::Kind> {
		self.state.object().and_then(|object| {
			let object = object.try_unwrap_error_ref().ok()?;
			object.kind()
		})
	}

	#[must_use]
	pub fn code(&self) -> Option<tg::error::Code> {
		self.state.object().and_then(|object| {
			let object = object.try_unwrap_error_ref().ok()?;
			object.code
		})
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

	#[must_use]
	pub fn trace(&self) -> Trace<'_> {
		super::trace::Trace(self)
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
					let options = error.to_referent().options;
					let node = error
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
					tg::Referent::new(node, options)
				});
				let mut object = Object {
					code: None,
					kind: None,
					message: Some(error.to_string()),
					location: None,
					stack: None,
					source,
					values: BTreeMap::new(),
					diagnostics: None,
				};
				object.kind = object.kind();
				Self::with_object(object)
			},
		}
	}
}

impl From<&(dyn std::error::Error + 'static)> for Error {
	fn from(value: &(dyn std::error::Error + 'static)) -> Self {
		let source = value.source().map(|s| {
			let error: Error = s.into();
			let options = error.to_referent().options;
			let node = error
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
			tg::Referent::new(node, options)
		});
		let mut object = Object {
			code: None,
			kind: value.downcast_ref::<Self>().and_then(Self::kind),
			message: Some(value.to_string()),
			location: None,
			stack: None,
			source,
			values: BTreeMap::new(),
			diagnostics: None,
		};
		object.kind = object.kind();
		Self::with_object(object)
	}
}
