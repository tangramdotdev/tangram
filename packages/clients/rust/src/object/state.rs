use {
	crate::prelude::*,
	std::{
		collections::{BTreeMap, BTreeSet},
		future::Future,
		sync::{Arc, RwLock},
	},
	tangram_futures::task::Shared,
};

#[derive(Clone, Debug)]
pub struct State(Arc<RwLock<Inner>>);

#[derive(derive_more::Debug)]
struct Inner {
	id: Option<tg::object::Id>,
	#[debug(ignore)]
	load: Option<Shared<tg::Result<Option<tg::object::Object>>>>,
	location: Option<tg::Location>,
	object: Option<tg::object::Object>,
	#[debug(ignore)]
	store: Option<StoreTask>,
	stored: bool,
	tokens: tg::authorization::Tokens,
}

#[derive(Clone)]
pub(crate) struct StoreTask {
	states: Arc<Vec<State>>,
	task: Shared<tg::Result<()>>,
}

pub(crate) enum StoreTaskStatus {
	Complete,
	Pending(Vec<StoreTask>),
	Started(StoreTask),
}

impl State {
	#[must_use]
	pub fn new(id: Option<tg::object::Id>, object: Option<impl Into<tg::object::Object>>) -> Self {
		assert!(id.is_some() || object.is_some());
		let object = object.map(Into::into);
		let stored = id.is_some();
		Self(Arc::new(RwLock::new(Inner {
			id,
			load: None,
			location: None,
			object,
			store: None,
			stored,
			tokens: tg::authorization::Tokens::default(),
		})))
	}

	#[must_use]
	pub fn with_id(id: impl Into<tg::object::Id>) -> Self {
		Self(Arc::new(RwLock::new(Inner {
			id: Some(id.into()),
			load: None,
			location: None,
			object: None,
			store: None,
			stored: true,
			tokens: tg::authorization::Tokens::default(),
		})))
	}

	#[must_use]
	pub fn with_object(object: impl Into<tg::object::Object>) -> Self {
		Self(Arc::new(RwLock::new(Inner {
			id: None,
			load: None,
			location: None,
			object: Some(object.into()),
			store: None,
			stored: false,
			tokens: tg::authorization::Tokens::default(),
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
		let inner = self.0.read().unwrap();
		if let Some(id) = inner.id.clone() {
			return id;
		}
		let object = inner.object.as_ref().unwrap();
		let data = object.to_data().without_location_and_tokens();
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

	#[must_use]
	pub(crate) fn identity(&self) -> usize {
		Arc::as_ptr(&self.0).addr()
	}

	#[must_use]
	pub(crate) fn store_task(&self) -> Option<StoreTask> {
		self.0.read().unwrap().store.clone()
	}

	pub(crate) fn start_store_task<F>(states: Vec<Self>, spawn: F) -> StoreTaskStatus
	where
		F: FnOnce(Vec<Self>) -> StoreTask,
	{
		// Deduplicate the states while preserving their topological order.
		let mut identities = BTreeSet::new();
		let states = states
			.into_iter()
			.filter(|state| identities.insert(state.identity()))
			.collect::<Vec<_>>();

		// Lock the states in a deterministic order.
		let states_by_identity = states
			.iter()
			.cloned()
			.map(|state| (state.identity(), state))
			.collect::<BTreeMap<_, _>>();
		let mut inners = states_by_identity
			.values()
			.map(|state| state.0.write().unwrap())
			.collect::<Vec<_>>();

		// Wait for any overlapping store tasks before planning another batch.
		let mut pending = Vec::new();
		for inner in &inners {
			let Some(task) = &inner.store else {
				continue;
			};
			if !pending
				.iter()
				.any(|pending: &StoreTask| pending.id() == task.id())
			{
				pending.push(task.clone());
			}
		}
		if !pending.is_empty() {
			return StoreTaskStatus::Pending(pending);
		}

		// Get the states that still need to be stored.
		let identities = states_by_identity
			.values()
			.zip(&inners)
			.filter(|(_, inner)| !inner.stored)
			.map(|(state, _)| state.identity())
			.collect::<BTreeSet<_>>();
		if identities.is_empty() {
			return StoreTaskStatus::Complete;
		}
		let states = states
			.into_iter()
			.filter(|state| identities.contains(&state.identity()))
			.collect::<Vec<_>>();

		// Start the task and claim the states.
		let task = spawn(states.clone());
		for (state, inner) in states_by_identity.values().zip(&mut inners) {
			if identities.contains(&state.identity()) {
				inner.store.replace(task.clone());
			}
		}
		drop(inners);
		Self::spawn_store_task_cleanup(task.clone());

		StoreTaskStatus::Started(task)
	}

	fn spawn_store_task_cleanup(mut task: StoreTask) {
		task.detach();
		tokio::spawn(async move {
			task.task.wait().await.ok();
			task.clear();
		});
	}

	pub(crate) fn finish_store(&self, object: tg::Referent<tg::object::Id>) -> tg::Result<()> {
		let mut inner = self.0.write().unwrap();
		if inner.id.as_ref() != Some(&object.node) {
			return Err(tg::error!("invalid object batch output"));
		}
		inner.location = object.options.location;
		inner.stored = true;
		inner.tokens = object.options.tokens;

		Ok(())
	}

	pub fn set_id(&self, id: tg::object::Id) {
		self.0.write().unwrap().id.replace(id);
	}

	pub fn set_stored(&self, stored: bool) {
		self.0.write().unwrap().stored = stored;
	}

	pub fn set_location(&self, location: Option<tg::Location>) {
		self.0.write().unwrap().location = location;
	}

	pub fn inherit_location(&self, location: Option<&tg::Location>) {
		let mut inner = self.0.write().unwrap();
		if inner.location.is_none() {
			inner.location = location.cloned();
		}
	}

	pub fn set_tokens(&self, tokens: tg::authorization::Tokens) {
		self.0.write().unwrap().tokens = tokens;
	}

	pub fn inherit_tokens(&self, tokens: &tg::authorization::Tokens) {
		self.0.write().unwrap().tokens.inherit(tokens);
	}

	pub fn set_object(&self, object: impl Into<tg::object::Object>) {
		self.0.write().unwrap().object.replace(object.into());
	}

	#[must_use]
	pub fn tokens(&self) -> tg::authorization::Tokens {
		self.0.read().unwrap().tokens.clone()
	}

	#[must_use]
	pub fn location(&self) -> Option<tg::Location> {
		self.0.read().unwrap().location.clone()
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

	pub async fn load(&self) -> tg::Result<tg::object::Object> {
		let handle = tg::handle()?;
		self.load_with_handle(handle).await
	}

	pub async fn load_with_handle<H>(&self, handle: &H) -> tg::Result<tg::object::Object>
	where
		H: tg::Handle,
	{
		self.load_with_arg_with_handle(handle, tg::object::get::Arg::default())
			.await
	}

	pub async fn load_with_arg(&self, arg: tg::object::get::Arg) -> tg::Result<tg::object::Object> {
		let handle = tg::handle()?;
		self.load_with_arg_with_handle(handle, arg).await
	}

	pub async fn load_with_arg_with_handle<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<tg::object::Object>
	where
		H: tg::Handle,
	{
		self.try_load_with_arg_with_handle(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load(&self) -> tg::Result<Option<tg::object::Object>> {
		let handle = tg::handle()?;
		self.try_load_with_handle(handle).await
	}

	pub async fn try_load_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<Option<tg::object::Object>>
	where
		H: tg::Handle,
	{
		self.try_load_with_arg_with_handle(handle, tg::object::get::Arg::default())
			.await
	}

	pub async fn try_load_with_arg(
		&self,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::Object>> {
		let handle = tg::handle()?;
		self.try_load_with_arg_with_handle(handle, arg).await
	}

	pub async fn try_load_with_arg_with_handle<H>(
		&self,
		handle: &H,
		mut arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::Object>>
	where
		H: tg::Handle,
	{
		// Get or start the load task.
		let (spawned, task) = {
			let mut inner = self.0.write().unwrap();
			if let Some(object) = inner.object.clone() {
				return Ok(Some(object));
			}
			if let Some(task) = &inner.load {
				(false, task.clone())
			} else {
				let id = inner.id.clone().unwrap();
				if arg.location.is_none() {
					arg.location = inner.location.clone().map(Into::into);
				}
				if arg.tokens.is_empty() {
					arg.tokens = inner.tokens.clone();
				}
				let handle = handle.clone();
				let state = self.clone();
				let task =
					Shared::spawn(move |_| async move { state.load_task(handle, id, arg).await });
				inner.load.replace(task.clone());
				(true, task)
			}
		};
		if spawned {
			self.spawn_load_task_cleanup(task.clone());
		}

		// Wait for the task.
		let task_id = task.id();
		let result = task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the load task panicked"))
			.and_then(|result| result);
		self.clear_load_task(task_id);

		result
	}

	async fn load_task<H>(
		&self,
		handle: H,
		id: tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::Object>>
	where
		H: tg::Handle,
	{
		// Load the object.
		let Some(output) = handle.try_get_object(&id, arg).await? else {
			return Ok(None);
		};

		// Deserialize the object.
		let data = tg::object::Data::deserialize(id.kind(), output.bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the data"))?;
		let object = tg::object::Object::try_from_data(data)?;

		// Update the state.
		let mut inner = self.0.write().unwrap();
		if !output.tokens.is_empty() {
			inner.tokens = output.tokens;
		}
		inner.object.replace(object.clone());

		Ok(Some(object))
	}

	fn spawn_load_task_cleanup(&self, mut task: Shared<tg::Result<Option<tg::object::Object>>>) {
		let state = self.clone();
		let task_id = task.id();
		task.detach();
		tokio::spawn(async move {
			task.wait().await.ok();
			state.clear_load_task(task_id);
		});
	}

	fn clear_load_task(&self, task_id: tokio::task::Id) {
		let mut inner = self.0.write().unwrap();
		if inner.load.as_ref().is_some_and(|load| load.id() == task_id) {
			inner.load.take();
		}
	}

	pub async fn children(&self) -> tg::Result<Vec<tg::Object>> {
		let handle = tg::handle()?;
		self.children_with_handle(handle).await
	}

	pub async fn children_with_handle<H>(&self, handle: &H) -> tg::Result<Vec<tg::Object>>
	where
		H: tg::Handle,
	{
		let object = self.load_with_handle(handle).await?;
		let children = object.children();
		let tokens = self.tokens();
		let location = self.location();

		for child in &children {
			child.inherit_location(location.as_ref());
			child.inherit_tokens(&tokens);
		}

		Ok(children)
	}
}

impl StoreTask {
	pub(crate) fn spawn<F, Fut>(states: Vec<State>, f: F) -> Self
	where
		F: FnOnce(Vec<State>) -> Fut,
		Fut: Future<Output = tg::Result<()>> + Send + 'static,
	{
		let task_states = states.clone();
		let task = Shared::spawn(move |_| f(task_states));
		let states = Arc::new(states);

		Self { states, task }
	}

	pub(crate) async fn wait(&self) -> tg::Result<()> {
		let result = self
			.task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the store task panicked"))
			.and_then(|result| result);
		self.clear();

		result
	}

	fn detach(&mut self) {
		self.task.detach();
	}

	pub(crate) fn id(&self) -> tokio::task::Id {
		self.task.id()
	}

	fn clear(&self) {
		let task_id = self.id();
		for state in self.states.iter() {
			let mut inner = state.0.write().unwrap();
			if inner
				.store
				.as_ref()
				.is_some_and(|store| store.id() == task_id)
			{
				inner.store.take();
			}
		}
	}
}
