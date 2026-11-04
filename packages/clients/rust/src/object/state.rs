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

	pub(crate) fn finish_store(&self, mut object: tg::Referent<tg::object::Id>) -> tg::Result<()> {
		let mut inner = self.0.write().unwrap();
		if inner.id.as_ref() != Some(&object.node) {
			return Err(tg::error!("invalid object batch output"));
		}
		inner.location = object.options.location;
		inner.stored = true;
		object
			.options
			.tokens
			.inherit_with_resource(&inner.tokens, Some(&object.node.into()));
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

	pub fn set_tokens(&self, mut tokens: tg::authorization::Tokens) {
		let id = self.id().into();
		tokens.normalize(Some(&id));
		self.0.write().unwrap().tokens = tokens;
	}

	pub fn inherit_tokens(&self, tokens: &tg::authorization::Tokens) {
		// Inherit the tokens, and get the loaded object if they changed.
		let id = self.id().into();
		let (tokens, object) = {
			let mut inner = self.0.write().unwrap();
			let previous = inner.tokens.clone();
			inner.tokens.inherit_with_resource(tokens, Some(&id));
			if inner.tokens == previous {
				return;
			}
			(inner.tokens.clone(), inner.object.clone())
		};

		// Pass the tokens to the loaded object's children, so that the children of an object that gains tokens after it is loaded inherit them.
		if let Some(object) = object {
			for child in object.children() {
				child.inherit_tokens(&tokens);
			}
		}
	}

	pub fn set_object(&self, object: impl Into<tg::object::Object>) {
		self.0.write().unwrap().object.replace(object.into());
	}

	#[must_use]
	pub fn tokens(&self) -> tg::authorization::Tokens {
		self.0.read().unwrap().tokens.clone()
	}

	#[must_use]
	pub fn collect_tokens(&self) -> tg::authorization::Tokens {
		// Discover the token locations on all loaded handles, deriving the children of each loaded handle once.
		let mut locations = BTreeSet::new();
		let mut children = BTreeMap::new();
		let mut visited = BTreeSet::new();
		let mut stack = vec![self.clone()];
		while let Some(state) = stack.pop() {
			if !visited.insert(state.identity()) {
				continue;
			}
			let inner = state.0.read().unwrap();
			locations.extend(inner.tokens.iter().map(|(location, _)| location.clone()));
			let object = inner.object.clone();
			drop(inner);

			// Derive the children without holding the lock, because deriving them can lock the children.
			if let Some(object) = object {
				let states = object
					.children()
					.into_iter()
					.map(|child| child.state())
					.collect::<Vec<_>>();
				stack.extend(states.iter().cloned());
				children.insert(state.identity(), states);
			}
		}

		// Collect uncovered authorization proofs and authorization tokens for pending syncs.
		let mut tokens = tg::authorization::Tokens::default();
		for location in locations {
			let mut entry = tg::authorization::tokens::Entry::default();
			let mut visited = BTreeSet::new();
			let mut stack = vec![(self.clone(), false)];
			while let Some((state, covered)) = stack.pop() {
				if !visited.insert((state.identity(), covered)) {
					continue;
				}

				// Get the ID of a handle with tokens at the location before reading its tokens, because computing the ID locks the handle.
				let has_entry = state.0.read().unwrap().tokens.get(&location).is_some();
				let id = has_entry.then(|| tg::Id::from(state.id()));

				// Collect the handle's tokens without copying them.
				let inner = state.0.read().unwrap();
				let mut covered = covered;
				if let (Some(id), Some(state_entry)) = (&id, inner.tokens.get(&location)) {
					entry.authorization.extend(
						state_entry
							.authorization
							.iter()
							.filter(|token| {
								!covered || token.body.resource.kind() == tg::id::Kind::Sync
							})
							.cloned(),
					);
					covered |= state_entry
						.authorization
						.iter()
						.any(|token| token.authorizes_object_subtree(id));
				}
				drop(inner);
				if let Some(states) = children.get(&state.identity()) {
					stack.extend(states.iter().map(|child| (child.clone(), covered)));
				}
			}
			if !entry.is_empty() {
				tokens.set(location, entry);
			}
		}

		tokens
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
		let instance = tg::instance()?;
		self.load_with_instance(instance).await
	}

	pub async fn load_with_instance<I>(&self, instance: &I) -> tg::Result<tg::object::Object>
	where
		I: tg::Instance,
	{
		self.load_with_arg_with_instance(instance, tg::object::get::Arg::default())
			.await
	}

	pub async fn load_with_arg(&self, arg: tg::object::get::Arg) -> tg::Result<tg::object::Object> {
		let instance = tg::instance()?;
		self.load_with_arg_with_instance(instance, arg).await
	}

	pub async fn load_with_arg_with_instance<I>(
		&self,
		instance: &I,
		arg: tg::object::get::Arg,
	) -> tg::Result<tg::object::Object>
	where
		I: tg::Instance,
	{
		self.try_load_with_arg_with_instance(instance, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load(&self) -> tg::Result<Option<tg::object::Object>> {
		let instance = tg::instance()?;
		self.try_load_with_instance(instance).await
	}

	pub async fn try_load_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<Option<tg::object::Object>>
	where
		I: tg::Instance,
	{
		self.try_load_with_arg_with_instance(instance, tg::object::get::Arg::default())
			.await
	}

	pub async fn try_load_with_arg(
		&self,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::Object>> {
		let instance = tg::instance()?;
		self.try_load_with_arg_with_instance(instance, arg).await
	}

	pub async fn try_load_with_arg_with_instance<I>(
		&self,
		instance: &I,
		mut arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::Object>>
	where
		I: tg::Instance,
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
				let instance = instance.clone();
				let state = self.clone();
				let task =
					Shared::spawn(move |_| async move { state.load_task(instance, id, arg).await });
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

	async fn load_task<I>(
		&self,
		instance: I,
		id: tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::Object>>
	where
		I: tg::Instance,
	{
		// Load the object.
		let Some(mut output) = instance.try_get_object(&id, arg).await? else {
			return Ok(None);
		};

		// Deserialize the object.
		let data = tg::object::Data::deserialize(id.kind(), output.bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the data"))?;
		let object = tg::object::Object::try_from_data(data)?;

		// Update the tokens.
		let tokens = {
			let mut inner = self.0.write().unwrap();
			if !output.tokens.is_empty() {
				output
					.tokens
					.inherit_with_resource(&inner.tokens, Some(&id.into()));
				inner.tokens = output.tokens;
			}
			inner.tokens.clone()
		};

		// Inherit the tokens on the children.
		for child in object.children() {
			if let Some(child_output) = output.children.get(&child.id()) {
				child.inherit_tokens(&child_output.tokens);
			}
			child.inherit_tokens(&tokens);
		}

		// Store the object, and pass any tokens inherited while the children were updated to the children.
		let current = {
			let mut inner = self.0.write().unwrap();
			inner.object.replace(object.clone());
			inner.tokens.clone()
		};
		if current != tokens {
			for child in object.children() {
				child.inherit_tokens(&current);
			}
		}

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
		let instance = tg::instance()?;
		self.children_with_instance(instance).await
	}

	pub async fn children_with_instance<I>(&self, instance: &I) -> tg::Result<Vec<tg::Object>>
	where
		I: tg::Instance,
	{
		let object = self.load_with_instance(instance).await?;
		let children = object.children();
		let location = self.location();
		for child in &children {
			child.inherit_location(location.as_ref());
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

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn accessing_a_loaded_object_does_not_copy_tokens_onto_children() {
		// Create a loaded parent with a child and give the parent a token.
		let key = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
		let child = tg::Directory::with_id(tg::directory::Id::new(b"child"));
		let parent =
			tg::Directory::with_entries(BTreeMap::from([("child".into(), child.clone().into())]));
		let body = tg::authorization::Body {
			expires_at: i64::MAX,
			permissions: vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)],
			resource: parent.id().into(),
		};
		let token = tg::authorization::Token::sign(body, &key).unwrap();
		parent
			.state()
			.set_tokens(tg::authorization::Tokens::with_authorization([token]));

		// Access the loaded parent.
		let arg = tg::Arg {
			url: Some("http://localhost:1".parse().unwrap()),
			..tg::Arg::default()
		};
		let client = tg::Client::new(arg).unwrap();
		parent.state().load_with_instance(&client).await.unwrap();

		assert!(child.state().tokens().is_empty());
	}

	#[test]
	fn collecting_tokens_does_not_lock_a_handle_while_deriving_its_children() {
		// Create a loaded error whose two children inherit a token when they are derived.
		let key = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
		let first = tg::Directory::with_id(tg::directory::Id::new(b"first"));
		let second = tg::Directory::with_id(tg::directory::Id::new(b"second"));
		let body = tg::authorization::Body {
			expires_at: i64::MAX,
			permissions: vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)],
			resource: first.id().into(),
		};
		let token = tg::authorization::Token::sign(body, &key).unwrap();
		let tokens = tg::authorization::Tokens::with_authorization([token]);
		let location = |child: &tg::Directory| {
			let node = tg::module::Source::Edge(tg::graph::Edge::Object(child.clone().into()));
			let module = tg::Module {
				kind: tg::module::Kind::Artifact,
				referent: tg::Referent::with_node_and_tokens(node, tokens.clone()),
			};
			let position = tg::Position {
				line: 0,
				character: 0,
			};
			let range = tg::Range {
				start: position,
				end: position,
			};
			tg::error::Location {
				symbol: None,
				file: tg::error::File::Module(module),
				range,
			}
		};
		let object = tg::error::Object {
			stack: Some(vec![location(&first), location(&second)]),
			..Default::default()
		};
		let parent = tg::Error::with_object(object);

		// Hold a lock on the second child, as a store that locks a child before its parent does, and collect the tokens.
		let guard = second.state().0.write().unwrap();
		let state = parent.state().clone();
		let thread = std::thread::spawn(move || state.collect_tokens());

		// Wait until the first child has inherited the token, so that deriving the children has started and cannot finish.
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while first.state().tokens().is_empty() && std::time::Instant::now() < deadline {
			std::thread::yield_now();
		}
		let derivation_started = !first.state().tokens().is_empty();

		// Try to lock the parent, as the store would next.
		let parent_is_available = parent.state().0.try_write().is_ok();

		// Release the second child before asserting, so that the thread always finishes.
		drop(guard);
		thread.join().unwrap();
		assert!(derivation_started);
		assert!(parent_is_available);
	}
}
