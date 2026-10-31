use {
	super::process::ConnectedEvent,
	dashmap::DashMap,
	indexmap::IndexMap,
	std::{collections::BTreeMap, sync::Arc},
	tangram_client::prelude::*,
};

pub struct SpawnSandboxTaskArg {
	pub allocation: crate::capacity::Allocation,
	pub arg: tg::sandbox::create::Arg,
	pub boot: u64,
	pub creator: tg::Principal,
	pub id: Option<tg::sandbox::Id>,
	pub location: tg::Location,
	pub owner: tg::Principal,
	pub process: Option<tg::runner::control::Process>,
	pub token: Option<String>,
}

#[must_use]
pub struct SpawnSandboxTaskOutput {
	pub events: tokio::sync::mpsc::UnboundedReceiver<tg::Result<Event>>,
}

pub enum Event {
	Destroyed,
	Ready(ReadyEvent),
}

#[derive(Clone, Debug)]
pub struct ReadyEvent {
	pub connected_event: Option<ConnectedEvent>,
	pub sandbox: tg::sandbox::Id,
}

#[derive(Default)]
pub struct Sandboxes {
	indexes: DashMap<tg::sandbox::Id, u64, tg::id::BuildHasher>,
	updated: tokio::sync::Notify,
	states: DashMap<u64, State, fnv::FnvBuildHasher>,
}

pub struct State {
	pub allocation: Option<Arc<tokio::sync::Mutex<Option<crate::capacity::Allocation>>>>,
	pub authorization_tokens: tg::authorization::Tokens,
	pub boot: u64,
	pub capacity: tg::runner::Capacity,
	pub changed: tokio::sync::watch::Sender<()>,
	pub control_sender: tangram_server_sandbox_core::control::local::Local,
	pub create: tg::sandbox::create::Arg,
	pub creator: tg::Principal,
	pub error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	pub id: tg::sandbox::Id,
	pub location: tg::Location,
	pub owner: tg::Principal,
	pub process_ids: IndexMap<tg::process::Id, (), tg::id::BuildHasher>,
	pub processes: Arc<tangram_server_process_core::Processes>,
	pub sandbox: Option<tangram_sandbox::Sandbox>,
	pub status: tg::sandbox::Status,
	pub token: tokio::sync::watch::Receiver<Option<String>>,
	pub tokens: BTreeMap<tg::artifact::Id, tg::authorization::Token>,
	pub usage: Option<tg::sandbox::Usage>,
	pub vfs: Option<std::sync::Arc<dyn tangram_server_vfs_core::Provider>>,
}

impl Sandboxes {
	#[must_use]
	pub fn get(&self, index: u64) -> Option<dashmap::mapref::one::Ref<'_, u64, State>> {
		self.states.get(&index)
	}

	#[must_use]
	pub fn get_by_id(
		&self,
		id: &tg::sandbox::Id,
	) -> Option<dashmap::mapref::one::Ref<'_, u64, State>> {
		let index = *self.indexes.get(id)?;

		self.states.get(&index)
	}

	#[must_use]
	pub fn get_mut_by_id(
		&self,
		id: &tg::sandbox::Id,
	) -> Option<dashmap::mapref::one::RefMut<'_, u64, State>> {
		let index = *self.indexes.get(id)?;

		self.states.get_mut(&index)
	}

	pub fn insert(&self, index: u64, state: State) {
		let id = state.id.clone();
		match self.states.entry(index) {
			dashmap::Entry::Occupied(_) => panic!("the sandbox index is already in use"),
			dashmap::Entry::Vacant(entry) => {
				entry.insert(state);
			},
		}
		self.indexes.insert(id, index);
		self.updated.notify_waiters();
	}

	#[must_use]
	pub fn iter(&self) -> dashmap::iter::Iter<'_, u64, State, fnv::FnvBuildHasher> {
		self.states.iter()
	}

	#[must_use]
	pub fn remove(&self, index: u64) -> Option<State> {
		let (_, state) = self.states.remove(&index)?;
		self.indexes
			.remove_if(&state.id, |_, current| *current == index);
		self.updated.notify_waiters();

		Some(state)
	}

	/// Get a future that completes when a sandbox's state is stored or removed. Create it before reading the state, so that a change between the read and the wait is not missed.
	pub fn updated(&self) -> tokio::sync::futures::Notified<'_> {
		self.updated.notified()
	}

	/// Wait until the sandbox's previous boot is stopped or destroyed, so that a new boot of the sandbox can start on its directory.
	pub async fn wait_settled(&self, id: &tg::sandbox::Id) {
		loop {
			let removed = self.updated.notified();
			let Some(sandbox) = self.get_by_id(id) else {
				return;
			};
			if sandbox.status.is_settled() {
				return;
			}
			let mut changed = sandbox.changed.subscribe();
			drop(sandbox);
			tokio::select! {
				_ = changed.changed() => {},
				() = removed => {},
			}
		}
	}
}

impl State {
	#[must_use]
	pub fn data(&self) -> tg::sandbox::get::Output {
		let id = self.id.clone();
		let arg = &self.create;
		let data = tg::sandbox::Data {
			boot: self.boot,
			cpu: arg.cpu,
			creator: self.creator.clone(),
			error: self.error.clone(),
			hostname: arg.hostname.clone(),
			id,
			memory: arg.memory,
			mounts: arg.mounts.clone(),
			network: arg.network.clone(),
			owner: self.owner.clone(),
			status: self.status,
			storage: arg.storage,
			ttl: arg.ttl,
			usage: self.usage.clone(),
		};
		tg::sandbox::get::Output {
			data,
			location: Some(self.location.clone()),
			tokens: self.authorization_tokens.clone(),
		}
	}

	#[must_use]
	pub fn processes(
		&self,
		position: u64,
		length: u64,
	) -> tg::sandbox::control::GetProcessesClientResponseOutput {
		let processes = self
			.process_ids
			.keys()
			.skip(usize::try_from(position).unwrap())
			.take(usize::try_from(length).unwrap())
			.cloned()
			.collect();
		tg::sandbox::control::GetProcessesClientResponseOutput {
			length: u64::try_from(self.process_ids.len()).unwrap(),
			processes,
			status: self.status,
		}
	}
}

impl<'a> IntoIterator for &'a Sandboxes {
	type Item = dashmap::mapref::multiple::RefMulti<'a, u64, State>;
	type IntoIter = dashmap::iter::Iter<'a, u64, State, fnv::FnvBuildHasher>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}
