use {
	crate::{Origin, Server},
	dashmap::DashMap,
	futures::future::BoxFuture,
	std::{collections::BTreeMap, sync::Arc},
	tangram_client::prelude::*,
	tangram_messenger::prelude::*,
};

pub mod control;
pub mod create;
pub mod destroy;
pub mod get;
pub mod isolation;
pub mod list;
pub mod processes;
pub mod status;

pub(crate) type ConnectionFuture = BoxFuture<'static, tg::Result<()>>;

#[derive(Default)]
pub struct Sandboxes {
	indexes: DashMap<tg::sandbox::Id, u64, tg::id::BuildHasher>,
	sandboxes: DashMap<u64, State, fnv::FnvBuildHasher>,
}

pub struct State {
	pub allocation: Option<Arc<tokio::sync::Mutex<Option<crate::runner::capacity::Allocation>>>>,
	pub authorization_tokens: tg::authorization::Tokens,
	pub data: tg::sandbox::control::Data,
	pub id: Option<tg::sandbox::Id>,
	pub location: tg::Location,
	pub processes: Arc<crate::process::Processes>,
	pub sandbox: Option<tangram_sandbox::Sandbox>,
	pub status: tg::sandbox::Status,
	pub token: Option<String>,
	pub tokens: BTreeMap<tg::artifact::Id, tg::authorization::Token>,
	pub usage: Option<tg::sandbox::get::Usage>,
}

pub type Tasks = tangram_futures::task::Map<String, ()>;

impl Sandboxes {
	#[must_use]
	pub fn get(&self, index: u64) -> Option<dashmap::mapref::one::Ref<'_, u64, State>> {
		self.sandboxes.get(&index)
	}

	#[must_use]
	pub fn get_by_id(
		&self,
		id: &tg::sandbox::Id,
	) -> Option<dashmap::mapref::one::Ref<'_, u64, State>> {
		let index = *self.indexes.get(id)?;

		self.sandboxes.get(&index)
	}

	#[must_use]
	pub fn get_mut_by_id(
		&self,
		id: &tg::sandbox::Id,
	) -> Option<dashmap::mapref::one::RefMut<'_, u64, State>> {
		let index = *self.indexes.get(id)?;

		self.sandboxes.get_mut(&index)
	}

	#[must_use]
	pub fn get_mut(&self, index: u64) -> Option<dashmap::mapref::one::RefMut<'_, u64, State>> {
		self.sandboxes.get_mut(&index)
	}

	pub fn insert(&self, index: u64, state: State) {
		assert!(state.id.is_none(), "the sandbox ID is already set");
		match self.sandboxes.entry(index) {
			dashmap::Entry::Occupied(_) => panic!("the sandbox index is already in use"),
			dashmap::Entry::Vacant(entry) => {
				entry.insert(state);
			},
		}
	}

	pub fn iter(&self) -> dashmap::iter::Iter<'_, u64, State, fnv::FnvBuildHasher> {
		self.sandboxes.iter()
	}

	pub fn remove(&self, index: u64) -> Option<State> {
		let (_, state) = self.sandboxes.remove(&index)?;
		if let Some(id) = &state.id {
			self.indexes.remove(id);
		}

		Some(state)
	}

	pub fn set_id(&self, index: u64, id: tg::sandbox::Id) {
		let mut sandbox = self
			.sandboxes
			.get_mut(&index)
			.expect("the sandbox index was not found");
		assert!(sandbox.id.is_none(), "the sandbox ID is already set");
		match self.indexes.entry(id.clone()) {
			dashmap::Entry::Occupied(_) => panic!("the sandbox ID is already in use"),
			dashmap::Entry::Vacant(entry) => {
				sandbox.id = Some(id);
				entry.insert(index);
			},
		}
	}
}

impl State {
	#[must_use]
	pub fn data(&self) -> Option<tg::sandbox::get::Output> {
		let id = self.id.clone()?;
		let arg = &self.data.arg;
		let output = tg::sandbox::get::Output {
			cpu: arg.cpu,
			creator: self.data.creator.clone(),
			hostname: arg.hostname.clone(),
			id,
			isolation: arg.isolation,
			location: Some(self.location.clone()),
			memory: arg.memory,
			mounts: arg.mounts.clone(),
			network: arg.network.clone(),
			owner: arg.owner.clone(),
			status: self.status,
			tokens: self.authorization_tokens.clone(),
			ttl: arg.ttl,
			usage: self.usage.clone(),
		};

		Some(output)
	}
}

impl Server {
	pub(crate) fn origin_has_network_access(&self, origin: Origin) -> tg::Result<bool> {
		let sandbox = self.try_get_request_origin_sandbox(origin)?;
		let has_network_access = sandbox.is_none_or(|sandbox| sandbox.data.arg.network.is_some());

		Ok(has_network_access)
	}

	pub(crate) fn try_get_request_origin_sandbox(
		&self,
		origin: Origin,
	) -> tg::Result<Option<dashmap::mapref::one::Ref<'_, u64, State>>> {
		let Ok(index) = origin.try_unwrap_sandbox() else {
			return Ok(None);
		};
		let sandbox = self
			.runner
			.state()
			.sandboxes()
			.get(index)
			.ok_or_else(|| tg::error!(%index, "failed to find the origin sandbox"))?;

		Ok(Some(sandbox))
	}

	pub(crate) fn spawn_publish_sandbox_status_task(&self, id: &tg::sandbox::Id) {
		let subject = format!("sandboxes.{id}.status");
		tokio::spawn({
			let server = self.clone();
			async move {
				let result = server.messenger.publish(subject, ()).await;
				if let Err(error) = result {
					tracing::error!(%error, "failed to publish the sandbox status message");
				}
			}
		});
	}

	pub(crate) fn validate_sandbox_resources(
		isolation: &tangram_sandbox::Isolation,
		cpu: Option<u64>,
		memory: Option<u64>,
		hostname: Option<&str>,
	) -> tg::Result<()> {
		if cpu == Some(0) {
			return Err(tg::error!("sandbox cpu must be greater than zero"));
		}
		if memory == Some(0) {
			return Err(tg::error!("sandbox memory must be greater than zero"));
		}
		if matches!(isolation, tangram_sandbox::Isolation::Seatbelt(_))
			&& (cpu.is_some() || memory.is_some())
		{
			return Err(tg::error!(
				"sandbox cpu and memory are not supported with seatbelt isolation"
			));
		}
		if matches!(isolation, tangram_sandbox::Isolation::Seatbelt(_)) && hostname.is_some() {
			return Err(tg::error!(
				"setting a hostname is not supported with seatbelt isolation"
			));
		}
		Ok(())
	}
}
