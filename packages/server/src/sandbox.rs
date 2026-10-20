use {
	crate::{Origin, Server},
	dashmap::DashMap,
	futures::future::BoxFuture,
	indexmap::IndexMap,
	std::{collections::BTreeMap, sync::Arc},
	tangram_client::prelude::*,
};

mod runner;

pub(crate) use runner::Runner;

pub mod backend;
pub mod control;
pub mod create;
pub mod destroy;
pub mod get;
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
	pub capacity: tg::runner::Capacity,
	pub changed: tokio::sync::watch::Sender<()>,
	pub(crate) control_sender: control::local::Local,
	pub create: tg::sandbox::create::Arg,
	pub creator: Option<tg::Principal>,
	pub id: tg::sandbox::Id,
	pub location: tg::Location,
	pub process_ids: IndexMap<tg::process::Id, (), tg::id::BuildHasher>,
	pub processes: Arc<crate::process::Processes>,
	pub sandbox: Option<tangram_sandbox::Sandbox>,
	pub status: tg::sandbox::Status,
	pub token: String,
	pub tokens: BTreeMap<tg::artifact::Id, tg::authorization::Token>,
	pub usage: Option<tg::sandbox::Usage>,
	pub(crate) vfs: Option<crate::vfs::provider::Weak>,
}

pub type Tasks = tangram_futures::task::Map<String, ()>;

/// Fill in the resource bounds that a sandbox omits from the default bounds, raising a default maximum to meet a larger minimum, and leaving a resource with no minimum and no maximum omitted.
pub(crate) fn resolve_sandbox_resources(
	defaults: &tg::runner::Capacity,
	arg: &mut tg::sandbox::create::Arg,
) {
	if clear_darwin_sandbox_resources(arg) {
		return;
	}
	arg.cpu = resolve_sandbox_resource(arg.cpu, defaults.cpu);
	arg.memory = resolve_sandbox_resource(arg.memory, defaults.memory);
	arg.storage = resolve_sandbox_resource(arg.storage, defaults.storage);
}

/// Remove the resource bounds of a sandbox for a darwin host, which neither applies nor enforces them, and return whether the host is darwin.
pub(crate) fn clear_darwin_sandbox_resources(arg: &mut tg::sandbox::create::Arg) -> bool {
	let darwin = match &arg.host {
		Some(host) => host.ends_with("-darwin"),
		None => cfg!(target_os = "macos"),
	};
	if darwin {
		arg.cpu = None;
		arg.memory = None;
		arg.storage = None;
	}
	darwin
}

/// Give each resource of a sandbox that has no maximum the maximum of the capacity that it was allocated.
pub(crate) fn limit_sandbox_resources(
	arg: &mut tg::sandbox::create::Arg,
	capacity: tg::runner::Capacity,
) {
	arg.cpu = limit_sandbox_resource(arg.cpu, capacity.cpu);
	arg.memory = limit_sandbox_resource(arg.memory, capacity.memory);
	arg.storage = limit_sandbox_resource(arg.storage, capacity.storage);
}

/// Get the capacity that a sandbox with resolved resources reserves.
#[must_use]
pub(crate) fn sandbox_capacity(arg: &tg::sandbox::create::Arg) -> tg::runner::Capacity {
	tg::runner::Capacity {
		cpu: arg.cpu.unwrap_or_default(),
		memory: arg.memory.unwrap_or_default(),
		sandboxes: 1,
		storage: arg.storage.unwrap_or_default(),
	}
}

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

	pub fn insert(&self, index: u64, state: State) {
		let id = state.id.clone();
		match self.sandboxes.entry(index) {
			dashmap::Entry::Occupied(_) => panic!("the sandbox index is already in use"),
			dashmap::Entry::Vacant(entry) => {
				entry.insert(state);
			},
		}
		match self.indexes.entry(id) {
			dashmap::Entry::Occupied(_) => panic!("the sandbox ID is already in use"),
			dashmap::Entry::Vacant(entry) => {
				entry.insert(index);
			},
		}
	}

	pub fn iter(&self) -> dashmap::iter::Iter<'_, u64, State, fnv::FnvBuildHasher> {
		self.sandboxes.iter()
	}

	pub fn remove(&self, index: u64) -> Option<State> {
		let (_, state) = self.sandboxes.remove(&index)?;
		self.indexes.remove(&state.id);

		Some(state)
	}
}

impl State {
	#[must_use]
	pub fn data(&self) -> tg::sandbox::get::Output {
		let id = self.id.clone();
		let arg = &self.create;
		let data = tg::sandbox::Data {
			cpu: arg.cpu,
			creator: self.creator.clone(),
			hostname: arg.hostname.clone(),
			id,
			memory: arg.memory,
			mounts: arg.mounts.clone(),
			network: arg.network.clone(),
			owner: arg.owner.clone(),
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

impl Server {
	pub(crate) fn origin_has_network_access(&self, origin: Origin) -> tg::Result<bool> {
		let sandbox = self.try_get_request_origin_sandbox(origin)?;
		let has_network_access = sandbox.is_none_or(|sandbox| sandbox.create.network.is_some());

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

	pub(crate) fn validate_sandbox_resources(
		isolation: &tangram_sandbox::Backend,
		cpu: Option<tg::sandbox::Cpu>,
		memory: Option<tg::sandbox::Resource<u64>>,
		hostname: Option<&str>,
		storage: Option<tg::sandbox::Resource<u64>>,
	) -> tg::Result<()> {
		// Validate the resource bounds.
		if let Some(cpu) = cpu {
			cpu.threads()?;
		}
		for resource in [memory, storage].into_iter().flatten() {
			resource.validate()?;
		}

		// Validate the backend resource support.
		if !matches!(
			isolation,
			tangram_sandbox::Backend::Builtin
				| tangram_sandbox::Backend::Bubblewrap { .. }
				| tangram_sandbox::Backend::Gvisor { .. }
		) && storage.is_some()
		{
			return Err(tg::error!("storage bounds require container isolation"));
		}
		if matches!(isolation, tangram_sandbox::Backend::CloudHypervisor(_))
			&& (cpu.is_some_and(|cpu| {
				cpu.min.millis() > 0 || cpu.max.is_some_and(|max| max.millis() % 1000 != 0)
			}) || memory.is_some_and(|memory| memory.min > 0))
		{
			return Err(tg::error!(
				"resource minima and fractional CPUs require container isolation"
			));
		}

		// Validate the memory limit.
		if memory.is_some_and(|memory| memory.max == Some(0)) {
			return Err(tg::error!("sandbox memory must be greater than zero"));
		}

		// Validate the seatbelt options.
		if matches!(isolation, tangram_sandbox::Backend::Seatbelt)
			&& (cpu.is_some() || memory.is_some())
		{
			return Err(tg::error!(
				"sandbox cpu and memory are not supported with seatbelt isolation"
			));
		}
		if matches!(isolation, tangram_sandbox::Backend::Seatbelt) && hostname.is_some() {
			return Err(tg::error!(
				"setting a hostname is not supported with seatbelt isolation"
			));
		}

		Ok(())
	}
}

fn limit_sandbox_resource<T>(
	requested: Option<tg::sandbox::Resource<T>>,
	capacity: tg::sandbox::Resource<T>,
) -> Option<tg::sandbox::Resource<T>>
where
	T: Copy + Default + PartialOrd,
{
	let Some(max) = capacity.max else {
		return requested;
	};
	let requested = requested.unwrap_or_default();
	if requested.max.is_some() {
		return Some(requested);
	}
	let max = if max < requested.min {
		requested.min
	} else {
		max
	};
	let resource = tg::sandbox::Resource {
		max: Some(max),
		min: requested.min,
	};

	Some(resource)
}

fn resolve_sandbox_resource<T>(
	requested: Option<tg::sandbox::Resource<T>>,
	default: tg::sandbox::Resource<T>,
) -> Option<tg::sandbox::Resource<T>>
where
	T: Copy + Default + PartialOrd,
{
	let min = requested.map_or(default.min, |requested| requested.min);
	let max = requested
		.and_then(|requested| requested.max)
		.or_else(|| default.max.map(|max| if max < min { min } else { max }));
	let resource = tg::sandbox::Resource { max, min };
	(resource != tg::sandbox::Resource::default()).then_some(resource)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn defaults_fill_omitted_bounds_and_rise_to_meet_a_minimum() {
		let default = tg::sandbox::Resource::from(4);
		for (requested, expected) in [
			(None, Some(tg::sandbox::Resource::new(0, 4))),
			(
				Some(tg::sandbox::Resource::with_min(2)),
				Some(tg::sandbox::Resource::new(2, 4)),
			),
			(
				Some(tg::sandbox::Resource::with_min(8)),
				Some(tg::sandbox::Resource::new(8, 8)),
			),
			(
				Some(tg::sandbox::Resource::new(1, 2)),
				Some(tg::sandbox::Resource::new(1, 2)),
			),
		] {
			assert_eq!(resolve_sandbox_resource(requested, default), expected);
		}
	}

	#[test]
	fn darwin_hosts_neither_apply_nor_enforce_resource_bounds() {
		let defaults = tg::runner::Capacity {
			cpu: tg::sandbox::Amount::from_millis(1000).into(),
			memory: tg::sandbox::Resource::from(1024),
			..Default::default()
		};
		let mut arg = tg::sandbox::create::Arg {
			cpu: Some(tg::sandbox::Amount::from_millis(2000).into()),
			host: Some("aarch64-darwin".to_owned()),
			storage: Some(tg::sandbox::Resource::from(1024)),
			..Default::default()
		};
		resolve_sandbox_resources(&defaults, &mut arg);
		assert_eq!((arg.cpu, arg.memory, arg.storage), (None, None, None));
	}

	#[test]
	fn borrowers_without_a_maximum_take_the_maximum_of_their_allocation() {
		let capacity = tg::sandbox::Resource::new(0, 4);
		for (requested, expected) in [
			(None, Some(tg::sandbox::Resource::new(0, 4))),
			(
				Some(tg::sandbox::Resource::with_min(2)),
				Some(tg::sandbox::Resource::new(2, 4)),
			),
			(
				Some(tg::sandbox::Resource::new(1, 2)),
				Some(tg::sandbox::Resource::new(1, 2)),
			),
		] {
			assert_eq!(limit_sandbox_resource(requested, capacity), expected);
		}
		assert_eq!(
			limit_sandbox_resource(None, tg::sandbox::Resource::<u64>::default()),
			None
		);
	}

	#[test]
	fn unset_defaults_leave_unbounded_resources_omitted() {
		let default = tg::sandbox::Resource::<u64>::default();
		assert_eq!(resolve_sandbox_resource(None, default), None);
		assert_eq!(
			resolve_sandbox_resource(Some(tg::sandbox::Resource::default()), default),
			None
		);
		assert_eq!(
			resolve_sandbox_resource(Some(tg::sandbox::Resource::with_min(2)), default),
			Some(tg::sandbox::Resource::with_min(2))
		);
	}
}
