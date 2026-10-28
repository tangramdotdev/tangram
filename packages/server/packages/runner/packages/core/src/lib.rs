use {
	std::sync::{
		Mutex,
		atomic::{AtomicU64, Ordering},
	},
	tangram_client::prelude::*,
};

pub mod capacity;
pub mod process;
#[cfg(target_os = "linux")]
pub mod project;
pub mod sandbox;

#[derive(Clone, Copy, Debug)]
pub struct Config {
	pub capacity: tg::runner::Capacity,

	#[cfg(target_os = "linux")]
	pub filesystem_project_ids: tangram_server_config::FilesystemProjectIds,

	pub overcommit: tg::runner::Overcommit,

	pub sandbox_pool_size: usize,
}

pub struct State {
	#[cfg(target_os = "linux")]
	pub cpu_shares: tangram_sandbox::cpu::Shares,

	#[cfg(target_os = "linux")]
	pub cpu_threads: tangram_sandbox::cpu::Threads,

	pub capacity: crate::capacity::Pool,

	#[cfg(target_os = "linux")]
	pub storage_pool: tangram_sandbox::budget::Pool,

	#[cfg(target_os = "linux")]
	pub filesystem_project_ids: crate::project::Pool,

	pub heartbeat_index: AtomicU64,

	pub id: Mutex<Option<tg::runner::Id>>,

	pub next_sandbox_index: AtomicU64,

	pub process_for_token: dashmap::DashMap<String, (u64, tg::process::Id)>,

	pub processes: tangram_server_process_core::Map,

	pub reservations: crate::capacity::Reservations,

	pub sandboxes: crate::sandbox::Sandboxes,

	pub scheduler: tokio::sync::watch::Sender<Option<tg::scheduler::Id>>,
}

impl State {
	#[must_use]
	pub fn new(config: Config) -> Self {
		let (scheduler, _) = tokio::sync::watch::channel(None);
		Self {
			capacity: crate::capacity::Pool::new(config.capacity, config.overcommit),

			#[cfg(target_os = "linux")]
			cpu_shares: tangram_sandbox::cpu::Shares::new(config.capacity.cpu.min.millis()),

			#[cfg(target_os = "linux")]
			cpu_threads: tangram_sandbox::cpu::Threads::new(),

			#[cfg(target_os = "linux")]
			filesystem_project_ids: crate::project::Pool::new(config.filesystem_project_ids),

			heartbeat_index: AtomicU64::new(0),

			id: Mutex::new(None),

			next_sandbox_index: AtomicU64::new(1),

			process_for_token: dashmap::DashMap::new(),

			processes: tangram_server_process_core::Map::default(),

			reservations: crate::capacity::Reservations::new(),

			sandboxes: crate::sandbox::Sandboxes::default(),

			scheduler,

			#[cfg(target_os = "linux")]
			storage_pool: tangram_sandbox::budget::Pool::new(
				config.capacity.storage.min,
				4096,
				4096,
			),
		}
	}

	#[must_use]
	pub fn heartbeat_index(&self) -> u64 {
		self.heartbeat_index.load(Ordering::Acquire)
	}

	#[must_use]
	pub fn capacity(&self) -> &crate::capacity::Pool {
		&self.capacity
	}

	#[must_use]
	pub fn processes(&self) -> &tangram_server_process_core::Map {
		&self.processes
	}

	#[must_use]
	pub fn process_for_token(&self) -> &dashmap::DashMap<String, (u64, tg::process::Id)> {
		&self.process_for_token
	}

	#[must_use]
	pub fn reservations(&self) -> &crate::capacity::Reservations {
		&self.reservations
	}

	#[must_use]
	pub fn sandboxes(&self) -> &crate::sandbox::Sandboxes {
		&self.sandboxes
	}

	pub async fn wait_for_scheduler(&self) -> tg::scheduler::Id {
		let mut scheduler = self.scheduler.subscribe();
		loop {
			if let Some(scheduler) = scheduler.borrow_and_update().clone() {
				return scheduler;
			}
			scheduler.changed().await.unwrap();
		}
	}

	pub async fn wait_for_scheduler_change(
		&self,
		current: &tg::scheduler::Id,
	) -> tg::scheduler::Id {
		let mut scheduler = self.scheduler.subscribe();
		loop {
			if let Some(scheduler) = scheduler.borrow_and_update().clone()
				&& scheduler != *current
			{
				return scheduler;
			}
			scheduler.changed().await.unwrap();
		}
	}

	pub fn set_scheduler(&self, scheduler: Option<tg::scheduler::Id>) {
		self.heartbeat_index.store(0, Ordering::Release);
		self.scheduler.send_replace(scheduler);
	}

	#[must_use]
	pub fn create_sandbox_index(&self) -> u64 {
		let index = self.next_sandbox_index.fetch_add(1, Ordering::Relaxed);
		assert_ne!(index, u64::MAX, "exhausted the sandbox indexes");

		index
	}

	#[cfg(target_os = "linux")]
	pub fn create_filesystem_project_id(&self) -> tg::Result<crate::project::Id> {
		self.filesystem_project_ids
			.acquire()
			.ok_or_else(|| tg::error!("the container filesystem project ID range is exhausted"))
	}

	#[must_use]
	pub fn id(&self) -> Option<tg::runner::Id> {
		self.id.lock().unwrap().clone()
	}

	#[must_use]
	pub fn started_process_count(&self) -> u64 {
		self.sandboxes
			.iter()
			.map(|sandbox| {
				sandbox
					.processes
					.iter()
					.filter(|process| process.value().data.status == tg::process::Status::Started)
					.count()
			})
			.sum::<usize>()
			.try_into()
			.unwrap()
	}

	#[must_use]
	pub fn try_get_sandbox(&self, id: &tg::sandbox::Id) -> Option<tg::sandbox::get::Output> {
		Some(self.sandboxes.get_by_id(id)?.data())
	}

	/// Get a process's data for the home instance, without the children whose starts the home instance has not acknowledged.
	#[must_use]
	pub fn try_get_process(&self, id: &tg::process::Id) -> Option<tg::process::Data> {
		let mut data = {
			let sandbox = self.try_get_process_sandbox(id)?;
			let sandbox = self.sandboxes.get_by_id(&sandbox)?;
			let process = sandbox.processes.get(id)?;
			process.data()
		};
		self.retain_acknowledged_children(&mut data);
		Some(data)
	}

	/// Get a page of a process's children for the home instance, without the children whose starts the home instance has not acknowledged.
	#[must_use]
	pub fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: u64,
		length: u64,
	) -> Option<tg::process::control::GetChildrenClientResponseOutput> {
		// Read the children, and release the process before checking each child, which reads other processes.
		let (children, status) = {
			let sandbox = self.try_get_process_sandbox(id)?;
			let sandbox = self.sandboxes.get_by_id(&sandbox)?;
			let process = sandbox.processes.get(id)?;
			let children = process
				.children
				.values()
				.map(|child| child.data.clone())
				.collect::<Vec<_>>();
			(children, process.data.status)
		};

		// Page the acknowledged children.
		let children = children
			.into_iter()
			.filter(|child| self.is_process_acknowledged(&child.process.node))
			.collect::<Vec<_>>();
		let children_length = u64::try_from(children.len()).unwrap();
		let start = usize::try_from(position.min(children_length)).unwrap();
		let end = usize::try_from(position.saturating_add(length).min(children_length)).unwrap();
		let children = children[start..end].to_vec();

		Some(tg::process::control::GetChildrenClientResponseOutput {
			children,
			length: children_length,
			status,
		})
	}

	/// Remove the children whose starts the home instance has not acknowledged from a process's data.
	pub fn retain_acknowledged_children(&self, data: &mut tg::process::Data) {
		if let Some(children) = &mut data.children {
			children.retain(|child| self.is_process_acknowledged(&child.process.node));
		}
	}

	/// Determine whether the home instance has acknowledged a process's start. A process that this runner does not run, such as a cached or scheduled child, is acknowledged.
	#[must_use]
	pub fn is_process_acknowledged(&self, id: &tg::process::Id) -> bool {
		let Some(sandbox) = self.try_get_process_sandbox(id) else {
			return true;
		};
		let Some(sandbox) = self.sandboxes.get_by_id(&sandbox) else {
			return true;
		};
		sandbox
			.processes
			.get(id)
			.is_none_or(|process| process.acknowledged)
	}

	pub fn try_update_process<T>(
		&self,
		id: &tg::process::Id,
		update: impl FnOnce(&mut tangram_server_process_core::State) -> T,
	) -> Option<T> {
		let sandbox = self.try_get_process_sandbox(id)?;
		let sandbox = self.sandboxes.get_by_id(&sandbox)?;
		let mut process = sandbox.processes.get_mut(id)?;
		Some(update(&mut process))
	}

	#[must_use]
	pub fn try_get_process_sandbox(&self, id: &tg::process::Id) -> Option<tg::sandbox::Id> {
		self.processes
			.get(id)
			.map(|sandbox| sandbox.value().clone())
	}

	pub fn remove_process(&self, id: &tg::process::Id) {
		let Some((_, sandbox)) = self.processes.remove(id) else {
			return;
		};
		if let Some(sandbox) = self.sandboxes.get_by_id(&sandbox) {
			sandbox.processes.remove(id);
		}
	}

	pub fn remove_sandbox(&self, id: &tg::sandbox::Id) {
		let Some(index) = self.sandboxes.get_by_id(id).map(|sandbox| *sandbox.key()) else {
			return;
		};
		if let Some(sandbox) = self.sandboxes.remove(index) {
			for process in sandbox.processes.iter() {
				self.processes.remove(process.key());
			}
			sandbox.processes.clear();
		}
	}
}
