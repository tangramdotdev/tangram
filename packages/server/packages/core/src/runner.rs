use {
	crate::{Server, Session},
	futures::{future::BoxFuture, stream::BoxStream},
	std::{ops::ControlFlow, sync::Mutex, sync::atomic::AtomicU64, sync::atomic::Ordering},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

pub mod assets;
pub mod capacity;
pub mod pool;
pub mod process;
#[cfg(target_os = "linux")]
pub mod project;
pub mod sandbox;

pub type RunnerSender =
	crate::control::Sender<tg::runner::control::ServerMessage, tg::runner::control::ClientMessage>;

/// The runner component.
pub trait Component: crate::component::Lifecycle {
	fn try_get_runner_data<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
	) -> BoxFuture<'a, tg::Result<Option<tg::runner::Data>>>;

	fn create_runner<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::runner::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::create::Output>>;

	fn create_runner_token<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::token::create::Output>>;

	fn try_delete_runner<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_delete_runner_token<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
		token: &'a tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_runners<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::runner::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::list::Output>>;

	fn list_runner_tokens<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
		arg: tg::runner::token::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::token::list::Output>>;

	fn get_runner_control_stream<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				(
					tg::runner::control::Header,
					BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>,
				),
				tg::Error,
			>,
		>,
	>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn sandbox_assets(&self) -> &assets::Assets;

	fn send_runner_control_request<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
		scheduler: &'a tg::scheduler::Id,
		connection_index: u64,
		arg: tg::runner::control::ServerRequestArg,
		options: crate::control::Options,
	) -> BoxFuture<'a, tg::Result<tg::Result<tg::runner::control::ClientResponseOutput>>>;

	fn spawn_sandbox_task<'a>(
		&'a self,
		server: &'a Server,
		arg: sandbox::SpawnSandboxTaskArg,
	) -> sandbox::SpawnSandboxTaskOutput;

	fn start_sandbox_pool<'a>(&'a self, session: &'a Session) -> ();

	fn stop_runner_task(&self);

	fn stop_sandbox_pool<'a>(&'a self, session: &'a Session) -> BoxFuture<'a, ()>;

	fn try_get_process_inner_token<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
	) -> Option<String>;
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
	pub capacity: tg::runner::Capacity,

	#[cfg(target_os = "linux")]
	pub filesystem_project_ids: crate::config::FilesystemProjectIds,

	pub overcommit: tg::runner::Overcommit,

	pub sandbox_pool_size: usize,
}

pub struct Runner {
	pub sandbox_pool: crate::runner::pool::Pool,
	pub state: State,
}

pub struct State {
	#[cfg(target_os = "linux")]
	pub cpu_shares: tangram_sandbox::cpu::Shares,

	#[cfg(target_os = "linux")]
	pub cpu_threads: tangram_sandbox::cpu::Threads,

	pub capacity: crate::runner::capacity::Pool,

	#[cfg(target_os = "linux")]
	pub storage_pool: tangram_sandbox::budget::Pool,

	#[cfg(target_os = "linux")]
	pub filesystem_project_ids: crate::runner::project::Pool,

	pub heartbeat_index: AtomicU64,

	pub id: Mutex<Option<tg::runner::Id>>,

	pub next_sandbox_index: AtomicU64,

	pub process_for_token: dashmap::DashMap<String, (u64, tg::process::Id)>,

	pub processes: crate::process::Map,

	pub reservations: crate::runner::capacity::Reservations,

	pub sandboxes: crate::sandbox::Sandboxes,

	pub scheduler: tokio::sync::watch::Sender<Option<tg::scheduler::Id>>,
}

impl Runner {
	#[must_use]
	pub fn new(config: Config) -> Self {
		let (scheduler, _) = tokio::sync::watch::channel(None);
		let state = State {
			capacity: crate::runner::capacity::Pool::new(config.capacity, config.overcommit),

			#[cfg(target_os = "linux")]
			cpu_shares: tangram_sandbox::cpu::Shares::new(config.capacity.cpu.min.millis()),

			#[cfg(target_os = "linux")]
			cpu_threads: tangram_sandbox::cpu::Threads::new(),

			#[cfg(target_os = "linux")]
			filesystem_project_ids: crate::runner::project::Pool::new(
				config.filesystem_project_ids,
			),

			heartbeat_index: AtomicU64::new(0),

			id: Mutex::new(None),

			next_sandbox_index: AtomicU64::new(1),

			process_for_token: dashmap::DashMap::new(),

			processes: crate::process::Map::default(),

			reservations: crate::runner::capacity::Reservations::new(),

			sandboxes: crate::sandbox::Sandboxes::default(),

			scheduler,

			#[cfg(target_os = "linux")]
			storage_pool: tangram_sandbox::budget::Pool::new(
				config.capacity.storage.min,
				4096,
				4096,
			),
		};
		let sandbox_pool = crate::runner::pool::Pool::new(config.sandbox_pool_size);

		Self {
			sandbox_pool,
			state,
		}
	}

	#[must_use]
	pub fn state(&self) -> &State {
		&self.state
	}
}

impl State {
	#[must_use]
	pub fn heartbeat_index(&self) -> u64 {
		self.heartbeat_index.load(Ordering::Acquire)
	}

	#[must_use]
	pub fn capacity(&self) -> &crate::runner::capacity::Pool {
		&self.capacity
	}

	#[must_use]
	pub fn processes(&self) -> &crate::process::Map {
		&self.processes
	}

	#[must_use]
	pub fn process_for_token(&self) -> &dashmap::DashMap<String, (u64, tg::process::Id)> {
		&self.process_for_token
	}

	#[must_use]
	pub fn reservations(&self) -> &crate::runner::capacity::Reservations {
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
	pub fn create_filesystem_project_id(&self) -> tg::Result<crate::runner::project::Id> {
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

	#[must_use]
	pub fn try_get_process(&self, id: &tg::process::Id) -> Option<tg::process::Data> {
		let sandbox = self.try_get_process_sandbox(id)?;
		let sandbox = self.sandboxes.get_by_id(&sandbox)?;
		let process = sandbox.processes.get(id)?;
		Some(process.data())
	}

	#[must_use]
	pub fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: u64,
		length: u64,
	) -> Option<tg::process::control::GetChildrenClientResponseOutput> {
		let sandbox = self.try_get_process_sandbox(id)?;
		let sandbox = self.sandboxes.get_by_id(&sandbox)?;
		let process = sandbox.processes.get(id)?;
		let children_length = u64::try_from(process.children.len()).unwrap();
		let start = usize::try_from(position.min(children_length)).unwrap();
		let end = usize::try_from(position.saturating_add(length).min(children_length)).unwrap();
		let children = process
			.children
			.get_range(start..end)
			.unwrap()
			.values()
			.map(|child| child.data.clone())
			.collect();
		Some(tg::process::control::GetChildrenClientResponseOutput {
			children,
			length: children_length,
			status: process.data.status,
		})
	}

	pub fn try_update_process<T>(
		&self,
		id: &tg::process::Id,
		update: impl FnOnce(&mut crate::process::State) -> T,
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
