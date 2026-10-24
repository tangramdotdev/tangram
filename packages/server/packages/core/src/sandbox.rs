use {
	crate::{Origin, Session, config::RunnerBackend},
	dashmap::DashMap,
	futures::{future::BoxFuture, stream::BoxStream},
	indexmap::IndexMap,
	std::{collections::BTreeMap, ops::ControlFlow, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_http::body::Boxed as BoxBody,
};

pub mod control;

pub type ConnectionFuture = BoxFuture<'static, tg::Result<()>>;

/// The sandbox component.
pub trait Component: crate::component::Lifecycle {
	fn create_sandbox<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::sandbox::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::sandbox::create::Output>>;

	fn try_get_sandbox<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::sandbox::get::Output>>>;

	fn try_get_sandbox_processes_stream<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>>,
	>;

	fn list_sandboxes<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::sandbox::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::sandbox::list::Output>>;

	fn try_destroy_sandbox<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> BoxFuture<'a, tg::Result<Option<bool>>>;

	fn try_get_sandbox_status_stream<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>>>;

	fn get_sandbox_control_stream<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				(
					tg::sandbox::control::Header,
					BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
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

	fn authorize_with_sandbox_control_service<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		principal: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn create_sandbox_status_stream_local_with_wakeups<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		initial: Option<tg::sandbox::Data>,
		wakeups: Option<BoxStream<'static, ()>>,
		source: tg::sandbox::Source,
	) -> BoxStream<'static, tg::Result<tg::sandbox::status::Event>>;

	fn create_sandbox_status_wakeup_stream<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, ()>>>;

	fn destroy_sandbox_when_available<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
		connection_future: ConnectionFuture,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn get_sandbox_from_index<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<tangram_index::sandbox::Sandbox>>;

	fn list_sandboxes_local<'a>(
		&'a self,
		session: &'a Session,
		creator: Option<&'a tg::Principal>,
		owner: Option<&'a tg::Principal>,
	) -> BoxFuture<'a, tg::Result<Vec<tg::sandbox::list::Item>>>;

	fn prepare_sandbox_control_index_arg<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		created_at: i64,
		create: tg::sandbox::create::Arg,
		creator: Option<tg::Principal>,
		runner: Option<tg::runner::Id>,
	) -> BoxFuture<'a, tg::Result<tangram_index::sandbox::put::Arg>>;

	fn request_sandbox_control<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::control::ServerRequestArg,
		options: crate::control::Options,
	) -> BoxFuture<'a, tg::Result<tg::Result<tg::sandbox::control::ClientResponseOutput>>>;

	fn subscribe_sandbox_connection<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<ConnectionFuture>>;

	fn try_get_sandbox_from_index<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<Option<tangram_index::sandbox::Sandbox>>>;

	fn validate_sandbox_create_arg_with_parent<'a>(
		&'a self,
		session: &'a Session,
		arg: &'a tg::sandbox::create::Arg,
		parent: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<()>>;
}

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
	pub control_sender: control::local::Local,
	pub create: tg::sandbox::create::Arg,
	pub creator: Option<tg::Principal>,
	pub id: tg::sandbox::Id,
	pub location: tg::Location,
	pub process_ids: IndexMap<tg::process::Id, (), tg::id::BuildHasher>,
	pub processes: Arc<crate::process::Processes>,
	pub sandbox: Option<tangram_sandbox::Sandbox>,
	pub status: tg::sandbox::Status,
	pub token: tokio::sync::watch::Receiver<Option<String>>,
	pub tokens: BTreeMap<tg::artifact::Id, tg::authorization::Token>,
	pub usage: Option<tg::sandbox::Usage>,
	pub vfs: Option<std::sync::Arc<dyn crate::vfs::Provider>>,
}

pub struct Runner {
	pub changed: tokio::sync::watch::Receiver<()>,
	pub index: u64,
	pub location: tg::Location,
	pub location_arg: tg::location::Arg,
}

impl crate::Server {
	pub fn origin_has_network_access(&self, origin: Origin) -> tg::Result<bool> {
		let sandbox = self.try_get_request_origin_sandbox(origin)?;
		let has_network_access = sandbox.is_none_or(|sandbox| sandbox.create.network.is_some());

		Ok(has_network_access)
	}

	pub fn try_get_request_origin_sandbox(
		&self,
		origin: Origin,
	) -> tg::Result<Option<dashmap::mapref::one::Ref<'_, u64, State>>> {
		let Ok(index) = origin.try_unwrap_sandbox() else {
			return Ok(None);
		};
		let sandbox = self
			.components
			.runner
			.state()
			.sandboxes()
			.get(index)
			.ok_or_else(|| tg::error!(%index, "failed to find the origin sandbox"))?;

		Ok(Some(sandbox))
	}

	pub fn resolve_sandbox_backend(&self) -> tg::Result<tangram_sandbox::Backend> {
		let backend = match &self.config.runner.backend {
			RunnerBackend::Builtin(_) => tangram_sandbox::Backend::Builtin,
			RunnerBackend::Bubblewrap(config) => tangram_sandbox::Backend::Bubblewrap {
				path: config.path.clone(),
			},
			RunnerBackend::CloudHypervisor(config) => {
				let image_path = self
					.components
					.runner
					.sandbox_assets()
					.image
					.clone()
					.ok_or_else(|| tg::error!("the cloud-hypervisor image is unavailable"))?;
				let snapshot = self.components.runner.sandbox_assets().snapshot.clone();
				tangram_sandbox::Backend::CloudHypervisor(tangram_sandbox::CloudHypervisor {
					cloud_hypervisor_path: config.cloud_hypervisor_path.clone(),
					dax: config.dax.map(|dax| dax.window_size as u64),
					image_path,
					kernel_path: config.kernel_path.clone(),
					max_cpu: config.max_cpu,
					max_memory: config.max_memory,
					network_interface_timeout: config.network_interface_timeout,
					ready_timeout: config.ready_timeout,
					serial_connect_interval: config.serial_connect_interval,
					serial_connect_timeout: config.serial_connect_timeout,
					snapshot,
					snapshot_cpu: config.snapshot_cpu,
					snapshot_memory: config.snapshot_memory,
				})
			},
			RunnerBackend::Gvisor(config) => tangram_sandbox::Backend::Gvisor {
				path: config.path.clone(),
				platform: config.platform,
			},
			RunnerBackend::Seatbelt => tangram_sandbox::Backend::Seatbelt,
		};
		if cfg!(target_os = "linux") && matches!(backend, tangram_sandbox::Backend::Seatbelt) {
			return Err(tg::error!("the seatbelt backend requires macos"));
		}
		if cfg!(target_os = "macos") && !matches!(backend, tangram_sandbox::Backend::Seatbelt) {
			return Err(tg::error!("the selected backend requires linux"));
		}
		Ok(backend)
	}

	#[must_use]
	pub fn sandbox_resources(&self) -> tangram_sandbox::Resources {
		self.config
			.runner
			.backend
			.resources(&self.config.runner.resources)
	}
}

impl crate::Session {
	#[must_use]
	pub fn try_get_sandbox_control_runner_inner(
		&self,
		id: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<control::local::Local> {
		let runner = self.try_get_sandbox_runner_inner(id, location)?;
		let sandbox = self
			.server
			.components
			.runner
			.state()
			.sandboxes()
			.get(runner.index)?;
		let sender = &sandbox.control_sender;
		(!sender.is_closed()).then(|| sender.clone())
	}

	#[must_use]
	pub fn try_get_sandbox_runner_inner(
		&self,
		id: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<Runner> {
		let sandbox = self
			.server
			.components
			.runner
			.state()
			.sandboxes()
			.get_by_id(id)?;
		// Regions guide subsequent routing, but do not constrain a runner state match.
		let location_arg = if let Some(location) = location {
			let components = location
				.0
				.iter()
				.filter(|component| match (component, &sandbox.location) {
					(tg::location::arg::Component::Local(_), tg::Location::Local(_)) => true,
					(
						tg::location::arg::Component::Remote(component),
						tg::Location::Remote(remote),
					) => component.name == remote.name,
					_ => false,
				})
				.cloned()
				.collect::<Vec<_>>();
			if components.is_empty() {
				return None;
			}
			tg::location::Arg(components)
		} else {
			sandbox.location.clone().into()
		};
		let changed = sandbox.changed.subscribe();
		let index = *sandbox.key();
		let location = sandbox.location.clone();
		let runner = Runner {
			changed,
			index,
			location,
			location_arg,
		};
		Some(runner)
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

	#[must_use]
	pub fn iter(&self) -> dashmap::iter::Iter<'_, u64, State, fnv::FnvBuildHasher> {
		self.sandboxes.iter()
	}

	#[must_use]
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

impl<'a> IntoIterator for &'a Sandboxes {
	type Item = dashmap::mapref::multiple::RefMulti<'a, u64, State>;
	type IntoIter = dashmap::iter::Iter<'a, u64, State, fnv::FnvBuildHasher>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}
