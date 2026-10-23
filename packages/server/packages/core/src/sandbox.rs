use {
	crate::{Origin, Session, config::RunnerBackend},
	dashmap::DashMap,
	futures::{future::BoxFuture, stream::BoxStream},
	indexmap::IndexMap,
	std::{
		collections::BTreeMap, net::IpAddr, net::Ipv4Addr, net::SocketAddr, ops::ControlFlow,
		sync::Arc, time::Duration,
	},
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

/// Fill in the resource bounds that a sandbox omits from the default bounds, raising a default maximum to meet a larger minimum, and leaving a resource with no minimum and no maximum omitted.
pub fn resolve_sandbox_resources(
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
pub fn clear_darwin_sandbox_resources(arg: &mut tg::sandbox::create::Arg) -> bool {
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
pub fn limit_sandbox_resources(arg: &mut tg::sandbox::create::Arg, capacity: tg::runner::Capacity) {
	arg.cpu = limit_sandbox_resource(arg.cpu, capacity.cpu);
	arg.memory = limit_sandbox_resource(arg.memory, capacity.memory);
	arg.storage = limit_sandbox_resource(arg.storage, capacity.storage);
}

/// Get the capacity that a sandbox with resolved resources reserves.
#[must_use]
pub fn sandbox_capacity(arg: &tg::sandbox::create::Arg) -> tg::runner::Capacity {
	tg::runner::Capacity {
		cpu: arg.cpu.unwrap_or_default(),
		memory: arg.memory.unwrap_or_default(),
		sandboxes: 1,
		storage: arg.storage.unwrap_or_default(),
	}
}

pub fn validate_sandbox_resources(
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

pub fn normalize_sandbox_create_arg(
	mut arg: tg::sandbox::create::Arg,
) -> tg::Result<tg::sandbox::create::Arg> {
	if let Some(tg::sandbox::Network::Bridge(bridge)) = &mut arg.network {
		bridge.ports = resolve_sandbox_ports(std::mem::take(&mut bridge.ports))?;
	}
	Ok(arg)
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
		let sandbox = self.server.runner.state().sandboxes().get(runner.index)?;
		let sender = &sandbox.control_sender;
		(!sender.is_closed()).then(|| sender.clone())
	}

	#[must_use]
	pub fn try_get_sandbox_runner_inner(
		&self,
		id: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<Runner> {
		let sandbox = self.server.runner.state().sandboxes().get_by_id(id)?;
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

fn resolve_sandbox_ports(ports: Vec<tg::sandbox::Port>) -> tg::Result<Vec<tg::sandbox::Port>> {
	let mut output = Vec::new();
	for port in ports {
		for mut port in port.expand() {
			if port.host.is_none() {
				let host = allocate_sandbox_port(&output, port.host_ip, port.protocol)?;
				port = port.with_host(tg::sandbox::PortRange::single(host));
			}
			validate_sandbox_port(&output, port)?;
			output.push(port);
		}
	}
	Ok(output)
}

fn allocate_sandbox_port(
	existing: &[tg::sandbox::Port],
	host_ip: Option<Ipv4Addr>,
	protocol: tg::sandbox::PortProtocol,
) -> tg::Result<u16> {
	for _ in 0..100 {
		let port = bind_ephemeral_port(host_ip, protocol)?;
		if !existing
			.iter()
			.any(|existing| host_port_conflicts(*existing, host_ip, port, protocol))
		{
			return Ok(port);
		}
	}
	Err(tg::error!("failed to allocate a host port"))
}

fn validate_sandbox_port(
	existing: &[tg::sandbox::Port],
	port: tg::sandbox::Port,
) -> tg::Result<()> {
	let host = port
		.host
		.ok_or_else(|| tg::error!("expected a resolved host port"))?;
	if !host.is_single() || !port.guest.is_single() {
		return Err(tg::error!("expected resolved port mappings"));
	}
	let host_port = host.start;
	if existing
		.iter()
		.any(|existing| host_port_conflicts(*existing, port.host_ip, host_port, port.protocol))
	{
		return Err(tg::error!(%host_port, "duplicate host port mapping"));
	}
	bind_specific_port(port.host_ip, host_port, port.protocol)?;
	Ok(())
}

fn host_port_conflicts(
	existing: tg::sandbox::Port,
	host_ip: Option<Ipv4Addr>,
	host_port: u16,
	protocol: tg::sandbox::PortProtocol,
) -> bool {
	existing.protocol == protocol
		&& existing
			.host
			.is_some_and(|host| host.start <= host_port && host.end >= host_port)
		&& host_ips_conflict(existing.host_ip, host_ip)
}

fn host_ips_conflict(a: Option<Ipv4Addr>, b: Option<Ipv4Addr>) -> bool {
	a.is_none() || b.is_none() || a == b
}

fn bind_ephemeral_port(
	host_ip: Option<Ipv4Addr>,
	protocol: tg::sandbox::PortProtocol,
) -> tg::Result<u16> {
	let addr = socket_addr(host_ip, 0);
	match protocol {
		tg::sandbox::PortProtocol::Tcp => {
			let listener = std::net::TcpListener::bind(addr)
				.map_err(|error| tg::error!(!error, "failed to bind a host port"))?;
			listener
				.local_addr()
				.map(|addr| addr.port())
				.map_err(|error| tg::error!(!error, "failed to get the host port"))
		},
		tg::sandbox::PortProtocol::Udp => {
			let socket = std::net::UdpSocket::bind(addr)
				.map_err(|error| tg::error!(!error, "failed to bind a host port"))?;
			socket
				.local_addr()
				.map(|addr| addr.port())
				.map_err(|error| tg::error!(!error, "failed to get the host port"))
		},
	}
}

fn bind_specific_port(
	host_ip: Option<Ipv4Addr>,
	port: u16,
	protocol: tg::sandbox::PortProtocol,
) -> tg::Result<()> {
	let addr = socket_addr(host_ip, port);
	match protocol {
		tg::sandbox::PortProtocol::Tcp => std::net::TcpListener::bind(addr)
			.map(drop)
			.map_err(|error| tg::error!(!error, %port, "failed to bind the host port")),
		tg::sandbox::PortProtocol::Udp => std::net::UdpSocket::bind(addr)
			.map(drop)
			.map_err(|error| tg::error!(!error, %port, "failed to bind the host port")),
	}
}

fn socket_addr(host_ip: Option<Ipv4Addr>, port: u16) -> SocketAddr {
	let host_ip = host_ip.unwrap_or(Ipv4Addr::UNSPECIFIED);
	SocketAddr::new(IpAddr::V4(host_ip), port)
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
