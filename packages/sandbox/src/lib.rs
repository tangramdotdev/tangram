use {
	crate::client::Client,
	futures::{FutureExt as _, Stream},
	std::{
		collections::{BTreeMap, BTreeSet},
		net::Ipv4Addr,
		path::{Path, PathBuf},
		sync::{
			Arc,
			atomic::{AtomicU64, Ordering},
		},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_uri::Uri,
};

mod backend;
#[cfg(target_os = "linux")]
mod cgroup;
mod client;
mod command;
#[cfg(target_os = "linux")]
mod filesystem;
mod libraries;
#[cfg(target_os = "linux")]
mod mount;
#[cfg(target_os = "linux")]
mod netlink;
mod paths;
mod pty;
#[cfg(target_os = "linux")]
mod seccomp;
mod server;
#[cfg(target_os = "linux")]
mod usage;
mod util;

pub use self::backend::{Backend, Kind, Platform};

#[cfg(target_os = "linux")]
pub mod bubblewrap;
#[cfg(target_os = "linux")]
pub mod budget;
#[cfg(target_os = "linux")]
pub mod builtin;
#[cfg(target_os = "linux")]
pub mod capacity;
#[cfg(target_os = "linux")]
pub mod cloud_hypervisor;
#[cfg(target_os = "linux")]
pub mod cpu;
#[cfg(target_os = "linux")]
pub mod gvisor;
#[cfg(target_os = "linux")]
pub mod host;
pub mod init;
#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "linux")]
pub mod network;
#[cfg(target_os = "linux")]
pub mod root;
pub mod run;
#[cfg(target_os = "macos")]
pub mod seatbelt;
pub mod serve;
pub mod stdio;

#[derive(Clone)]
pub struct Sandbox(Arc<State>);

pub struct State {
	#[cfg(target_os = "linux")]
	_storage: Option<budget::Allocation>,

	#[cfg(target_os = "linux")]
	accounting: tokio::sync::Mutex<Option<usage::Accounting>>,

	arg: Arg,

	#[cfg(target_os = "linux")]
	cgroup: tokio::sync::Mutex<Option<crate::cgroup::Cgroup>>,

	#[cfg(target_os = "macos")]
	cleanup: seatbelt::Cleanup,

	client: Client,

	destroy: tokio::sync::Mutex<()>,

	#[cfg(target_os = "linux")]
	filesystem: Option<crate::filesystem::Filesystem>,

	#[cfg(target_os = "linux")]
	#[expect(dead_code)]
	network: Option<crate::network::Network>,

	next_process_index: AtomicU64,

	pid: u32,

	process: tokio::sync::Mutex<tokio::process::Child>,

	started: std::sync::atomic::AtomicBool,
}

#[derive(Clone, Debug)]
pub struct Process {
	index: u64,
}

#[derive(Clone, Debug)]
pub struct SpawnArg {
	pub command: Command,
	pub token: String,
	pub tty: Option<tg::process::Tty>,
	pub url: tangram_uri::Uri,
}

#[derive(Clone, Debug)]
pub struct Arg {
	pub backend: Backend,

	#[cfg(target_os = "linux")]
	pub cgroup_cleanup_interval: Duration,

	#[cfg(target_os = "linux")]
	pub cgroup_cleanup_timeout: Duration,

	pub connect_timeout: Duration,

	pub cpu: Option<tg::sandbox::Cpu>,

	#[cfg(target_os = "linux")]
	pub cpu_affinity: bool,

	#[cfg(target_os = "linux")]
	pub cpu_parent: Option<cpu::Lease>,

	#[cfg(target_os = "linux")]
	pub cpu_pool: Option<cpu::Pool>,

	pub dns: Vec<Ipv4Addr>,

	#[cfg(target_os = "linux")]
	pub firewall: Firewall,

	#[cfg(target_os = "linux")]
	pub fuse_fd: Option<Arc<std::os::fd::OwnedFd>>,

	pub hostname: Option<String>,

	pub http2: tangram_http::http2::Config,

	pub identity: PathBuf,

	pub index: u64,

	#[cfg(target_os = "linux")]
	pub ip_pool: crate::network::ip::Pool,

	pub memory: Option<tg::sandbox::Resource<u64>>,

	pub memory_sampling_interval: Duration,

	pub mounts: Vec<tg::sandbox::Mount>,

	pub network: Option<Network>,

	#[cfg(target_os = "linux")]
	pub network_startup_timeout: Duration,

	pub nice: u8,

	pub path: PathBuf,

	pub request_timeout: Duration,

	pub resources: Resources,

	pub rootfs_path: PathBuf,

	pub shutdown_timeout: Duration,

	pub storage: Option<tg::sandbox::Resource<u64>>,

	#[cfg(target_os = "linux")]
	pub storage_pool: Option<budget::Pool>,

	pub store_path: PathBuf,

	pub swap: Option<tg::sandbox::Resource<u64>>,

	#[cfg(target_os = "linux")]
	pub swap_pool: Option<budget::Pool>,

	pub tangram_path: PathBuf,

	pub tangram_socket_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub usage: tg::sandbox::Usage,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	derive_more::Display,
	derive_more::FromStr,
	serde::Deserialize,
	serde::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum Firewall {
	Iptables,

	#[default]
	Nft,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Command {
	pub args: Vec<String>,
	pub cwd: PathBuf,
	pub env: BTreeMap<String, String>,
	pub executable: PathBuf,
	pub stderr: Stdio,
	pub stdin: Stdio,
	pub stdout: Stdio,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Resources {
	pub capabilities: Option<Vec<Capability>>,
	pub cgroup_readonly: bool,
	pub filesystem_project_id: Option<u32>,
	pub gid_map: Option<IdMap>,
	pub max_duration: Option<Duration>,
	pub max_filesystem_inodes: Option<u64>,
	pub max_filesystem_size: Option<u64>,
	pub max_open_files: Option<u64>,
	pub max_pids: Option<u64>,
	pub memory_swap: Option<u64>,
	pub seccomp: Option<SeccompPolicy>,
	pub uid_map: Option<IdMap>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IdMap {
	pub count: u32,
	pub helper: PathBuf,
	pub host: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SeccompPolicy {
	Default,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
	AuditWrite,
	Chown,
	DacOverride,
	DacReadSearch,
	Fowner,
	Fsetid,
	Kill,
	Mknod,
	NetBindService,
	NetRaw,
	Setfcap,
	Setgid,
	Setpcap,
	Setuid,
	SysChroot,
}

impl Capability {
	#[must_use]
	pub fn number(self) -> u32 {
		match self {
			Self::Chown => 0,
			Self::DacOverride => 1,
			Self::DacReadSearch => 2,
			Self::Fowner => 3,
			Self::Fsetid => 4,
			Self::Kill => 5,
			Self::Setgid => 6,
			Self::Setuid => 7,
			Self::Setpcap => 8,
			Self::NetBindService => 10,
			Self::NetRaw => 13,
			Self::SysChroot => 18,
			Self::Mknod => 27,
			Self::AuditWrite => 29,
			Self::Setfcap => 31,
		}
	}
}

/// The capabilities a root command keeps. Inside a user namespace the kernel scopes them to the namespace's own ids, so the full set is safe. Without one they apply to the host's files and devices, so only the ownership and identity capabilities most software expects of root are kept.
#[must_use]
pub fn default_capabilities(user_namespace: bool) -> &'static [Capability] {
	if user_namespace {
		&[
			Capability::AuditWrite,
			Capability::Chown,
			Capability::DacOverride,
			Capability::Fowner,
			Capability::Fsetid,
			Capability::Kill,
			Capability::Mknod,
			Capability::NetBindService,
			Capability::NetRaw,
			Capability::Setfcap,
			Capability::Setgid,
			Capability::Setpcap,
			Capability::Setuid,
			Capability::SysChroot,
		]
	} else {
		&[
			Capability::Chown,
			Capability::DacOverride,
			Capability::Fowner,
			Capability::Fsetid,
			Capability::Kill,
			Capability::Setgid,
			Capability::Setuid,
		]
	}
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CloudHypervisor {
	pub cloud_hypervisor_path: Option<PathBuf>,
	pub dax: Option<u64>,
	pub image_path: PathBuf,
	pub kernel_path: PathBuf,
	pub max_cpu: u64,
	pub max_memory: u64,
	pub network_interface_timeout: Duration,
	pub ready_timeout: Duration,
	pub serial_connect_interval: Duration,
	pub serial_connect_timeout: Duration,
	pub snapshot: Option<PathBuf>,
	pub snapshot_cpu: u64,
	pub snapshot_memory: u64,
}

#[derive(
	Clone,
	Debug,
	Default,
	derive_more::IsVariant,
	Eq,
	PartialEq,
	serde::Serialize,
	serde::Deserialize,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Network {
	Bridge(Bridge),

	#[default]
	Default,

	Host,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Bridge {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub ports: Vec<tg::sandbox::Port>,
}

impl Network {
	#[must_use]
	pub fn ports(&self) -> &[tg::sandbox::Port] {
		match self {
			Self::Bridge(bridge) => &bridge.ports,
			Self::Default | Self::Host => &[],
		}
	}
}

#[derive(
	Clone,
	Copy,
	Debug,
	derive_more::Display,
	derive_more::FromStr,
	serde::Serialize,
	serde::Deserialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum Stdio {
	Null,
	Pipe,
	Tty,
}

#[cfg(target_os = "linux")]
pub fn filesystem_project_usage(path: &Path, project: u32) -> tg::Result<u64> {
	let directory = std::fs::File::open(path)
		.map_err(|error| tg::error!(!error, "failed to open the filesystem project meter"))?;
	filesystem::usage(&directory, project)
}

impl Sandbox {
	#[tracing::instrument(name = "sandbox.create_physical", level = "debug", skip_all, fields(sandbox_index = arg.index), err(level = "debug"))]
	pub async fn new(arg: Arg) -> tg::Result<Self> {
		// Validate the sandbox resources and mount targets.
		validate_resources(&arg.backend, arg.cpu, arg.memory)?;
		for resource in [arg.memory, arg.storage, arg.swap].into_iter().flatten() {
			resource.validate()?;
		}
		if !matches!(
			arg.backend,
			Backend::Builtin | Backend::Bubblewrap { .. } | Backend::Gvisor { .. }
		) && (arg.storage.is_some() || arg.swap.is_some())
		{
			return Err(tg::error!(
				"storage and swap bounds require container isolation"
			));
		}
		validate_options(&arg)?;
		#[cfg(target_os = "linux")]
		if arg.cpu.is_some_and(|cpu| cpu.min.millis() > 0) && arg.cpu_pool.is_none() {
			return Err(tg::error!(
				"CPU reservations require an exclusive sandbox CPU pool"
			));
		}

		// Validate the mounts.
		let mut targets = BTreeSet::new();
		for mount in &arg.mounts {
			if !mount.source.is_absolute() {
				return Err(tg::error!(
					source = %mount.source.display(),
					"sandbox mount sources must be absolute paths"
				));
			}
			if !mount.target.is_absolute() {
				return Err(tg::error!(
					target = %mount.target.display(),
					"sandbox mount targets must be absolute paths"
				));
			}
			if mount.target == Path::new("/") {
				return Err(tg::error!(
					target = %mount.target.display(),
					"mounting to / is not supported"
				));
			}
			if mount
				.target
				.components()
				.any(|component| component == std::path::Component::ParentDir)
			{
				return Err(tg::error!(
					target = %mount.target.display(),
					"sandbox mount targets may not contain parent directory components"
				));
			}
			if ["/dev", "/proc", "/sys"]
				.iter()
				.any(|path| mount.target.starts_with(path))
			{
				return Err(tg::error!(
					target = %mount.target.display(),
					"sandbox mounts at or beneath /dev, /proc, and /sys are not supported"
				));
			}
			if !targets.insert(mount.target.clone()) {
				return Err(tg::error!(
					target = %mount.target.display(),
					"duplicate mount targets are not supported"
				));
			}
		}

		// Get the library paths.
		let library_paths = {
			#[cfg(target_os = "macos")]
			{
				let path = arg.rootfs_path.join("lib");
				path.exists().then_some(path).into_iter().collect()
			}
			#[cfg(target_os = "linux")]
			{
				Vec::new()
			}
		};

		let (listener, url) = match arg.backend {
			Backend::Builtin
			| Backend::Bubblewrap { .. }
			| Backend::Gvisor { .. }
			| Backend::Seatbelt => {
				let uri = Uri::builder()
					.scheme("http+stdio")
					.path("")
					.build()
					.map_err(|error| tg::error!(source = error, "failed to build the URL"))?;
				(None, uri)
			},
			Backend::CloudHypervisor(_) => {
				let (listener, uri) = Self::listen(&arg.backend, &arg.path).await?;
				(Some(listener), uri)
			},
		};

		// Create the sandbox server configuration.
		let serve_arg = self::serve::Arg {
			http2: arg.http2,
			library_paths,
			listen: false,
			output_path: Self::guest_output_path_from_root(&arg.path),
			tangram_path: Self::guest_tangram_path_from_host_tangram_path(&arg.tangram_path),
			url,
		};
		let deadline = tokio::time::Instant::now() + arg.connect_timeout;
		#[cfg(target_os = "linux")]
		let (mut process, network, filesystem, cgroup, storage) = match &arg.backend {
			Backend::Builtin | Backend::Bubblewrap { .. } | Backend::Gvisor { .. } => {
				let mut network = crate::network::linux::create(&arg)?;
				let cgroup = self::cgroup::create(&arg)?;
				let output = tokio::time::timeout_at(
					deadline,
					self::run::spawn(&arg, &serve_arg, network.as_mut(), &cgroup),
				)
				.await
				.map_err(|error| tg::error!(!error, "timed out preparing the sandbox"))??;
				(
					output.process,
					network,
					output.filesystem,
					Some(cgroup),
					output.storage,
				)
			},
			Backend::Seatbelt => {
				return Err(tg::error!("seatbelt isolation is not supported on linux"));
			},
			Backend::CloudHypervisor(_) => {
				let ports = arg.network.as_ref().map(Network::ports).unwrap_or_default();
				let network = crate::cloud_hypervisor::network::create(
					arg.index,
					&arg.identity,
					arg.firewall,
					arg.network.as_ref(),
					&arg.ip_pool,
					ports,
				)?;
				let cgroup = self::cgroup::create(&arg)?;
				let (mut command, config) =
					self::cloud_hypervisor::run::command(&arg, &serve_arg, network.as_ref())?;
				let process = self::run::spawn_command(&mut command, &cgroup.handle()?)?;
				tokio::time::timeout_at(deadline, config.finish())
					.await
					.map_err(|error| tg::error!(!error, "timed out configuring the sandbox"))??;
				(process, network, None, Some(cgroup), None)
			},
		};

		// Release the temporary parent lease and the inherited FUSE socket.
		#[cfg(target_os = "linux")]
		let arg = Arg {
			cpu_parent: None,
			fuse_fd: None,
			..arg
		};

		// Connect to the sandbox server.
		#[cfg(target_os = "macos")]
		let (mut process, cleanup) = match &arg.backend {
			Backend::Builtin | Backend::Bubblewrap { .. } | Backend::Gvisor { .. } => {
				return Err(tg::error!("container isolation is not supported on macos"));
			},
			Backend::Seatbelt => {
				tokio::time::timeout_at(deadline, self::seatbelt::spawn(&arg, &serve_arg))
					.await
					.map_err(|error| tg::error!(!error, "timed out preparing the sandbox"))??
			},
			Backend::CloudHypervisor(_) => {
				return Err(tg::error!("vm isolation is not supported on macos"));
			},
		};

		let pid = process
			.id()
			.ok_or_else(|| tg::error!("the sandbox process has exited"))?;
		let connect = match listener.as_ref() {
			None => {
				let stdin = process
					.stdin
					.take()
					.ok_or_else(|| tg::error!("failed to take the sandbox stdin"))?;
				let stdout = process
					.stdout
					.take()
					.ok_or_else(|| tg::error!("failed to take the sandbox stdout"))?;
				let stream = tokio::io::join(stdout, stdin);
				Client::with_stream(stream, arg.http2, arg.request_timeout).left_future()
			},
			Some(listener) => {
				Client::with_listener(listener, arg.http2, arg.request_timeout).right_future()
			},
		};

		let client = match tokio::time::timeout_at(deadline, async {
			tokio::select! {
				result = connect => result,
				result = process.wait() => {
					let status = result
						.map_err(|error| tg::error!(!error, "failed to wait for the sandbox process"))?;
					Err(tg::error!(status = %status, "the sandbox process exited before connecting"))
				}
			}
		})
		.await
		{
			Ok(Ok(client)) => client,
			Ok(Err(error)) => {
				process.start_kill().ok();
				process.wait().await.ok();
				return Err(tg::error!(!error, "failed to start the sandbox"));
			},
			Err(error) => {
				process.start_kill().ok();
				process.wait().await.ok();
				return Err(tg::error!(
					!error,
					"timed out waiting for the sandbox to connect"
				));
			},
		};

		// Create the sandbox state.
		let sandbox = Self(Arc::new(State {
			#[cfg(target_os = "linux")]
			_storage: storage,

			#[cfg(target_os = "linux")]
			accounting: tokio::sync::Mutex::new(None),

			arg,

			#[cfg(target_os = "linux")]
			cgroup: tokio::sync::Mutex::new(cgroup),

			#[cfg(target_os = "macos")]
			cleanup,

			client,

			destroy: tokio::sync::Mutex::new(()),

			#[cfg(target_os = "linux")]
			filesystem,

			#[cfg(target_os = "linux")]
			network,

			next_process_index: AtomicU64::new(1),

			pid,

			process: tokio::sync::Mutex::new(process),

			started: std::sync::atomic::AtomicBool::new(false),
		}));

		Ok(sandbox)
	}

	#[cfg(target_os = "linux")]
	pub async fn cpu_lease(&self) -> Option<cpu::Lease> {
		self.0
			.cgroup
			.lock()
			.await
			.as_ref()
			.and_then(cgroup::Cgroup::cpu_lease)
	}

	/// Activate the assigned sandbox before spawning workloads.
	pub async fn start(&self) -> tg::Result<()> {
		// Serialize activation with sandbox destruction.
		let _destroy = self.0.destroy.lock().await;
		if self.0.started.load(Ordering::Relaxed) {
			return Ok(());
		}

		#[cfg(target_os = "linux")]
		{
			// Select the usage source.
			let source = {
				let cgroup = self.0.cgroup.lock().await;
				let cgroup = cgroup
					.as_ref()
					.ok_or_else(|| tg::error!("the sandbox accounting cgroup is missing"))?;
				let (path, project) = if self.0.arg.backend.uses_linux_namespaces() {
					let container = &self.0.arg.resources;
					let project = container.filesystem_project_id;
					let path = if project.is_some() {
						Self::host_filesystem_path_from_root(&self.0.arg.path)
					} else {
						self.0.arg.path.clone()
					};
					(path, project)
				} else {
					(self.0.arg.path.clone(), None)
				};
				usage::Source::new(cgroup.handle()?, &path, project)?
			};

			// Start charging for the assigned sandbox.
			let resources = tg::runner::Capacity {
				cpu: self
					.0
					.arg
					.cpu
					.unwrap_or(tg::sandbox::Amount::from_millis(u64::from(u32::MAX)).into()),
				memory: self.0.arg.memory.unwrap_or(u64::MAX.into()),
				storage: self.0.arg.storage.unwrap_or(u64::MAX.into()),
				swap: self.0.arg.swap.unwrap_or(u64::MAX.into()),
			};
			*self.0.accounting.lock().await = Some(usage::Accounting::new(
				source,
				resources,
				self.0.arg.memory_sampling_interval,
			)?);
		}
		self.0.started.store(true, Ordering::Relaxed);

		// Schedule the duration limit.
		if let Some(duration) = self.0.arg.resources.max_duration {
			let state = Arc::downgrade(&self.0);
			tokio::spawn(async move {
				tokio::time::sleep(duration).await;
				let Some(state) = state.upgrade() else {
					return;
				};
				let sandbox = Sandbox(state);
				if let Err(error) = sandbox.destroy().await {
					tracing::warn!(
						error = %error,
						"failed to destroy a sandbox after its maximum duration"
					);
				}
			});
		}

		Ok(())
	}

	/// Return the current sandbox resource usage without stopping accounting.
	#[cfg(target_os = "linux")]
	pub async fn usage(&self) -> tg::Result<tg::sandbox::Usage> {
		let mut accounting = self.0.accounting.lock().await;
		let accounting = accounting
			.as_mut()
			.ok_or_else(|| tg::error!("the sandbox accounting has not started"))?;
		let usage = accounting.usage().await?;
		Ok(usage)
	}

	#[cfg(target_os = "linux")]
	async fn finish_accounting(&self) -> tg::Result<Option<tg::sandbox::Usage>> {
		let mut accounting = self.0.accounting.lock().await;
		let Some(accounting) = accounting.as_mut() else {
			return Ok(None);
		};
		let usage = accounting.finish().await?;
		Ok(Some(usage))
	}

	#[cfg(target_os = "macos")]
	pub async fn usage(&self) -> tg::Result<tg::sandbox::Usage> {
		Ok(tg::sandbox::Usage {
			cpu: tg::sandbox::ResourceUsage::default(),
			memory: tg::sandbox::ResourceUsage::default(),
			storage: tg::sandbox::ResourceUsage::default(),
			swap: tg::sandbox::ResourceUsage::default(),
		})
	}

	#[cfg(target_os = "macos")]
	async fn finish_accounting(&self) -> tg::Result<Option<tg::sandbox::Usage>> {
		Ok(None)
	}

	#[must_use]
	pub fn index(&self) -> u64 {
		self.0.arg.index
	}

	/// Return the host process ID of the sandbox launcher.
	#[must_use]
	pub fn pid(&self) -> u32 {
		self.0.pid
	}

	async fn listen(
		backend: &Backend,
		root_path: &Path,
	) -> tg::Result<(crate::server::Listener, Uri)> {
		#[cfg(target_os = "linux")]
		{
			match backend {
				Backend::Builtin | Backend::Bubblewrap { .. } | Backend::Gvisor { .. } => {
					Self::listen_unix(root_path).await
				},
				Backend::Seatbelt => {
					Err(tg::error!("seatbelt isolation is not supported on linux"))
				},
				Backend::CloudHypervisor(_) => Self::listen_cloud_hypervisor(root_path).await,
			}
		}

		#[cfg(not(target_os = "linux"))]
		{
			match backend {
				Backend::Builtin | Backend::Bubblewrap { .. } | Backend::Gvisor { .. } => {
					Err(tg::error!("container isolation is not supported on macos"))
				},
				Backend::Seatbelt => Self::listen_unix(root_path).await,
				Backend::CloudHypervisor(_) => {
					Err(tg::error!("vm isolation is not supported on macos"))
				},
			}
		}
	}

	async fn listen_unix(root_path: &Path) -> tg::Result<(crate::server::Listener, Uri)> {
		// Prepare the host socket directory.
		let host_path = Self::host_listen_path_from_root(root_path);
		tokio::fs::create_dir_all(host_path.parent().unwrap())
			.await
			.map_err(|error| tg::error!(!error, "failed to create the host path"))?;

		// Select the socket path for the platform.
		let host_path_string = host_path
			.to_str()
			.ok_or_else(|| tg::error!("invalid path"))?;
		let max_socket_path_len = if cfg!(target_os = "macos") {
			100
		} else {
			usize::MAX
		};

		if host_path_string.len() <= max_socket_path_len {
			std::fs::remove_file(&host_path).ok();
			let host_url = Uri::builder()
				.scheme("http+unix")
				.authority(host_path_string)
				.path("")
				.build()
				.map_err(|error| tg::error!(source = error, "failed to build the URL"))?;
			let listener = crate::server::Server::listen(&host_url).await?;
			let url = {
				#[cfg(target_os = "linux")]
				{
					let guest_path = Self::guest_listen_path_from_root(root_path);
					let guest_path = guest_path
						.to_str()
						.ok_or_else(|| tg::error!("invalid path"))?;
					Uri::builder()
						.scheme("http+unix")
						.authority(guest_path)
						.path("")
						.build()
						.map_err(|error| tg::error!(source = error, "failed to build the URL"))?
				}
				#[cfg(not(target_os = "linux"))]
				{
					Uri::builder()
						.scheme("http+unix")
						.authority(host_path_string)
						.path("")
						.build()
						.map_err(|error| tg::error!(!error, "failed to build the URL"))?
				}
			};
			Ok((listener, url))
		} else {
			let host_url = "http://localhost:0"
				.parse()
				.map_err(|error| tg::error!(source = error, "failed to parse the URL"))?;
			let listener = crate::server::Server::listen(&host_url).await?;
			let crate::server::Listener::Tcp(tcp) = &listener else {
				unreachable!();
			};
			let port = tcp
				.local_addr()
				.map_err(|error| tg::error!(!error, "failed to get the local address"))?
				.port();
			let url = format!("http://localhost:{port}")
				.parse()
				.map_err(|error| tg::error!(source = error, "failed to parse the URL"))?;
			Ok((listener, url))
		}
	}

	#[cfg(feature = "vsock")]
	#[allow(dead_code)]
	async fn listen_vsock() -> tg::Result<(crate::server::Listener, Uri)> {
		#[cfg(not(target_os = "linux"))]
		{
			Err(tg::error!("vsock requires linux"))
		}
		#[cfg(target_os = "linux")]
		{
			let port = 6748;
			let host_url = format!(
				"http+vsock://{}:{port}",
				self::cloud_hypervisor::VMADDR_CID_ANY
			)
			.parse()
			.map_err(|_| tg::error!("failed to parse host url"))?;
			let guest_url = format!(
				"http+vsock://{}:{port}",
				self::cloud_hypervisor::VMADDR_CID_ANY
			)
			.parse()
			.map_err(|_| tg::error!("failed to parse guest url"))?;
			let listener = crate::server::Server::listen(&host_url).await?;
			Ok((listener, guest_url))
		}
	}

	#[cfg(target_os = "linux")]
	async fn listen_cloud_hypervisor(
		root_path: &Path,
	) -> tg::Result<(crate::server::Listener, Uri)> {
		// Create the host socket directory.
		let port = 6748;
		let socket = format!(
			"{}_{port}",
			cloud_hypervisor::run::CLOUD_HYPERVISOR_VSOCK_SOCKET_NAME
		);
		let path = root_path.join("vm").join(socket);
		tokio::fs::create_dir_all(path.parent().unwrap())
			.await
			.map_err(|error| tg::error!(!error, "failed to create the vm directory"))?;

		// Create the host and guest URLs.
		let path = path.to_str().ok_or_else(|| tg::error!("invalid path"))?;
		let host_url = Uri::builder()
			.scheme("http+unix")
			.authority(path)
			.path("")
			.build()
			.map_err(|error| tg::error!(source = error, "failed to build the URL"))?;
		let guest_url = format!(
			"http+vsock://{}:{port}",
			self::cloud_hypervisor::VMADDR_CID_HOST
		)
		.parse()
		.map_err(|error| tg::error!(source = error, "failed to parse the URL"))?;

		// Create the listener.
		let listener = crate::server::Server::listen(&host_url).await?;

		Ok((listener, guest_url))
	}

	#[tracing::instrument(name = "sandbox.destroy_physical", level = "debug", skip_all, fields(sandbox_index = self.index()), err(level = "debug"))]
	pub async fn destroy(&self) -> tg::Result<Output> {
		let _destroy = self.0.destroy.lock().await;
		self.destroy_inner().await?;
		let usage = self
			.finish_accounting()
			.await?
			.unwrap_or(tg::sandbox::Usage {
				cpu: tg::sandbox::ResourceUsage::default(),
				memory: tg::sandbox::ResourceUsage::default(),
				storage: tg::sandbox::ResourceUsage::default(),
				swap: tg::sandbox::ResourceUsage::default(),
			});
		let output = Output { usage };
		Ok(output)
	}

	async fn destroy_inner(&self) -> tg::Result<()> {
		#[cfg(target_os = "macos")]
		self.0.cleanup.stop(self.0.arg.shutdown_timeout).await?;
		let mut process = self.0.process.lock().await;
		let status = process
			.try_wait()
			.map_err(|error| tg::error!(!error, "failed to query the sandbox process"))?;
		if status.is_some() {
			drop(process);
			self.cleanup_cgroup().await?;
			return Ok(());
		}

		// Gracefully shut down the sandbox process.
		let result = tokio::time::timeout(self.0.arg.shutdown_timeout, async {
			self.0.client.shutdown().await?;
			process
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "failed to wait for the sandbox process"))?;
			Ok::<_, tg::Error>(())
		})
		.await;
		if matches!(result, Ok(Ok(()))) {
			drop(process);
			self.cleanup_cgroup().await?;
			return Ok(());
		}

		// Force the sandbox process to exit.
		if process
			.try_wait()
			.map_err(|error| tg::error!(!error, "failed to query the sandbox process"))?
			.is_none()
		{
			process
				.start_kill()
				.map_err(|error| tg::error!(!error, "failed to kill the sandbox process"))?;
		}
		process
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "failed to wait for the sandbox process"))?;
		drop(process);
		self.cleanup_cgroup().await?;

		Ok(())
	}

	#[cfg(target_os = "linux")]
	async fn cleanup_cgroup(&self) -> tg::Result<()> {
		let cgroup = self.0.cgroup.lock().await.take();
		let Some(cgroup) = cgroup else {
			return Ok(());
		};
		let cgroup = tokio::task::spawn_blocking(move || {
			cgroup.stop()?;
			Ok::<_, tg::Error>(cgroup)
		})
		.await
		.map_err(|error| tg::error!(!error, "the cgroup stop task panicked"))??;

		if let Some(accounting) = self.0.accounting.lock().await.as_mut() {
			accounting.usage().await?;
		}
		if self.0.arg.storage.is_some() {
			let path = Self::host_filesystem_path_from_root(&self.0.arg.path);
			tangram_util::fs::remove(&path)
				.await
				.map_err(|error| tg::error!(!error, "failed to remove the sandbox storage"))?;
		}

		let usage = self.finish_accounting().await;
		tokio::task::spawn_blocking(move || cgroup.cleanup())
			.await
			.map_err(|error| tg::error!(!error, "the cgroup cleanup task panicked"))??;
		usage?;

		Ok(())
	}

	#[cfg(not(target_os = "linux"))]
	async fn cleanup_cgroup(&self) -> tg::Result<()> {
		Ok(())
	}

	#[must_use]
	pub fn create_process(&self) -> Process {
		let index = self.0.next_process_index.fetch_add(1, Ordering::Relaxed);
		assert_ne!(index, u64::MAX, "exhausted the process indexes");

		Process { index }
	}

	#[tracing::instrument(name = "sandbox.spawn_process", level = "debug", skip_all, fields(process_index = process.index, sandbox_index = self.index()), err(level = "debug"))]
	pub async fn spawn(&self, process: &Process, arg: SpawnArg) -> tg::Result<()> {
		let spawn_arg = crate::client::spawn::Arg {
			command: arg.command,
			index: process.index,
			token: arg.token.clone(),
			tty: arg.tty,
			url: arg.url,
		};
		if let Err(error) = self.0.client.spawn(spawn_arg).await {
			let status = self
				.0
				.process
				.lock()
				.await
				.try_wait()
				.map_err(|source| tg::error!(!source, "failed to query the sandbox process"))?;
			let Some(status) = status else {
				return Err(error);
			};
			return Err(tg::error!(
				!error,
				status = ?status,
				"failed to spawn a process in the sandbox"
			));
		}
		Ok(())
	}

	pub async fn set_tty_size(
		&self,
		process: &Process,
		size: tg::process::tty::Size,
	) -> tg::Result<()> {
		let arg = crate::client::tty::SizeArg { size };
		self.0.client.set_tty_size(process.index, arg).await?;
		Ok(())
	}

	pub async fn read_stdio(
		&self,
		process: &Process,
		streams: Vec<tg::process::stdio::Stream>,
	) -> tg::Result<impl Stream<Item = tg::Result<crate::stdio::read::Event>> + Send + 'static> {
		let arg = crate::client::stdio::Arg { streams };
		self.0.client.read_stdio(process.index, arg).await
	}

	pub async fn write_stdio(
		&self,
		process: &Process,
		streams: Vec<tg::process::stdio::Stream>,
		input: impl Stream<Item = tg::Result<crate::stdio::read::Event>> + Send + 'static,
	) -> tg::Result<impl Stream<Item = tg::Result<crate::stdio::write::Event>> + Send + 'static> {
		let arg = crate::client::stdio::Arg { streams };
		self.0.client.write_stdio(process.index, arg, input).await
	}

	#[tracing::instrument(name = "sandbox.kill_process", level = "debug", skip_all, fields(process_index = process.index, sandbox_index = self.index(), signal = %signal), err(level = "debug"))]
	pub async fn kill(&self, process: &Process, signal: tg::process::Signal) -> tg::Result<()> {
		let arg = crate::client::kill::Arg { signal };
		self.0.client.kill(process.index, arg).await?;
		Ok(())
	}

	pub async fn wait(
		&self,
		process: &Process,
	) -> tg::Result<impl Future<Output = tg::Result<u8>> + Send + 'static> {
		let future = match self.0.client.wait(process.index).await {
			Ok(future) => future,
			Err(error) => return Err(self.termination_error(error).await),
		};
		let sandbox = self.clone();
		Ok(async move {
			let result = future.await.and_then(|outcome| {
				outcome.ok_or_else(|| tg::error!("failed to wait for the process"))
			});

			match result {
				Ok(outcome) => Ok(outcome.status),
				Err(error) => Err(sandbox.termination_error(error).await),
			}
		})
	}

	async fn termination_error(&self, error: tg::Error) -> tg::Error {
		#[cfg(target_os = "linux")]
		if let Some(cgroup) = self.0.cgroup.lock().await.as_ref() {
			match cgroup.oom_kill() {
				Err(source) => {
					tracing::warn!(%source, "failed to read the sandbox OOM kill count");
				},
				Ok(0) => (),
				Ok(_) => {
					let memory_limit = self
						.0
						.arg
						.memory
						.map_or_else(|| "unlimited".to_owned(), |memory| memory.max.to_string());
					return tg::error!(
						!error,
						%memory_limit,
						"the sandbox was killed by the out-of-memory killer"
					);
				},
			}
		}

		error
	}

	pub async fn try_get_process(
		&self,
		index: u64,
	) -> tg::Result<Option<crate::client::get::Output>> {
		self.0.client.try_get_process(index).await
	}
}

impl Process {
	#[must_use]
	pub fn index(&self) -> u64 {
		self.index
	}
}

fn validate_resources(
	backend: &Backend,
	cpu: Option<tg::sandbox::Cpu>,
	memory: Option<tg::sandbox::Resource<u64>>,
) -> tg::Result<()> {
	if let Some(cpu) = cpu {
		cpu.total()?;
	}
	if matches!(backend, Backend::CloudHypervisor(_)) {
		validate_vm_resources(cpu, memory)?;
	}
	if memory.is_some_and(|memory| memory.max == 0) {
		return Err(tg::error!("sandbox memory must be greater than zero"));
	}
	if matches!(backend, Backend::Seatbelt) && (cpu.is_some() || memory.is_some()) {
		return Err(tg::error!(
			"sandbox cpu and memory are not supported with seatbelt isolation"
		));
	}
	Ok(())
}

fn validate_vm_resources(
	cpu: Option<tg::sandbox::Cpu>,
	memory: Option<tg::sandbox::Resource<u64>>,
) -> tg::Result<()> {
	if cpu.is_some_and(|cpu| cpu.min.millis() > 0 || cpu.max.millis() % 1000 != 0)
		|| memory.is_some_and(|memory| memory.min > 0)
	{
		return Err(tg::error!(
			"resource minima and fractional CPUs require container isolation"
		));
	}
	Ok(())
}

fn validate_options(arg: &Arg) -> tg::Result<()> {
	if arg.resources.max_open_files == Some(0) {
		return Err(tg::error!(
			"the maximum number of container sandbox open files must be greater than zero"
		));
	}
	if arg.resources.max_pids == Some(0) {
		return Err(tg::error!(
			"the maximum number of container sandbox pids must be greater than zero"
		));
	}
	if matches!(arg.backend, Backend::Seatbelt) && arg.hostname.is_some() {
		return Err(tg::error!(
			"setting a hostname is not supported with seatbelt isolation"
		));
	}
	Ok(())
}

impl std::ops::Deref for Sandbox {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[cfg(test)]
mod resource_tests {
	use super::*;

	#[test]
	fn vm_resources_reject_promises_the_guest_cannot_enforce() {
		assert!(validate_vm_resources(Some(1.into()), Some(1_073_741_824.into())).is_ok());
		let fractional = tg::sandbox::Amount::from_millis(250).into();
		assert!(validate_vm_resources(Some(fractional), None).is_err());
		let reserved = tg::sandbox::Cpu::new(
			tg::sandbox::Amount::from_millis(250),
			tg::sandbox::Amount::from_millis(1000),
		);
		assert!(validate_vm_resources(Some(reserved), None).is_err());
		assert!(validate_vm_resources(None, Some(tg::sandbox::Resource::new(256, 1024))).is_err());
	}
}
