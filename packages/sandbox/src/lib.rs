use {
	crate::client::Client,
	futures::Stream,
	std::{
		collections::{BTreeMap, BTreeSet}, path::{Path, PathBuf}, sync::Arc, time::Duration
	},
	tangram_client::prelude::*,
	tangram_uri::Uri,
};

mod client;
#[cfg(target_os = "linux")]
mod network;
mod pty;
mod server;
mod util;

#[cfg(target_os = "linux")]
pub mod container;
pub mod root;
#[cfg(target_os = "macos")]
pub mod seatbelt;
pub mod serve;
#[cfg(target_os = "linux")]
pub mod vm;

#[derive(Clone)]
pub struct Sandbox(Arc<State>);

pub struct State {
	artifacts_path: PathBuf,
	client: Client,
	isolation: Isolation,
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	mounts: Vec<tg::sandbox::Mount>,
	path: PathBuf,
	#[expect(dead_code)]
	process: tokio::process::Child,
	tangram_path: PathBuf,
}

pub struct Process {
	id: tg::process::Id,
}

#[derive(Clone, Debug)]
pub struct Arg {
	pub artifacts_path: PathBuf,
	pub cpu: Option<u64>,
	pub hostname: Option<String>,
	pub isolation: Isolation,
	pub memory: Option<u64>,
	pub mounts: Vec<tg::sandbox::Mount>,
	pub network: bool,
	pub path: PathBuf,
	pub rootfs_path: PathBuf,
	pub tangram_path: PathBuf,
	pub user: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Command {
	pub args: Vec<String>,
	pub cwd: PathBuf,
	pub env: BTreeMap<String, String>,
	pub executable: PathBuf,
	pub stderr: Stdio,
	pub stdin: Stdio,
	pub stdout: Stdio,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Isolation {
	Container(ContainerIsolation),
	Seatbelt(SeatbeltIsolation),
	Vm(VmIsolation),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ContainerIsolation {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SeatbeltIsolation {}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VmIsolation {
	pub kernel: PathBuf,
	pub host_subnet: String,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum Stdio {
	Null,
	Pipe,
	Tty,
}

impl Sandbox {
	pub async fn new(arg: Arg) -> tg::Result<Self> {
		validate_resources(&arg.isolation, arg.cpu, arg.memory)?;

		// Validate the mounts.
		let mut targets = BTreeSet::new();
		for mount in &arg.mounts {
			if mount.target == Path::new("/") {
				return Err(tg::error!(
					target = %mount.target.display(),
					"mounting to / is not supported"
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

		let (listener, url) = Self::listen(&arg.isolation, &arg.path).await?;
		let serve_arg = self::serve::Arg {
			library_paths,
			listen: false,
			tangram_path: Self::guest_tangram_path_from_host_tangram_path(&arg.tangram_path),
			url,
		};

		let mut process = match arg.isolation {
			#[cfg(target_os = "linux")]
			Isolation::Container(_) => self::container::spawn(&arg, &serve_arg)?,
			#[cfg(target_os = "linux")]
			Isolation::Seatbelt(_) => {
				return Err(tg::error!("seatbelt isolation is not supported on linux"));
			},
			#[cfg(target_os = "macos")]
			Isolation::Container(_) => {
				return Err(tg::error!(
					"{} isolation is not supported on macos",
					arg.isolation
				));
			},
			#[cfg(target_os = "macos")]
			Isolation::Seatbelt(_) => self::seatbelt::spawn(&arg, &serve_arg)?,
			#[cfg(target_os = "linux")]
			Isolation::Vm(_) => self::vm::spawn(&arg, &serve_arg)?,
			#[cfg(target_os = "macos")]
			Isolation::Vm(_) => {
				return Err(tg::error!(
					"{} isolation is not supported on macos",
					arg.isolation
				));
			},
		};

		let client = match tokio::time::timeout(Duration::from_secs(200), async {
			tokio::select! {
				result = Client::with_listener(&listener) => result,
				result = process.wait() => {
					let status = result
						.map_err(|source| tg::error!(!source, "failed to wait for the sandbox process"))?;
					Err(tg::error!(status = %status, "the sandbox process exited before connecting"))
				},
			}
		})
		.await
		{
			Ok(Ok(client)) => client,
			Ok(Err(source)) => {
				process.start_kill().ok();
				process.wait().await.ok();
				return Err(tg::error!(!source, "failed to start the sandbox"));
			},
			Err(source) => {
				process.start_kill().ok();
				process.wait().await.ok();
				return Err(tg::error!(
					!source,
					"timed out waiting for the sandbox to connect"
				));
			},
		};

		let sandbox = Self(Arc::new(State {
			artifacts_path: arg.artifacts_path,
			client,
			isolation: arg.isolation,
			mounts: arg.mounts,
			path: arg.path,
			process,
			tangram_path: arg.tangram_path,
		}));

		Ok(sandbox)
	}

	async fn listen(
		isolation: &Isolation,
		root_path: &Path,
	) -> tg::Result<(crate::server::Listener, Uri)> {
		#[cfg(target_os = "linux")]
		{
			match isolation {
				Isolation::Container(_) => Self::listen_unix(root_path).await,
				Isolation::Seatbelt(_) => {
					Err(tg::error!("seatbelt isolation is not supported on linux"))
				},
				Isolation::Vm(_) => Self::listen_vsock(root_path).await,
			}
		}

		#[cfg(not(target_os = "linux"))]
		{
			match isolation {
				Isolation::Container(_) => Err(tg::error!(
					"{isolation} isolation is not supported on macos"
				)),
				Isolation::Seatbelt(_) => Self::listen_unix(root_path).await,
				Isolation::Vm(_) => Err(tg::error!(
					"{isolation} isolation is not supported on macos"
				)),
			}
		}
	}

	async fn listen_unix(root_path: &Path) -> tg::Result<(crate::server::Listener, Uri)> {
		let host_path = Self::host_listen_path_from_root(root_path);
		tokio::fs::create_dir_all(host_path.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the host path"))?;

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
				.map_err(|source| tg::error!(source = source, "failed to build the URL"))?;
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
						.map_err(|source| tg::error!(source = source, "failed to build the URL"))?
				}
				#[cfg(not(target_os = "linux"))]
				{
					Uri::builder()
						.scheme("http+unix")
						.authority(host_path_string)
						.path("")
						.build()
						.map_err(|source| tg::error!(source = source, "failed to build the URL"))?
				}
			};
			Ok((listener, url))
		} else {
			let host_url = "http://localhost:0"
				.parse()
				.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?;
			let listener = crate::server::Server::listen(&host_url).await?;
			let crate::server::Listener::Tcp(tcp) = &listener else {
				unreachable!();
			};
			let port = tcp
				.local_addr()
				.map_err(|source| tg::error!(!source, "failed to get the local address"))?
				.port();
			let url = format!("http://localhost:{port}")
				.parse()
				.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?;
			Ok((listener, url))
		}
	}

	#[cfg(target_os = "linux")]
	async fn listen_vsock(root_path: &Path) -> tg::Result<(crate::server::Listener, Uri)> {
		#[cfg(not(feature = "vsock"))]
		{
			Err(tg::error!("vsock is not enabled"))
		}
		#[cfg(feature = "vsock")]
		{
			let port = 6748;
			let socket = format!("{}_{port}", vm::run::CLOUD_HYPERVISOR_VSOCK_SOCKET_NAME);
			let path = root_path.join("vm").join(socket);
			tokio::fs::create_dir_all(path.parent().unwrap())
				.await
				.map_err(|source| tg::error!(!source, "failed to create the vm directory"))?;
			let path = path.to_str().ok_or_else(|| tg::error!("invalid path"))?;
			let host_url = Uri::builder()
				.scheme("http+unix")
				.authority(path)
				.path("")
				.build()
				.map_err(|source| tg::error!(source = source, "failed to build the URL"))?;
			let guest_url = format!("http+vsock://{}:{port}", self::vm::VMADDR_CID_HOST)
				.parse()
				.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?;
			let listener = crate::server::Server::listen(&host_url).await?;
			Ok((listener, guest_url))
		}
	}

	pub async fn spawn(
		&self,
		command: Command,
		id: tg::process::Id,
		tty: Option<tg::process::Tty>,
		location: Option<tg::location::Location>,
		retry: bool,
	) -> tg::Result<Process> {
		let arg = crate::client::spawn::Arg {
			command,
			id: id.clone(),
			location,
			retry,
			tty,
		};
		self.0.client.spawn(arg).await?;
		Ok(Process { id })
	}

	pub async fn set_tty_size(
		&self,
		process: &Process,
		size: tg::process::tty::Size,
	) -> tg::Result<()> {
		let arg = crate::client::tty::SizeArg { size };
		self.0.client.set_tty_size(&process.id, arg).await?;
		Ok(())
	}

	pub async fn read_stdio(
		&self,
		process: &Process,
		streams: Vec<tg::process::stdio::Stream>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static>
	{
		let arg = crate::client::stdio::Arg { streams };
		self.0.client.read_stdio(&process.id, arg).await
	}

	pub async fn write_stdio(
		&self,
		process: &Process,
		streams: Vec<tg::process::stdio::Stream>,
		input: impl Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::stdio::write::Event>> + Send + 'static>
	{
		let arg = crate::client::stdio::Arg { streams };
		self.0.client.write_stdio(&process.id, arg, input).await
	}

	pub async fn kill(&self, process: &Process, signal: tg::process::Signal) -> tg::Result<()> {
		let arg = crate::client::kill::Arg { signal };
		self.0.client.kill(&process.id, arg).await?;
		Ok(())
	}

	pub async fn wait(
		&self,
		process: &Process,
	) -> tg::Result<impl std::future::Future<Output = tg::Result<u8>> + Send + 'static> {
		let future = self.0.client.wait(&process.id).await?;
		Ok(async move {
			let output = future
				.await?
				.ok_or_else(|| tg::error!("failed to wait for the process"))?;
			Ok(output.status)
		})
	}

	pub async fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<crate::client::get::Output>> {
		self.0.client.try_get_process(id).await
	}
}

fn validate_resources(
	isolation: &Isolation,
	cpu: Option<u64>,
	memory: Option<u64>,
) -> tg::Result<()> {
	if cpu == Some(0) {
		return Err(tg::error!("sandbox cpu must be greater than zero"));
	}
	if memory == Some(0) {
		return Err(tg::error!("sandbox memory must be greater than zero"));
	}
	if matches!(isolation, Isolation::Seatbelt(_)) && (cpu.is_some() || memory.is_some()) {
		return Err(tg::error!(
			"sandbox cpu and memory are not supported with seatbelt isolation"
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

impl std::fmt::Display for Stdio {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Null => write!(f, "null"),
			Self::Pipe => write!(f, "pipe"),
			Self::Tty => write!(f, "tty"),
		}
	}
}

impl std::str::FromStr for Stdio {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"null" => Ok(Stdio::Null),
			"pipe" => Ok(Stdio::Pipe),
			"tty" => Ok(Stdio::Tty),
			s => Err(tg::error!(string = %s, "invalid stdio {s}")),
		}
	}
}
