use {
	crate::client::Client,
	std::{
		collections::{BTreeMap, BTreeSet},
		io::Read as _,
		os::fd::{AsRawFd as _, RawFd},
		path::{Path, PathBuf},
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
};

mod common;
#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;

mod client;
mod server;

#[cfg(target_os = "linux")]
const ROOTFS: include_dir::Dir<'static> = include_dir::include_dir!("$OUT_DIR/rootfs");

#[derive(Clone)]
pub struct Manager(Arc<ManagerState>);

pub struct ManagerState {
	artifacts_path: PathBuf,
	library_paths: Vec<PathBuf>,
	rootfs_path: PathBuf,
	tangram_path: PathBuf,
}

#[derive(Clone)]
pub struct Directory(PathBuf);

pub struct Sandbox(Arc<SandboxState>);

pub struct SandboxState {
	client: Client,
	_process: tokio::process::Child,
}

pub struct Process {
	id: tg::process::Id,
}

#[derive(Clone, Debug)]
pub struct ManagerArg {
	pub artifacts_path: PathBuf,
	pub rootfs_path: PathBuf,
	pub tangram_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct SpawnArg {
	pub hostname: Option<String>,
	pub mounts: Vec<tg::sandbox::Mount>,
	pub network: bool,
	pub path: PathBuf,
	pub user: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RunArg {
	pub library_paths: Vec<PathBuf>,
	pub path: PathBuf,
	pub tangram_path: PathBuf,
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

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum Stdio {
	Null,
	Pipe,
	Tty,
}

impl Manager {
	pub fn new(arg: ManagerArg) -> tg::Result<Self> {
		let library_paths = prepare_runtime_libraries(&arg)?;
		let state = ManagerState {
			artifacts_path: arg.artifacts_path,
			library_paths,
			rootfs_path: arg.rootfs_path,
			tangram_path: arg.tangram_path,
		};
		Ok(Self(Arc::new(state)))
	}

	#[must_use]
	pub fn tangram_path(&self) -> &Path {
		&self.0.tangram_path
	}

	pub async fn spawn(&self, arg: SpawnArg) -> tg::Result<Sandbox> {
		validate_mounts(&arg.mounts)?;
		Sandbox::new(self, arg).await
	}
}

fn validate_mounts(mounts: &[tg::sandbox::Mount]) -> tg::Result<()> {
	let mut targets = BTreeSet::new();
	for mount in mounts {
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
	Ok(())
}

impl Sandbox {
	async fn new(manager: &Manager, arg: SpawnArg) -> tg::Result<Self> {
		let directory = Directory::new(arg.path.clone());
		let run_arg = RunArg {
			library_paths: manager.library_paths.clone(),
			path: arg.path.clone(),
			tangram_path: manager.tangram_path.clone(),
		};

		let (mut ready_reader, ready_writer) = std::io::pipe()
			.map_err(|source| tg::error!(!source, "failed to create the sandbox ready pipe"))?;
		let flags = unsafe { libc::fcntl(ready_writer.as_raw_fd(), libc::F_GETFD) };
		if flags < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to get the ready pipe flags"
			));
		}
		let ret = unsafe {
			libc::fcntl(
				ready_writer.as_raw_fd(),
				libc::F_SETFD,
				flags & !libc::FD_CLOEXEC,
			)
		};
		if ret < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to set the ready pipe flags"
			));
		}
		let ready_fd = ready_writer.as_raw_fd();

		#[cfg(target_os = "linux")]
		let mut process = crate::linux::spawn_jailer(manager, &arg, &run_arg, ready_fd)?;

		#[cfg(target_os = "macos")]
		let mut process = crate::darwin::spawn_jailer(manager, &arg, &run_arg, ready_fd)?;

		drop(ready_writer);

		let task = tokio::task::spawn_blocking(move || {
			let mut bytes = [0u8; 3];
			ready_reader.read_exact(&mut bytes)?;
			Ok::<_, std::io::Error>(bytes)
		});
		let ready = tokio::time::timeout(Duration::from_secs(5), task)
			.await
			.map_err(|source| tg::error!(!source, "timed out waiting for the sandbox ready signal"))
			.and_then(|output| {
				output.map_err(|source| tg::error!(!source, "the sandbox ready task panicked"))
			})
			.and_then(|output| {
				output.map_err(|source| {
					tg::error!(!source, "failed to read the sandbox ready signal")
				})
			})
			.and_then(|bytes| {
				if bytes[0] != 0x00 {
					return Err(tg::error!("received an invalid ready byte {}", bytes[0]));
				}
				Ok(u16::from_be_bytes([bytes[1], bytes[2]]))
			});
		let port = match ready {
			Ok(port) => port,
			Err(source) => {
				process.start_kill().ok();
				process.wait().await.ok();
				return Err(tg::error!(!source, "failed to start the sandbox"));
			},
		};

		let client = if port == 0 {
			Client::new_unix(directory.host_listen_path())
		} else {
			Client::new_tcp(port)
		};
		client
			.connect()
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the sandbox"))?;

		Ok(Self(Arc::new(SandboxState {
			client,
			_process: process,
		})))
	}

	pub async fn spawn(
		&self,
		command: Command,
		id: tg::process::Id,
		tty: Option<tg::process::Tty>,
	) -> tg::Result<Process> {
		let arg = crate::client::spawn::Arg { command, id, tty };
		let output = self.0.client.spawn(arg).await?;
		Ok(Process { id: output.id })
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
	) -> tg::Result<
		impl futures::Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static,
	> {
		let arg = crate::client::stdio::Arg { streams };
		self.0.client.read_stdio(&process.id, arg).await
	}

	pub async fn write_stdio(
		&self,
		process: &Process,
		streams: Vec<tg::process::stdio::Stream>,
		input: futures::stream::BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<
		impl futures::Stream<Item = tg::Result<tg::process::stdio::write::Event>> + Send + 'static,
	> {
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
}

pub fn run(arg: &RunArg, ready_fd: Option<RawFd>) -> tg::Result<()> {
	use std::{io::Write as _, os::fd::FromRawFd as _};

	let directory = Directory::new(arg.path.clone());
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|source| tg::error!(!source, "failed to create the runtime"))?;

	let (listener, port): (_, u16) = {
		#[cfg(target_os = "linux")]
		{
			let path = directory.guest_listen_path();
			let listener = std::os::unix::net::UnixListener::bind(&path)
				.map_err(|source| tg::error!(!source, path = %path.display(), "failed to bind"))?;
			listener
				.set_nonblocking(true)
				.map_err(|source| tg::error!(!source, "failed to set nonblocking mode"))?;
			(tokio_util::either::Either::Left(listener), 0)
		}

		#[cfg(target_os = "macos")]
		{
			std::fs::create_dir_all(directory.host_socket_path())
				.map_err(|source| tg::error!(!source, "failed to create the socket directory"))?;
			let listen_path = directory.host_listen_path();
			const MAX_SOCKET_PATH_LEN: usize = 100;
			if listen_path
				.to_str()
				.is_none_or(|string| string.len() > MAX_SOCKET_PATH_LEN)
			{
				let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
					.map_err(|source| tg::error!(!source, "failed to bind"))?;
				listener
					.set_nonblocking(true)
					.map_err(|source| tg::error!(!source, "failed to set nonblocking mode"))?;
				let port = listener
					.local_addr()
					.map_err(|source| tg::error!(!source, "failed to get the local address"))?
					.port();
				(tokio_util::either::Either::Right(listener), port)
			} else {
				let listener = std::os::unix::net::UnixListener::bind(&listen_path).map_err(
					|source| tg::error!(!source, path = %listen_path.display(), "failed to bind"),
				)?;
				listener
					.set_nonblocking(true)
					.map_err(|source| tg::error!(!source, "failed to set nonblocking mode"))?;
				(tokio_util::either::Either::Left(listener), 0)
			}
		}
	};

	if let Some(fd) = ready_fd {
		let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
		let port_bytes = port.to_be_bytes();
		file.write_all(&[0x00, port_bytes[0], port_bytes[1]])
			.map_err(|source| tg::error!(!source, "failed to write the ready signal"))?;
	}

	runtime.block_on(async move {
		let listener = match listener {
			tokio_util::either::Either::Left(listener) => {
				let listener = tokio::net::UnixListener::from_std(listener)
					.map_err(|source| tg::error!(!source, "failed to create the unix listener"))?;
				tokio_util::either::Either::Left(listener)
			},
			tokio_util::either::Either::Right(listener) => {
				let listener = tokio::net::TcpListener::from_std(listener)
					.map_err(|source| tg::error!(!source, "failed to create the tcp listener"))?;
				tokio_util::either::Either::Right(listener)
			},
		};
		let server = crate::server::Server::new(crate::server::Arg {
			library_paths: arg.library_paths.clone(),
			tangram_path: arg.tangram_path.clone(),
		});
		server.serve(listener).await;
		Ok::<_, tg::Error>(())
	})?;
	Ok(())
}

fn prepare_runtime_libraries(arg: &ManagerArg) -> tg::Result<Vec<PathBuf>> {
	#[cfg(target_os = "linux")]
	{
		crate::linux::prepare_runtime_libraries(arg)
	}
	#[cfg(target_os = "macos")]
	{
		crate::darwin::prepare_runtime_libraries(arg)
	}
}

fn append_run_args(command: &mut tokio::process::Command, arg: &RunArg, ready_fd: RawFd) {
	command
		.arg("sandbox")
		.arg("run")
		.arg("--path")
		.arg(&arg.path)
		.arg("--ready-fd")
		.arg(ready_fd.to_string())
		.arg("--tangram-path")
		.arg(&arg.tangram_path);
	for path in &arg.library_paths {
		command.arg("--library-path").arg(path);
	}
}

fn prepare_command_for_spawn(
	command: &mut Command,
	tangram_path: &Path,
	library_paths: &[PathBuf],
) -> tg::Result<()> {
	#[cfg(target_os = "linux")]
	set_home_for_command(command);
	#[cfg(target_os = "macos")]
	set_home_for_command(command)?;
	#[cfg(target_os = "linux")]
	{
		crate::linux::prepare_command_for_spawn(command, tangram_path, library_paths)
	}
	#[cfg(target_os = "macos")]
	{
		crate::darwin::prepare_command_for_spawn(command, tangram_path, library_paths)
	}
}

#[cfg(target_os = "linux")]
fn set_home_for_command(command: &mut Command) {
	if command.env.contains_key("HOME") {
		return;
	}
	command.env.insert("HOME".to_owned(), "/root".to_owned());
}

#[cfg(target_os = "macos")]
fn set_home_for_command(command: &mut Command) -> tg::Result<()> {
	if command.env.contains_key("HOME") {
		return Ok(());
	}
	let home = std::env::var("HOME")
		.map_err(|source| tg::error!(!source, "failed to get the home directory"))?;
	command.env.insert("HOME".to_owned(), home);
	Ok(())
}

fn append_directories_to_path(command: &mut Command, directories: &[&Path]) -> tg::Result<()> {
	let mut paths = command
		.env
		.get("PATH")
		.map(|path| std::env::split_paths(path).collect::<Vec<_>>())
		.unwrap_or_default();
	paths.extend(directories.iter().map(|path| path.to_path_buf()));
	let path = std::env::join_paths(paths)
		.map_err(|source| tg::error!(!source, "failed to build `PATH`"))?;
	let path = path
		.to_str()
		.ok_or_else(|| tg::error!("failed to encode `PATH` as valid UTF-8"))?;
	command.env.insert("PATH".to_owned(), path.to_owned());
	Ok(())
}

#[cfg(target_os = "macos")]
fn command_resolves_to_path(command: &Command, target: &Path) -> bool {
	let resolved = if command.executable.is_absolute() {
		command.executable.clone()
	} else {
		let Some(path) = command.env.get("PATH") else {
			return false;
		};
		let Some(resolved) = crate::common::which(Path::new(path), &command.executable) else {
			return false;
		};
		resolved
	};
	canonicalized_paths_match(&resolved, target)
}

#[cfg(target_os = "macos")]
fn canonicalized_paths_match(lhs: &Path, rhs: &Path) -> bool {
	let Ok(lhs) = std::fs::canonicalize(lhs) else {
		return false;
	};
	let Ok(rhs) = std::fs::canonicalize(rhs) else {
		return false;
	};
	lhs == rhs
}

#[cfg(target_os = "linux")]
fn set_rootfs_permissions(
	rootfs_path: &Path,
	directory: &include_dir::Dir<'_>,
	permissions: &std::fs::Permissions,
) -> tg::Result<()> {
	for entry in directory.entries() {
		match entry {
			include_dir::DirEntry::Dir(directory) => {
				set_rootfs_permissions(rootfs_path, directory, permissions)?;
			},
			include_dir::DirEntry::File(file) => {
				let path = rootfs_path.join(file.path());
				std::fs::set_permissions(&path, permissions.clone()).map_err(|source| {
					tg::error!(
						!source,
						path = %path.display(),
						"failed to set sandbox file permissions"
					)
				})?;
			},
		}
	}
	Ok(())
}

impl Directory {
	#[must_use]
	pub fn new(path: PathBuf) -> Self {
		Self(path)
	}

	fn socket_name_for_process(id: &tg::process::Id) -> String {
		let id = id.to_string();
		let id = id.strip_prefix("pcs_").unwrap_or(&id);
		if id.len() > 16 {
			id[id.len() - 16..].to_owned()
		} else {
			id.to_owned()
		}
	}

	#[must_use]
	pub fn host_etc_path(&self) -> PathBuf {
		self.0.join("etc")
	}

	#[must_use]
	pub fn host_listen_path(&self) -> PathBuf {
		self.host_socket_path().join("ctl")
	}

	#[must_use]
	pub fn host_nsswitch_path(&self) -> PathBuf {
		self.host_etc_path().join("nsswitch.conf")
	}

	#[must_use]
	pub fn host_output_path(&self) -> PathBuf {
		self.0.join("output")
	}

	#[must_use]
	pub fn host_output_path_for_process(&self, id: &tg::process::Id) -> PathBuf {
		self.host_output_path().join(id.to_string())
	}

	#[must_use]
	pub fn host_passwd_path(&self) -> PathBuf {
		self.host_etc_path().join("passwd")
	}

	#[must_use]
	pub fn host_profile_path(&self) -> PathBuf {
		self.0.join("sandbox.sb")
	}

	#[must_use]
	pub fn host_resolv_conf_path(&self) -> PathBuf {
		self.host_etc_path().join("resolv.conf")
	}

	#[must_use]
	pub fn host_scratch_path(&self) -> PathBuf {
		self.0.join("scratch")
	}

	#[must_use]
	pub fn host_tmp_path(&self) -> PathBuf {
		self.0.join("tmp")
	}

	#[must_use]
	pub fn host_socket_path(&self) -> PathBuf {
		self.0.join("s")
	}

	#[must_use]
	pub fn host_upper_path(&self) -> PathBuf {
		self.0.join("upper")
	}

	#[must_use]
	pub fn host_work_path(&self) -> PathBuf {
		self.0.join("work")
	}

	#[must_use]
	pub fn host_socket_path_for_process(&self, id: &tg::process::Id) -> PathBuf {
		self.host_socket_path()
			.join(Self::socket_name_for_process(id))
	}

	#[must_use]
	pub fn guest_artifacts_path(&self) -> PathBuf {
		"/opt/tangram/artifacts".into()
	}

	#[must_use]
	pub fn guest_listen_path(&self) -> PathBuf {
		self.guest_socket_path().join("ctl")
	}

	#[must_use]
	pub fn guest_output_path(&self) -> PathBuf {
		"/opt/tangram/output".into()
	}

	#[must_use]
	pub fn guest_output_path_for_process(&self, id: &tg::process::Id) -> PathBuf {
		self.guest_output_path().join(id.to_string())
	}

	#[must_use]
	pub fn guest_socket_path(&self) -> PathBuf {
		"/opt/tangram/socket".into()
	}

	#[must_use]
	pub fn guest_socket_path_for_process(&self, id: &tg::process::Id) -> PathBuf {
		self.guest_socket_path()
			.join(Self::socket_name_for_process(id))
	}

	#[must_use]
	pub fn guest_tangram_path(&self) -> PathBuf {
		"/opt/tangram/bin/tangram".into()
	}

	#[must_use]
	pub fn guest_tmp_path(&self) -> PathBuf {
		"/tmp".into()
	}

	#[must_use]
	pub fn guest_libexec_tangram_path(&self) -> PathBuf {
		"/opt/tangram/libexec/tangram".into()
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

impl std::ops::Deref for Manager {
	type Target = ManagerState;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl std::ops::Deref for Sandbox {
	type Target = SandboxState;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
