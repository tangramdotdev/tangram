use {
	crate::{client::Client, server::Server},
	std::{
		collections::BTreeMap,
		io::{Read as _, Write as _},
		ops::Deref,
		os::fd::{AsRawFd as _, FromRawFd as _, RawFd},
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

#[expect(clippy::struct_field_names)]
struct ManagerState {
	artifacts_path: PathBuf,
	rootfs_path: PathBuf,
	tangram_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct Directory(PathBuf);

pub struct Sandbox(Arc<SandboxState>);

#[expect(dead_code)]
pub struct SandboxState {
	client: Client,
	arg: RunArg,
	process: tokio::process::Child,
}

#[expect(dead_code)]
pub struct Process {
	command: Command,
	id: tg::process::Id,
}

#[derive(Debug, Clone)]
pub struct ManagerArg {
	pub artifacts_path: PathBuf,
	pub rootfs_path: PathBuf,
	pub tangram_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SpawnArg {
	pub hostname: Option<String>,
	pub mounts: Vec<tg::process::Mount>,
	pub network: bool,
	pub path: PathBuf,
	pub user: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunArg {
	pub artifacts_path: PathBuf,
	pub hostname: Option<String>,
	pub mounts: Vec<tg::process::Mount>,
	pub network: bool,
	pub path: PathBuf,
	pub rootfs_path: PathBuf,
	pub tangram_path: PathBuf,
	pub user: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Command {
	pub args: Vec<String>,
	pub cwd: PathBuf,
	pub env: BTreeMap<String, String>,
	pub executable: PathBuf,
	pub stderr: Stdio,
	pub stdin: Stdio,
	pub stdout: Stdio,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum Stdio {
	Null,
	Pipe,
	Tty,
}

impl Manager {
	pub fn new(arg: ManagerArg) -> tg::Result<Self> {
		#[cfg(target_os = "linux")]
		{
			std::fs::remove_dir_all(&arg.rootfs_path).ok();
			std::fs::create_dir_all(&arg.rootfs_path)
				.map_err(|source| tg::error!(!source, "failed to create the sandbox directory"))?;
			let permissions =
				<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755);
			ROOTFS
				.extract(&arg.rootfs_path)
				.map_err(|source| tg::error!(!source, "failed to extract the sandbox rootfs"))?;
			set_rootfs_permissions(&arg.rootfs_path, &ROOTFS, &permissions)?;

			let lib_path = arg.rootfs_path.join("opt/tangram/lib");
			let output = std::process::Command::new("ldd")
				.arg(&arg.tangram_path)
				.output()
				.map_err(|source| {
					if source.kind() == std::io::ErrorKind::NotFound {
						tg::error!(
							"failed to prepare the sandbox rootfs: could not execute `ldd`; install `ldd` on this Linux host"
						)
					} else {
						tg::error!(
							!source,
							path = %arg.tangram_path.display(),
							"failed to execute `ldd`"
						)
					}
				})?;
			if !output.status.success() {
				let stderr = String::from_utf8_lossy(&output.stderr);
				let stdout = String::from_utf8_lossy(&output.stdout);
				return Err(tg::error!(
					status = %output.status,
					path = %arg.tangram_path.display(),
					stderr = %stderr.trim(),
					stdout = %stdout.trim(),
					"`ldd` failed"
				));
			}
			let stdout = String::from_utf8(output.stdout)
				.map_err(|source| tg::error!(!source, "failed to parse the `ldd` output"))?;
			for line in stdout.lines() {
				let line = line.trim();
				if line.is_empty() || line.starts_with("linux-vdso") {
					continue;
				}
				let parsed = if let Some((name, path)) = line.split_once("=>") {
					let name = name.trim();
					let path = path.trim();
					if path == "not found" {
						return Err(tg::error!(
							dependency = %name,
							executable = %arg.tangram_path.display(),
							"`ldd` reported a missing dependency"
						));
					}
					let path = path.split_whitespace().next().ok_or_else(|| {
						tg::error!("failed to parse a path from the `ldd` output")
					})?;
					path.starts_with('/').then(|| PathBuf::from(path))
				} else if line.starts_with('/') {
					let path = line.split_whitespace().next().ok_or_else(|| {
						tg::error!("failed to parse a path from the `ldd` output")
					})?;
					Some(PathBuf::from(path))
				} else {
					None
				};
				let Some(dependency_path) = parsed else {
					continue;
				};
				let source = std::fs::canonicalize(&dependency_path).map_err(|source| {
					tg::error!(
						!source,
						path = %dependency_path.display(),
						"failed to canonicalize the library path"
					)
				})?;
				let name = dependency_path
					.file_name()
					.and_then(|name| name.to_str())
					.ok_or_else(|| {
						tg::error!(
							path = %dependency_path.display(),
							"failed to get the library file name"
						)
					})?;
				let target = lib_path.join(name);
				if target.exists() {
					continue;
				}
				if std::fs::hard_link(&source, &target).is_err() {
					std::fs::copy(&source, &target).map_err(|error| {
						tg::error!(
							!error,
							source = %source.display(),
							target = %target.display(),
							"failed to stage the shared library"
						)
					})?;
				}
				std::fs::set_permissions(&target, permissions.clone()).map_err(|source| {
					tg::error!(
						!source,
						path = %target.display(),
						"failed to set sandbox file permissions"
					)
				})?;
			}
		}

		#[cfg(not(target_os = "linux"))]
		std::fs::create_dir_all(&arg.rootfs_path)
			.map_err(|source| tg::error!(!source, "failed to create the sandbox directory"))?;

		let state = ManagerState {
			artifacts_path: arg.artifacts_path,
			rootfs_path: arg.rootfs_path,
			tangram_path: arg.tangram_path,
		};

		let manager = Self(Arc::new(state));

		Ok(manager)
	}

	#[must_use]
	pub fn tangram_path(&self) -> &Path {
		&self.0.tangram_path
	}

	pub async fn spawn(&self, arg: SpawnArg) -> tg::Result<Sandbox> {
		let arg = RunArg {
			artifacts_path: self.0.artifacts_path.clone(),
			hostname: arg.hostname,
			mounts: arg.mounts,
			network: arg.network,
			path: arg.path,
			rootfs_path: self.0.rootfs_path.clone(),
			tangram_path: self.0.tangram_path.clone(),
			user: arg.user,
		};
		let sandbox = Sandbox::new(arg).await?;
		Ok(sandbox)
	}
}

impl Sandbox {
	async fn new(arg: RunArg) -> tg::Result<Self> {
		let directory = Directory::new(arg.path.clone());

		// Spawn the process.
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
		let mut command = tokio::process::Command::new(&arg.tangram_path);
		tokio::fs::create_dir_all(directory.host_scratch_path())
			.await
			.ok();
		command.arg("sandbox").arg("run");
		command
			.arg("--artifacts-path")
			.arg(&arg.artifacts_path)
			.arg("--path")
			.arg(directory.host_path())
			.arg("--ready-fd")
			.arg(ready_fd.to_string())
			.arg("--rootfs-path")
			.arg(&arg.rootfs_path)
			.arg("--tangram-path")
			.arg(&arg.tangram_path);
		if let Some(hostname) = &arg.hostname {
			command.arg("--hostname").arg(hostname);
		}
		for mount in &arg.mounts {
			command.arg("--mount").arg(mount.to_string());
		}
		if arg.network {
			command.arg("--network");
		} else {
			command.arg("--no-network");
		}
		if let Some(user) = &arg.user {
			command.arg("--user").arg(user);
		}
		command
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::inherit())
			.stderr(std::process::Stdio::inherit())
			.kill_on_drop(true);
		let mut process = command
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the sandbox process"))?;
		drop(ready_writer);

		// Wait for the sandbox to be ready.
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
				let port = u16::from_be_bytes([bytes[1], bytes[2]]);
				Ok(port)
			});
		let port = match ready {
			Ok(port) => port,
			Err(source) => {
				process.start_kill().ok();
				process.wait().await.ok();
				return Err(tg::error!(!source, "failed to start the sandbox"));
			},
		};

		// Connect the client.
		let client = if port == 0 {
			let path = directory.host_listen_path();
			Client::new_unix(path)
		} else {
			Client::new_tcp(port)
		};
		client
			.connect()
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the sandbox"))?;

		let state = SandboxState {
			client,
			arg,
			process,
		};

		let sandbox = Self(Arc::new(state));

		Ok(sandbox)
	}

	pub async fn spawn(
		&self,
		command: Command,
		id: tg::process::Id,
		tty: Option<tg::process::Tty>,
	) -> tg::Result<Process> {
		let arg = crate::client::spawn::Arg {
			command: command.clone(),
			id,
			tty,
		};
		let output = self.client.spawn(arg).await?;
		let process = Process {
			command,
			id: output.id,
		};
		Ok(process)
	}

	pub async fn set_tty_size(
		&self,
		process: &Process,
		size: tg::process::tty::Size,
	) -> tg::Result<()> {
		let arg = crate::client::tty::SizeArg {
			id: process.id.clone(),
			size,
		};
		self.client.set_tty_size(arg).await?;
		Ok(())
	}

	pub async fn read_stdio(
		&self,
		process: &Process,
		streams: Vec<tg::process::stdio::Stream>,
	) -> tg::Result<
		impl futures::Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static,
	> {
		let arg = crate::client::stdio::Arg {
			id: process.id.clone(),
			streams,
		};
		self.client.read_stdio(arg).await
	}

	pub async fn write_stdio(
		&self,
		process: &Process,
		streams: Vec<tg::process::stdio::Stream>,
		input: futures::stream::BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<
		impl futures::Stream<Item = tg::Result<tg::process::stdio::write::Event>> + Send + 'static,
	> {
		let arg = crate::client::stdio::Arg {
			id: process.id.clone(),
			streams,
		};
		self.client.write_stdio(arg, input).await
	}

	pub async fn kill(&self, process: &Process, signal: tg::process::Signal) -> tg::Result<()> {
		let arg = crate::client::kill::Arg {
			id: process.id.clone(),
			signal,
		};
		self.client.kill(arg).await?;
		Ok(())
	}

	pub async fn wait(&self, process: &Process) -> tg::Result<u8> {
		let arg = crate::client::wait::Arg {
			id: process.id.clone(),
		};
		let output = self.client.wait(arg).await?;
		Ok(output.status)
	}
}

pub fn run(arg: &RunArg, ready_fd: Option<RawFd>) -> tg::Result<()> {
	let directory = Directory::new(arg.path.clone());

	// Create a current thread tokio runtime.
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|source| tg::error!(!source, "failed to create the runtime"))?;

	// Bind the listener.
	let listen_path = directory.host_listen_path();
	#[cfg(target_os = "macos")]
	let listen_arg = {
		const MAX_SOCKET_PATH_LEN: usize = 100;
		if listen_path
			.to_str()
			.is_none_or(|string| string.len() > MAX_SOCKET_PATH_LEN)
		{
			tokio_util::either::Either::Right(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
		} else {
			tokio_util::either::Either::Left(listen_path.as_path())
		}
	};
	#[cfg(target_os = "linux")]
	let listen_arg = tokio_util::either::Either::Left(listen_path.as_path());
	let guard = runtime.enter();
	let (listener, port) = Server::listen(&listen_arg)?;
	drop(guard);

	// Enter the sandbox.
	#[cfg(target_os = "macos")]
	{
		crate::darwin::enter(arg)
			.map_err(|source| tg::error!(!source, "failed to enter the sandbox"))?;
	}
	#[cfg(target_os = "linux")]
	{
		crate::linux::enter(arg)
			.map_err(|source| tg::error!(!source, "failed to enter the sandbox"))?;
	}

	// Signal the ready fd.
	if let Some(fd) = ready_fd {
		let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
		let port_bytes = port.to_be_bytes();
		file.write_all(&[0x00, port_bytes[0], port_bytes[1]])
			.map_err(|source| tg::error!(!source, "failed to write the ready signal"))?;
	}

	// Run the server.
	runtime.block_on(async move {
		let server = Server::new();
		server.serve(listener).await;
	});

	Ok(())
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

	#[must_use]
	pub fn host_path(&self) -> &Path {
		&self.0
	}

	#[must_use]
	pub fn host_listen_path(&self) -> PathBuf {
		self.0.join("sandbox.socket")
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
	pub fn host_root_path(&self) -> PathBuf {
		self.0.join("root")
	}

	#[must_use]
	pub fn host_scratch_path(&self) -> PathBuf {
		self.0.join("scratch")
	}

	#[must_use]
	pub fn host_socket_path(&self) -> PathBuf {
		self.0.join("server.socket")
	}

	#[must_use]
	pub fn guest_artifacts_path(&self) -> PathBuf {
		"/opt/tangram/artifacts".into()
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
	pub fn guest_root_path(&self) -> PathBuf {
		"/".into()
	}

	#[must_use]
	pub fn guest_socket_path(&self) -> PathBuf {
		"/opt/tangram/socket".into()
	}

	#[must_use]
	pub fn guest_tangram_path(&self) -> PathBuf {
		"/opt/tangram/bin/tangram".into()
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

impl Deref for Sandbox {
	type Target = SandboxState;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
