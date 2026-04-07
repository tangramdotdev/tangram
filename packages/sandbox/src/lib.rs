use {
	crate::client::Client,
	std::{
		collections::{BTreeMap, BTreeSet},
		path::{Path, PathBuf},
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_uri::Uri,
};

#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;
mod pty;
mod util;

mod client;
mod server;

#[cfg(target_os = "linux")]
const ROOTFS: include_dir::Dir<'static> = include_dir::include_dir!("$OUT_DIR/rootfs");

#[derive(Clone)]
pub struct Sandbox(Arc<State>);

pub struct State {
	artifacts_path: PathBuf,
	client: Client,
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	mounts: Vec<tg::sandbox::Mount>,
	path: PathBuf,
	_process: tokio::process::Child,
	tangram_path: PathBuf,
}

pub struct Process {
	id: tg::process::Id,
}

#[derive(Clone, Debug)]
pub struct PrepareRootfsArg {
	pub path: PathBuf,
	pub tangram_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct SpawnArg {
	pub artifacts_path: PathBuf,
	pub hostname: Option<String>,
	pub mounts: Vec<tg::sandbox::Mount>,
	pub network: bool,
	pub path: PathBuf,
	pub rootfs_path: PathBuf,
	pub tangram_path: PathBuf,
	pub user: Option<String>,
}

#[derive(Clone, Debug)]
pub struct InitArg {
	pub library_paths: Vec<PathBuf>,
	pub path: PathBuf,
	pub tangram_path: PathBuf,
	pub url: Uri,
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

pub fn prepare_rootfs(arg: &PrepareRootfsArg) -> tg::Result<()> {
	prepare_runtime_libraries(arg)
}

impl Sandbox {
	pub async fn new(arg: SpawnArg) -> tg::Result<Self> {
		validate_mounts(&arg.mounts)?;
		let (listener, url) = Self::listen(&arg.path).await?;
		let init_arg = InitArg {
			library_paths: library_paths(&arg.rootfs_path),
			path: arg.path.clone(),
			tangram_path: arg.tangram_path.clone(),
			url,
		};

		#[cfg(target_os = "macos")]
		let mut process = crate::darwin::spawn_jailer(&arg, &init_arg)?;

		#[cfg(target_os = "linux")]
		let mut process = crate::linux::spawn_jailer(&arg, &init_arg)?;

		let client = match tokio::time::timeout(Duration::from_secs(5), async {
			tokio::select! {
				result = Client::with_listener(&listener) => result,
				result = process.wait() => {
					let status = result
						.map_err(|source| tg::error!(!source, "failed to wait for the sandbox process"))?;
					Err(tg::error!(status = %status, "the sandbox init process exited before connecting"))
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
			mounts: arg.mounts,
			path: arg.path,
			_process: process,
			tangram_path: arg.tangram_path,
		}));

		Ok(sandbox)
	}

	async fn listen(root_path: &Path) -> tg::Result<(crate::server::Listener, Uri)> {
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
			let port = match &listener {
				crate::server::Listener::Tcp(listener) => listener
					.local_addr()
					.map_err(|source| tg::error!(!source, "failed to get the local address"))?
					.port(),
				_ => unreachable!(),
			};
			let url = format!("http://localhost:{port}")
				.parse()
				.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?;
			Ok((listener, url))
		}
	}

	pub async fn spawn(
		&self,
		command: Command,
		id: tg::process::Id,
		tty: Option<tg::process::Tty>,
		remote: Option<String>,
		retry: bool,
	) -> tg::Result<Process> {
		let arg = crate::client::spawn::Arg {
			command,
			id,
			remote,
			retry,
			tty,
		};
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

	pub async fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<crate::client::get::Output>> {
		self.0.client.try_get_process(id).await
	}
}

pub fn init(arg: &InitArg) -> tg::Result<()> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|source| tg::error!(!source, "failed to create the runtime"))?;

	runtime.block_on(async move {
		let server = crate::server::Server::new(crate::server::Arg {
			library_paths: arg.library_paths.clone(),
			tangram_path: arg.tangram_path.clone(),
		});
		server.serve_url(&arg.url).await?;
		Ok::<_, tg::Error>(())
	})?;

	Ok(())
}

fn prepare_runtime_libraries(arg: &PrepareRootfsArg) -> tg::Result<()> {
	#[cfg(target_os = "macos")]
	{
		crate::darwin::prepare_runtime_libraries(arg)
	}
	#[cfg(target_os = "linux")]
	{
		crate::linux::prepare_runtime_libraries(arg)
	}
}

fn library_paths(rootfs_path: &Path) -> Vec<PathBuf> {
	#[cfg(target_os = "macos")]
	{
		let path = rootfs_path.join("lib");
		path.exists().then_some(path).into_iter().collect()
	}
	#[cfg(target_os = "linux")]
	{
		let _ = rootfs_path;
		Vec::new()
	}
}

fn append_init_args(command: &mut tokio::process::Command, arg: &InitArg) {
	command
		.arg("sandbox")
		.arg("init")
		.arg("--path")
		.arg(&arg.path)
		.arg("--url")
		.arg(arg.url.to_string())
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
	#[cfg(target_os = "macos")]
	set_home_for_command(command)?;
	#[cfg(target_os = "linux")]
	set_home_for_command(command);
	#[cfg(target_os = "macos")]
	{
		crate::darwin::prepare_command_for_spawn(command, tangram_path, library_paths)
	}
	#[cfg(target_os = "linux")]
	{
		crate::linux::prepare_command_for_spawn(command, tangram_path, library_paths)
	}
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

#[cfg(target_os = "linux")]
fn set_home_for_command(command: &mut Command) {
	if command.env.contains_key("HOME") {
		return;
	}
	command.env.insert("HOME".to_owned(), "/root".to_owned());
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
		let Some(resolved) = crate::util::which(Path::new(path), &command.executable) else {
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

impl Sandbox {
	#[must_use]
	pub fn host_path_for_guest_path(&self, path: &Path) -> Option<PathBuf> {
		#[cfg(target_os = "macos")]
		{
			Some(path.to_owned())
		}

		#[cfg(target_os = "linux")]
		{
			let mut path_maps = self
				.0
				.mounts
				.iter()
				.map(|mount| (mount.target.clone(), mount.source.clone()))
				.collect::<Vec<_>>();
			path_maps.extend([
				(
					Self::guest_listen_path_from_root(&self.0.path),
					Self::host_listen_path_from_root(&self.0.path),
				),
				(
					self.guest_tangram_socket_path(),
					self.host_tangram_socket_path(),
				),
				(
					self.guest_tmp_path(),
					Self::host_tmp_path_from_root(&self.0.path),
				),
				(self.guest_output_path(), self.host_output_path()),
				(self.guest_artifacts_path(), self.0.artifacts_path.clone()),
			]);
			Self::map_path(
				path,
				path_maps
					.iter()
					.map(|(guest, host)| (guest.as_path(), host.as_path())),
			)
		}
	}

	#[must_use]
	pub fn guest_artifacts_path(&self) -> PathBuf {
		Self::guest_artifacts_path_from_host_artifacts_path(&self.0.artifacts_path)
	}

	#[must_use]
	pub fn guest_output_path(&self) -> PathBuf {
		Self::guest_output_path_from_root(&self.0.path)
	}

	#[must_use]
	pub fn guest_output_path_for_process(&self, id: &tg::process::Id) -> PathBuf {
		self.guest_output_path().join(id.to_string())
	}

	#[must_use]
	pub fn guest_path_for_host_path(&self, path: &Path) -> Option<PathBuf> {
		#[cfg(target_os = "macos")]
		{
			Some(path.to_owned())
		}

		#[cfg(target_os = "linux")]
		{
			let mut path_maps = self
				.0
				.mounts
				.iter()
				.map(|mount| (mount.source.clone(), mount.target.clone()))
				.collect::<Vec<_>>();
			path_maps.extend([
				(
					Self::host_listen_path_from_root(&self.0.path),
					Self::guest_listen_path_from_root(&self.0.path),
				),
				(
					self.host_tangram_socket_path(),
					self.guest_tangram_socket_path(),
				),
				(
					Self::host_tmp_path_from_root(&self.0.path),
					self.guest_tmp_path(),
				),
				(self.host_output_path(), self.guest_output_path()),
				(self.0.artifacts_path.clone(), self.guest_artifacts_path()),
			]);
			Self::map_path(
				path,
				path_maps
					.iter()
					.map(|(host, guest)| (host.as_path(), guest.as_path())),
			)
		}
	}

	#[must_use]
	pub fn guest_tangram_socket_path(&self) -> PathBuf {
		Self::guest_tangram_socket_path_from_root(&self.0.path)
	}

	#[must_use]
	pub fn guest_tangram_path(&self) -> PathBuf {
		Self::guest_tangram_path_from_host_tangram_path(&self.0.tangram_path)
	}

	#[must_use]
	pub fn guest_tmp_path(&self) -> PathBuf {
		Self::guest_tmp_path_from_root(&self.0.path)
	}

	#[must_use]
	pub fn host_output_path(&self) -> PathBuf {
		Self::host_output_path_from_root(&self.0.path)
	}

	#[must_use]
	pub fn host_output_path_for_process(&self, id: &tg::process::Id) -> PathBuf {
		self.host_output_path().join(id.to_string())
	}

	#[must_use]
	pub fn host_scratch_path(&self) -> PathBuf {
		Self::host_scratch_path_from_root(&self.0.path)
	}

	#[must_use]
	pub fn host_tangram_socket_path(&self) -> PathBuf {
		Self::host_tangram_socket_path_from_root(&self.0.path)
	}
}

impl Sandbox {
	#[must_use]
	pub(crate) fn guest_artifacts_path_from_host_artifacts_path(artifacts_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			artifacts_path.to_owned()
		}
		#[cfg(target_os = "linux")]
		{
			let _ = artifacts_path;
			"/opt/tangram/artifacts".into()
		}
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn guest_libexec_tangram_path() -> PathBuf {
		"/opt/tangram/libexec/tangram".into()
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn guest_listen_path_from_root(root_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			Self::host_listen_path_from_root(root_path)
		}
		#[cfg(target_os = "linux")]
		{
			let _ = root_path;
			"/socket".into()
		}
	}

	#[must_use]
	pub(crate) fn guest_output_path_from_root(root_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			Self::host_output_path_from_root(root_path)
		}
		#[cfg(target_os = "linux")]
		{
			let _ = root_path;
			"/opt/tangram/output".into()
		}
	}

	#[must_use]
	pub(crate) fn guest_tangram_socket_path_from_root(root_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			Self::host_tangram_socket_path_from_root(root_path)
		}
		#[cfg(target_os = "linux")]
		{
			let _ = root_path;
			"/opt/tangram/socket".into()
		}
	}

	#[must_use]
	pub(crate) fn guest_tangram_path_from_host_tangram_path(tangram_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			tangram_path.to_owned()
		}
		#[cfg(target_os = "linux")]
		{
			let _ = tangram_path;
			"/opt/tangram/bin/tangram".into()
		}
	}

	#[must_use]
	pub(crate) fn guest_tmp_path_from_root(root_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			Self::host_tmp_path_from_root(root_path)
		}
		#[cfg(target_os = "linux")]
		{
			let _ = root_path;
			"/tmp".into()
		}
	}

	#[must_use]
	pub(crate) fn host_etc_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("etc")
	}

	#[must_use]
	pub(crate) fn host_listen_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("socket")
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn host_nsswitch_path_from_root(root_path: &Path) -> PathBuf {
		Self::host_etc_path_from_root(root_path).join("nsswitch.conf")
	}

	#[must_use]
	pub(crate) fn host_output_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("output")
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn host_passwd_path_from_root(root_path: &Path) -> PathBuf {
		Self::host_etc_path_from_root(root_path).join("passwd")
	}

	#[cfg(target_os = "macos")]
	#[must_use]
	pub(crate) fn host_profile_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("sandbox.sb")
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn host_resolv_conf_path_from_root(root_path: &Path) -> PathBuf {
		Self::host_etc_path_from_root(root_path).join("resolv.conf")
	}

	#[must_use]
	pub(crate) fn host_scratch_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("scratch")
	}

	#[must_use]
	pub(crate) fn host_tangram_socket_path_from_root(root_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			root_path.join("tg")
		}
		#[cfg(target_os = "linux")]
		{
			Self::host_upper_path_from_root(root_path).join("opt/tangram/socket")
		}
	}

	#[must_use]
	pub(crate) fn host_tmp_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("tmp")
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn host_upper_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("upper")
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn host_work_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("work")
	}

	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	fn map_path<'a>(
		path: &Path,
		path_maps: impl Iterator<Item = (&'a Path, &'a Path)>,
	) -> Option<PathBuf> {
		let mut best: Option<(&Path, &Path, usize)> = None;
		for (from, to) in path_maps {
			if !path.starts_with(from) {
				continue;
			}
			let len = from.components().count();
			if best.is_none_or(|(_, _, best_len)| len >= best_len) {
				best = Some((from, to, len));
			}
		}
		let (from, to, _) = best?;
		let suffix = path.strip_prefix(from).unwrap();
		Some(to.join(suffix))
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

impl std::ops::Deref for Sandbox {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
