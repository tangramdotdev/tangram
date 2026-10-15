use {
	std::{
		io::Write as _,
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
			unix::{fs::OpenOptionsExt as _, process::CommandExt as _},
		},
		path::Path,
		process::ExitCode,
	},
	tangram_client::prelude::*,
};

#[cfg(target_os = "linux")]
mod spawn;

#[cfg(target_os = "linux")]
pub(crate) use self::spawn::{spawn, spawn_command};
pub use crate::command::Command;

#[cfg(target_os = "linux")]
pub type Bind = crate::mount::Bind;
#[cfg(target_os = "linux")]
pub type Mounts = crate::mount::Arg;
#[cfg(target_os = "linux")]
pub type Overlay = crate::mount::Overlay;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Arg {
	pub backend: Backend,

	pub command: Command,

	#[serde(default)]
	pub options: Options,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case", tag = "kind", deny_unknown_fields)]
pub enum Backend {
	#[cfg(target_os = "linux")]
	Builtin(crate::builtin::run::Arg),

	#[cfg(target_os = "linux")]
	Bubblewrap(crate::bubblewrap::run::Arg),

	#[cfg(target_os = "linux")]
	CloudHypervisor(crate::cloud_hypervisor::run::Arg),

	#[cfg(target_os = "linux")]
	Gvisor(crate::gvisor::run::Arg),

	#[cfg(target_os = "macos")]
	Seatbelt(crate::seatbelt::run::Arg),
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
	#[cfg(target_os = "linux")]
	pub cgroup: Option<Cgroup>,

	#[cfg(target_os = "linux")]
	pub cgroup_delegate: bool,

	#[cfg(target_os = "linux")]
	pub cgroup_readonly: bool,

	#[cfg(target_os = "linux")]
	pub filesystem_fd: Option<i32>,

	#[cfg(target_os = "linux")]
	pub filesystem_path: Option<std::path::PathBuf>,

	#[cfg(target_os = "linux")]
	pub fuse_fd: Option<i32>,

	#[cfg(target_os = "linux")]
	pub fuse_path: Option<std::path::PathBuf>,

	#[cfg(target_os = "linux")]
	pub index: u64,

	#[cfg(target_os = "linux")]
	pub isolate_root: bool,

	#[cfg(target_os = "linux")]
	pub mounts: Mounts,

	#[cfg(target_os = "linux")]
	pub network: Option<Network>,

	#[cfg(target_os = "linux")]
	pub nice: u8,

	#[cfg(target_os = "linux")]
	pub rlimit_nofile: Option<u64>,

	#[cfg(target_os = "linux")]
	pub user_namespace_fd: Option<i32>,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Cgroup {
	pub cpu: Option<u64>,
	pub memory: Option<u64>,
	pub memory_oom_group: bool,
	pub memory_swap: Option<u64>,
	pub name: String,
	pub pids: Option<u64>,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Network {
	Host,
	Pasta {
		fd: i32,
	},
	Veth {
		fd: i32,
		gateway: std::net::Ipv4Addr,
		guest: std::net::Ipv4Addr,
	},
}

pub struct Config {
	reader: OwnedFd,
	task: std::thread::JoinHandle<std::io::Result<()>>,
}

#[cfg(target_os = "linux")]
struct Prepared {
	_filesystem: Option<OwnedFd>,
	cgroup: Option<crate::cgroup::Cgroup>,
	cgroup_handle: Option<crate::cgroup::Handle>,
	root: Option<tangram_util::fs::Temp>,
	root_path: Option<std::path::PathBuf>,
}

pub fn run(arg: Arg) -> tg::Result<ExitCode> {
	// Validate the command before preparing the sandbox.
	arg.command.validate()?;

	#[cfg(target_os = "linux")]
	{
		// Prepare the common sandbox resources.
		let command = &arg.command;
		let backend = arg.backend;
		let mut options = arg.options;
		let prepared = prepare(&mut options, &backend)?;

		// Fork the backend while retaining ownership of its resources.
		let child = crate::linux::fork()?;
		if child == 0 {
			if let Err(error) = child_main(command, &options, backend, &prepared) {
				eprintln!("{}", error.trace());
				std::process::exit(105);
			}
			std::process::exit(0);
		}

		// Wait for the backend and detach the root.
		let status = crate::linux::wait_for_child(child);
		if prepared.root.is_some()
			&& let Some(root) = &prepared.root_path
		{
			let root = std::ffi::CString::new(root.as_os_str().as_encoded_bytes())
				.map_err(|error| tg::error!(!error, "failed to encode the prepared root"))?;

			// SAFETY: The root is a live, terminated path in the preparation mount namespace.
			if unsafe { libc::umount2(root.as_ptr(), libc::MNT_DETACH) } != 0 {
				let error = std::io::Error::last_os_error();
				if !matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOENT)) {
					return Err(tg::error!(!error, "failed to detach the prepared root"));
				}
			}
		}

		// Clean up the cgroup after the backend exits.
		drop(prepared.cgroup_handle);
		let cleanup = prepared
			.cgroup
			.map(crate::cgroup::Cgroup::cleanup)
			.transpose();
		let status = status?;
		cleanup?;

		Ok(ExitCode::from(status))
	}

	#[cfg(target_os = "macos")]
	{
		let Backend::Seatbelt(backend) = arg.backend;
		let output = crate::seatbelt::run::run(&arg.command, &backend)?;

		Ok(ExitCode::from(output))
	}
}

pub fn configure_command(command: &mut tokio::process::Command, arg: &Arg) -> tg::Result<Config> {
	// Serialize the configuration.
	let contents = serde_json::to_vec(arg).map_err(|error| {
		tg::error!(
			!error,
			"failed to serialize the sandbox launch configuration"
		)
	})?;

	// Create the pipe.
	let mut descriptors = [-1; 2];

	// SAFETY: The array has space for both descriptors returned by the pipe syscall.
	#[cfg(target_os = "linux")]
	let result = unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) };

	// SAFETY: The array has space for both descriptors returned by the pipe syscall.
	#[cfg(not(target_os = "linux"))]
	let result = unsafe { libc::pipe(descriptors.as_mut_ptr()) };
	if result != 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to create the sandbox configuration pipe"
		));
	}

	// Own the pipe descriptors.
	// SAFETY: A successful pipe call returns two newly owned descriptors.
	let (reader, writer) = unsafe {
		(
			OwnedFd::from_raw_fd(descriptors[0]),
			OwnedFd::from_raw_fd(descriptors[1]),
		)
	};

	// Close the descriptors when executing other commands.
	for fd in [&reader, &writer] {
		// SAFETY: The owned descriptors remain valid while their flags are set.
		if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to protect the sandbox configuration descriptor"
			));
		}
	}

	// Set the launch arguments.
	let fd = reader.as_raw_fd();
	command
		.arg("sandbox")
		.arg("run")
		.arg("--config-fd")
		.arg(fd.to_string());

	// Inherit the reader in the sandbox command.
	// SAFETY: The closure only invokes an async-signal-safe syscall, and Config retains the descriptor until spawn completes.
	unsafe {
		command.as_std_mut().pre_exec(move || {
			if libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		});
	}

	// Start the configuration writer.
	let task = std::thread::Builder::new()
		.spawn(move || {
			let mut writer = std::fs::File::from(writer);
			writer.write_all(&contents)
		})
		.map_err(|error| tg::error!(!error, "failed to start the sandbox configuration writer"))?;
	let config = Config { reader, task };

	Ok(config)
}

pub fn write_config<T: serde::Serialize>(path: &Path, arg: &T) -> tg::Result<()> {
	// Serialize the configuration.
	let contents = serde_json::to_vec(arg).map_err(|error| {
		tg::error!(
			!error,
			"failed to serialize the sandbox launch configuration"
		)
	})?;

	// Create the file.
	let mut file = std::fs::OpenOptions::new()
		.create_new(true)
		.mode(0o600)
		.write(true)
		.open(path)
		.map_err(|error| {
			tg::error!(!error, path = %path.display(), "failed to create the sandbox launch configuration")
		})?;

	// Write the configuration.
	file.write_all(&contents).map_err(|error| {
		tg::error!(!error, path = %path.display(), "failed to write the sandbox launch configuration")
	})?;

	Ok(())
}

impl Config {
	pub async fn finish(self) -> tg::Result<()> {
		drop(self.reader);
		tokio::task::spawn_blocking(move || self.task.join())
			.await
			.map_err(|error| tg::error!(!error, "the configuration task panicked"))?
			.map_err(|_| tg::error!("the sandbox configuration writer panicked"))?
			.map_err(|error| {
				tg::error!(!error, "failed to write the sandbox launch configuration")
			})?;
		Ok(())
	}
}

#[cfg(target_os = "linux")]
fn prepare(arg: &mut Options, backend: &Backend) -> tg::Result<Prepared> {
	// Validate the launch configuration.
	if arg.nice > 19 {
		return Err(tg::error!("the CPU priority must be between zero and 19"));
	}
	if arg.rlimit_nofile == Some(0) {
		return Err(tg::error!("the open file limit must be greater than zero"));
	}
	if arg.filesystem_fd.is_some() != arg.filesystem_path.is_some() {
		return Err(tg::error!(
			"the filesystem descriptor and path must be provided together"
		));
	}
	if arg.fuse_fd.is_some() != arg.fuse_path.is_some() {
		return Err(tg::error!(
			"the FUSE descriptor and path must be provided together"
		));
	}

	// Validate the common resource limits.
	if let Some(cgroup) = &arg.cgroup {
		if cgroup.name.is_empty() {
			return Err(tg::error!("a nonempty cgroup name is required"));
		}
		if cgroup.cpu == Some(0) || cgroup.memory == Some(0) || cgroup.pids == Some(0) {
			return Err(tg::error!(
				"the cgroup CPU, memory, and process limits must be greater than zero"
			));
		}
	}
	if arg.cgroup_delegate && (arg.cgroup.is_some() || arg.cgroup_readonly) {
		return Err(tg::error!(
			"cgroup delegation cannot be combined with a cgroup or a read-only cgroup mount"
		));
	}

	// Validate the transferred descriptors before taking ownership of them.
	let network_fd = match &arg.network {
		Some(Network::Pasta { fd } | Network::Veth { fd, .. }) => Some(*fd),
		None | Some(Network::Host) => None,
	};
	let mut descriptors = std::collections::BTreeSet::new();
	for fd in [
		arg.filesystem_fd,
		arg.fuse_fd,
		arg.user_namespace_fd,
		network_fd,
	]
	.into_iter()
	.flatten()
	{
		if fd < 3 {
			return Err(tg::error!(%fd, "control descriptors must be greater than two"));
		}
		crate::linux::validate_descriptor(fd)?;
		if !descriptors.insert(fd) {
			return Err(tg::error!(%fd, "control descriptors must be distinct"));
		}
	}

	// Validate the filesystem paths and mount configuration.
	for path in [arg.filesystem_path.as_ref(), arg.fuse_path.as_ref()]
		.into_iter()
		.flatten()
	{
		if !path.is_absolute() || path.as_os_str().as_encoded_bytes().contains(&0) {
			return Err(
				tg::error!(path = %path.display(), "filesystem paths must be absolute and contain no null bytes"),
			);
		}
	}
	crate::mount::validate(&arg.mounts)?;

	// Validate the namespace preparation options.
	if matches!(backend, Backend::CloudHypervisor(_))
		&& (arg.filesystem_fd.is_some()
			|| arg.fuse_fd.is_some()
			|| arg.user_namespace_fd.is_some()
			|| arg.isolate_root
			|| arg.cgroup_delegate
			|| arg.cgroup_readonly
			|| arg.network.is_some()
			|| !arg.mounts.binds.is_empty()
			|| !arg.mounts.ro_binds.is_empty()
			|| !arg.mounts.overlays.is_empty()
			|| !arg.mounts.overlay_sources.is_empty()
			|| !arg.mounts.devs.is_empty()
			|| !arg.mounts.procs.is_empty()
			|| !arg.mounts.tmpfs.is_empty()
			|| arg.mounts.filesystem_mount_fd.is_some()
			|| arg.mounts.nodev
			|| arg.mounts.nosuid)
	{
		return Err(tg::error!(
			"cloud-hypervisor does not support namespace preparation options"
		));
	}
	if arg.fuse_fd.is_some() && !matches!(backend, Backend::Builtin(_)) {
		return Err(tg::error!(
			"direct FUSE mounting requires the builtin backend"
		));
	}

	// Configure the parent death signal.
	crate::util::set_parent_death_signal(libc::SIGKILL)?;

	// Create the cgroup.
	let cgroup = arg
		.cgroup
		.as_ref()
		.map(|options| {
			let entry = crate::cgroup::Options {
				cpu: options.cpu,
				memory: options.memory.map(Into::into),
				memory_oom_group: options.memory_oom_group,
				memory_swap: options.memory_swap,
				pids: options.pids,
				..Default::default()
			};
			crate::cgroup::Cgroup::new(&options.name, &entry)
		})
		.transpose()?;
	let cgroup_handle = cgroup
		.as_ref()
		.map(crate::cgroup::Cgroup::handle)
		.transpose()?;

	// Resolve the identity used to prepare the filesystem.
	let (uid, gid) = match backend {
		Backend::Builtin(arg) => (arg.uid, arg.gid),
		Backend::Bubblewrap(arg) => (arg.uid, arg.gid),
		Backend::CloudHypervisor(_) => {
			return Ok(Prepared {
				_filesystem: None,
				cgroup,
				cgroup_handle,
				root: None,
				root_path: None,
			});
		},
		Backend::Gvisor(arg) => (arg.uid, arg.gid),
	};

	// Create the prepared root before entering the user namespace, so that the runner owns it.
	let root = if arg.isolate_root
		|| arg
			.mounts
			.binds
			.iter()
			.chain(&arg.mounts.ro_binds)
			.any(|bind| bind.target == Path::new("/"))
		|| arg
			.mounts
			.overlays
			.iter()
			.any(|overlay| overlay.target == Path::new("/"))
	{
		let temp = tangram_util::fs::Temp::new()
			.map_err(|error| tg::error!(!error, "failed to create the prepared root"))?;
		std::fs::create_dir_all(temp.path().join("root"))
			.map_err(|error| tg::error!(!error, "failed to create the prepared root"))?;
		Some(temp)
	} else {
		None
	};
	let root_path = root
		.as_ref()
		.map(|root| root.path().join("root"))
		.or_else(|| match backend {
			Backend::Builtin(arg) => arg.root.clone(),
			Backend::Bubblewrap(arg) => arg.root.clone(),
			Backend::Gvisor(arg) => arg.root.clone(),
			Backend::CloudHypervisor(_) => None,
		});

	// Enter the user namespace.
	let filesystems = if let Some(fd) = arg.user_namespace_fd {
		let count = if arg.filesystem_fd.is_some() {
			1 + arg.mounts.overlay_sources.len()
		} else {
			0
		};
		crate::linux::enter_user_namespace_with_parent(fd, count)?
	} else {
		crate::linux::enter_user_namespace(uid, gid)?;
		Vec::new()
	};

	// Make the current cgroup the cgroup namespace root and delegate its controllers.
	if arg.cgroup_delegate {
		let cgroup = crate::cgroup::Handle::current()?;
		crate::linux::unshare(
			libc::CLONE_NEWCGROUP,
			"failed to create the resource cgroup namespace",
		)?;
		cgroup.delegate()?;
	}

	// Prepare the network and mount namespaces.
	setup_network(arg)?;
	crate::linux::unshare(
		libc::CLONE_NEWNS | libc::CLONE_NEWPID,
		"failed to create the resource setup namespaces",
	)?;

	// Prepare the sandbox filesystem.
	let filesystem = prepare_filesystem(arg, filesystems, uid, gid)?;
	if let (Some(fd), Some(path)) = (arg.fuse_fd, &arg.fuse_path) {
		crate::builtin::run::mount_fuse(fd, path)?;
	}

	let output = Prepared {
		_filesystem: filesystem,
		cgroup,
		cgroup_handle,
		root,
		root_path,
	};

	Ok(output)
}

#[cfg(target_os = "linux")]
fn child_main(
	command: &Command,
	arg: &Options,
	backend: Backend,
	prepared: &Prepared,
) -> tg::Result<()> {
	// Resolve the prepared resources.
	let root = prepared.root_path.as_deref();
	let cgroup = prepared.cgroup_handle.as_ref();

	// Configure the process lifetime and priority.
	crate::util::set_parent_death_signal(libc::SIGKILL)?;

	// SAFETY: nice takes a scalar priority adjustment and errno is thread local.
	unsafe {
		*libc::__errno_location() = 0;
	}

	// SAFETY: nice takes a scalar priority adjustment.
	if unsafe { libc::nice(i32::from(arg.nice)) } == -1
		&& std::io::Error::last_os_error().raw_os_error() != Some(0)
	{
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to lower the sandbox CPU priority"
		));
	}

	// Enter the cgroup and configure the resource limits.
	if let Some(cgroup) = cgroup {
		cgroup.move_self()?;
	}
	if !matches!(backend, Backend::CloudHypervisor(_)) && !arg.cgroup_delegate {
		crate::linux::unshare(
			libc::CLONE_NEWCGROUP,
			"failed to create the resource cgroup namespace",
		)?;
	}
	crate::linux::set_rlimit_nofile(arg.rlimit_nofile)?;

	// Run the VM with the common resource settings applied.
	let backend = match backend {
		Backend::CloudHypervisor(arg) => {
			let status = crate::cloud_hypervisor::run::run(command, &arg)?;
			std::process::exit(i32::from(status));
		},
		backend => backend,
	};

	// Prepare the root filesystem.
	if prepared.root.is_some()
		&& let Some(root) = root
		&& !arg
			.mounts
			.binds
			.iter()
			.chain(&arg.mounts.ro_binds)
			.any(|bind| bind.target == Path::new("/"))
		&& !arg
			.mounts
			.overlays
			.iter()
			.any(|overlay| overlay.target == Path::new("/"))
	{
		crate::mount::mount_tmpfs(root)?;
	}
	crate::mount::apply(&arg.mounts, root)?;

	// Mount the cgroup filesystem.
	let cgroup_path = root.map_or_else(
		|| std::path::PathBuf::from("/sys/fs/cgroup"),
		|root| root.join("sys/fs/cgroup"),
	);
	crate::mount::mount_cgroup(&cgroup_path, arg.cgroup_readonly)?;

	// Close preparation descriptors before invoking the backend.
	crate::linux::close_non_std_fds()?;

	// Pass only the prepared isolation inputs to the backend.
	match backend {
		Backend::Builtin(mut arg) => {
			if let Some(root) = root {
				arg.root = Some(root.to_owned());
			}
			let status = crate::builtin::run::run(command, &arg)?;
			std::process::exit(i32::from(status));
		},
		Backend::Bubblewrap(mut arg) => {
			if let Some(root) = root {
				arg.root = Some(root.to_owned());
			}
			let status = crate::bubblewrap::run::run(command, &arg)?;
			std::process::exit(i32::from(status));
		},
		Backend::Gvisor(backend_arg) => {
			// Resolve the prepared root and mounts.
			let root = root
				.or(backend_arg.root.as_deref())
				.ok_or_else(|| tg::error!("gvisor requires a prepared root"))?;
			let mut mounts = backend_arg.mounts.clone();
			for (bind, readonly) in arg
				.mounts
				.binds
				.iter()
				.map(|bind| (bind, false))
				.chain(arg.mounts.ro_binds.iter().map(|bind| (bind, true)))
			{
				if bind.target == Path::new("/") {
					continue;
				}
				let target = bind
					.target
					.strip_prefix("/")
					.map_err(|error| tg::error!(!error, "invalid gvisor mount target"))?;
				let mount = tg::sandbox::Mount {
					readonly,
					source: root.join(target),
					target: bind.target.clone(),
				};
				mounts.push(mount);
			}

			// Describe the prepared execution inputs.
			let network = match arg.network {
				None => None,
				Some(Network::Host) => Some(crate::gvisor::run::Network::Host),
				Some(Network::Pasta { .. } | Network::Veth { .. }) => {
					Some(crate::gvisor::run::Network::Sandbox)
				},
			};
			let prepared = crate::gvisor::run::Prepared {
				mounts,
				network,
				rlimit_nofile: arg.rlimit_nofile,
				root: root.to_owned(),
			};

			// Invoke the backend with its configuration and prepared inputs.
			let status = crate::gvisor::run::run(command, &backend_arg, &prepared)?;
			std::process::exit(i32::from(status));
		},
		Backend::CloudHypervisor(_) => unreachable!(),
	}
}

#[cfg(target_os = "linux")]
fn setup_network(arg: &Options) -> tg::Result<()> {
	match &arg.network {
		Some(Network::Host) => {},
		Some(Network::Pasta { fd } | Network::Veth { fd, .. }) => {
			crate::linux::unshare(
				libc::CLONE_NEWNET,
				"failed to create the resource network namespace",
			)?;
			if *fd < 0 {
				return Err(tg::error!(
					"the network requires a valid synchronization descriptor"
				));
			}

			// SAFETY: The inherited synchronization descriptor is exclusively owned here.
			let fd = unsafe { OwnedFd::from_raw_fd(*fd) };
			use std::io::{Read as _, Write as _};
			let mut socket = std::os::unix::net::UnixStream::from(fd);
			socket
				.write_all(&[0])
				.map_err(|error| tg::error!(!error, "failed to signal network readiness"))?;
			socket
				.read_exact(&mut [0])
				.map_err(|error| tg::error!(!error, "failed to wait for network setup"))?;

			if let Some(Network::Veth { gateway, guest, .. }) = &arg.network {
				let suffix = format!("{:09}", arg.index % 1_000_000_000);
				let name = format!("tg-vc-{suffix}");
				let mut nl = crate::netlink::Netlink::new()?;
				nl.link_rename(&name, "eth0")?;
				crate::network::host::disable_ipv6("eth0")?;
				nl.addr_add_v4("eth0", *guest, 16)?;
				nl.link_set_up("eth0")?;
				nl.link_set_up("lo")?;
				nl.route_add_default_v4(*gateway)?;
			}
		},
		None => crate::linux::unshare(
			libc::CLONE_NEWNET,
			"failed to create the resource network namespace",
		)?,
	}

	Ok(())
}

#[cfg(target_os = "linux")]
fn prepare_filesystem(
	arg: &mut Options,
	filesystems: Vec<OwnedFd>,
	uid: u32,
	gid: u32,
) -> tg::Result<Option<OwnedFd>> {
	// Open and prepare the sandbox filesystem.
	let (Some(path), Some(socket)) = (arg.filesystem_path.clone(), arg.filesystem_fd) else {
		return Ok(None);
	};
	let mut filesystems = filesystems.into_iter();
	let filesystem = if let Some(filesystem) = filesystems.next() {
		// Attach the ID-mapped mounts only in the private sandbox mount namespace, so that overlayfs can clone its layers.
		crate::mount::make_mounts_private()?;
		crate::filesystem::attach(&filesystem, &path)?;
		for (filesystem, path) in filesystems.zip(&arg.mounts.overlay_sources) {
			crate::filesystem::attach(&filesystem, path)?;
		}

		// Create files as the workload, so that the mapped mounts preserve the runner's on-disk ownership.
		crate::linux::set_filesystem_ids(uid, gid)?;

		filesystem
	} else {
		crate::filesystem::open(&path)?
	};
	crate::filesystem::prepare(&filesystem, uid, gid)?;

	// Rewrite the mounts to use the prepared filesystem.
	arg.mounts.filesystem_mount_fd = Some(filesystem.as_raw_fd());
	let target = crate::filesystem::path(&filesystem);
	for bind in arg.mounts.binds.iter_mut().chain(&mut arg.mounts.ro_binds) {
		crate::filesystem::relocate(&mut bind.source, &path, &target);
	}
	for overlay in &mut arg.mounts.overlays {
		crate::filesystem::relocate(&mut overlay.upperdir, &path, &target);
		crate::filesystem::relocate(&mut overlay.workdir, &path, &target);
	}

	// Send the filesystem descriptor to the parent.
	// SAFETY: The inherited filesystem control descriptor is exclusively owned here.
	let socket = unsafe { OwnedFd::from_raw_fd(socket) };
	crate::filesystem::send(&socket, &filesystem)?;

	Ok(Some(filesystem))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
	use super::*;

	#[test]
	fn configuration_round_trip() {
		for backend in [
			Backend::Builtin(crate::builtin::run::Arg::default()),
			Backend::Bubblewrap(crate::bubblewrap::run::Arg {
				path: "bwrap".into(),
				..Default::default()
			}),
			Backend::Gvisor(crate::gvisor::run::Arg {
				path: "runsc".into(),
				platform: crate::Platform::Kvm,
				..Default::default()
			}),
		] {
			let arg = Arg {
				backend,
				command: Command::default(),
				options: Options::default(),
			};
			let value = serde_json::to_value(&arg).unwrap();
			let decoded: Arg = serde_json::from_value(value.clone()).unwrap();
			assert_eq!(serde_json::to_value(decoded).unwrap(), value);
		}
	}

	#[tokio::test]
	async fn large_configuration_pipe() {
		// Create a configuration larger than the pipe buffer.
		let mut command_arg = Command::default();
		for index in 0..64 {
			command_arg
				.env
				.insert(format!("KEY_{index}"), "x".repeat(4096));
		}
		let arg = Arg {
			backend: Backend::Builtin(crate::builtin::run::Arg::default()),
			command: command_arg,
			options: Options::default(),
		};
		let contents = serde_json::to_vec(&arg).unwrap();

		// Create a command that counts the configuration bytes.
		let mut command = tokio::process::Command::new("/bin/sh");
		command
			.arg("-c")
			.arg("for arg; do fd=$arg; done; eval \"wc -c <&$fd\"")
			.arg("transport-test");
		command.stdout(std::process::Stdio::piped());

		// Spawn the command and finish writing the configuration.
		let config = configure_command(&mut command, &arg).unwrap();
		let child = command.spawn().unwrap();
		config.finish().await.unwrap();

		// Verify that the command received the complete configuration.
		let output = child.wait_with_output().await.unwrap();
		assert!(output.status.success());
		assert_eq!(
			String::from_utf8(output.stdout)
				.unwrap()
				.trim()
				.parse::<usize>()
				.unwrap(),
			contents.len()
		);
	}
}
