use {
	crate::{linux, mount},
	std::{
		collections::BTreeMap,
		ffi::{CString, OsString},
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
			unix::ffi::OsStrExt as _,
		},
		path::{Path, PathBuf},
		process::ExitCode,
	},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Arg {
	pub backend: crate::Backend,
	pub cgroup: Option<Cgroup>,
	pub cgroup_readonly: bool,
	pub filesystem_fd: Option<i32>,
	pub filesystem_path: Option<PathBuf>,
	pub fuse_fd: Option<i32>,
	pub fuse_path: Option<PathBuf>,
	pub gid: u32,
	pub index: u64,
	pub(crate) mounts: mount::Arg,
	pub network: Option<Network>,
	pub nice: u8,
	pub process: Process,
	pub rlimit_nofile: Option<u64>,
	pub seccomp: Option<crate::SeccompPolicy>,
	pub uid: u32,
	pub user_namespace_fd: Option<i32>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Cgroup {
	pub cpu: Option<u64>,
	pub memory: Option<u64>,
	pub memory_oom_group: bool,
	pub memory_swap: Option<u64>,
	pub name: String,
	pub pids: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
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

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Process {
	pub arguments: Vec<String>,
	pub directory: PathBuf,
	pub environment: BTreeMap<String, String>,
	pub hostname: Option<String>,
}

pub(crate) struct Prepared<'a> {
	pub arg: &'a Arg,
	pub root: Option<&'a Path>,
	pub seccomp_fd: Option<i32>,
}

pub fn run(mut arg: Arg) -> tg::Result<ExitCode> {
	if arg.process.arguments.is_empty() {
		return Err(tg::error!("a command is required"));
	}
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
	crate::util::set_parent_death_signal(libc::SIGKILL)?;
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
	if let Some(fd) = arg.user_namespace_fd {
		linux::enter_user_namespace_with_parent(fd)?;
	} else {
		linux::enter_user_namespace(arg.uid, arg.gid)?;
	}
	let host_namespace = if arg.user_namespace_fd.is_some() && arg.filesystem_fd.is_some() {
		Some(crate::filesystem::host_namespace(arg.uid, arg.gid)?)
	} else {
		None
	};
	setup_network(&arg)?;
	linux::unshare(
		libc::CLONE_NEWNS | libc::CLONE_NEWPID,
		"failed to create the resource setup namespaces",
	)?;
	let _filesystem = prepare_filesystem(&mut arg, host_namespace.as_ref())?;
	if let (Some(fd), Some(path)) = (arg.fuse_fd, &arg.fuse_path) {
		if !matches!(arg.backend, crate::Backend::Builtin) {
			return Err(tg::error!(
				"direct FUSE mounting requires the builtin backend"
			));
		}
		crate::builtin::run::mount_fuse(fd, path)?;
	}
	let root = if arg
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
	let root_path = root.as_ref().map(|root| root.path().join("root"));
	let child = linux::fork()?;
	if child == 0 {
		if let Err(error) = child_main(&arg, cgroup_handle.as_ref(), root_path.as_deref()) {
			eprintln!("{}", error.trace());
			std::process::exit(105);
		}
		std::process::exit(0);
	}
	let status = linux::wait_for_child(child);
	if let Some(root) = &root_path {
		let root = CString::new(root.as_os_str().as_bytes())
			.map_err(|error| tg::error!(!error, "failed to encode the prepared root"))?;
		// SAFETY: The root is a live, terminated path in the setup mount namespace.
		if unsafe { libc::umount2(root.as_ptr(), libc::MNT_DETACH) } != 0 {
			let error = std::io::Error::last_os_error();
			if !matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOENT)) {
				return Err(tg::error!(!error, "failed to detach the prepared root"));
			}
		}
	}
	let cleanup = cgroup.map(crate::cgroup::Cgroup::cleanup).transpose();
	let status = status?;
	cleanup?;
	Ok(ExitCode::from(status))
}

impl Default for Arg {
	fn default() -> Self {
		Self {
			backend: crate::Backend::Builtin,
			cgroup: None,
			cgroup_readonly: false,
			filesystem_fd: None,
			filesystem_path: None,
			fuse_fd: None,
			fuse_path: None,
			gid: 0,
			index: 0,
			mounts: mount::Arg::default(),
			network: None,
			nice: 0,
			process: Process::default(),
			rlimit_nofile: None,
			seccomp: None,
			uid: 0,
			user_namespace_fd: None,
		}
	}
}
impl Default for Process {
	fn default() -> Self {
		Self {
			arguments: Vec::new(),
			directory: PathBuf::from("/"),
			environment: BTreeMap::new(),
			hostname: None,
		}
	}
}

fn child_main(
	arg: &Arg,
	cgroup: Option<&crate::cgroup::Handle>,
	root: Option<&Path>,
) -> tg::Result<()> {
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
	if let Some(cgroup) = cgroup {
		cgroup.move_self()?;
	}
	linux::unshare(
		libc::CLONE_NEWCGROUP,
		"failed to create the resource cgroup namespace",
	)?;
	linux::set_rlimit_nofile(arg.rlimit_nofile)?;
	mount::apply(&arg.mounts, root)?;
	let cgroup_path = root.map_or_else(
		|| PathBuf::from("/sys/fs/cgroup"),
		|root| root.join("sys/fs/cgroup"),
	);
	mount::mount_cgroup(&cgroup_path, arg.cgroup_readonly)?;
	linux::close_non_std_fds()?;
	let seccomp_fd = if matches!(
		arg.backend,
		crate::Backend::Builtin | crate::Backend::Bubblewrap { .. }
	) {
		arg.seccomp.map(crate::seccomp::descriptor).transpose()?
	} else {
		None
	};
	let prepared = Prepared {
		arg,
		root,
		seccomp_fd,
	};
	match &arg.backend {
		crate::Backend::Builtin => {
			let command = crate::bubblewrap::command(&prepared, Path::new("builtin"), true)?;
			let arguments = command.get_args().map(OsString::from).collect();
			let status = crate::builtin::run::run(arguments)?;
			std::process::exit(i32::from(status));
		},
		crate::Backend::Bubblewrap { path } => {
			let command = crate::bubblewrap::command(&prepared, path, false)?;
			execute(command)
		},
		crate::Backend::Gvisor { path, platform } => {
			crate::gvisor::exec(&prepared, path, *platform)
		},
		crate::Backend::CloudHypervisor(_) | crate::Backend::Seatbelt => {
			Err(tg::error!("the backend does not use Linux resource setup"))
		},
	}
}

fn execute(mut command: std::process::Command) -> tg::Result<()> {
	use std::os::unix::process::CommandExt as _;
	let error = command.exec();
	Err(tg::error!(!error, "failed to execute the sandbox backend"))
}

fn setup_network(arg: &Arg) -> tg::Result<()> {
	match &arg.network {
		Some(Network::Host) => {},
		Some(Network::Pasta { fd } | Network::Veth { fd, .. }) => {
			linux::unshare(
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
		None => linux::unshare(
			libc::CLONE_NEWNET,
			"failed to create the resource network namespace",
		)?,
	}
	Ok(())
}

fn prepare_filesystem(
	arg: &mut Arg,
	host_namespace: Option<&OwnedFd>,
) -> tg::Result<Option<OwnedFd>> {
	let (Some(path), Some(socket)) = (arg.filesystem_path.clone(), arg.filesystem_fd) else {
		return Ok(None);
	};
	let filesystem = crate::filesystem::open(&path)?;
	crate::filesystem::prepare(&filesystem, arg.uid, arg.gid)?;
	arg.mounts.filesystem_mount_fd = Some(filesystem.as_raw_fd());
	let target = crate::filesystem::path(&filesystem);
	for bind in arg.mounts.binds.iter_mut().chain(&mut arg.mounts.ro_binds) {
		crate::filesystem::relocate(&mut bind.source, &path, &target);
	}
	for overlay in &mut arg.mounts.overlays {
		crate::filesystem::relocate(&mut overlay.upperdir, &path, &target);
		crate::filesystem::relocate(&mut overlay.workdir, &path, &target);
	}
	// SAFETY: The inherited filesystem control descriptor is exclusively owned here.
	let socket = unsafe { OwnedFd::from_raw_fd(socket) };
	let host_filesystem = host_namespace
		.map(|namespace| crate::filesystem::host_mount(&filesystem, namespace))
		.transpose()?;
	crate::filesystem::send(&socket, host_filesystem.as_ref().unwrap_or(&filesystem))?;
	Ok(Some(filesystem))
}
