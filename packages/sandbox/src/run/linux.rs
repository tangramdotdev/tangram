use {
	super::validate::validate,
	crate::builtin::util::CStringVec,
	crate::cgroup,
	crate::linux::{
		close_non_std_fds, drop_capabilities, enter_user_namespace,
		enter_user_namespace_with_parent, fork, open_pidfd, set_hostname, set_no_new_privs,
		set_rlimit_nofile, set_securebits, setresgid, setresuid, start_session, unshare,
		validate_parent, wait_for_child,
	},
	crate::{mount, seccomp},
	std::{
		collections::BTreeMap,
		ffi::{CString, OsStr, OsString},
		io::{Read as _, Write as _},
		net::Ipv4Addr,
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd},
			unix::{ffi::OsStrExt as _, net::UnixStream},
		},
		path::PathBuf,
		process::ExitCode,
	},
	tangram_client::prelude::*,
};

// The Linux clone3 ABI defines CLONE_INTO_CGROUP as bit 33.
const CLONE_INTO_CGROUP: u64 = 1 << 33;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub as_pid_1: bool,
	pub backend: crate::Kind,
	pub binds: Vec<Bind>,
	pub cgroup: Option<String>,
	pub cgroup_cpu: Option<u64>,
	pub cgroup_entered: bool,
	pub cgroup_fd: Option<i32>,
	pub cgroup_memory: Option<u64>,
	pub cgroup_memory_oom_group: bool,
	pub cgroup_memory_swap: Option<u64>,
	pub cgroup_pids: Option<u64>,
	pub cgroup_readonly: bool,
	pub chdir: PathBuf,
	pub clearenv: bool,
	pub command: Vec<OsString>,
	pub devs: Vec<PathBuf>,
	pub die_with_parent: bool,
	pub filesystem_fd: Option<i32>,
	pub filesystem_inodes: Option<u64>,
	pub filesystem_mount_fd: Option<i32>,
	pub filesystem_path: Option<PathBuf>,
	pub filesystem_size: Option<u64>,
	pub fuse_fd: Option<i32>,
	pub fuse_path: Option<PathBuf>,
	pub gateway_ip: Option<Ipv4Addr>,
	pub gid: libc::gid_t,
	pub guest_ip: Option<Ipv4Addr>,
	pub hostname: Option<String>,
	pub index: u64,
	pub network: Option<Network>,
	pub network_fd: Option<i32>,
	pub new_session: bool,
	pub nice: u8,
	pub overlay_sources: Vec<PathBuf>,
	pub overlays: Vec<Overlay>,
	pub platform: crate::Platform,
	pub procs: Vec<PathBuf>,
	pub rlimit_nofile: Option<u64>,
	pub ro_binds: Vec<Bind>,
	pub runtime_path: Option<PathBuf>,
	pub seccomp: Option<crate::SeccompPolicy>,
	pub setenvs: Vec<SetEnv>,
	pub tmpfs: Vec<PathBuf>,
	pub uid: libc::uid_t,
	pub unshare_all: bool,
	pub user_namespace_fd: Option<i32>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Bind {
	pub source: PathBuf,
	pub target: PathBuf,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Network {
	Host,
	Pasta,
	Veth,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Overlay {
	pub target: PathBuf,
	pub upperdir: PathBuf,
	pub workdir: PathBuf,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SetEnv {
	pub key: String,
	pub value: String,
}

pub fn run(mut arg: Arg) -> tg::Result<ExitCode> {
	validate(&arg)?;
	if arg.die_with_parent {
		crate::util::set_parent_death_signal(libc::SIGKILL)?;
	}

	let cgroup = arg
		.cgroup
		.as_deref()
		.map(|name| {
			let entry = cgroup::Options {
				cpu: arg.cgroup_cpu,
				cpu_parent: None,
				cpu_pool: None,
				cpu_request: None,
				memory: arg.cgroup_memory.map(Into::into),
				memory_oom_group: arg.cgroup_memory_oom_group,
				memory_swap: arg.cgroup_memory_swap,
				pids: arg.cgroup_pids,
				..Default::default()
			};
			cgroup::Cgroup::new(name, &entry)
		})
		.transpose()
		.map_err(|error| tg::error!(!error, "failed to create the cgroup"))?;
	let cgroup_handle = if let Some(fd) = arg.cgroup_fd {
		if fd < 0 {
			return Err(tg::error!(fd = %fd, "the cgroup requires a valid descriptor"));
		}
		// SAFETY: The descriptor is inherited from the parent and exclusively owned here.
		Some(unsafe { cgroup::Handle::from_raw_fd(fd) })
	} else {
		cgroup.as_ref().map(cgroup::Cgroup::handle).transpose()?
	};
	let mut _filesystem = None;
	if arg.unshare_all {
		if let Some(fd) = arg.user_namespace_fd {
			enter_user_namespace_with_parent(fd)?;
		} else {
			enter_user_namespace(arg.uid, arg.gid)?;
		}
		// Create the host mapping before entering the PID namespace and dropping setup privileges.
		let host_namespace = if arg.user_namespace_fd.is_some() && arg.filesystem_fd.is_some() {
			Some(crate::filesystem::host_namespace(arg.uid, arg.gid)?)
		} else {
			None
		};
		match &arg.network {
			Some(Network::Host) => (),
			Some(Network::Pasta) => {
				// Unshare the network namespace before synchronizing with the host. The child
				// first signals readiness, then waits for the parent to start pasta. Pasta
				// installs eth0, the addresses, and the default route, so no netlink setup is
				// needed here.
				unshare(
					libc::CLONE_NEWNET,
					"failed to unshare the network namespace",
				)?;
				let network_fd = arg
					.network_fd
					.ok_or_else(|| tg::error!("pasta networking requires a sync fd"))?;
				if network_fd < 0 {
					return Err(tg::error!(
						fd = %network_fd,
						"pasta networking requires a valid sync fd"
					));
				}
				let network_fd = unsafe { OwnedFd::from_raw_fd(network_fd) };
				let mut socket = UnixStream::from(network_fd);
				socket
					.write_all(&[0u8])
					.map_err(|error| tg::error!(!error, "failed to signal ready to the host"))?;
				let mut buf = [0u8; 1];
				socket
					.read_exact(&mut buf)
					.map_err(|error| tg::error!(!error, "failed to wait for go from the host"))?;
			},
			Some(Network::Veth) => {
				unshare(
					libc::CLONE_NEWNET,
					"failed to unshare the network namespace",
				)?;
				let network_fd = arg
					.network_fd
					.ok_or_else(|| tg::error!("veth networking requires a sync fd"))?;
				if network_fd < 0 {
					return Err(tg::error!(
						fd = %network_fd,
						"veth networking requires a valid sync fd"
					));
				}
				let network_fd = unsafe { OwnedFd::from_raw_fd(network_fd) };
				let mut socket = UnixStream::from(network_fd);
				socket
					.write_all(&[0u8])
					.map_err(|error| tg::error!(!error, "failed to signal ready to the host"))?;
				let mut buf = [0u8; 1];
				socket
					.read_exact(&mut buf)
					.map_err(|error| tg::error!(!error, "failed to wait for go from the host"))?;
				let guest_ip = arg
					.guest_ip
					.ok_or_else(|| tg::error!("veth networking requires a guest ip"))?;
				let gateway_ip = arg
					.gateway_ip
					.ok_or_else(|| tg::error!("veth networking requires a gateway ip"))?;
				let suffix = format!("{:09}", arg.index % 1_000_000_000);
				let guest_name = format!("tg-vc-{suffix}");
				let mut nl = crate::netlink::Netlink::new()?;
				nl.link_rename(&guest_name, "eth0")?;
				crate::network::host::disable_ipv6("eth0")?;
				nl.addr_add_v4("eth0", guest_ip, 16)?;
				nl.link_set_up("eth0")?;
				nl.link_set_up("lo")?;
				nl.route_add_default_v4(gateway_ip)?;
			},
			None => {
				unshare(
					libc::CLONE_NEWNET,
					"failed to unshare the network namespace",
				)?;
			},
		}
		let mut flags = libc::CLONE_NEWNS | libc::CLONE_NEWIPC;
		if arg.as_pid_1 {
			flags |= libc::CLONE_NEWPID;
		}
		if arg.hostname.is_some() {
			flags |= libc::CLONE_NEWUTS;
		}
		unshare(flags, "failed to unshare the sandbox namespaces")?;
		_filesystem = prepare_filesystem(&mut arg, host_namespace.as_ref())?;

		// Mount the FUSE filesystem in the new namespaces and send its descriptor to the host.
		if let Some(fuse_fd) = arg.fuse_fd {
			let path = arg
				.fuse_path
				.as_ref()
				.ok_or_else(|| tg::error!("the FUSE mount requires a path"))?;
			if fuse_fd < 0 {
				return Err(tg::error!(fd = %fuse_fd, "the FUSE mount requires a valid socket fd"));
			}
			// SAFETY: The descriptor is inherited from the host and is exclusively owned here.
			let sendfd = unsafe { OwnedFd::from_raw_fd(fuse_fd) };
			tangram_vfs::fuse::mount_dev_fuse(&sendfd, path)
				.map_err(|error| tg::error!(!error, "failed to mount the FUSE filesystem"))?;
		}
	}

	if let Some(hostname) = &arg.hostname {
		set_hostname(hostname)?;
	}

	let root = prepare_root(&arg)?;
	let root_path = root.as_ref().map(|root| root.path().join("root"));

	let parent = arg.die_with_parent.then(open_pidfd).transpose()?;
	let (child, move_child_to_cgroup) = if arg.cgroup_entered {
		(fork()?, false)
	} else if let Some(cgroup) = &cgroup_handle {
		fork_with_cgroup(cgroup)?
	} else {
		(fork()?, false)
	};
	if child == 0 {
		match child_main(
			&arg,
			cgroup_handle.as_ref(),
			move_child_to_cgroup,
			parent.as_ref(),
			root_path.as_ref(),
		) {
			Ok(()) => std::process::exit(0),
			Err(error) => {
				eprintln!("{error}");
				eprintln!("{}", error.trace());
				std::process::exit(105);
			},
		}
	}
	let status = wait_for_child(child);
	// Detach the prepared root before removing the scratch directory so its writable layer is retained.
	if arg.backend != crate::Kind::Builtin
		&& let Some(root) = &root_path
	{
		let root = CString::new(root.as_os_str().as_bytes()).unwrap();
		// SAFETY: The pointer references a valid, terminated path in the launcher's mount namespace.
		let result = unsafe { libc::umount2(root.as_ptr(), libc::MNT_DETACH) };
		if result != 0 {
			let error = std::io::Error::last_os_error();
			if !matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOENT)) {
				return Err(tg::error!(
					!error,
					"failed to detach the external backend root"
				));
			}
		}
	}
	let cleanup = cgroup.map(cgroup::Cgroup::cleanup).transpose();
	let status = status?;
	cleanup?;
	Ok(ExitCode::from(status))
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
	arg.filesystem_mount_fd = Some(filesystem.as_raw_fd());
	let target = crate::filesystem::path(&filesystem);
	for bind in arg.binds.iter_mut().chain(&mut arg.ro_binds) {
		crate::filesystem::relocate(&mut bind.source, &path, &target);
	}
	for overlay in &mut arg.overlays {
		crate::filesystem::relocate(&mut overlay.upperdir, &path, &target);
		crate::filesystem::relocate(&mut overlay.workdir, &path, &target);
	}
	// SAFETY: The descriptor is inherited from the parent and is exclusively owned here.
	let socket = unsafe { OwnedFd::from_raw_fd(socket) };
	let host_filesystem = host_namespace
		.map(|namespace| crate::filesystem::host_mount(&filesystem, namespace))
		.transpose();
	let host_filesystem = match host_filesystem {
		Ok(host_filesystem) => host_filesystem,
		Err(error) => {
			std::fs::remove_dir_all(&path).map_err(|cleanup_error| {
				tg::error!(
					source = cleanup_error,
					%error,
					path = %path.display(),
					"failed to clean up the sandbox filesystem after mapping it for the host"
				)
			})?;
			return Err(error);
		},
	};
	crate::filesystem::send(&socket, host_filesystem.as_ref().unwrap_or(&filesystem))?;

	Ok(Some(filesystem))
}

fn fork_with_cgroup(cgroup: &cgroup::Handle) -> tg::Result<(libc::pid_t, bool)> {
	let cgroup_fd = cgroup.open_fd()?;
	match fork_into_cgroup(cgroup_fd.as_raw_fd()) {
		Ok(pid) => Ok((pid, false)),
		Err(source) if clone3_cgroup_should_fallback(&source) => fork().map(|pid| (pid, true)),
		Err(error) => Err(tg::error!(!error, "failed to clone3 into the cgroup")),
	}
}

fn fork_into_cgroup(cgroup_fd: RawFd) -> std::io::Result<libc::pid_t> {
	let mut args: libc::clone_args = unsafe { std::mem::zeroed() };
	args.flags = CLONE_INTO_CGROUP;
	args.exit_signal = libc::SIGCHLD as u64;
	args.cgroup = cgroup_fd.try_into().unwrap();
	let ret = unsafe {
		libc::syscall(
			libc::SYS_clone3,
			&raw const args,
			std::mem::size_of::<libc::clone_args>(),
		)
	};
	if ret < 0 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(ret.try_into().unwrap())
}

fn clone3_cgroup_should_fallback(source: &std::io::Error) -> bool {
	matches!(
		source.raw_os_error(),
		Some(libc::ENOSYS | libc::EINVAL | libc::EPERM)
	)
}

fn child_main(
	arg: &Arg,
	cgroup: Option<&cgroup::Handle>,
	move_self: bool,
	parent: Option<&OwnedFd>,
	root: Option<&PathBuf>,
) -> tg::Result<()> {
	unsafe {
		*libc::__errno_location() = 0;
	}
	let result = unsafe { libc::nice(libc::c_int::from(arg.nice)) };
	if result == -1 {
		let error = std::io::Error::last_os_error();
		if error.raw_os_error() != Some(0) {
			return Err(tg::error!(
				!error,
				"failed to lower the sandbox cpu priority"
			));
		}
	}
	if arg.die_with_parent {
		crate::util::set_parent_death_signal(libc::SIGKILL)?;
	}
	if let Some(cgroup) = cgroup {
		if move_self {
			cgroup.move_self()?;
		}
		// Unshare the cgroup namespace only once the process is in its cgroup, so that cgroup becomes the namespace root and the mount below exposes nothing above it.
		unshare(
			libc::CLONE_NEWCGROUP,
			"failed to unshare the cgroup namespace",
		)?;
	}
	if arg.new_session {
		start_session()?;
	}
	set_rlimit_nofile(arg.rlimit_nofile)?;
	mount::apply(arg, root.map(PathBuf::as_path))?;
	if arg.backend != crate::Kind::Builtin {
		unshare(
			libc::CLONE_NEWNS,
			"failed to isolate the external backend mounts",
		)?;
		close_non_std_fds()?;
		let root = root.ok_or_else(|| {
			tg::error!("the external backend requires a prepared root filesystem")
		})?;
		return match arg.backend {
			crate::Kind::Bubblewrap => crate::bubblewrap::exec(arg, root),
			crate::Kind::Gvisor => crate::gvisor::exec(arg, root),
			crate::Kind::Builtin | crate::Kind::CloudHypervisor | crate::Kind::Seatbelt => {
				Err(tg::error!("invalid linux launch backend"))
			},
		};
	}
	if let Some(root) = root {
		mount::pivot_root_into(root)?;
	}
	mount::change_directory(&arg.chdir)?;
	set_rlimit_nofile(arg.rlimit_nofile)?;
	set_securebits()?;
	setresgid(arg.gid)?;
	setresuid(arg.uid)?;
	if arg.die_with_parent {
		crate::util::set_parent_death_signal(libc::SIGKILL)?;
		// The launcher is outside the PID namespace, so getppid cannot detect its death during the credential change.
		validate_parent(parent.unwrap())?;
	}
	set_no_new_privs()?;
	drop_capabilities()?;
	close_non_std_fds()?;
	if let Some(policy) = arg.seccomp {
		seccomp::install(policy)?;
	}
	exec_command(arg)
}

fn prepare_root(arg: &Arg) -> tg::Result<Option<tangram_util::fs::Temp>> {
	if !arg
		.overlays
		.iter()
		.any(|overlay| overlay.target == std::path::Path::new("/"))
	{
		return Ok(None);
	}
	let path = tangram_util::fs::Temp::new()
		.map_err(|error| tg::error!(!error, "failed to create the scratch path"))?;
	std::fs::create_dir(path.path())
		.map_err(|error| tg::error!(!error, "failed to create the scratch path"))?;
	let root = path.path().join("root");
	std::fs::remove_dir_all(&root).ok();
	std::fs::create_dir_all(&root).map_err(|error| {
		tg::error!(
			!error,
			path = %root.display(),
			"failed to create the sandbox root"
		)
	})?;
	Ok(Some(path))
}

fn exec_command(arg: &Arg) -> tg::Result<()> {
	let executable = CString::new(arg.command[0].as_os_str().as_bytes())
		.map_err(|error| tg::error!(!error, "failed to encode the executable"))?;
	let argv = arg
		.command
		.iter()
		.map(|arg| {
			CString::new(arg.as_os_str().as_bytes())
				.map_err(|error| tg::error!(!error, "failed to encode an argument"))
		})
		.collect::<Result<CStringVec, _>>()?;
	let envp = environment(arg)?
		.into_iter()
		.map(|(key, value)| envstring(key.as_os_str(), value.as_os_str()))
		.collect::<Result<CStringVec, _>>()?;
	unsafe {
		libc::execvpe(
			executable.as_ptr(),
			argv.as_ptr().cast(),
			envp.as_ptr().cast(),
		);
	}
	let error = std::io::Error::last_os_error();
	Err(tg::error!(!error, "failed to execute the command"))
}

pub(crate) fn environment(arg: &Arg) -> tg::Result<BTreeMap<OsString, OsString>> {
	let mut env = if arg.clearenv {
		BTreeMap::new()
	} else {
		std::env::vars_os().collect()
	};
	for setenv in &arg.setenvs {
		let key = OsString::from(&setenv.key);
		let value = OsString::from(&setenv.value);
		if key.as_os_str().as_bytes().contains(&0) || value.as_os_str().as_bytes().contains(&0) {
			return Err(tg::error!("environment entries may not contain NUL bytes"));
		}
		env.insert(key, value);
	}
	Ok(env)
}

fn envstring(key: &OsStr, value: &OsStr) -> tg::Result<CString> {
	let mut bytes = key.as_bytes().to_vec();
	bytes.push(b'=');
	bytes.extend_from_slice(value.as_bytes());
	CString::new(bytes).map_err(|error| tg::error!(!error, "failed to encode an environment entry"))
}

impl std::str::FromStr for Network {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"host" => Ok(Self::Host),
			"pasta" => Ok(Self::Pasta),
			"veth" => Ok(Self::Veth),
			s => Err(tg::error!(option = %s, "unknown network option")),
		}
	}
}
