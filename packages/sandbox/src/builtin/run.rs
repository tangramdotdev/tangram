use {
	crate::{builtin::util::CStringVec, linux, mount},
	clap::Parser as _,
	std::{
		collections::BTreeMap,
		ffi::{CString, OsStr, OsString},
		io::Read as _,
		os::{
			fd::{FromRawFd as _, OwnedFd},
			unix::ffi::OsStrExt as _,
		},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

#[derive(clap::Parser)]
#[command(name = "builtin")]
struct Arg {
	#[arg(long)]
	as_pid_1: bool,
	#[arg(long = "bind-fd", action = clap::ArgAction::Append, num_args = 2)]
	bind_fds: Vec<OsString>,
	#[arg(long = "bind", action = clap::ArgAction::Append, num_args = 2)]
	binds: Vec<PathBuf>,
	#[arg(long = "cap-drop", action = clap::ArgAction::Append)]
	cap_drop: Vec<String>,
	#[arg(long, default_value = "/")]
	chdir: PathBuf,
	#[arg(long)]
	clearenv: bool,
	#[arg(trailing_var_arg = true)]
	command: Vec<OsString>,
	#[arg(long = "dev-bind", action = clap::ArgAction::Append, num_args = 2)]
	dev_binds: Vec<PathBuf>,
	#[arg(long = "dev", action = clap::ArgAction::Append)]
	devs: Vec<PathBuf>,
	#[arg(long)]
	die_with_parent: bool,
	#[arg(long)]
	fuse_fd: Option<i32>,
	#[arg(long)]
	fuse_path: Option<PathBuf>,
	#[arg(long)]
	gid: Option<u32>,
	#[arg(long)]
	hostname: Option<String>,
	#[arg(long)]
	new_session: bool,
	#[arg(long = "overlay-src", action = clap::ArgAction::Append)]
	overlay_sources: Vec<PathBuf>,
	#[arg(long = "overlay", action = clap::ArgAction::Append, num_args = 3)]
	overlays: Vec<PathBuf>,
	#[arg(long = "proc", action = clap::ArgAction::Append)]
	procs: Vec<PathBuf>,
	#[arg(long = "ro-bind", action = clap::ArgAction::Append, num_args = 2)]
	ro_binds: Vec<PathBuf>,
	#[arg(long)]
	seccomp: Option<i32>,
	#[arg(long = "setenv", action = clap::ArgAction::Append, num_args = 2)]
	setenvs: Vec<OsString>,
	#[arg(long)]
	share_net: bool,
	#[arg(long = "tmpfs", action = clap::ArgAction::Append)]
	tmpfs: Vec<PathBuf>,
	#[arg(long)]
	uid: Option<u32>,
	#[arg(long)]
	unshare_all: bool,
	#[arg(long)]
	unshare_cgroup: bool,
	#[arg(long)]
	unshare_ipc: bool,
	#[arg(long)]
	unshare_net: bool,
	#[arg(long)]
	unshare_pid: bool,
	#[arg(long)]
	unshare_user: bool,
	#[arg(long)]
	unshare_uts: bool,
	#[arg(long)]
	userns: Option<i32>,
}

pub fn run(arguments: Vec<OsString>) -> tg::Result<u8> {
	let arg = match Arg::try_parse_from(std::iter::once(OsString::from("builtin")).chain(arguments))
	{
		Ok(arg) => arg,
		Err(error)
			if matches!(
				error.kind(),
				clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
			) =>
		{
			error
				.print()
				.map_err(|error| tg::error!(!error, "failed to print the builtin help"))?;
			return Ok(0);
		},
		Err(error) => return Err(tg::error!(!error, "invalid builtin arguments")),
	};
	if arg.command.is_empty() {
		return Err(tg::error!("a command is required"));
	}
	if arg.hostname.is_some() && !arg.unshare_all && !arg.unshare_uts {
		return Err(tg::error!(
			"--hostname requires --unshare-uts or --unshare-all"
		));
	}
	if arg.cap_drop.iter().any(|capability| capability != "ALL") {
		return Err(tg::error!("the builtin supports --cap-drop ALL"));
	}
	if arg.unshare_user && arg.userns.is_some() {
		return Err(tg::error!(
			"--userns and --unshare-user are mutually exclusive"
		));
	}
	if arg.fuse_fd.is_some() != arg.fuse_path.is_some() {
		return Err(tg::error!(
			"--fuse-fd and --fuse-path must be provided together"
		));
	}
	let uid = arg
		.uid
		.unwrap_or_else(|| rustix::process::getuid().as_raw());
	let gid = arg
		.gid
		.unwrap_or_else(|| rustix::process::getgid().as_raw());
	if arg.die_with_parent {
		crate::util::set_parent_death_signal(libc::SIGKILL)?;
	}
	if let Some(fd) = arg.userns {
		if fd < 0 {
			return Err(tg::error!("--userns requires a valid descriptor"));
		}
		let current = std::fs::File::open("/proc/self/ns/user")
			.map_err(|error| tg::error!(!error, "failed to open the user namespace"))?;
		let current = rustix::fs::fstat(&current)
			.map_err(|error| tg::error!(!error, "failed to inspect the user namespace"))?;
		// SAFETY: The descriptor is borrowed for the namespace comparison and remains owned by the caller.
		let borrowed = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
		let selected = rustix::fs::fstat(borrowed)
			.map_err(|error| tg::error!(!error, "failed to inspect the selected user namespace"))?;
		if current.st_ino != selected.st_ino || current.st_dev != selected.st_dev {
			// SAFETY: The descriptor was checked and setns does not retain it.
			if unsafe { libc::setns(fd, libc::CLONE_NEWUSER) } != 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to enter the user namespace"
				));
			}
		}
	} else if arg.unshare_user || arg.unshare_all || !rustix::process::geteuid().is_root() {
		linux::enter_user_namespace(uid, gid)?;
	}
	let mut flags = if arg.fuse_fd.is_some() {
		libc::CLONE_NEWNS
	} else {
		0
	};
	if arg.unshare_all || arg.unshare_ipc {
		flags |= libc::CLONE_NEWIPC;
	}
	if arg.unshare_all || arg.unshare_pid {
		flags |= libc::CLONE_NEWPID;
	}
	if (arg.unshare_all && !arg.share_net) || arg.unshare_net {
		flags |= libc::CLONE_NEWNET;
	}
	if arg.unshare_all || arg.unshare_uts {
		flags |= libc::CLONE_NEWUTS;
	}
	if arg.unshare_all || arg.unshare_cgroup {
		flags |= libc::CLONE_NEWCGROUP;
	}
	linux::unshare(flags, "failed to unshare the builtin namespaces")?;
	if let Some(hostname) = &arg.hostname {
		linux::set_hostname(hostname)?;
	}
	if let (Some(fd), Some(path)) = (arg.fuse_fd, &arg.fuse_path) {
		mount_fuse(fd, path)?;
	}
	let mounts = mounts(&arg)?;
	let root = tangram_util::fs::Temp::new()
		.map_err(|error| tg::error!(!error, "failed to create the builtin root"))?;
	let root_path = root.path().join("root");
	std::fs::create_dir_all(&root_path)
		.map_err(|error| tg::error!(!error, "failed to create the builtin root"))?;

	let parent = arg.die_with_parent.then(linux::open_pidfd).transpose()?;
	let child = linux::fork()?;
	if child == 0 {
		if !arg.as_pid_1 && (arg.unshare_pid || arg.unshare_all) {
			if let Err(error) = reap(&arg, &mounts, &root_path, uid, gid, parent.as_ref()) {
				eprintln!("{}", error.trace());
				std::process::exit(105);
			}
		} else if let Err(error) =
			child_main(&arg, &mounts, Some(&root_path), uid, gid, parent.as_ref())
		{
			eprintln!("{}", error.trace());
			std::process::exit(105);
		}
		std::process::exit(0);
	}
	let status = linux::wait_for_child(child)?;
	Ok(status)
}

pub(crate) fn mount_fuse(fd: i32, path: &Path) -> tg::Result<()> {
	if fd < 0 {
		return Err(tg::error!("the FUSE mount requires a valid descriptor"));
	}
	// SAFETY: The control descriptor is transferred by the launcher and exclusively owned here.
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };
	tangram_vfs::fuse::mount_dev_fuse(&fd, path)
		.map_err(|error| tg::error!(!error, "failed to mount the FUSE filesystem"))?;
	Ok(())
}

fn mounts(arg: &Arg) -> tg::Result<mount::Arg> {
	let mut binds = Vec::new();
	for (values, devices) in [(&arg.binds, false), (&arg.dev_binds, true)] {
		for pair in values.as_chunks::<2>().0 {
			binds.push(mount::Bind {
				devices,
				fd: None,
				source: pair[0].clone(),
				target: pair[1].clone(),
			});
		}
	}
	for pair in arg.bind_fds.as_chunks::<2>().0 {
		let fd = pair[0]
			.to_str()
			.and_then(|value| value.parse::<i32>().ok())
			.filter(|fd| *fd >= 0)
			.ok_or_else(|| tg::error!("--bind-fd requires a valid descriptor"))?;
		binds.push(mount::Bind {
			devices: false,
			fd: Some(fd),
			source: PathBuf::new(),
			target: pair[1].clone().into(),
		});
	}
	let ro_binds = arg
		.ro_binds
		.as_chunks::<2>()
		.0
		.iter()
		.map(|pair| mount::Bind {
			devices: false,
			fd: None,
			source: pair[0].clone(),
			target: pair[1].clone(),
		})
		.collect();
	let overlays = arg
		.overlays
		.as_chunks::<3>()
		.0
		.iter()
		.map(|triple| mount::Overlay {
			target: triple[2].clone(),
			upperdir: triple[0].clone(),
			workdir: triple[1].clone(),
		})
		.collect();
	let mounts = mount::Arg {
		binds,
		devs: arg.devs.clone(),
		filesystem_mount_fd: None,
		nodev: true,
		nosuid: true,
		overlay_sources: arg.overlay_sources.clone(),
		overlays,
		procs: arg.procs.clone(),
		ro_binds,
		tmpfs: arg.tmpfs.clone(),
	};
	Ok(mounts)
}

fn reap(
	arg: &Arg,
	mounts: &mount::Arg,
	root: &Path,
	uid: u32,
	gid: u32,
	parent: Option<&OwnedFd>,
) -> tg::Result<()> {
	// Protect the reaper's host filesystem and inherited descriptors from the workload.
	// SAFETY: prctl takes scalar arguments and the operation does not retain pointers.
	if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } != 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to protect the builtin reaper"
		));
	}
	if arg.die_with_parent {
		crate::util::set_parent_death_signal(libc::SIGKILL)?;
	}
	let child = linux::fork()?;
	if child == 0 {
		child_main(arg, mounts, Some(root), uid, gid, parent)?;
		std::process::exit(0);
	}
	loop {
		let mut status = 0;
		// SAFETY: The status pointer is valid for the duration of the waitpid call.
		let reaped = unsafe { libc::waitpid(-1, &raw mut status, 0) };
		if reaped < 0 {
			let error = std::io::Error::last_os_error();
			if error.kind() == std::io::ErrorKind::Interrupted {
				continue;
			}
			return Err(tg::error!(!error, "failed to reap the builtin workload"));
		}
		if reaped == child {
			let code = if libc::WIFEXITED(status) {
				libc::WEXITSTATUS(status)
			} else {
				(128 + libc::WTERMSIG(status)).min(255)
			};
			std::process::exit(code);
		}
	}
}

fn child_main(
	arg: &Arg,
	mounts: &mount::Arg,
	root: Option<&Path>,
	uid: u32,
	gid: u32,
	parent: Option<&OwnedFd>,
) -> tg::Result<()> {
	linux::unshare(
		libc::CLONE_NEWNS,
		"failed to create the builtin mount namespace",
	)?;
	if arg.die_with_parent {
		crate::util::set_parent_death_signal(libc::SIGKILL)?;
	}
	if arg.new_session {
		linux::start_session()?;
	}
	if let Some(root) = root
		&& !mounts
			.binds
			.iter()
			.chain(&mounts.ro_binds)
			.any(|bind| bind.target == Path::new("/"))
		&& !mounts
			.overlays
			.iter()
			.any(|overlay| overlay.target == Path::new("/"))
	{
		mount::mount_tmpfs(root)?;
	}

	mount::apply(mounts, root)?;
	if let Some(root) = root {
		mount::pivot_root_into(root)?;
	}
	mount::change_directory(&arg.chdir)?;
	let filter = arg.seccomp.map(read_filter).transpose()?;
	linux::set_securebits()?;
	linux::setresgid(gid)?;
	linux::setresuid(uid)?;
	if arg.die_with_parent {
		crate::util::set_parent_death_signal(libc::SIGKILL)?;
		linux::validate_parent(parent.unwrap())?;
	}
	linux::set_no_new_privs()?;
	linux::drop_capabilities()?;
	linux::close_non_std_fds()?;
	if let Some(filter) = filter {
		crate::seccomp::install_filter(filter)?;
	}
	exec(arg)
}

fn read_filter(fd: i32) -> tg::Result<Vec<libc::sock_filter>> {
	if fd < 0 {
		return Err(tg::error!("--seccomp requires a valid descriptor"));
	}
	// SAFETY: The filter descriptor is transferred by the launcher and exclusively owned here.
	let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
	let mut bytes = Vec::new();
	file.read_to_end(&mut bytes)
		.map_err(|error| tg::error!(!error, "failed to read the seccomp filter"))?;
	if bytes.is_empty() || bytes.len() % std::mem::size_of::<libc::sock_filter>() != 0 {
		return Err(tg::error!("invalid seccomp filter size"));
	}
	let filter = bytes
		.as_chunks::<8>()
		.0
		.iter()
		.map(|bytes| libc::sock_filter {
			code: u16::from_ne_bytes([bytes[0], bytes[1]]),
			jt: bytes[2],
			jf: bytes[3],
			k: u32::from_ne_bytes(bytes[4..8].try_into().unwrap()),
		})
		.collect();
	Ok(filter)
}

fn exec(arg: &Arg) -> tg::Result<()> {
	let executable = CString::new(arg.command[0].as_bytes())
		.map_err(|error| tg::error!(!error, "failed to encode the executable"))?;
	let argv = arg
		.command
		.iter()
		.map(|value| {
			CString::new(value.as_bytes())
				.map_err(|error| tg::error!(!error, "failed to encode an argument"))
		})
		.collect::<tg::Result<CStringVec>>()?;
	let mut environment: BTreeMap<OsString, OsString> = if arg.clearenv {
		BTreeMap::new()
	} else {
		std::env::vars_os().collect()
	};
	for pair in arg.setenvs.as_chunks::<2>().0 {
		environment.insert(pair[0].clone(), pair[1].clone());
	}
	let envp = environment
		.iter()
		.map(|(key, value)| envstring(key, value))
		.collect::<tg::Result<CStringVec>>()?;
	// SAFETY: The terminated strings and pointer arrays remain live until exec replaces the process.
	unsafe {
		libc::execvpe(
			executable.as_ptr(),
			argv.as_ptr().cast(),
			envp.as_ptr().cast(),
		);
	}
	Err(tg::error!(
		source = std::io::Error::last_os_error(),
		"failed to execute the command"
	))
}

fn envstring(key: &OsStr, value: &OsStr) -> tg::Result<CString> {
	let mut bytes = key.as_bytes().to_vec();
	bytes.push(b'=');
	bytes.extend_from_slice(value.as_bytes());
	let value = CString::new(bytes)
		.map_err(|error| tg::error!(!error, "failed to encode an environment entry"))?;
	Ok(value)
}
