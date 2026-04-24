use {
	super::{cgroup, mount, util::CStringVec, validate::validate},
	num::ToPrimitive,
	std::{
		collections::BTreeMap,
		ffi::{CString, OsStr, OsString},
		io::{Read as _, Write as _},
		net::Ipv4Addr,
		os::{
			fd::{FromRawFd as _, OwnedFd, RawFd},
			unix::{ffi::OsStrExt as _, net::UnixStream},
		},
		path::PathBuf,
		process::ExitCode,
	},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Arg {
	pub as_pid_1: bool,
	pub binds: Vec<Bind>,
	pub bridge_fd: Option<i32>,
	pub bridge_ip: Option<Ipv4Addr>,
	pub cgroup: Option<String>,
	pub cgroup_cpu: Option<u64>,
	pub cgroup_memory: Option<u64>,
	pub cgroup_memory_oom_group: bool,
	pub chdir: PathBuf,
	pub command: Vec<OsString>,
	pub devs: Vec<PathBuf>,
	pub die_with_parent: bool,
	pub gid: libc::gid_t,
	pub guest_ip: Option<Ipv4Addr>,
	pub hostname: Option<String>,
	pub id: tg::sandbox::Id,
	pub net: Net,
	pub new_session: bool,
	pub overlay_sources: Vec<PathBuf>,
	pub overlays: Vec<Overlay>,
	pub procs: Vec<PathBuf>,
	pub ro_binds: Vec<Bind>,
	pub setenvs: Vec<SetEnv>,
	pub tmpfs: Vec<PathBuf>,
	pub uid: libc::uid_t,
	pub unshare_all: bool,
}

#[derive(Clone, Debug)]
pub struct Bind {
	pub source: PathBuf,
	pub target: PathBuf,
}

#[derive(Clone, Debug)]
pub enum Net {
	None,
	Host,
	Bridge(String),
}

#[derive(Clone, Debug)]
pub struct Overlay {
	pub target: PathBuf,
	pub upperdir: PathBuf,
	pub workdir: PathBuf,
}

#[derive(Clone, Debug)]
pub struct SetEnv {
	pub key: String,
	pub value: String,
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	validate(arg)?;
	if arg.die_with_parent {
		set_parent_death_signal(libc::SIGKILL)?;
	}
	let cgroup = arg
		.cgroup
		.as_deref()
		.map(|name| {
			cgroup::create(
				name,
				arg.cgroup_cpu,
				arg.cgroup_memory,
				arg.cgroup_memory_oom_group,
			)
		})
		.transpose()
		.map_err(|source| tg::error!(!source, "failed to create the cgroup"))?;

	if let Some(cgroup) = &cgroup {
		cgroup::move_self(cgroup)?;
	}

	if arg.unshare_all {
		enter_user_namespace(arg.uid, arg.gid)?;
		match &arg.net {
			Net::Host => (), /* share networking */
			Net::None => {
				unshare(
					libc::CLONE_NEWNET,
					"failed to unshare the network namespace",
				)?;
			},
			Net::Bridge(_name) => {
				unshare(
					libc::CLONE_NEWNET,
					"failed to unshare the network namespace",
				)?;
				let bridge_fd = arg
					.bridge_fd
					.ok_or_else(|| tg::error!("bridge networking requires a sync fd"))?;
				let bridge_fd = unsafe { OwnedFd::from_raw_fd(bridge_fd) };
				let mut socket = UnixStream::from(bridge_fd);
				socket
					.write_all(&[0u8])
					.map_err(|source| tg::error!(!source, "failed to signal ready to the host"))?;
				let mut buf = [0u8; 1];
				socket
					.read_exact(&mut buf)
					.map_err(|source| tg::error!(!source, "failed to wait for go from the host"))?;
				let guest_ip = arg
					.guest_ip
					.ok_or_else(|| tg::error!("bridge networking requires a guest ip"))?;
				let bridge_ip = arg
					.bridge_ip
					.ok_or_else(|| tg::error!("bridge networking requires a bridge ip"))?;
				let id_str = arg.id.to_string();
				let truncated = &id_str[..9];
				let guest_name = format!("tg-vc-{truncated}");
				let mut nl = crate::netlink::Netlink::new()?;
				nl.link_rename(&guest_name, "eth0")?;
				nl.addr_add_v4("eth0", guest_ip, 16)?;
				nl.link_set_up("eth0")?;
				nl.link_set_up("lo")?;
				nl.route_add_default_v4(bridge_ip)?;
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
	}

	if let Some(hostname) = &arg.hostname {
		set_hostname(hostname)?;
	}

	let root = prepare_root(arg)?;
	let root_path = root.as_ref().map(|root| root.path().join("root"));

	let child = unsafe { libc::fork() };
	if child < 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to fork the sandbox child"));
	}
	if child == 0 {
		match child_main(arg, root_path.as_ref()) {
			Ok(()) => std::process::exit(0),
			Err(error) => {
				eprintln!("{error}");
				eprintln!("{}", error.trace());
				std::process::exit(105);
			},
		}
	}

	let status = wait_for_child(child)?;
	if let Some(cgroup) = &cgroup {
		cgroup::remove(cgroup);
	}
	Ok(ExitCode::from(status))
}

fn child_main(arg: &Arg, root: Option<&PathBuf>) -> tg::Result<()> {
	if arg.die_with_parent {
		set_parent_death_signal(libc::SIGKILL)?;
	}
	if arg.new_session {
		start_session()?;
	}
	mount::apply(arg, root.map(PathBuf::as_path))?;
	if let Some(root) = root {
		mount::pivot_root_into(root)?;
	}
	mount::change_directory(&arg.chdir)?;
	set_securebits()?;
	setresgid(arg.gid)?;
	setresuid(arg.uid)?;
	set_no_new_privs()?;
	drop_capabilities()?;
	close_non_std_fds()?;
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
		.map_err(|source| tg::error!(!source, "failed to create the scratch path"))?;
	std::fs::create_dir(path.path())
		.map_err(|source| tg::error!(!source, "failed to create the scratch path"))?;
	let root = path.path().join("root");
	std::fs::remove_dir_all(&root).ok();
	std::fs::create_dir_all(&root).map_err(|source| {
		tg::error!(
			!source,
			path = %root.display(),
			"failed to create the sandbox root"
		)
	})?;
	Ok(Some(path))
}

fn enter_user_namespace(uid: libc::uid_t, gid: libc::gid_t) -> tg::Result<()> {
	let host_uid = unsafe { libc::getuid() };
	let host_gid = unsafe { libc::getgid() };
	unshare(libc::CLONE_NEWUSER, "failed to unshare the user namespace")?;
	std::fs::write("/proc/self/uid_map", format!("{uid} {host_uid} 1\n"))
		.map_err(|source| tg::error!(!source, "failed to write the uid map"))?;
	std::fs::write("/proc/self/setgroups", "deny")
		.map_err(|source| tg::error!(!source, "failed to deny setgroups"))?;
	std::fs::write("/proc/self/gid_map", format!("{gid} {host_gid} 1\n"))
		.map_err(|source| tg::error!(!source, "failed to write the gid map"))?;
	Ok(())
}

fn exec_command(arg: &Arg) -> tg::Result<()> {
	let executable = CString::new(arg.command[0].as_os_str().as_bytes())
		.map_err(|source| tg::error!(!source, "failed to encode the executable"))?;
	let argv = arg
		.command
		.iter()
		.map(|arg| {
			CString::new(arg.as_os_str().as_bytes())
				.map_err(|source| tg::error!(!source, "failed to encode an argument"))
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
	let source = std::io::Error::last_os_error();
	Err(tg::error!(!source, "failed to execute the command"))
}

fn environment(arg: &Arg) -> tg::Result<BTreeMap<OsString, OsString>> {
	let mut env = std::env::vars_os().collect::<BTreeMap<_, _>>();
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
	CString::new(bytes)
		.map_err(|source| tg::error!(!source, "failed to encode an environment entry"))
}

fn unshare(flags: libc::c_int, message: &'static str) -> tg::Result<()> {
	let result = unsafe { libc::unshare(flags) };
	if result != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "{}", message));
	}
	Ok(())
}

fn set_hostname(hostname: &str) -> tg::Result<()> {
	let result = unsafe { libc::sethostname(hostname.as_ptr().cast(), hostname.len()) };
	if result != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to set the hostname"));
	}
	Ok(())
}

fn set_parent_death_signal(signal: libc::c_int) -> tg::Result<()> {
	let result = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, signal, 0, 0, 0) };
	if result != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to set the parent death signal"));
	}
	Ok(())
}

fn start_session() -> tg::Result<()> {
	let ret = unsafe { libc::setsid() };
	if ret < 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to start a new session"));
	}
	Ok(())
}

fn setresuid(uid: libc::uid_t) -> tg::Result<()> {
	let ret = unsafe { libc::setresuid(uid, uid, uid) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, uid = %uid, "failed to set the uid"));
	}
	Ok(())
}

fn setresgid(gid: libc::gid_t) -> tg::Result<()> {
	let ret = unsafe { libc::setresgid(gid, gid, gid) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, gid = %gid, "failed to set the gid"));
	}
	Ok(())
}

fn set_no_new_privs() -> tg::Result<()> {
	let ret = unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to set no_new_privs"));
	}
	Ok(())
}

fn set_securebits() -> tg::Result<()> {
	const SECBIT_NOROOT: libc::c_int = 1 << 0;
	const SECBIT_NOROOT_LOCKED: libc::c_int = 1 << 1;
	const SECBIT_NO_SETUID_FIXUP: libc::c_int = 1 << 2;
	const SECBIT_NO_SETUID_FIXUP_LOCKED: libc::c_int = 1 << 3;
	const SECBIT_KEEP_CAPS_LOCKED: libc::c_int = 1 << 5;
	const SECBIT_NO_CAP_AMBIENT_RAISE: libc::c_int = 1 << 6;
	const SECBIT_NO_CAP_AMBIENT_RAISE_LOCKED: libc::c_int = 1 << 7;
	const PR_SET_SECUREBITS: libc::c_int = 28;

	let securebits = SECBIT_NOROOT
		| SECBIT_NOROOT_LOCKED
		| SECBIT_NO_SETUID_FIXUP
		| SECBIT_NO_SETUID_FIXUP_LOCKED
		| SECBIT_KEEP_CAPS_LOCKED
		| SECBIT_NO_CAP_AMBIENT_RAISE
		| SECBIT_NO_CAP_AMBIENT_RAISE_LOCKED;
	let ret = unsafe { libc::prctl(PR_SET_SECUREBITS, securebits, 0, 0, 0) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to set securebits"));
	}
	Ok(())
}

fn drop_capabilities() -> tg::Result<()> {
	#[repr(C)]
	struct CapUserHeader {
		version: u32,
		pid: i32,
	}

	#[repr(C)]
	#[derive(Clone, Copy)]
	struct CapUserData {
		effective: u32,
		permitted: u32,
		inheritable: u32,
	}

	const CAPABILITY_VERSION_3: u32 = 0x2008_0522;
	const PR_CAP_AMBIENT: libc::c_int = 47;
	const PR_CAP_AMBIENT_CLEAR_ALL: libc::c_int = 4;
	const PR_CAPBSET_DROP: libc::c_int = 24;

	let ret = unsafe { libc::prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to clear ambient capabilities"));
	}
	let max = std::fs::read_to_string("/proc/sys/kernel/cap_last_cap")
		.ok()
		.and_then(|contents| contents.trim().parse::<u32>().ok())
		.unwrap_or(63);
	for capability in 0..=max {
		let ret = unsafe { libc::prctl(PR_CAPBSET_DROP, capability, 0, 0, 0) };
		if ret != 0 {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(
				!source,
				capability = %capability,
				"failed to drop a capability from the bounding set"
			));
		}
	}
	let mut header = CapUserHeader {
		version: CAPABILITY_VERSION_3,
		pid: 0,
	};
	let mut data = [CapUserData {
		effective: 0,
		permitted: 0,
		inheritable: 0,
	}; 2];
	let ret = unsafe {
		libc::syscall(
			libc::SYS_capset,
			std::ptr::addr_of_mut!(header),
			data.as_mut_ptr(),
		)
	};
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to drop capabilities"));
	}
	Ok(())
}

fn close_non_std_fds() -> tg::Result<()> {
	const CLOSE_RANGE_UNSHARE: libc::c_uint = 1 << 1;
	let max = libc::c_uint::MAX;
	let result = unsafe { libc::syscall(libc::SYS_close_range, 3u32, max, CLOSE_RANGE_UNSHARE) };
	if result == 0 {
		return Ok(());
	}
	let error = std::io::Error::last_os_error();
	if !matches!(
		error.raw_os_error(),
		Some(libc::ENOSYS | libc::EINVAL | libc::EPERM)
	) {
		return Err(tg::error!(!error, "failed to close extra file descriptors"));
	}
	close_non_std_fds_fallback()
}

fn close_non_std_fds_fallback() -> tg::Result<()> {
	let entries = std::fs::read_dir("/proc/self/fd")
		.map_err(|source| tg::error!(!source, "failed to enumerate file descriptors"))?;
	let mut fds = Vec::new();
	for entry in entries {
		let entry =
			entry.map_err(|source| tg::error!(!source, "failed to read a directory entry"))?;
		let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
			continue;
		};
		let Ok(fd) = name.parse::<RawFd>() else {
			continue;
		};
		if fd <= libc::STDERR_FILENO {
			continue;
		}
		fds.push(fd);
	}
	for fd in fds {
		unsafe {
			libc::close(fd);
		}
	}
	Ok(())
}

fn wait_for_child(child: libc::pid_t) -> tg::Result<u8> {
	unsafe {
		let mut status = 0;
		let result = libc::waitpid(child, std::ptr::addr_of_mut!(status), 0);
		if result < 0 {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(!source, "failed to wait for the sandbox child"));
		}
		if libc::WIFEXITED(status) {
			return Ok(libc::WEXITSTATUS(status).min(255).to_u8().unwrap());
		}
		if libc::WIFSIGNALED(status) {
			let signal = libc::WTERMSIG(status);
			return Ok((128 + signal).min(255).to_u8().unwrap());
		}
		Ok(1)
	}
}

impl std::str::FromStr for Net {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"none" => Ok(Self::None),
			"host" => Ok(Self::Host),
			s if s.starts_with("bridge") => {
				let name = s.split('=').nth(1).unwrap_or("tangram0").to_owned();
				Ok(Self::Bridge(name))
			},
			_ => Err(tg::error!(option = %s, "unknown network option")),
		}
	}
}
