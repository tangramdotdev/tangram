use {
	crate::{Sandbox, container},
	std::{
		collections::hash_map::DefaultHasher,
		ffi::{CStr, CString, OsStr},
		fmt::Write as _,
		hash::{Hash as _, Hasher as _},
		net::Ipv4Addr,
		os::{
			fd::{AsRawFd as _, IntoRawFd as _, RawFd},
			unix::{
				ffi::OsStrExt as _,
				process::{CommandExt as _, ExitStatusExt as _},
			},
		},
		path::{Path, PathBuf},
		process::ExitCode,
		time::Duration,
	},
	tangram_client::prelude::*,
};

pub const CLOUD_HYPERVISOR_VSOCK_SOCKET_NAME: &str = "cloud-hypervisor-vsock.sock";
const HELPER_WAIT_INTERVAL: Duration = Duration::from_millis(10);

#[cfg(target_arch = "aarch64")]
const KERNEL_PATH: &str = "/tmp/tangram-cloud-hypervisor-kernel/Image-arm64";

#[cfg(target_arch = "x86_64")]
const KERNEL_PATH: &str = "/tmp/tangram-cloud-hypervisor-kernel/vmlinux-x86_64";

const ROOTFS_TAG: &str = "root";
const VIRTIOFSD_SOCKET_NAME: &str = "virtiofsd.sock";

#[derive(Clone, Debug)]
pub struct Arg {
	pub artifacts_path: PathBuf,
	pub cpu: Option<u64>,
	pub kernel_path: PathBuf,
	pub hostname: Option<String>,
	pub host_subnet: Ipv4Addr,
	pub memory: Option<u64>,
	pub mounts: Vec<tg::sandbox::Mount>,
	pub network: bool,
	pub path: PathBuf,
	pub rootfs_path: PathBuf,
	pub tangram_path: PathBuf,
	pub url: tangram_uri::Uri,
	pub user: Option<String>,
}

struct User {
	gid: libc::gid_t,
	home: PathBuf,
	name: String,
	uid: libc::uid_t,
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	if arg.cpu == Some(0) {
		return Err(tg::error!("sandbox cpu must be greater than zero"));
	}
	if arg.memory == Some(0) {
		return Err(tg::error!("sandbox memory must be greater than zero"));
	}

	if let Some(hostname) = &arg.hostname
		&& hostname.chars().any(char::is_whitespace)
	{
		return Err(tg::error!(
			%hostname,
			"hostname may not contain whitespace"
		));
	}

	let user = resolve_user(arg.user.as_deref())?;
	prepare_sandbox_directory(&arg.path)?;
	prepare_etc_files(&arg.path, &user)?;

	let virtiofsd_socket_path = host_vm_path_from_root(&arg.path).join(VIRTIOFSD_SOCKET_NAME);
	let socket = std::os::unix::net::UnixListener::bind(&virtiofsd_socket_path)
		.map_err(|source| tg::error!(!source, "failed to open the virtiofsd socket"))?;
	let helper_pid = spawn_virtiofsd_helper(arg, &user, socket.into_raw_fd())?;

	let tap = if arg.network {
		let mut hasher = DefaultHasher::new();
		arg.path.hash(&mut hasher);
		let id = format!("{:x}", hasher.finish());
		Some(crate::network::Tap::new(&id)?)
	} else {
		None
	};
	let network = tap.as_ref().map(|t| crate::vm::Network {
		dns_servers: vec![
			std::net::Ipv4Addr::new(1, 1, 1, 1),
			std::net::Ipv4Addr::new(8, 8, 8, 8),
		],
		gateway_ip: t.host_ip,
		guest_ip: t.guest_ip,
		netmask: t.netmask,
	});

	let mut command = std::process::Command::new("cloud-hypervisor");
	command
		.arg("--kernel")
		.arg(KERNEL_PATH)
		.args(
			arg.cpu
				.map(|cpu| vec!["--cpus".to_owned(), format!("boot={cpu},max={cpu}")])
				.unwrap_or_default(),
		)
		.args(arg.memory.map_or_else(
			|| vec!["--memory".to_owned(), "shared=on".to_owned()],
			|memory| vec!["--memory".to_owned(), format!("size={memory},shared=on")],
		))
		.arg("--cmdline")
		.arg(kernel_cmdline(arg, &user, network.as_ref()))
		.arg("--fs")
		.arg(format!(
			"tag={ROOTFS_TAG},socket={}",
			virtiofsd_socket_path.display()
		))
		.arg("--vsock")
		.arg(format!(
			"cid={},socket={}",
			guest_cid(&arg.path),
			host_vm_path_from_root(&arg.path)
				.join(CLOUD_HYPERVISOR_VSOCK_SOCKET_NAME)
				.display(),
		))
		.arg("--console")
		.arg("off")
		.arg("--serial")
		.arg("file=/dev/fd/2")
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit());
	if let Some(tap) = tap.as_ref() {
		command.arg("--net");
		command.arg(format!("fd={},mac={}", tap.fd.as_raw_fd(), tap.mac));
	}
	let _tap = tap;
	unsafe {
		command.pre_exec(|| set_parent_death_signal_io(libc::SIGKILL));
	}
	let mut child = match command.spawn() {
		Ok(child) => child,
		Err(source) => {
			kill_pid(helper_pid);
			wait_for_pid(helper_pid);
			return Err(tg::error!(!source, "failed to spawn cloud-hypervisor"));
		},
	};

	loop {
		if let Some(status) = child
			.try_wait()
			.map_err(|source| tg::error!(!source, "failed to poll cloud-hypervisor"))?
		{
			kill_pid(helper_pid);
			wait_for_pid(helper_pid);
			let code = status
				.code()
				.or(status.signal().map(|signal| 128 + signal))
				.unwrap_or(1)
				.try_into()
				.unwrap_or(u8::MAX);
			return Ok(ExitCode::from(code));
		}

		if let Some(code) = try_wait_for_pid(helper_pid)? {
			child.kill().ok();
			child.wait().ok();
			return Err(tg::error!(
				helper_status = %code,
				"the virtiofsd helper exited unexpectedly"
			));
		}

		std::thread::sleep(HELPER_WAIT_INTERVAL);
	}
}

fn build_mount_arg(arg: &Arg) -> container::run::Arg {
	let mut binds = vec![
		container::run::Bind {
			source: Sandbox::host_tmp_path_from_root(&arg.path),
			target: Sandbox::guest_tmp_path_from_root(&arg.path),
		},
		container::run::Bind {
			source: Sandbox::host_output_path_from_root(&arg.path),
			target: Sandbox::guest_output_path_from_root(&arg.path),
		},
	];
	let mut ro_binds = vec![
		container::run::Bind {
			source: Sandbox::host_passwd_path_from_root(&arg.path),
			target: "/etc/passwd".into(),
		},
		container::run::Bind {
			source: Sandbox::host_nsswitch_path_from_root(&arg.path),
			target: "/etc/nsswitch.conf".into(),
		},
		container::run::Bind {
			source: arg.artifacts_path.clone(),
			target: Sandbox::guest_artifacts_path_from_host_artifacts_path(&arg.artifacts_path),
		},
		container::run::Bind {
			source: arg.tangram_path.clone(),
			target: Sandbox::guest_libexec_tangram_path(),
		},
	];
	for mount in &arg.mounts {
		let bind = container::run::Bind {
			source: mount.source.clone(),
			target: mount.target.clone(),
		};
		if mount.readonly {
			ro_binds.push(bind);
		} else {
			binds.push(bind);
		}
	}
	container::run::Arg {
		as_pid_1: false,
		binds,
		cgroup: None,
		cgroup_cpu: None,
		cgroup_memory: None,
		cgroup_memory_oom_group: false,
		chdir: "/".into(),
		command: Vec::new(),
		devs: Vec::new(),
		die_with_parent: false,
		gid: 0,
		hostname: None,
		new_session: false,
		overlay_sources: vec![arg.rootfs_path.clone()],
		overlays: vec![container::run::Overlay {
			target: "/".into(),
			upperdir: Sandbox::host_upper_path_from_root(&arg.path),
			workdir: Sandbox::host_work_path_from_root(&arg.path),
		}],
		procs: Vec::new(),
		ro_binds,
		setenvs: Vec::new(),
		share_net: false,
		tmpfs: Vec::new(),
		uid: 0,
		unshare_all: false,
	}
}

fn helper_child_main(arg: &Arg, user: &User, socket: RawFd) -> tg::Result<()> {
	set_parent_death_signal(libc::SIGKILL)?;
	enter_user_namespace(user.uid, user.gid)?;
	unshare(
		libc::CLONE_NEWNS | libc::CLONE_NEWIPC,
		"failed to unshare the sandbox namespaces",
	)?;
	unshare(
		libc::CLONE_NEWNET,
		"failed to unshare the network namespace",
	)?;

	let root = host_vm_root_path_from_root(&arg.path);
	std::fs::remove_dir_all(&root).ok();
	std::fs::create_dir_all(&root)
		.map_err(|source| tg::error!(!source, "failed to create the vm root"))?;

	let mount_arg = build_mount_arg(arg);
	container::mount::apply(&mount_arg, Some(&root))?;

	set_no_new_privs()?;
	setresgid(user.gid)?;
	setresuid(user.uid)?;

	let mut command = std::process::Command::new("virtiofsd");
	command
		.arg("--shared-dir")
		.arg(&root)
		.arg("--fd")
		.arg(socket.to_string())
		.arg("--sandbox")
		.arg("none")
		.arg("--cache")
		.arg("never")
		.arg("--inode-file-handles=never")
		.arg("--xattr")
		.arg("--log-level")
		.arg("warn")
		.env("HOME", &user.home)
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit());
	unsafe {
		command.pre_exec(move || {
			let flags = libc::fcntl(socket, libc::F_GETFD);
			if flags < 0 {
				return Err(std::io::Error::last_os_error());
			}
			if libc::fcntl(socket, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		});
	}
	let error = command.exec();
	Err(tg::error!(!error, "failed to execute virtiofsd"))
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

fn guest_cid(path: &Path) -> u32 {
	let mut hasher = DefaultHasher::new();
	path.hash(&mut hasher);
	let [a, b, c, d, _, _, _, _] = hasher.finish().to_ne_bytes();
	let cid = u32::from_ne_bytes([a, b, c, d]) | (1 << 30);
	if cid < 3 { 3 } else { cid }
}

fn host_vm_path_from_root(root_path: &Path) -> PathBuf {
	root_path.join("vm")
}

fn host_vm_root_path_from_root(root_path: &Path) -> PathBuf {
	root_path.join("vmroot")
}

fn kernel_cmdline(arg: &Arg, user: &User, network: Option<&crate::vm::Network>) -> String {
	let tangram_path = Sandbox::guest_tangram_path_from_host_tangram_path(&arg.tangram_path);
	let mut cmdline = String::from("console=ttyS0 rootfstype=virtiofs");
	write!(
		&mut cmdline,
		" root={ROOTFS_TAG} init={} -- sandbox vm init --url {} --tangram-path {} --uid {} --gid {}",
		tangram_path.display(),
		arg.url,
		tangram_path.display(),
		user.uid,
		user.gid,
	)
	.unwrap();
	if let Some(hostname) = &arg.hostname {
		cmdline.push_str(" --hostname ");
		cmdline.push_str(hostname);
	}
	if let Some(network) = network {
		write!(
			&mut cmdline,
			" --network --guest-ip {} --gateway-ip {} --netmask {}",
			network.guest_ip, network.gateway_ip, network.netmask,
		)
		.unwrap();
		for dns in &network.dns_servers {
			write!(&mut cmdline, " --dns {dns}").unwrap();
		}
	}
	cmdline
}

fn kill_pid(pid: libc::pid_t) {
	unsafe {
		libc::kill(pid, libc::SIGKILL);
	}
}

fn prepare_sandbox_directory(sandbox_path: &Path) -> tg::Result<()> {
	for path in [
		Sandbox::host_output_path_from_root(sandbox_path),
		Sandbox::host_scratch_path_from_root(sandbox_path),
		Sandbox::host_tmp_path_from_root(sandbox_path),
		Sandbox::host_etc_path_from_root(sandbox_path),
		Sandbox::host_upper_path_from_root(sandbox_path),
		Sandbox::host_work_path_from_root(sandbox_path),
		host_vm_path_from_root(sandbox_path),
		host_vm_root_path_from_root(sandbox_path),
	] {
		std::fs::create_dir_all(&path).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to create the sandbox path"),
		)?;
	}
	let permissions =
		<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o1777);
	let tmp_path = Sandbox::host_tmp_path_from_root(sandbox_path);
	std::fs::set_permissions(&tmp_path, permissions).map_err(|source| {
		tg::error!(
			!source,
			path = %tmp_path.display(),
			"failed to set sandbox path permissions"
		)
	})?;
	let upper_path = Sandbox::host_upper_path_from_root(sandbox_path);
	let tangram_path = upper_path.join("opt/tangram");
	std::fs::create_dir_all(&tangram_path).map_err(|source| {
		tg::error!(
			!source,
			path = %tangram_path.display(),
			"failed to create the sandbox path"
		)
	})?;
	for path in [
		host_vm_path_from_root(sandbox_path).join(VIRTIOFSD_SOCKET_NAME),
		host_vm_path_from_root(sandbox_path).join(CLOUD_HYPERVISOR_VSOCK_SOCKET_NAME),
	] {
		std::fs::remove_file(path).ok();
	}
	Ok(())
}

fn prepare_etc_files(sandbox_path: &Path, user: &User) -> tg::Result<()> {
	let mut passwd = String::from(
		"root:!:0:0:root:/root:/bin/false\nnobody:!:65534:65534:nobody:/nonexistent:/bin/false\n",
	);
	if user.uid != 0 && user.uid != 65534 {
		use std::fmt::Write as _;
		writeln!(
			passwd,
			"{}:!:{}:{}:{}:{}:/bin/false",
			user.name,
			user.uid,
			user.gid,
			user.name,
			user.home.display(),
		)
		.unwrap();
	}
	std::fs::write(Sandbox::host_passwd_path_from_root(sandbox_path), passwd)
		.map_err(|source| tg::error!(!source, "failed to write /etc/passwd"))?;
	let nsswitch = indoc::indoc!(
		"
			passwd: files compat
			shadow: files compat
			hosts: files dns compat
		"
	);
	std::fs::write(
		Sandbox::host_nsswitch_path_from_root(sandbox_path),
		nsswitch,
	)
	.map_err(|source| tg::error!(!source, "failed to write /etc/nsswitch.conf"))?;
	Ok(())
}

fn resolve_user(name: Option<&str>) -> tg::Result<User> {
	let ptr = unsafe {
		if let Some(name) = name {
			let name = CString::new(OsStr::new(name).as_bytes())
				.map_err(|source| tg::error!(!source, "failed to encode the user name"))?;
			libc::getpwnam(name.as_ptr())
		} else {
			libc::getpwuid(libc::getuid())
		}
	};
	if ptr.is_null() {
		return Err(tg::error!("failed to resolve the user"));
	}
	let passwd = unsafe { &*ptr };
	let name = unsafe { CStr::from_ptr(passwd.pw_name) }
		.to_string_lossy()
		.into_owned();
	let home = unsafe { CStr::from_ptr(passwd.pw_dir) }
		.to_string_lossy()
		.into_owned();
	Ok(User {
		gid: passwd.pw_gid,
		home: PathBuf::from(home),
		name,
		uid: passwd.pw_uid,
	})
}

fn set_no_new_privs() -> tg::Result<()> {
	let result = unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
	if result != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to set no_new_privs"));
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

fn set_parent_death_signal_io(signal: libc::c_int) -> std::io::Result<()> {
	let result = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, signal, 0, 0, 0) };
	if result != 0 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

fn setresgid(gid: libc::gid_t) -> tg::Result<()> {
	let result = unsafe { libc::setresgid(gid, gid, gid) };
	if result != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to set the gid"));
	}
	Ok(())
}

fn setresuid(uid: libc::uid_t) -> tg::Result<()> {
	let result = unsafe { libc::setresuid(uid, uid, uid) };
	if result != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to set the uid"));
	}
	Ok(())
}

fn spawn_virtiofsd_helper(arg: &Arg, user: &User, socket: RawFd) -> tg::Result<libc::pid_t> {
	let child = unsafe { libc::fork() };
	if child < 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to fork the virtiofsd helper"));
	}
	if child == 0 {
		match helper_child_main(arg, user, socket) {
			Ok(()) => std::process::exit(0),
			Err(error) => {
				eprintln!("{error}");
				eprintln!("{}", error.trace());
				std::process::exit(105);
			},
		}
	}
	Ok(child)
}

fn try_wait_for_pid(pid: libc::pid_t) -> tg::Result<Option<u8>> {
	let mut status = 0;
	let result = unsafe { libc::waitpid(pid, std::ptr::addr_of_mut!(status), libc::WNOHANG) };
	if result == 0 {
		return Ok(None);
	}
	if result < 0 {
		let source = std::io::Error::last_os_error();
		if source.raw_os_error() == Some(libc::EINTR) {
			return Ok(None);
		}
		return Err(tg::error!(!source, "failed to wait for the helper"));
	}
	Ok(Some(exit_code_from_status(status)))
}

fn unshare(flags: libc::c_int, message: &'static str) -> tg::Result<()> {
	let result = unsafe { libc::unshare(flags) };
	if result != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "{}", message));
	}
	Ok(())
}

fn wait_for_pid(pid: libc::pid_t) {
	let mut status = 0;
	unsafe {
		libc::waitpid(pid, std::ptr::addr_of_mut!(status), 0);
	}
}

fn exit_code_from_status(status: libc::c_int) -> u8 {
	if libc::WIFEXITED(status) {
		return u8::try_from(libc::WEXITSTATUS(status).min(255)).unwrap_or(u8::MAX);
	}
	if libc::WIFSIGNALED(status) {
		let signal = libc::WTERMSIG(status);
		return u8::try_from((128 + signal).min(255)).unwrap_or(u8::MAX);
	}
	1
}
