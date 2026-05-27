use {
	crate::{Sandbox, vm::Network},
	std::{
		ffi::{CStr, CString},
		net::Ipv4Addr,
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
			unix::ffi::OsStrExt as _,
		},
		path::{Path, PathBuf},
		process::ExitCode,
		time::{Duration, Instant},
	},
	tangram_client::prelude::*,
};

const NETWORK_INTERFACE_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

const SANDBOX_FS_TAG: &str = "sandbox";
const ARTIFACTS_FS_TAG: &str = "artifacts";
const HOST_MOUNT_POINT: &str = "/mnt/host";
const ARTIFACTS_MOUNT_POINT: &str = "/mnt/host/opt/tangram/artifacts";
const HOST_INIT_CONFIG_PATH: &str = "/mnt/host/init.json";
const ROOTVIEW_SCRATCH: &str = "/mnt/root";
const ROOTVIEW_UPPER: &str = "/mnt/root/upper";
const ROOTVIEW_WORK: &str = "/mnt/root/work";
const ROOTVIEW_MERGED: &str = "/mnt/root/merged";

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Config {
	pub gid: u32,
	pub hostname: Option<String>,
	#[serde(default)]
	pub library_paths: Vec<PathBuf>,
	#[serde(default)]
	pub mounts: Vec<MountConfig>,
	pub network: Option<NetworkConfig>,
	pub output_path: PathBuf,
	pub tangram_path: PathBuf,
	pub uid: u32,
	pub url: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct MountConfig {
	pub directory: bool,
	pub readonly: bool,
	pub source: PathBuf,
	pub target: PathBuf,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct NetworkConfig {
	pub dns_servers: Vec<Ipv4Addr>,
	pub gateway_ip: Ipv4Addr,
	pub guest_ip: Ipv4Addr,
	pub netmask: Ipv4Addr,
}

pub fn run() -> tg::Result<ExitCode> {
	tracing::trace!("starting");

	mount_proc(Path::new("/proc"))
		.map_err(|source| tg::error!(!source, "failed to mount /proc"))?;
	tracing::trace!("mounted /proc");

	mount_sys(Path::new("/sys")).map_err(|source| tg::error!(!source, "failed to mount /sys"))?;
	tracing::trace!("mounted /sys");

	mount_dev(Path::new("/dev")).map_err(|source| tg::error!(!source, "failed to mount /dev"))?;
	tracing::trace!("mounted /dev");

	configure_memory_hotplug();
	online_cpus();

	wait_for_virtiofs()
		.map_err(|source| tg::error!(!source, "error waiting for virtiofs tags to connect"))?;
	tracing::trace!("virtiofs tags ready");
	online_cpus();

	mount_virtiofs(SANDBOX_FS_TAG, HOST_MOUNT_POINT)?;
	tracing::trace!("mounted {HOST_MOUNT_POINT}");
	mount_virtiofs(ARTIFACTS_FS_TAG, ARTIFACTS_MOUNT_POINT)?;
	tracing::trace!("mounted {ARTIFACTS_MOUNT_POINT}");

	let bytes = std::fs::read(HOST_INIT_CONFIG_PATH)
		.map_err(|error| tg::error!(!error, "failed to read the init config"))?;
	tracing::trace!("read {} bytes from {HOST_INIT_CONFIG_PATH}", bytes.len());

	let config: Config = serde_json::from_slice(&bytes)
		.map_err(|error| tg::error!(!error, "failed to parse the init config"))?;
	tracing::trace!("parsed config: uid={} gid={}", config.uid, config.gid);

	setup_rootfs(&config).map_err(|source| tg::error!(!source, "failed to setup rootfs"))?;
	tracing::trace!("created rootfs");

	make_root_private().map_err(|source| tg::error!(!source, "failed to make rootfs private"))?;
	tracing::trace!("made rootfs private");

	chroot(ROOTVIEW_MERGED)?;
	tracing::trace!("chrooted to new rootview");

	let url = config
		.url
		.parse()
		.map_err(|error| tg::error!(!error, "failed to parse the init config url"))?;
	let network = config.network.map(|n| Network {
		dns_servers: n.dns_servers,
		gateway_ip: n.gateway_ip,
		guest_ip: n.guest_ip,
		netmask: n.netmask,
	});

	if let Some(hostname) = &config.hostname {
		set_hostname(hostname)?;
		tracing::trace!("set hostname={hostname}");
	}

	if let Some(network) = &network {
		configure_network(network)
			.map_err(|source| tg::error!(!source, "failed to configure network"))?;
		prime_network(network);
		write_resolv_conf(&network.dns_servers)
			.map_err(|source| tg::error!(!source, "failed to write /etc/resolv.conf"))?;
		tracing::trace!("configured network guest_ip={}", network.guest_ip);
	}

	let home = resolve_home(config.uid);

	setresgid(config.gid).map_err(|source| tg::error!(!source, "failed to set gid"))?;
	setresuid(config.uid).map_err(|source| tg::error!(!source, "failed to set uid"))?;
	tracing::trace!("set uid={} gid={}", config.uid, config.gid);

	let serve = crate::serve::Arg {
		library_paths: config.library_paths,
		listen: false,
		output_path: config.output_path,
		tangram_path: config.tangram_path,
		url,
	};

	let child = spawn_server(&serve, home.as_deref())
		.map_err(|source| tg::error!(!source, "failed to spawn server"))?;
	tracing::trace!("spawned sandbox server");
	let pid: libc::pid_t = child
		.id()
		.try_into()
		.map_err(|_| tg::error!("failed to get the sandbox server pid"))?;
	tracing::trace!("spawned pid={pid}; entering reap loop");
	loop {
		let (reaped_pid, status) = wait_for_child()?;
		if reaped_pid == pid {
			tracing::trace!("sandbox server exited status={status}");
			return Ok(ExitCode::from(status));
		}
	}
}

fn mount_virtiofs(tag: &str, target: &str) -> tg::Result<()> {
	std::fs::create_dir_all(target)
		.map_err(|error| tg::error!(!error, %target, "failed to create the virtiofs target"))?;
	let source = CString::new(tag)
		.map_err(|error| tg::error!(!error, "failed to encode the virtiofs tag"))?;
	let target_c = CString::new(target)
		.map_err(|error| tg::error!(!error, "failed to encode the virtiofs target"))?;
	let fstype = CString::new("virtiofs").unwrap();
	let result = unsafe {
		libc::mount(
			source.as_ptr(),
			target_c.as_ptr(),
			fstype.as_ptr(),
			0,
			std::ptr::null(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, %tag, %target, "failed to mount virtiofs"));
	}
	Ok(())
}

fn mount_tmpfs(target: &str) -> tg::Result<()> {
	let source = CString::new("tmpfs").unwrap();
	let target_c = CString::new(target)
		.map_err(|error| tg::error!(!error, "failed to encode the tmpfs target"))?;
	let fstype = CString::new("tmpfs").unwrap();
	let data = CString::new("mode=0755").unwrap();
	let result = unsafe {
		libc::mount(
			source.as_ptr(),
			target_c.as_ptr(),
			fstype.as_ptr(),
			0,
			data.as_ptr().cast(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, %target, "failed to mount tmpfs"));
	}
	Ok(())
}

fn setup_rootfs(config: &Config) -> tg::Result<()> {
	mount_tmpfs(ROOTVIEW_SCRATCH)?;
	for path in [ROOTVIEW_UPPER, ROOTVIEW_WORK, ROOTVIEW_MERGED] {
		std::fs::create_dir(path).map_err(
			|error| tg::error!(!error, %path, "failed to create the overlay scratch directory"),
		)?;
	}

	mount_overlay()?;

	let init_json = Path::new(ROOTVIEW_MERGED).join("init.json");
	if let Err(error) = std::fs::remove_file(&init_json)
		&& error.kind() != std::io::ErrorKind::NotFound
	{
		return Err(tg::error!(
			!error,
			path = %init_json.display(),
			"failed to mask init.json",
		));
	}

	let merged_mnt = Path::new(ROOTVIEW_MERGED).join("mnt");
	mount_tmpfs(
		merged_mnt
			.to_str()
			.ok_or_else(|| tg::error!("invalid /mnt path"))?,
	)?;

	for leaf in ["proc", "sys", "dev"] {
		let source = Path::new("/").join(leaf);
		let target = Path::new(ROOTVIEW_MERGED).join(leaf);
		rbind(&source, &target)?;
	}

	let output_source = Path::new(HOST_MOUNT_POINT).join("opt/tangram/output");
	let output_target = Path::new(ROOTVIEW_MERGED).join("opt/tangram/output");
	bind(&output_source, &output_target)?;

	let tmp_source = Path::new(HOST_MOUNT_POINT).join("tmp");
	let tmp_target = Path::new(ROOTVIEW_MERGED).join("tmp");
	bind(&tmp_source, &tmp_target)?;

	for (source, target, directory, readonly) in [
		(
			Path::new(HOST_MOUNT_POINT).join("etc/passwd"),
			Path::new(ROOTVIEW_MERGED).join("etc/passwd"),
			false,
			true,
		),
		(
			Path::new(HOST_MOUNT_POINT).join("etc/nsswitch.conf"),
			Path::new(ROOTVIEW_MERGED).join("etc/nsswitch.conf"),
			false,
			true,
		),
		(
			Path::new(HOST_MOUNT_POINT).join("opt/tangram/artifacts"),
			Path::new(ROOTVIEW_MERGED).join("opt/tangram/artifacts"),
			true,
			true,
		),
	] {
		ensure_mount_target(&target, directory)?;
		bind(&source, &target)?;
		if readonly {
			remount_readonly(&source, &target)?;
		}
	}

	for mount in &config.mounts {
		let target = guest_target_in_rootview(&mount.target)?;
		ensure_mount_target(&target, mount.directory)?;
		bind(&mount.source, &target)?;
		if mount.readonly {
			remount_readonly(&mount.source, &target)?;
		}
	}

	Ok(())
}

fn mount_overlay() -> tg::Result<()> {
	let source = CString::new("overlay").unwrap();
	let target = CString::new(ROOTVIEW_MERGED).unwrap();
	let fstype = CString::new("overlay").unwrap();
	let data = CString::new(format!(
		"lowerdir=/,upperdir={ROOTVIEW_UPPER},workdir={ROOTVIEW_WORK}"
	))
	.map_err(|error| tg::error!(!error, "failed to encode the overlay options"))?;
	let result = unsafe {
		libc::mount(
			source.as_ptr(),
			target.as_ptr(),
			fstype.as_ptr(),
			0,
			data.as_ptr().cast(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to mount the overlay rootview"));
	}
	Ok(())
}

fn rbind(source: &Path, target: &Path) -> tg::Result<()> {
	bind_with_flags(source, target, libc::MS_BIND | libc::MS_REC)
}

fn bind(source: &Path, target: &Path) -> tg::Result<()> {
	bind_with_flags(source, target, libc::MS_BIND)
}

fn remount_readonly(source: &Path, target: &Path) -> tg::Result<()> {
	bind_with_flags(
		source,
		target,
		libc::MS_BIND | libc::MS_REC | libc::MS_REMOUNT | libc::MS_RDONLY,
	)
}

fn bind_with_flags(source: &Path, target: &Path, flags: libc::c_ulong) -> tg::Result<()> {
	let source_c = CString::new(source.as_os_str().as_bytes())
		.map_err(|error| tg::error!(!error, "failed to encode the bind source"))?;
	let target_c = CString::new(target.as_os_str().as_bytes())
		.map_err(|error| tg::error!(!error, "failed to encode the bind target"))?;
	let result = unsafe {
		libc::mount(
			source_c.as_ptr(),
			target_c.as_ptr(),
			std::ptr::null(),
			flags,
			std::ptr::null(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			source = %source.display(),
			target = %target.display(),
			"failed to bind",
		));
	}
	Ok(())
}

fn ensure_mount_target(target: &Path, directory: bool) -> tg::Result<()> {
	if let Ok(metadata) = std::fs::metadata(target) {
		if metadata.is_dir() != directory {
			let expected = if directory { "a directory" } else { "a file" };
			let found = if metadata.is_dir() {
				"a directory"
			} else {
				"a file"
			};
			return Err(tg::error!(
				path = %target.display(),
				"expected mount target to be {expected}, but found {found}",
			));
		}
		return Ok(());
	}
	if directory {
		std::fs::create_dir_all(target).map_err(
			|error| tg::error!(!error, path = %target.display(), "failed to create a guest directory"),
		)?;
	} else {
		if let Some(parent) = target.parent() {
			std::fs::create_dir_all(parent).map_err(
				|error| tg::error!(!error, path = %parent.display(), "failed to create a guest parent directory"),
			)?;
		}
		std::fs::OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(false)
			.open(target)
			.map_err(
				|error| tg::error!(!error, path = %target.display(), "failed to create a guest file"),
			)?;
	}
	Ok(())
}

fn guest_target_in_rootview(target: &Path) -> tg::Result<PathBuf> {
	let suffix = target.strip_prefix("/").map_err(|error| {
		tg::error!(
			!error,
			path = %target.display(),
			"expected an absolute guest path",
		)
	})?;
	Ok(Path::new(ROOTVIEW_MERGED).join(suffix))
}

fn make_root_private() -> tg::Result<()> {
	let target = CString::new("/").unwrap();
	let result = unsafe {
		libc::mount(
			std::ptr::null(),
			target.as_ptr(),
			std::ptr::null(),
			libc::MS_REC | libc::MS_PRIVATE,
			std::ptr::null(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to make the rootview private"));
	}
	Ok(())
}

fn chroot(target: &str) -> tg::Result<()> {
	let target_c = CString::new(target)
		.map_err(|error| tg::error!(!error, "failed to encode the chroot target"))?;
	if unsafe { libc::chdir(target_c.as_ptr()) } != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, %target, "failed to chdir before chroot"));
	}
	if unsafe { libc::chroot(c".".as_ptr()) } != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, %target, "failed to chroot"));
	}
	let root = CString::new("/").unwrap();
	if unsafe { libc::chdir(root.as_ptr()) } != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to chdir to / post-chroot"));
	}
	Ok(())
}

fn signal_ready() {
	use std::io::Write as _;
	let stderr = std::io::stderr();
	let mut handle = stderr.lock();
	let _ = handle.write_all(&[0]);
	let _ = handle.flush();
}

fn configure_memory_hotplug() {
	let _ = std::fs::write("/sys/devices/system/memory/auto_online_blocks", "online\n");
}

fn online_cpus() {
	let Ok(entries) = std::fs::read_dir("/sys/devices/system/cpu") else {
		return;
	};
	for entry in entries.flatten() {
		let name = entry.file_name();
		let Some(name) = name.to_str() else { continue };
		if !name.starts_with("cpu") || name[3..].parse::<u64>().is_err() {
			continue;
		}
		let _ = std::fs::write(entry.path().join("online"), "1\n");
	}
}

fn wait_for_virtiofs() -> tg::Result<()> {
	const NETLINK_KOBJECT_UEVENT: libc::c_int = 15;
	let socket = unsafe {
		libc::socket(
			libc::AF_NETLINK,
			libc::SOCK_DGRAM | libc::SOCK_CLOEXEC,
			NETLINK_KOBJECT_UEVENT,
		)
	};
	if socket < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to open the uevent socket"));
	}
	let mut addr: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
	addr.nl_family = libc::AF_NETLINK.try_into().unwrap();
	addr.nl_groups = 1;
	let result = unsafe {
		libc::bind(
			socket,
			std::ptr::addr_of!(addr).cast(),
			std::mem::size_of::<libc::sockaddr_nl>().try_into().unwrap(),
		)
	};
	if result < 0 {
		let error = std::io::Error::last_os_error();
		unsafe { libc::close(socket) };
		return Err(tg::error!(!error, "failed to bind the uevent socket"));
	}

	let mut pending: Vec<&str> = vec![SANDBOX_FS_TAG, ARTIFACTS_FS_TAG];
	if let Ok(entries) = std::fs::read_dir("/sys/fs/virtiofs") {
		for entry in entries.flatten() {
			let tag_path = entry.path().join("tag");
			if let Ok(content) = std::fs::read_to_string(&tag_path) {
				let tag = content.trim();
				pending.retain(|expected| *expected != tag);
			}
		}
	}
	if pending.is_empty() {
		unsafe { libc::close(socket) };
		return Ok(());
	}

	signal_ready();

	let mut buf = [0u8; 8192];
	while !pending.is_empty() {
		let n = unsafe { libc::recv(socket, buf.as_mut_ptr().cast(), buf.len(), 0) };
		if n < 0 {
			let error = std::io::Error::last_os_error();
			if error.raw_os_error() == Some(libc::EINTR) {
				continue;
			}
			unsafe { libc::close(socket) };
			return Err(tg::error!(!error, "failed to read a uevent"));
		}
		let message = &buf[..usize::try_from(n).unwrap()];
		let mut subsystem: Option<&[u8]> = None;
		let mut tag: Option<&[u8]> = None;
		let mut action: Option<&[u8]> = None;
		for entry in message.split(|&b| b == 0) {
			if let Some(value) = entry.strip_prefix(b"SUBSYSTEM=") {
				subsystem = Some(value);
			} else if let Some(value) = entry.strip_prefix(b"TAG=") {
				tag = Some(value);
			} else if let Some(value) = entry.strip_prefix(b"ACTION=") {
				action = Some(value);
			}
		}
		if action != Some(b"add".as_ref()) || subsystem != Some(b"virtiofs".as_ref()) {
			continue;
		}
		let Some(tag) = tag else { continue };
		let Ok(tag) = std::str::from_utf8(tag) else {
			continue;
		};
		pending.retain(|expected| *expected != tag);
	}
	unsafe { libc::close(socket) };
	Ok(())
}

fn mount_proc(target: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(target)
		.map_err(|error| tg::error!(!error, "failed to create /proc"))?;
	let source = CString::new("proc").unwrap();
	let target = CString::new(target.as_os_str().as_bytes()).unwrap();
	let fstype = CString::new("proc").unwrap();
	let result = unsafe {
		libc::mount(
			source.as_ptr(),
			target.as_ptr(),
			fstype.as_ptr(),
			libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
			std::ptr::null(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to mount /proc"));
	}
	Ok(())
}

fn mount_sys(target: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(target).map_err(|error| tg::error!(!error, "failed to create /sys"))?;
	let source = CString::new("sysfs").unwrap();
	let target = CString::new(target.as_os_str().as_bytes()).unwrap();
	let fstype = CString::new("sysfs").unwrap();
	let result = unsafe {
		libc::mount(
			source.as_ptr(),
			target.as_ptr(),
			fstype.as_ptr(),
			libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
			std::ptr::null(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to mount /sys"));
	}
	Ok(())
}

fn mount_dev(target: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(target).map_err(|error| tg::error!(!error, "failed to create /dev"))?;
	let source = CString::new("devtmpfs").unwrap();
	let target_cstring = CString::new(target.as_os_str().as_bytes()).unwrap();
	let fstype = CString::new("devtmpfs").unwrap();
	let data = CString::new("mode=0755,size=64k").unwrap();
	let result = unsafe {
		libc::mount(
			source.as_ptr(),
			target_cstring.as_ptr(),
			fstype.as_ptr(),
			libc::MS_NOSUID | libc::MS_STRICTATIME,
			data.as_ptr().cast(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		if error.raw_os_error() != Some(libc::EBUSY) {
			return Err(tg::error!(!error, "failed to mount /dev"));
		}
	}

	let pts = target.join("pts");
	std::fs::create_dir_all(&pts)
		.map_err(|error| tg::error!(!error, "failed to create /dev/pts"))?;
	let source = CString::new("devpts").unwrap();
	let pts_target = CString::new(pts.as_os_str().as_bytes()).unwrap();
	let fstype = CString::new("devpts").unwrap();
	let data = CString::new("newinstance,ptmxmode=0666,mode=0620").unwrap();
	let result = unsafe {
		libc::mount(
			source.as_ptr(),
			pts_target.as_ptr(),
			fstype.as_ptr(),
			libc::MS_NOSUID | libc::MS_NOEXEC,
			data.as_ptr().cast(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to mount /dev/pts"));
	}

	configure_dev(target)?;
	Ok(())
}

fn configure_dev(target: &Path) -> tg::Result<()> {
	for name in ["fd", "stdin", "stdout", "stderr", "ptmx"] {
		let path = target.join(name);
		if path.exists() {
			std::fs::remove_file(&path).ok();
		}
	}
	std::os::unix::fs::symlink("../proc/self/fd", target.join("fd"))
		.map_err(|error| tg::error!(!error, "failed to create /dev/fd"))?;
	std::os::unix::fs::symlink("../proc/self/fd/0", target.join("stdin"))
		.map_err(|error| tg::error!(!error, "failed to create /dev/stdin"))?;
	std::os::unix::fs::symlink("../proc/self/fd/1", target.join("stdout"))
		.map_err(|error| tg::error!(!error, "failed to create /dev/stdout"))?;
	std::os::unix::fs::symlink("../proc/self/fd/2", target.join("stderr"))
		.map_err(|error| tg::error!(!error, "failed to create /dev/stderr"))?;
	std::os::unix::fs::symlink("pts/ptmx", target.join("ptmx"))
		.map_err(|error| tg::error!(!error, "failed to create /dev/ptmx"))?;
	Ok(())
}

fn configure_network(network: &Network) -> tg::Result<()> {
	let interface = wait_for_network_interface()?;
	let socket = open_network_socket()?;
	set_interface_up(socket.as_raw_fd(), "lo")?;
	set_interface_address(
		socket.as_raw_fd(),
		&interface,
		libc::SIOCSIFADDR,
		network.guest_ip,
		"failed to configure the guest IP address",
	)?;
	set_interface_address(
		socket.as_raw_fd(),
		&interface,
		libc::SIOCSIFNETMASK,
		network.netmask,
		"failed to configure the guest netmask",
	)?;
	set_interface_up(socket.as_raw_fd(), &interface)?;
	add_default_route(socket.as_raw_fd(), &interface, network.gateway_ip)?;
	Ok(())
}

fn add_default_route(fd: libc::c_int, interface: &str, gateway_ip: Ipv4Addr) -> tg::Result<()> {
	let mut route = unsafe { std::mem::zeroed::<libc::rtentry>() };
	route.rt_dst = sockaddr_from_ipv4(Ipv4Addr::UNSPECIFIED);
	route.rt_gateway = sockaddr_from_ipv4(gateway_ip);
	route.rt_genmask = sockaddr_from_ipv4(Ipv4Addr::UNSPECIFIED);
	route.rt_flags = libc::RTF_GATEWAY | libc::RTF_UP;
	let interface = CString::new(interface)
		.map_err(|error| tg::error!(!error, "failed to encode the network interface name"))?;
	route.rt_dev = interface.as_ptr().cast_mut();
	let result = unsafe { libc::ioctl(fd, libc::SIOCADDRT, std::ptr::addr_of!(route)) };
	if result == 0 {
		return Ok(());
	}
	let error = std::io::Error::last_os_error();
	if error.raw_os_error() == Some(libc::EEXIST) {
		return Ok(());
	}
	Err(tg::error!(!error, "failed to add the default route"))
}

fn copy_interface_name(ifreq: &mut libc::ifreq, interface: &str) -> tg::Result<()> {
	let interface = CString::new(interface)
		.map_err(|error| tg::error!(!error, "failed to encode the network interface name"))?;
	let bytes = interface.as_bytes_with_nul();
	if bytes.len() > libc::IFNAMSIZ {
		return Err(tg::error!("the network interface name is too long"));
	}
	unsafe {
		std::ptr::copy_nonoverlapping(
			bytes.as_ptr().cast(),
			ifreq.ifr_name.as_mut_ptr(),
			bytes.len(),
		);
	}
	Ok(())
}

fn open_network_socket() -> tg::Result<OwnedFd> {
	let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
	if fd < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to create the network socket"));
	}
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };
	Ok(fd)
}

fn prime_network(network: &Network) {
	let Ok(socket) = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) else {
		return;
	};
	let _ = socket.send_to(&[0], (network.gateway_ip, 9));
}

fn resolve_home(uid: libc::uid_t) -> Option<PathBuf> {
	let ptr = unsafe { libc::getpwuid(uid) };
	if ptr.is_null() {
		return None;
	}
	let passwd = unsafe { &*ptr };
	let home = unsafe { CStr::from_ptr(passwd.pw_dir) }
		.to_string_lossy()
		.into_owned();
	Some(home.into())
}

fn setresgid(gid: libc::gid_t) -> tg::Result<()> {
	let result = unsafe { libc::setresgid(gid, gid, gid) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set the gid"));
	}
	Ok(())
}

fn setresuid(uid: libc::uid_t) -> tg::Result<()> {
	let result = unsafe { libc::setresuid(uid, uid, uid) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set the uid"));
	}
	Ok(())
}

fn set_interface_address(
	fd: libc::c_int,
	interface: &str,
	request: libc::c_ulong,
	address: Ipv4Addr,
	message: &'static str,
) -> tg::Result<()> {
	let mut ifreq = unsafe { std::mem::zeroed::<libc::ifreq>() };
	copy_interface_name(&mut ifreq, interface)?;
	ifreq.ifr_ifru.ifru_addr = sockaddr_from_ipv4(address);
	let result = unsafe { libc::ioctl(fd, request, std::ptr::addr_of_mut!(ifreq)) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "{}", message));
	}
	Ok(())
}

fn set_interface_up(fd: libc::c_int, interface: &str) -> tg::Result<()> {
	let mut ifreq = unsafe { std::mem::zeroed::<libc::ifreq>() };
	copy_interface_name(&mut ifreq, interface)?;
	let result = unsafe { libc::ioctl(fd, libc::SIOCGIFFLAGS, std::ptr::addr_of_mut!(ifreq)) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			"failed to read the network interface flags"
		));
	}
	unsafe {
		ifreq.ifr_ifru.ifru_flags |= libc::c_short::try_from(libc::IFF_UP).unwrap();
	}
	let result = unsafe { libc::ioctl(fd, libc::SIOCSIFFLAGS, std::ptr::addr_of_mut!(ifreq)) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			"failed to set the network interface flags"
		));
	}
	Ok(())
}

fn set_hostname(hostname: &str) -> tg::Result<()> {
	let result = unsafe { libc::sethostname(hostname.as_ptr().cast(), hostname.len()) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set the hostname"));
	}
	Ok(())
}

fn sockaddr_from_ipv4(address: Ipv4Addr) -> libc::sockaddr {
	let sockaddr = libc::sockaddr_in {
		sin_family: libc::AF_INET.try_into().unwrap(),
		sin_port: 0,
		sin_addr: libc::in_addr {
			s_addr: u32::from_ne_bytes(address.octets()),
		},
		sin_zero: [0; 8],
	};
	unsafe { std::mem::transmute(sockaddr) }
}

fn spawn_server(arg: &crate::serve::Arg, home: Option<&Path>) -> tg::Result<std::process::Child> {
	let mut command = std::process::Command::new(&arg.tangram_path);
	command
		.arg("sandbox")
		.arg("serve")
		.arg(if arg.listen { "--listen" } else { "--connect" })
		.arg("--output-path")
		.arg(&arg.output_path)
		.arg("--url")
		.arg(arg.url.to_string())
		.arg("--tangram-path")
		.arg(&arg.tangram_path)
		.env("SSL_CERT_DIR", Sandbox::guest_ssl_cert_dir())
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit());
	for path in &arg.library_paths {
		command.arg("--library-path").arg(path);
	}
	if let Some(home) = home {
		command.env("HOME", home);
	}
	command
		.spawn()
		.map_err(|error| tg::error!(!error, "failed to spawn the sandbox server"))
}

fn wait_for_network_interface() -> tg::Result<String> {
	let inotify = watch_network_interfaces()?;
	let deadline = Instant::now() + NETWORK_INTERFACE_WAIT_TIMEOUT;
	loop {
		if let Some(interface) = find_network_interface()? {
			return Ok(interface);
		}
		wait_for_network_interface_event(&inotify, deadline)?;
	}
}

fn find_network_interface() -> tg::Result<Option<String>> {
	let mut interfaces = std::fs::read_dir("/sys/class/net")
		.map_err(|error| tg::error!(!error, "failed to read the network interfaces"))?
		.filter_map(|entry| {
			let entry = entry.ok()?;
			let name = entry.file_name();
			let name = name.to_str()?;
			(name != "lo").then(|| name.to_owned())
		})
		.collect::<Vec<_>>();
	interfaces.sort();
	Ok(interfaces.into_iter().next())
}

fn watch_network_interfaces() -> tg::Result<OwnedFd> {
	let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
	if fd < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			"failed to create the network interface watcher"
		));
	}
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };
	let result = unsafe {
		libc::inotify_add_watch(
			fd.as_raw_fd(),
			c"/sys/class/net".as_ptr(),
			libc::IN_CREATE | libc::IN_MOVED_TO | libc::IN_ATTRIB,
		)
	};
	if result < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to watch the network interfaces"));
	}
	Ok(fd)
}

fn wait_for_network_interface_event(inotify: &OwnedFd, deadline: Instant) -> tg::Result<()> {
	let now = Instant::now();
	if now >= deadline {
		return Err(tg::error!("timed out waiting for the network interface"));
	}
	let timeout = deadline.duration_since(now).as_millis();
	let timeout = i32::try_from(timeout).unwrap_or(i32::MAX);
	let mut event = libc::pollfd {
		fd: inotify.as_raw_fd(),
		events: libc::POLLIN,
		revents: 0,
	};
	let result = unsafe { libc::poll(std::ptr::addr_of_mut!(event), 1, timeout) };
	if result < 0 {
		let error = std::io::Error::last_os_error();
		if error.raw_os_error() == Some(libc::EINTR) {
			return Ok(());
		}
		return Err(tg::error!(
			!error,
			"failed to wait for a network interface event"
		));
	}
	if result == 0 {
		return Err(tg::error!("timed out waiting for the network interface"));
	}
	drain_network_interface_events(inotify)
}

fn drain_network_interface_events(inotify: &OwnedFd) -> tg::Result<()> {
	let mut buffer = [0u8; 4096];
	loop {
		let result = unsafe {
			libc::read(
				inotify.as_raw_fd(),
				buffer.as_mut_ptr().cast(),
				buffer.len(),
			)
		};
		if result > 0 {
			continue;
		}
		if result == 0 {
			return Ok(());
		}
		let error = std::io::Error::last_os_error();
		if matches!(error.raw_os_error(), Some(libc::EAGAIN | libc::EINTR)) {
			return Ok(());
		}
		return Err(tg::error!(
			!error,
			"failed to read a network interface event"
		));
	}
}

fn wait_for_child() -> tg::Result<(libc::pid_t, u8)> {
	loop {
		let mut status = 0;
		let pid = unsafe { libc::waitpid(-1, std::ptr::addr_of_mut!(status), 0) };
		if pid >= 0 {
			return Ok((pid, exit_code_from_status(status)));
		}
		let error = std::io::Error::last_os_error();
		if error.raw_os_error() == Some(libc::EINTR) {
			continue;
		}
		return Err(tg::error!(!error, "failed to wait for a sandbox child"));
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

fn write_resolv_conf(dns_servers: &[Ipv4Addr]) -> tg::Result<()> {
	use std::fmt::Write as _;

	std::fs::create_dir_all("/etc").map_err(|error| tg::error!(!error, "failed to create /etc"))?;
	let resolv_conf = dns_servers
		.iter()
		.fold(String::new(), |mut output, dns_server| {
			writeln!(output, "nameserver {dns_server}").unwrap();
			output
		});
	std::fs::write("/etc/resolv.conf", resolv_conf)
		.map_err(|error| tg::error!(!error, "failed to write /etc/resolv.conf"))?;
	Ok(())
}
