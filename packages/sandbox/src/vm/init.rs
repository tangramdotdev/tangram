use {
	crate::{Sandbox, vm::Network},
	std::{
		ffi::{CStr, CString},
		io::Read as _,
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

const NETWORK_INTERFACE_WAIT_INTERVAL: Duration = Duration::from_millis(10);
const NETWORK_INTERFACE_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

const INIT_CONFIG_PATH: &str = "/etc/init.json";
const SANDBOX_FS_TAG: &str = "sandbox";
const SANDBOX_MOUNT_POINT: &str = "/run/sandbox";
const SANDBOX_INIT_CONFIG_PATH: &str = "/run/sandbox/init.json";
const SANDBOX_OUTPUT_PATH: &str = "/run/sandbox/output";
const GUEST_OUTPUT_PATH: &str = "/opt/tangram/output";
const ARTIFACTS_FS_TAG: &str = "artifacts";
const ARTIFACTS_MOUNT_POINT: &str = "/opt/tangram/artifacts";

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Config {
	pub gid: u32,
	pub hostname: Option<String>,
	#[serde(default)]
	pub library_paths: Vec<PathBuf>,
	pub network: Option<NetworkConfig>,
	pub output_path: PathBuf,
	pub tangram_path: PathBuf,
	pub uid: u32,
	pub url: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct NetworkConfig {
	pub dns_servers: Vec<Ipv4Addr>,
	pub gateway_ip: Ipv4Addr,
	pub guest_ip: Ipv4Addr,
	pub netmask: Ipv4Addr,
}

pub fn run() -> tg::Result<ExitCode> {	
	eprintln!("[tg-init] starting");
	mount_proc(Path::new("/proc"))?;
	eprintln!("[tg-init] mounted /proc");
	mount_sys(Path::new("/sys"))?;
	eprintln!("[tg-init] mounted /sys");
	mount_dev(Path::new("/dev"))?;
	eprintln!("[tg-init] mounted /dev");

	// Per-sandbox virtiofs devices are not present in the snapshot; the
	// host attaches them and then writes a byte to the serial console to
	// unblock this read.
	eprintln!("[tg-init] waiting for ready signal");
	let mut buf = [0u8; 1];
	std::io::stdin().read_exact(&mut buf).map_err(|error| {
		tg::error!(!error, "failed to read the ready signal from the console")
	})?;
	eprintln!("[tg-init] received ready signal");

	wait_for_virtiofs_tags(&[SANDBOX_FS_TAG, ARTIFACTS_FS_TAG])?;

	let source = CString::new(SANDBOX_FS_TAG).unwrap();
	let target = CString::new(SANDBOX_MOUNT_POINT).unwrap();
	let fstype = CString::new("virtiofs").unwrap();
	let result = unsafe {
		libc::mount(
			source.as_ptr(),
			target.as_ptr(),
			fstype.as_ptr(),
			0,
			std::ptr::null(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to mount the sandbox virtio-fs"));
	}
	eprintln!("[tg-init] mounted {SANDBOX_MOUNT_POINT}");

	let source = CString::new(ARTIFACTS_FS_TAG).unwrap();
	let target = CString::new(ARTIFACTS_MOUNT_POINT).unwrap();
	let fstype = CString::new("virtiofs").unwrap();
	let result = unsafe {
		libc::mount(
			source.as_ptr(),
			target.as_ptr(),
			fstype.as_ptr(),
			0,
			std::ptr::null(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			"failed to mount the artifacts virtio-fs"
		));
	}
	eprintln!("[tg-init] mounted {ARTIFACTS_MOUNT_POINT}");

	let source = CString::new(SANDBOX_OUTPUT_PATH).unwrap();
	let target = CString::new(GUEST_OUTPUT_PATH).unwrap();
	let result = unsafe {
		libc::mount(
			source.as_ptr(),
			target.as_ptr(),
			std::ptr::null(),
			libc::MS_BIND,
			std::ptr::null(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to bind {GUEST_OUTPUT_PATH}"));
	}
	eprintln!("[tg-init] bound {GUEST_OUTPUT_PATH}");

	let source = CString::new(SANDBOX_INIT_CONFIG_PATH).unwrap();
	let target = CString::new(INIT_CONFIG_PATH).unwrap();
	let result = unsafe {
		libc::mount(
			source.as_ptr(),
			target.as_ptr(),
			std::ptr::null(),
			libc::MS_BIND,
			std::ptr::null(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to bind the init config"));
	}
	let bytes = std::fs::read(INIT_CONFIG_PATH)
		.map_err(|error| tg::error!(!error, "failed to read the init config"))?;
	eprintln!("[tg-init] read {} bytes from {INIT_CONFIG_PATH}", bytes.len());
	let config: Config = serde_json::from_slice(&bytes)
		.map_err(|error| tg::error!(!error, "failed to parse the init config"))?;

	eprintln!("[tg-init] loaded config: uid={} gid={}", config.uid, config.gid);

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
		eprintln!("[tg-init] setting hostname={hostname}");
		set_hostname(hostname)?;
	}

	if let Some(network) = &network {
		eprintln!("[tg-init] configuring network guest_ip={}", network.guest_ip);
		configure_network(network)?;
		prime_network(network);
		write_resolv_conf(&network.dns_servers)?;
	}

	let home = resolve_home(config.uid);

	eprintln!("[tg-init] setting uid={} gid={}", config.uid, config.gid);
	setresgid(config.gid)?;
	setresuid(config.uid)?;

	let serve = crate::serve::Arg {
		library_paths: config.library_paths,
		listen: false,
		output_path: config.output_path,
		tangram_path: config.tangram_path,
		url,
	};

	eprintln!("[tg-init] spawning sandbox server");
	let child = spawn_server(&serve, home.as_deref())?;
	let pid: libc::pid_t = child
		.id()
		.try_into()
		.map_err(|_| tg::error!("failed to get the sandbox server pid"))?;
	
	eprintln!("[tg-init] sandbox server spawned pid={pid}; entering reap loop");

	loop {
		let (reaped_pid, status) = wait_for_child()?;
		if reaped_pid == pid {
			eprintln!("[tg-init] sandbox server exited status={status}");
			return Ok(ExitCode::from(status));
		}
	}
}

fn wait_for_virtiofs_tags(tags: &[&str]) -> tg::Result<()> {
	const MAX_RETRIES: usize = 20;
	const INTERVAL: Duration = Duration::from_millis(1);
	let visible = |tag: &str| -> bool {
		let Ok(entries) = std::fs::read_dir("/sys/fs/virtiofs") else {
			return false;
		};
		for entry in entries.flatten() {
			let tag_path = entry.path().join("tag");
			if let Ok(content) = std::fs::read_to_string(&tag_path)
				&& content.trim() == tag
			{
				return true;
			}
		}
		false
	};
	for attempt in 0..MAX_RETRIES {
		if tags.iter().all(|tag| visible(tag)) {
			return Ok(());
		}
		if attempt + 1 < MAX_RETRIES {
			std::thread::sleep(INTERVAL);
		}
	}
	Err(tg::error!(
		?tags,
		"timed out waiting for virtio-fs tags to appear in sysfs"
	))
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
	// Send one packet so user-mode backends can learn the guest endpoint before port forwards arrive.
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
	let start = Instant::now();
	loop {
		let mut interfaces = std::fs::read_dir("/sys/class/net")
			.map_err(|error| tg::error!(!error, "failed to read the network interfaces"))?
			.filter_map(|entry| {
				let entry = entry.ok()?;
				let name = entry.file_name();
				let name = name.to_str()?;
				if name == "lo" {
					None
				} else {
					Some(name.to_owned())
				}
			})
			.collect::<Vec<_>>();
		interfaces.sort();
		if let Some(interface) = interfaces.into_iter().next() {
			return Ok(interface);
		}
		if start.elapsed() >= NETWORK_INTERFACE_WAIT_TIMEOUT {
			return Err(tg::error!("timed out waiting for the network interface"));
		}
		std::thread::sleep(NETWORK_INTERFACE_WAIT_INTERVAL);
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
