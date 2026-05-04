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

const NETWORK_INTERFACE_WAIT_INTERVAL: Duration = Duration::from_millis(10);
const NETWORK_INTERFACE_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct Arg {
	pub gid: libc::gid_t,
	pub hostname: Option<String>,
	pub network: Option<Network>,
	pub serve: crate::serve::Arg,
	pub uid: libc::uid_t,
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	if let Some(hostname) = &arg.hostname {
		set_hostname(hostname)?;
	}

	mount_proc(Path::new("/proc"))?;
	mount_sys(Path::new("/sys"))?;
	mount_dev(Path::new("/dev"))?;

	if let Some(network) = &arg.network {
		configure_network(network)?;
		write_resolv_conf(&network.dns_servers)?;
	}

	let home = resolve_home(arg.uid);

	setresgid(arg.gid)?;
	setresuid(arg.uid)?;

	let child = spawn_server(&arg.serve, home.as_deref())?;
	let pid: libc::pid_t = child
		.id()
		.try_into()
		.map_err(|_| tg::error!("failed to get the sandbox server pid"))?;

	loop {
		let (reaped_pid, status) = wait_for_child()?;
		if reaped_pid == pid {
			return Ok(ExitCode::from(status));
		}
	}
}

fn mount_proc(target: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(target)
		.map_err(|source| tg::error!(!source, "failed to create /proc"))?;
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
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to mount /proc"));
	}
	Ok(())
}

fn mount_sys(target: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(target)
		.map_err(|source| tg::error!(!source, "failed to create /sys"))?;
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
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to mount /sys"));
	}
	Ok(())
}

fn mount_dev(target: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(target)
		.map_err(|source| tg::error!(!source, "failed to create /dev"))?;
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
		// The kernel auto-mounts devtmpfs at /dev when built with
		// CONFIG_DEVTMPFS_MOUNT. Treat EBUSY as "already mounted".
		if error.raw_os_error() != Some(libc::EBUSY) {
			return Err(tg::error!(!error, "failed to mount /dev"));
		}
	}

	let pts = target.join("pts");
	std::fs::create_dir_all(&pts)
		.map_err(|source| tg::error!(!source, "failed to create /dev/pts"))?;
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
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to mount /dev/pts"));
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
		.map_err(|source| tg::error!(!source, "failed to create /dev/fd"))?;
	std::os::unix::fs::symlink("../proc/self/fd/0", target.join("stdin"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/stdin"))?;
	std::os::unix::fs::symlink("../proc/self/fd/1", target.join("stdout"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/stdout"))?;
	std::os::unix::fs::symlink("../proc/self/fd/2", target.join("stderr"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/stderr"))?;
	std::os::unix::fs::symlink("pts/ptmx", target.join("ptmx"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/ptmx"))?;
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
		.map_err(|source| tg::error!(!source, "failed to encode the network interface name"))?;
	route.rt_dev = interface.as_ptr().cast_mut();
	let result = unsafe { libc::ioctl(fd, libc::SIOCADDRT, std::ptr::addr_of!(route)) };
	if result == 0 {
		return Ok(());
	}
	let source = std::io::Error::last_os_error();
	if source.raw_os_error() == Some(libc::EEXIST) {
		return Ok(());
	}
	Err(tg::error!(!source, "failed to add the default route"))
}

fn copy_interface_name(ifreq: &mut libc::ifreq, interface: &str) -> tg::Result<()> {
	let interface = CString::new(interface)
		.map_err(|source| tg::error!(!source, "failed to encode the network interface name"))?;
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
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to create the network socket"));
	}
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };
	Ok(fd)
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
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "{}", message));
	}
	Ok(())
}

fn set_interface_up(fd: libc::c_int, interface: &str) -> tg::Result<()> {
	let mut ifreq = unsafe { std::mem::zeroed::<libc::ifreq>() };
	copy_interface_name(&mut ifreq, interface)?;
	let result = unsafe { libc::ioctl(fd, libc::SIOCGIFFLAGS, std::ptr::addr_of_mut!(ifreq)) };
	if result != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(
			!source,
			"failed to read the network interface flags"
		));
	}
	unsafe {
		ifreq.ifr_ifru.ifru_flags |= libc::c_short::try_from(libc::IFF_UP).unwrap();
	}
	let result = unsafe { libc::ioctl(fd, libc::SIOCSIFFLAGS, std::ptr::addr_of_mut!(ifreq)) };
	if result != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(
			!source,
			"failed to set the network interface flags"
		));
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
		.map_err(|source| tg::error!(!source, "failed to spawn the sandbox server"))
}

fn wait_for_network_interface() -> tg::Result<String> {
	let start = Instant::now();
	loop {
		let mut interfaces = std::fs::read_dir("/sys/class/net")
			.map_err(|source| tg::error!(!source, "failed to read the network interfaces"))?
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
		let source = std::io::Error::last_os_error();
		if source.raw_os_error() == Some(libc::EINTR) {
			continue;
		}
		return Err(tg::error!(!source, "failed to wait for a sandbox child"));
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

	std::fs::create_dir_all("/etc")
		.map_err(|source| tg::error!(!source, "failed to create /etc"))?;
	let resolv_conf = dns_servers
		.iter()
		.fold(String::new(), |mut output, dns_server| {
			writeln!(output, "nameserver {dns_server}").unwrap();
			output
		});
	std::fs::write("/etc/resolv.conf", resolv_conf)
		.map_err(|source| tg::error!(!source, "failed to write /etc/resolv.conf"))?;
	Ok(())
}
