use {
	crate::network::ip,
	std::{
		ffi::CString,
		net::Ipv4Addr,
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
			unix::ffi::OsStrExt as _,
		},
		path::{Path, PathBuf},
		process::{Child, Command},
	},
	tangram_client::prelude::*,
};

const SOCKET_NAME: &str = "passt.sock";
const TIMEOUT_MS: u64 = 5000;

pub(crate) struct Network {
	guest: ip::Lease,
	host: ip::Lease,
}

pub(crate) struct Device {
	child: Option<Child>,
	guest_ip: Ipv4Addr,
	host_ip: Ipv4Addr,
	mac: String,
	netmask: Ipv4Addr,
	pid_file: PathBuf,
	socket: PathBuf,
}

impl Network {
	pub(crate) fn new(host: ip::Lease, guest: ip::Lease) -> Self {
		Self { guest, host }
	}

	pub(crate) fn guest_ip(&self) -> Ipv4Addr {
		self.guest.addr
	}

	pub(crate) fn host_ip(&self) -> Ipv4Addr {
		self.host.addr
	}
}

impl Device {
	pub(crate) fn new(
		path: &Path,
		dns: &[Ipv4Addr],
		host_ip: Ipv4Addr,
		guest_ip: Ipv4Addr,
		ports: &[tg::sandbox::Port],
	) -> tg::Result<Self> {
		let netmask = Ipv4Addr::new(255, 255, 255, 252);
		let socket = path.join(SOCKET_NAME);
		let pid_file = socket.with_extension("pid");
		remove_file_if_exists(&pid_file, "failed to remove the stale pid file")?;
		create_empty_pid_file(&pid_file)?;
		remove_file_if_exists(&socket, "failed to remove the stale vhost-user socket")?;
		let bytes = rand::random::<[u8; 5]>();
		let mac = format!(
			"{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
			0x02, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4],
		);
		let executable = std::path::Path::new("passt");
		let mut command = Command::new(executable);
		command
			.arg("--no-map-gw")
			.arg("--vhost-user")
			.arg("--foreground")
			.arg("--pid")
			.arg(&pid_file)
			.arg("--quiet")
			.arg("--socket")
			.arg(&socket)
			.arg("-a")
			.arg(guest_ip.to_string())
			.arg("-n")
			.arg(netmask.to_string())
			.arg("-g")
			.arg(host_ip.to_string());
		append_port_args(&mut command, ports)?;
		command.arg("--dns-forward").arg(host_ip.to_string());
		for dns_host in dns {
			command.arg("--dns-host").arg(dns_host.to_string());
		}
		let mut child = command
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn {}", executable.display()))?;
		if let Err(error) = wait_for_pid_file(&pid_file, executable) {
			let pid = child.id().try_into().unwrap();
			unsafe {
				libc::kill(pid, libc::SIGTERM);
			}
			let _ = child.wait();
			return Err(error);
		}
		Ok(Self {
			child: Some(child),
			guest_ip,
			host_ip,
			mac,
			netmask,
			pid_file,
			socket,
		})
	}

	pub(crate) fn mac(&self) -> &str {
		&self.mac
	}

	pub(crate) fn guest_ip(&self) -> Ipv4Addr {
		self.guest_ip
	}

	pub(crate) fn host_ip(&self) -> Ipv4Addr {
		self.host_ip
	}

	pub(crate) fn netmask(&self) -> Ipv4Addr {
		self.netmask
	}
}

fn append_port_args(command: &mut Command, ports: &[tg::sandbox::Port]) -> tg::Result<()> {
	append_port_args_for_protocol(command, ports, tg::sandbox::PortProtocol::Tcp, "-t")?;
	append_port_args_for_protocol(command, ports, tg::sandbox::PortProtocol::Udp, "-u")?;
	Ok(())
}

fn append_port_args_for_protocol(
	command: &mut Command,
	ports: &[tg::sandbox::Port],
	protocol: tg::sandbox::PortProtocol,
	flag: &str,
) -> tg::Result<()> {
	let mut found = false;
	for port in ports.iter().filter(|port| port.protocol == protocol) {
		found = true;
		command.arg(flag).arg(port_spec(port)?);
	}
	if !found {
		command.arg(flag).arg("none");
	}
	Ok(())
}

fn port_spec(port: &tg::sandbox::Port) -> tg::Result<String> {
	let host = port
		.host
		.ok_or_else(|| tg::error!("expected a resolved host port"))?;
	if !host.is_single() || !port.guest.is_single() {
		return Err(tg::error!("expected resolved port mappings"));
	}
	let mut spec = String::new();
	if let Some(host_ip) = port.host_ip {
		spec.push_str(&host_ip.to_string());
		spec.push('/');
	}
	spec.push_str(&host.start.to_string());
	spec.push(':');
	spec.push_str(&port.guest.start.to_string());
	Ok(spec)
}

impl Drop for Device {
	fn drop(&mut self) {
		if let Some(child) = self.child.as_mut() {
			let pid = child.id().try_into().unwrap();
			unsafe {
				libc::kill(pid, libc::SIGTERM);
			}
			let _ = child.wait();
		}
		std::fs::remove_file(&self.pid_file).ok();
		std::fs::remove_file(&self.socket).ok();
	}
}

fn wait_for_pid_file(path: &Path, executable: &Path) -> tg::Result<()> {
	if pid_file_ready(path)? {
		return Ok(());
	}
	let inotify = watch_pid_file(path)?;
	if pid_file_ready(path)? {
		return Ok(());
	}
	wait_for_pid_file_write(&inotify, executable)?;
	if pid_file_ready(path)? {
		return Ok(());
	}
	Err(tg::error!(
		"{} wrote an invalid pid file",
		executable.display()
	))
}

fn watch_pid_file(path: &Path) -> tg::Result<OwnedFd> {
	let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
	if fd < 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to create the pid file watcher"));
	}
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };
	let path = CString::new(path.as_os_str().as_bytes())
		.map_err(|source| tg::error!(!source, "the pid file path is invalid"))?;
	let result =
		unsafe { libc::inotify_add_watch(fd.as_raw_fd(), path.as_ptr(), libc::IN_CLOSE_WRITE) };
	if result < 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to watch the pid file"));
	}
	Ok(fd)
}

fn wait_for_pid_file_write(inotify: &OwnedFd, executable: &Path) -> tg::Result<()> {
	let mut event = libc::pollfd {
		fd: inotify.as_raw_fd(),
		events: libc::POLLIN,
		revents: 0,
	};
	let result = unsafe {
		libc::poll(
			std::ptr::addr_of_mut!(event),
			1,
			i32::try_from(TIMEOUT_MS).unwrap(),
		)
	};
	if result < 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to wait for the pid file"));
	}
	if result == 0 {
		return Err(tg::error!(
			"timed out waiting for {} to write the pid file",
			executable.display()
		));
	}
	Ok(())
}

fn create_empty_pid_file(path: &Path) -> tg::Result<()> {
	std::fs::File::create(path).map(drop).map_err(
		|source| tg::error!(!source, path = %path.display(), "failed to create the pid file"),
	)
}

fn pid_file_ready(path: &Path) -> tg::Result<bool> {
	let contents = match std::fs::read_to_string(path) {
		Ok(contents) => contents,
		Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(false),
		Err(source) => {
			return Err(tg::error!(
				!source,
				path = %path.display(),
				"failed to read the pid file"
			));
		},
	};
	let contents = contents.trim();
	if contents.is_empty() {
		return Ok(false);
	}
	contents.parse::<libc::pid_t>().map_err(
		|source| tg::error!(!source, path = %path.display(), "failed to parse the pid file"),
	)?;
	Ok(true)
}

fn remove_file_if_exists(path: &Path, message: &'static str) -> tg::Result<()> {
	match std::fs::remove_file(path) {
		Ok(()) => Ok(()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(source) => Err(tg::error!(!source, path = %path.display(), "{}", message)),
	}
}
