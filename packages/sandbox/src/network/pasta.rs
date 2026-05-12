use {
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
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
};

const TIMEOUT_MS: u64 = 5000;
pub(crate) const DNS_FORWARD_IP: &str = "169.254.1.1";

pub(crate) struct Network {
	child: Option<Child>,
	dns: Vec<Ipv4Addr>,
	executable: Option<PathBuf>,
	guest_pipe: Option<std::os::unix::net::UnixStream>,
	host_pipe: Option<tokio::net::UnixStream>,
	interface: Option<String>,
	pid_file: PathBuf,
	ports: Vec<tg::sandbox::Port>,
}

#[derive(Default)]
pub struct Options {
	pub dns: Vec<Ipv4Addr>,
	pub executable: Option<PathBuf>,
	pub interface: Option<String>,
	pub ports: Vec<tg::sandbox::Port>,
}

impl Network {
	pub fn new(options: Options) -> tg::Result<Self> {
		let pid_file = temporary_pid_file();
		remove_file_if_exists(&pid_file, "failed to remove the stale pid file")?;
		create_empty_pid_file(&pid_file)?;
		let (host_pipe, guest_pipe) = tokio::net::UnixStream::pair()
			.map_err(|source| tg::error!(!source, "failed to create the pasta sync socket pair"))?;
		let guest_pipe = guest_pipe
			.into_std()
			.map_err(|source| tg::error!(!source, "failed to convert the guest pipe to std"))?;
		guest_pipe
			.set_nonblocking(false)
			.map_err(|source| tg::error!(!source, "failed to set the guest pipe blocking"))?;
		let raw = guest_pipe.as_raw_fd();
		let flags = unsafe { libc::fcntl(raw, libc::F_GETFD) };
		if flags < 0 {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(!source, "failed to get the guest pipe flags"));
		}
		if unsafe { libc::fcntl(raw, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(
				!source,
				"failed to clear FD_CLOEXEC on the guest pipe"
			));
		}
		Ok(Self {
			child: None,
			dns: options.dns,
			executable: options.executable,
			guest_pipe: Some(guest_pipe),
			host_pipe: Some(host_pipe),
			interface: options.interface,
			pid_file,
			ports: options.ports,
		})
	}

	pub(crate) fn guest_pipe(&self) -> Option<&std::os::unix::net::UnixStream> {
		self.guest_pipe.as_ref()
	}

	pub(crate) fn take_guest_pipe(&mut self) {
		drop(self.guest_pipe.take());
	}

	pub async fn start_netns(&mut self, pid: libc::pid_t) -> tg::Result<()> {
		let host_pipe = self
			.host_pipe
			.as_mut()
			.ok_or_else(|| tg::error!("the host sync pipe is missing"))?;

		// Wait for the container child to finish unsharing its netns.
		host_pipe
			.read_u8()
			.await
			.map_err(|source| tg::error!(!source, "child process failed"))?;
		let executable = self
			.executable
			.as_deref()
			.unwrap_or_else(|| std::path::Path::new("pasta"))
			.to_owned();
		let mut command = Command::new(&executable);
		command
			.arg("--foreground") // run pasta in the foreground, die
			.arg("--config-net") // setup addr/routes in the net namespace
			.arg("--no-map-gw"); // disable loopback to the host
		append_port_args(&mut command, &self.ports)?;
		command
			.arg("-T")
			.arg("none") // no TCP port forwarding guest -> host
			.arg("-U")
			.arg("none") // no UDP port forwarding guest -> host
			.arg("--pid")
			.arg(&self.pid_file)
			.arg("--quiet");
		if !self.dns.is_empty() {
			command.arg("--dns-forward").arg(DNS_FORWARD_IP);
			for dns_host in &self.dns {
				command.arg("--dns-host").arg(dns_host.to_string());
			}
		}
		if let Some(interface) = &self.interface {
			command.arg("-i").arg(interface);
		}
		command.arg(pid.to_string());
		let child = command
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn pasta"))?;
		self.child = Some(child);

		wait_for_pid_file_async(self.pid_file.clone(), executable).await?;

		// Signal the child to proceed.
		host_pipe
			.write_u8(0)
			.await
			.map_err(|source| tg::error!(!source, "child process failed"))?;
		Ok(())
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
	if !host.is_single() || !port.container.is_single() {
		return Err(tg::error!("expected resolved port mappings"));
	}
	let mut spec = String::new();
	if let Some(host_ip) = port.host_ip {
		spec.push_str(&host_ip.to_string());
		spec.push('/');
	}
	spec.push_str(&host.start.to_string());
	spec.push(':');
	spec.push_str(&port.container.start.to_string());
	Ok(spec)
}

impl Drop for Network {
	fn drop(&mut self) {
		if let Some(child) = self.child.as_mut() {
			let pid = child.id().try_into().unwrap();
			unsafe {
				libc::kill(pid, libc::SIGTERM);
			}
			let _ = child.wait();
		}
		std::fs::remove_file(&self.pid_file).ok();
	}
}

async fn wait_for_pid_file_async(path: PathBuf, executable: PathBuf) -> tg::Result<()> {
	tokio::task::spawn_blocking(move || wait_for_pid_file(&path, &executable))
		.await
		.map_err(|source| tg::error!(!source, "the pid file wait task panicked"))?
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

fn temporary_pid_file() -> PathBuf {
	let name = format!("tangram-pasta-{:016x}.pid", rand::random::<u64>());
	std::env::temp_dir().join(name)
}
