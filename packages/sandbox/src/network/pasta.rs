use {
	std::{
		net::Ipv4Addr,
		os::fd::AsRawFd as _,
		path::PathBuf,
		process::{Child, Command},
		time::{Duration, Instant},
	},
	tangram_client as tg,
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
};

#[derive(Default)]
pub struct Options {
	pub executable: Option<PathBuf>,
	pub mode: Mode,
}

pub enum Mode {
	Netns {
		interface: Option<String>,
	},
	VhostUser {
		socket: PathBuf,
		address: Ipv4Addr,
		netmask: Ipv4Addr,
		gateway: Ipv4Addr,
	},
}

/// A network provided by a supervised `pasta` (or `passt`) process.
pub struct Network {
	pub child: Option<Child>,
	pub guest_pipe: Option<std::os::unix::net::UnixStream>,
	pub host_pipe: Option<tokio::net::UnixStream>,
	pub options: Options,
}

const TIMEOUT_MS: u64 = 5000;
const POLL_INTERVAL_MS: u64 = 16;
pub const DNS_FORWARD_IP: &str = "169.254.1.1";

impl Network {
	pub fn new(options: Options) -> tg::Result<Self> {
		let (host_pipe, guest_pipe) = match &options.mode {
			Mode::Netns { .. } => {
				let (host_pipe, guest_pipe) = tokio::net::UnixStream::pair().map_err(|source| {
					tg::error!(!source, "failed to create the pasta sync socket pair")
				})?;
				let guest_pipe = guest_pipe.into_std().map_err(|source| {
					tg::error!(!source, "failed to convert the guest pipe to std")
				})?;
				guest_pipe.set_nonblocking(false).map_err(|source| {
					tg::error!(!source, "failed to set the guest pipe blocking")
				})?;
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
				(Some(host_pipe), Some(guest_pipe))
			},
			Mode::VhostUser { socket, .. } => {
				match std::fs::remove_file(socket) {
					Ok(()) => (),
					Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
					Err(source) => {
						return Err(tg::error!(
							!source,
							path = %socket.display(),
							"failed to remove the stale vhost-user socket"
						));
					},
				}
				(None, None)
			},
		};

		Ok(Self {
			child: None,
			guest_pipe,
			host_pipe,
			options,
		})
	}

	pub async fn start_netns(&mut self, pid: libc::pid_t) -> tg::Result<()> {
		let Mode::Netns { interface } = &self.options.mode else {
			return Err(tg::error!("connect is only valid in netns mode"));
		};
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
			.options
			.executable
			.as_deref()
			.unwrap_or_else(|| std::path::Path::new("pasta"));
		let mut command = Command::new(executable);
		command
			.arg("--foreground") // run pasta in the foreground, die
			.arg("--config-net") // setup addr/routes in the net namespace
			.arg("--no-map-gw") // disable loopback to the host
			.arg("-t")
			.arg("none") // no TCP port forwarding host → guest
			.arg("-u")
			.arg("none") // no UDP port forwarding host → guest
			.arg("-T")
			.arg("none") // no TCP port forwarding guest → host
			.arg("-U")
			.arg("none") // no UDP port forwarding guest → host
			.arg("--dns-forward")
			.arg(DNS_FORWARD_IP)
			.arg("--quiet");
		if let Some(interface) = interface {
			command.arg("-i").arg(interface);
		}
		command.arg(pid.to_string());
		let child = command
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn pasta"))?;
		self.child = Some(child);

		// Poll the child's view of the routing table to wait until pasta has finished.
		let path = format!("/proc/{pid}/net/route");
		tokio::time::timeout(std::time::Duration::from_millis(TIMEOUT_MS), async {
			loop {
				match tokio::fs::read_to_string(&path).await {
					Ok(contents) if contents.lines().nth(1).is_some() => break,
					Ok(_) => (),
					Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
						return Err(tg::error!(
							"the sandbox child exited before pasta was ready"
						));
					},
					Err(error) => {
						return Err(tg::error!(
							!error,
							"failed to read the sandbox child's routing table"
						));
					},
				}
				tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
			}
			Ok::<_, tg::Error>(())
		})
		.await
		.map_err(|_| tg::error!("timed out waiting for passt to start"))??;

		// Signal the child to proceed.
		host_pipe
			.write_u8(0)
			.await
			.map_err(|source| tg::error!(!source, "child process failed"))?;
		Ok(())
	}

	pub fn start_vhost_user(&mut self) -> tg::Result<()> {
		let Mode::VhostUser {
			socket,
			address,
			netmask,
			gateway,
		} = &self.options.mode
		else {
			return Err(tg::error!("start is only valid in vhost-user mode"));
		};
		let executable = self
			.options
			.executable
			.as_deref()
			.unwrap_or_else(|| std::path::Path::new("passt"));
		let mut command = Command::new(executable);
		let dns_forward = *gateway;
		let resolv_conf = std::fs::read_to_string("/etc/resolv.conf")
			.map_err(|source| tg::error!(!source, "failed to read /etc/resolv.conf"))?;
		let dns_host = resolv_conf
			.lines()
			.find_map(|line| {
				let line = line.trim_start();
				if line.starts_with('#') || line.starts_with(';') {
					return None;
				}
				let rest = line.strip_prefix("nameserver")?;
				rest.trim().parse::<Ipv4Addr>().ok()
			})
			.ok_or_else(|| tg::error!("/etc/resolv.conf has no IPv4 nameserver entry"))?;
		command
			.arg("--no-map-gw")
			.arg("--vhost-user")
			.arg("--foreground")
			.arg("--quiet")
			.arg("--socket")
			.arg(socket)
			.arg("-a")
			.arg(address.to_string())
			.arg("-n")
			.arg(netmask.to_string())
			.arg("-g")
			.arg(gateway.to_string())
			.arg("--dns-forward")
			.arg(dns_forward.to_string())
			.arg("--dns-host")
			.arg(dns_host.to_string());
		let child = command
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn {}", executable.display()))?;
		self.child = Some(child);

		// Poll for the listening socket to appear so cloud-hypervisor can connect on its first attempt.
		let deadline = Instant::now() + Duration::from_millis(TIMEOUT_MS);
		loop {
			if std::fs::metadata(socket).is_ok() {
				break;
			}
			if let Some(child) = self.child.as_mut()
				&& let Some(status) = child.try_wait().map_err(|source| {
					tg::error!(!source, "failed to poll {}", executable.display())
				})? {
				return Err(tg::error!(
					%status,
					"{} passt exited before opening the vhost-user socket",
					executable.display()
				));
			}
			if Instant::now() >= deadline {
				return Err(tg::error!(
					"timed out waiting for {} to open the vhost-user socket",
					executable.display(),
				));
			}
			std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
		}
		Ok(())
	}
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
		if let Mode::VhostUser { socket, .. } = &self.options.mode {
			std::fs::remove_file(socket).ok();
		}
	}
}

impl Default for Mode {
	fn default() -> Self {
		Self::Netns { interface: None }
	}
}
