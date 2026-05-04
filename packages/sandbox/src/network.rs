use {
	crate::netlink::Netlink,
	std::{
		net::Ipv4Addr,
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
	},
	tangram_client as tg,
	tokio::io::{AsyncReadExt, AsyncWriteExt},
};

pub const HOST_TAP_PREFIX: &str = "tg-";

// TUNSETIFF: _IOW('T', 202, int) = 0x400454ca.
const TUNSETIFF: libc::c_ulong = 0x4004_54ca;

#[derive(Debug)]
pub struct Tap {
	pub fd: OwnedFd,
	pub guest_ip: Ipv4Addr,
	pub host_ip: Ipv4Addr,
	pub mac: String,
	pub name: String,
	pub netlink: Netlink,
	pub netmask: Ipv4Addr,
}

impl Tap {
	pub fn new(id: &str, host_ip: Ipv4Addr, guest_ip: Ipv4Addr) -> tg::Result<Tap> {
		let netmask = Ipv4Addr::new(255, 255, 255, 252);
		let name = tap_name(id);
		let bytes = rand::random::<[u8; 5]>();
		let mac = format!(
			"{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
			0x02, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4],
		);

		let fd = open_tap(&name)?;

		let mut netlink = Netlink::new()?;
		netlink.link_set_up(&name)?;
		netlink.addr_add_v4(&name, host_ip, 30)?;

		enable_ipv4_forwarding()?;
		ensure_iptables_rules()?;

		// Clear FD_CLOEXEC so the fd survives exec() into cloud-hypervisor.
		let raw = fd.as_raw_fd();
		let flags = unsafe { libc::fcntl(raw, libc::F_GETFD) };
		if flags < 0 {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(!source, "failed to get tap fd flags"));
		}
		if unsafe { libc::fcntl(raw, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(!source, "failed to clear O_CLOEXEC on tap fd"));
		}

		Ok(Tap {
			fd,
			guest_ip,
			host_ip,
			mac,
			name,
			netlink,
			netmask,
		})
	}
}

impl Drop for Tap {
	fn drop(&mut self) {
		if let Err(error) = self.netlink.link_delete(&self.name) {
			tracing::error!(%error, "failed to clean up the tap");
		}
	}
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Bridge {
	pub guest_name: String,
	pub guest_pipe: Option<std::os::unix::net::UnixStream>,
	pub host_name: String,
	pub host_pipe: tokio::net::UnixStream,
	pub netlink: Netlink,
}

#[allow(dead_code)]
impl Bridge {
	pub fn new(id: &tg::sandbox::Id, bridge: &str) -> tg::Result<Self> {
		let id_str = id.to_string();
		if id_str.len() < 9 {
			return Err(tg::error!(%id, "the sandbox id is too short"));
		}
		let truncated = &id_str[..9];
		let host_name = format!("tg-vh-{truncated}");
		let guest_name = format!("tg-vc-{truncated}");

		let mut netlink = Netlink::new()?;
		netlink.link_add_veth_pair(&host_name, &guest_name)?;
		netlink.link_set_master(&host_name, bridge)?;
		netlink.link_set_up(&host_name)?;

		let (host_pipe, guest_pipe) = tokio::net::UnixStream::pair()
			.map_err(|source| tg::error!(!source, "failed to create socket pair"))?;
		let guest_pipe = guest_pipe
			.into_std()
			.map_err(|source| tg::error!(!source, "failed to create the guest pipe"))?;
		guest_pipe
			.set_nonblocking(false)
			.map_err(|source| tg::error!(!source, "failed to set the pipe as non blocking"))?;

		unsafe {
			let raw = guest_pipe.as_raw_fd();
			let flags = libc::fcntl(raw, libc::F_GETFD);
			if flags < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"fcntl failed"
				));
			}
			let result = libc::fcntl(raw, libc::F_SETFD, flags & !libc::FD_CLOEXEC);
			if result < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"fcntl failed"
				));
			}
		}

		Ok(Self {
			guest_name,
			guest_pipe: Some(guest_pipe),
			host_name,
			host_pipe,
			netlink,
		})
	}

	pub async fn connect(&mut self, child: libc::pid_t) -> tg::Result<()> {
		self.host_pipe
			.read_u8()
			.await
			.map_err(|source| tg::error!(!source, "child process failed"))?;
		let pid = u32::try_from(child)
			.map_err(|source| tg::error!(!source, "the child pid does not fit in a u32"))?;
		self.netlink.link_set_netns_pid(&self.guest_name, pid)?;
		self.host_pipe
			.write_u8(0)
			.await
			.map_err(|source| tg::error!(!source, "child process failed"))?;
		Ok(())
	}
}

#[allow(dead_code)]
pub fn create_bridge(name: &str, addr: Ipv4Addr) -> tg::Result<()> {
	let mut netlink = Netlink::new()?;
	if !netlink.link_exists(name)? {
		netlink.link_add_bridge(name)?;
	}
	netlink.addr_replace_v4(name, addr, 16)?;
	netlink.link_set_up(name)?;
	enable_ipv4_forwarding()?;
	ensure_bridge_iptables_rules(name, addr)?;
	Ok(())
}

/// Best-effort removal of host-wide iptables rules that prior runs of the server
/// may have left behind. If the `iptables` binary reports a permission error,
/// log a warning and return Ok rather than failing — the server can still start
/// without networking, and the operator may not have CAP_NET_ADMIN.
#[allow(dead_code)]
pub fn cleanup_persistent_rules(bridge: Option<&str>) -> tg::Result<()> {
	let tap_prefix = format!("{HOST_TAP_PREFIX}+");
	delete_iptables_rule(
		&["-t", "nat"],
		&[
			"POSTROUTING",
			"-s",
			"172.16.0.0/12",
			"!",
			"-o",
			tap_prefix.as_str(),
			"-j",
			"MASQUERADE",
		],
	)?;
	delete_iptables_rule(&[], &["FORWARD", "-i", tap_prefix.as_str(), "-j", "ACCEPT"])?;
	delete_iptables_rule(
		&[],
		&[
			"FORWARD",
			"-o",
			tap_prefix.as_str(),
			"-m",
			"conntrack",
			"--ctstate",
			"ESTABLISHED,RELATED",
			"-j",
			"ACCEPT",
		],
	)?;
	if let Some(bridge) = bridge {
		delete_iptables_rule(&[], &["FORWARD", "-i", bridge, "-j", "ACCEPT"])?;
		delete_iptables_rule(
			&[],
			&[
				"FORWARD",
				"-o",
				bridge,
				"-m",
				"conntrack",
				"--ctstate",
				"ESTABLISHED,RELATED",
				"-j",
				"ACCEPT",
			],
		)?;
		delete_bridge_masquerade_rules(bridge)?;
	}
	Ok(())
}

#[allow(dead_code)]
fn ensure_bridge_iptables_rules(bridge: &str, addr: Ipv4Addr) -> tg::Result<()> {
	let octets = addr.octets();
	let subnet = Ipv4Addr::new(octets[0], octets[1], 0, 0);
	let cidr = format!("{subnet}/16");
	get_or_set_iptables_rule(
		&["-t", "nat"],
		&[
			"POSTROUTING",
			"-s",
			cidr.as_str(),
			"!",
			"-o",
			bridge,
			"-j",
			"MASQUERADE",
		],
	)?;
	get_or_set_iptables_rule(&[], &["FORWARD", "-i", bridge, "-j", "ACCEPT"])?;
	get_or_set_iptables_rule(
		&[],
		&[
			"FORWARD",
			"-o",
			bridge,
			"-m",
			"conntrack",
			"--ctstate",
			"ESTABLISHED,RELATED",
			"-j",
			"ACCEPT",
		],
	)?;
	Ok(())
}

fn tap_name(id: &str) -> String {
	let (_, id) = id.split_at(3);
	format!(
		"{HOST_TAP_PREFIX}{id}",
	)
}

// TODO: simplify and remove unnecessary unsafe blocks
fn open_tap(name: &str) -> tg::Result<OwnedFd> {
	let fd = unsafe { libc::open(c"/dev/net/tun".as_ptr(), libc::O_RDWR | libc::O_NONBLOCK) };
	if fd < 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to open /dev/net/tun"));
	}
	let tap = unsafe { OwnedFd::from_raw_fd(fd) };

	let bytes = name.as_bytes();
	if bytes.len() >= libc::IFNAMSIZ {
		return Err(tg::error!(%name, "the tap name is too long"));
	}
	let mut ifr = unsafe { std::mem::zeroed::<libc::ifreq>() };
	unsafe {
		std::ptr::copy_nonoverlapping(
			bytes.as_ptr().cast(),
			ifr.ifr_name.as_mut_ptr(),
			bytes.len(),
		);
		ifr.ifr_ifru.ifru_flags =
			libc::c_short::try_from(libc::IFF_TAP | libc::IFF_NO_PI | libc::IFF_VNET_HDR).unwrap();
	}
	if unsafe { libc::ioctl(tap.as_raw_fd(), TUNSETIFF, std::ptr::addr_of_mut!(ifr)) } < 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "TUNSETIFF failed"));
	}
	Ok(tap)
}

fn enable_ipv4_forwarding() -> tg::Result<()> {
	std::fs::write("/proc/sys/net/ipv4/ip_forward", "1\n")
		.map_err(|source| tg::error!(!source, "failed to enable ipv4 forwarding"))
}

fn ensure_iptables_rules() -> tg::Result<()> {
	let prefix = format!("{HOST_TAP_PREFIX}+");

	// This rule ensures that outgoing packets from the guest VM are forwarded with the host IP as their source, preventing eg. a WIFI access point from rejecting the packets. It is as if the packets come from the host instead of the guest.
	get_or_set_iptables_rule(
		&["-t", "nat"],
		&[
			"POSTROUTING",
			"-s",
			"172.16.0.0/12",
			"!",
			"-o",
			prefix.as_str(),
			"-j",
			"MASQUERADE",
		],
	)?;

	// Forward the packets out of the guest.
	get_or_set_iptables_rule(&[], &["FORWARD", "-i", prefix.as_str(), "-j", "ACCEPT"])?;

	// Allow response packets to come back to the guest. Two rules instead of one to avoid outside connections from reaching the guest.
	get_or_set_iptables_rule(
		&[],
		&[
			"FORWARD",
			"-o",
			prefix.as_str(),
			"-m",
			"conntrack",
			"--ctstate",
			"ESTABLISHED,RELATED",
			"-j",
			"ACCEPT",
		],
	)?;

	Ok(())
}

fn get_or_set_iptables_rule(table: &[&str], rule: &[&str]) -> tg::Result<()> {
	let mut check: Vec<&str> = Vec::with_capacity(table.len() + 1 + rule.len());
	check.extend_from_slice(table);
	check.push("-C");
	check.extend_from_slice(rule);
	let output = std::process::Command::new("iptables")
		.args(&check)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|source| tg::error!(!source, "failed to spawn iptables"))?;
	if output.status.success() {
		return Ok(());
	}
	// Insert at the top so we land before any chains that default-drop.
	let mut insert: Vec<&str> = Vec::with_capacity(table.len() + 1 + rule.len());
	insert.extend_from_slice(table);
	insert.push("-I");
	insert.extend_from_slice(rule);
	let output = std::process::Command::new("iptables")
		.args(&insert)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|source| tg::error!(!source, "failed to spawn iptables"))?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		let rule = rule.join(" ");
		return Err(tg::error!(%stderr, %rule, "failed to install iptables rule"));
	}
	Ok(())
}

fn delete_iptables_rule(table: &[&str], rule: &[&str]) -> tg::Result<()> {
	loop {
		let mut args: Vec<&str> = Vec::with_capacity(table.len() + 1 + rule.len());
		args.extend_from_slice(table);
		args.push("-D");
		args.extend_from_slice(rule);
		let output = match std::process::Command::new("iptables")
			.args(&args)
			.stderr(std::process::Stdio::piped())
			.output()
		{
			Ok(output) => output,
			Err(source) => {
				if source.kind() == std::io::ErrorKind::NotFound {
					tracing::warn!("iptables not found; skipping rule cleanup");
					return Ok(());
				}
				return Err(tg::error!(!source, "failed to spawn iptables"));
			},
		};
		if output.status.success() {
			continue;
		}
		let stderr = String::from_utf8_lossy(&output.stderr);
		if is_iptables_permission_error(&stderr) {
			tracing::warn!(rule = %rule.join(" "), %stderr, "iptables cleanup denied");
			return Ok(());
		}
		// Any other failure (typically "rule does not exist") ends the loop.
		return Ok(());
	}
}

fn delete_bridge_masquerade_rules(bridge: &str) -> tg::Result<()> {
	let output = match std::process::Command::new("iptables")
		.args(["-t", "nat", "-S", "POSTROUTING"])
		.stderr(std::process::Stdio::piped())
		.output()
	{
		Ok(output) => output,
		Err(source) => {
			if source.kind() == std::io::ErrorKind::NotFound {
				tracing::warn!("iptables not found; skipping rule cleanup");
				return Ok(());
			}
			return Err(tg::error!(!source, "failed to spawn iptables"));
		},
	};
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		if is_iptables_permission_error(&stderr) {
			tracing::warn!(%stderr, "iptables cleanup denied");
		}
		return Ok(());
	}
	let stdout = String::from_utf8_lossy(&output.stdout);
	let needle = format!("! -o {bridge} ");
	for line in stdout.lines() {
		let Some(rest) = line.strip_prefix("-A POSTROUTING ") else {
			continue;
		};
		if !rest.contains(&needle) || !rest.contains("MASQUERADE") {
			continue;
		}
		let mut args = vec!["-t", "nat", "-D", "POSTROUTING"];
		args.extend(rest.split_whitespace());
		let output = match std::process::Command::new("iptables")
			.args(&args)
			.stderr(std::process::Stdio::piped())
			.output()
		{
			Ok(output) => output,
			Err(source) => return Err(tg::error!(!source, "failed to spawn iptables")),
		};
		if output.status.success() {
			continue;
		}
		let stderr = String::from_utf8_lossy(&output.stderr);
		if is_iptables_permission_error(&stderr) {
			tracing::warn!(rule = %rest, %stderr, "iptables cleanup denied");
			return Ok(());
		}
	}
	Ok(())
}

fn is_iptables_permission_error(stderr: &str) -> bool {
	let stderr = stderr.to_ascii_lowercase();
	stderr.contains("permission denied")
		|| stderr.contains("operation not permitted")
		|| stderr.contains("you must be root")
}
