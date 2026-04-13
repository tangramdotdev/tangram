use {
	std::{
		hash::{DefaultHasher, Hash as _, Hasher as _},
		net::Ipv4Addr,
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
	},
	tangram_client as tg,
};

pub const HOST_TAP_PREFIX: &str = "tg-";

// TUNSETIFF: _IOW('T', 202, int) = 0x400454ca.
const TUNSETIFF: libc::c_ulong = 0x4004_54ca;

#[derive(Debug)]
pub struct Tap {
	pub name: String,
	pub mac: String,
	pub guest_ip: Ipv4Addr,
	pub host_ip: Ipv4Addr,
	pub netmask: Ipv4Addr,
	pub tap: OwnedFd,
}

impl Tap {
	pub fn new(id: &str) -> tg::Result<Tap> {
		if !crate::util::user_is_root() {
			return Err(tg::error!("networking requires root permissions"));
		}

		// Derive a /30 slice of 172.16.0.0/12 from a hash of the id, skipping
		// 172.17.0.0/16 to avoid collision with Docker's default bridge.
		const SUBNETS_PER_16: u64 = 16_384;
		const USABLE_16S: u64 = 15;
		const TOTAL_SUBNETS: u64 = SUBNETS_PER_16 * USABLE_16S;
		let mut hasher = DefaultHasher::new();
		id.hash(&mut hasher);
		let idx = hasher.finish() % TOTAL_SUBNETS;

		let sixteen_idx = idx / SUBNETS_PER_16;
		let within_idx = u32::try_from(idx % SUBNETS_PER_16).unwrap();
		let second_octet = if sixteen_idx == 0 {
			16
		} else {
			u8::try_from(17 + sixteen_idx).unwrap()
		};
		let base = u32::from_be_bytes([172, second_octet, 0, 0]) + within_idx * 4;
		let host_ip = Ipv4Addr::from(base + 1);
		let guest_ip = Ipv4Addr::from(base + 2);
		let netmask = Ipv4Addr::new(255, 255, 255, 252);

		let name = tap_name(id);

		let bytes = rand::random::<[u8; 5]>();
		let mac = format!(
			"{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
			0x02, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4],
		);

		let tap = open_tap(&name)?;

		ip(&format!("link set {name} up"))?;
		ip(&format!("addr add {host_ip}/30 dev {name}"))?;

		enable_ipv4_forwarding()?;
		ensure_iptables_rules()?;

		// Clear FD_CLOEXEC so the fd survives exec() into cloud-hypervisor.
		let raw = tap.as_raw_fd();
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
			name,
			mac,
			guest_ip,
			host_ip,
			netmask,
			tap,
		})
	}
}

impl Drop for Tap {
	fn drop(&mut self) {
		ip(&format!("link delete {}", self.name))
			.inspect_err(|error| tracing::error!(%error, "failed to clean up the tap"))
			.ok();
	}
}

fn tap_name(id: &str) -> String {
	let mut hasher = DefaultHasher::new();
	id.hash(&mut hasher);
	format!(
		"{HOST_TAP_PREFIX}{:012x}",
		hasher.finish() & 0xFFF_FFFF_FFFF
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
	let status = std::process::Command::new("iptables")
		.args(&check)
		.stdout(std::process::Stdio::null())
		.stderr(std::process::Stdio::null())
		.status()
		.map_err(|source| tg::error!(!source, "failed to spawn iptables"))?;
	if status.success() {
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

fn ip(cli: &str) -> tg::Result<()> {
	let output = std::process::Command::new("ip")
		.args(cli.split(' '))
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|source| tg::error!(!source, "failed to spawn `ip`"))?;
	if !output.status.success() {
		let code = output.status.code();
		let stderr = String::from_utf8_lossy(&output.stderr);
		return Err(tg::error!(?code, %stderr, "ip {cli} failed"));
	}
	Ok(())
}
