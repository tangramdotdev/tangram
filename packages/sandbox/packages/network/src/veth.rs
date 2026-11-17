use {
	crate::{host, ip, netlink::Netlink},
	std::{net::Ipv4Addr, os::fd::AsRawFd as _, path::Path},
	tangram_client::prelude::*,
	tokio::io::{AsyncReadExt, AsyncWriteExt},
};

pub const BRIDGE_NAME: &str = "tangram0";
const GUEST_INTERFACE_NAME_PREFIX: &str = "tg-vc-";
const HOST_INTERFACE_NAME_PREFIX: &str = "tg-vh-";

pub struct Network {
	bridge_name: String,
	dns: Vec<Ipv4Addr>,
	gateway_ip: Ipv4Addr,
	guest: ip::Lease,
	port_forwarding_rules: Vec<host::FirewallRuleGuard>,
}

#[derive(Debug)]
pub struct Pair {
	attached: bool,
	created: bool,
	pub guest_name: String,
	pub guest_pipe: Option<std::os::unix::net::UnixStream>,
	host_name: String,
	pub host_pipe: tokio::net::UnixStream,
	pub netlink: Netlink,
}

impl Network {
	pub fn new(
		index: u64,
		identity: &Path,
		firewall: crate::Firewall,
		dns: Vec<Ipv4Addr>,
		guest: ip::Lease,
		ports: &[tg::sandbox::Port],
	) -> tg::Result<Self> {
		let bridge_name = BRIDGE_NAME.to_owned();
		let mut netlink = Netlink::new()?;
		netlink.neigh_delete_v4(&bridge_name, guest.addr)?;
		let port_forwarding_rules = host::add_port_forwarding_rules(
			firewall,
			index,
			identity,
			&bridge_name,
			gateway_ip(),
			guest.addr,
			ports,
		)?;
		Ok(Self {
			bridge_name,
			dns,
			gateway_ip: gateway_ip(),
			guest,
			port_forwarding_rules,
		})
	}

	#[must_use]
	pub fn bridge_name(&self) -> &str {
		&self.bridge_name
	}

	#[must_use]
	pub fn dns(&self) -> &[Ipv4Addr] {
		&self.dns
	}

	#[must_use]
	pub fn gateway_ip(&self) -> Ipv4Addr {
		self.gateway_ip
	}

	#[must_use]
	pub fn guest_ip(&self) -> Ipv4Addr {
		self.guest.addr
	}
}

impl Pair {
	pub fn new(bridge_name: &str) -> tg::Result<Self> {
		let suffix = interface_name_suffix();
		let host_name = format!("{HOST_INTERFACE_NAME_PREFIX}{suffix}");
		let guest_name = format!("{GUEST_INTERFACE_NAME_PREFIX}{suffix}");

		let netlink = Netlink::new()?;

		let (host_pipe, guest_pipe) = tokio::net::UnixStream::pair()
			.map_err(|error| tg::error!(!error, "failed to create socket pair"))?;
		let guest_pipe = guest_pipe
			.into_std()
			.map_err(|error| tg::error!(!error, "failed to create the guest pipe"))?;
		guest_pipe
			.set_nonblocking(false)
			.map_err(|error| tg::error!(!error, "failed to make the guest pipe blocking"))?;

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

		let mut pair = Self {
			attached: false,
			created: false,
			guest_name,
			guest_pipe: Some(guest_pipe),
			host_name,
			host_pipe,
			netlink,
		};
		pair.netlink
			.link_add_veth_pair(&pair.host_name, &pair.guest_name)?;
		pair.created = true;
		pair.netlink.link_set_master(&pair.host_name, bridge_name)?;
		host::disable_ipv6(&pair.host_name)?;
		pair.netlink.link_set_up(&pair.host_name)?;
		isolate_bridge_port(&pair.host_name)?;

		Ok(pair)
	}

	/// Attach the guest link to an already prepared network namespace.
	pub fn attach(&mut self, pid: u32) -> tg::Result<()> {
		self.netlink.link_set_netns_pid(&self.guest_name, pid)?;
		self.attached = true;
		Ok(())
	}

	pub async fn connect(&mut self, child: libc::pid_t) -> tg::Result<()> {
		self.host_pipe
			.read_u8()
			.await
			.map_err(|error| tg::error!(!error, "child process failed"))?;
		let pid = u32::try_from(child)
			.map_err(|error| tg::error!(!error, "the child pid does not fit in a u32"))?;
		self.netlink.link_set_netns_pid(&self.guest_name, pid)?;
		self.attached = true;
		self.host_pipe
			.write_u8(0)
			.await
			.map_err(|error| tg::error!(!error, "child process failed"))?;
		Ok(())
	}
}

impl Drop for Network {
	fn drop(&mut self) {
		// Remove the port forwarding rules before the guest address lease is released.
		self.port_forwarding_rules.clear();
	}
}

impl Drop for Pair {
	fn drop(&mut self) {
		if self.created && !self.attached {
			self.netlink.link_delete(&self.host_name).ok();
		}
	}
}

pub fn setup(firewall: crate::Firewall, dns: &[Ipv4Addr]) -> tg::Result<Vec<Ipv4Addr>> {
	// Keep the resolver configuration consistent with the firewall for the runner's lifetime.
	static SETUP: std::sync::OnceLock<tg::Result<Vec<Ipv4Addr>>> = std::sync::OnceLock::new();
	SETUP
		.get_or_init(|| {
			let dns = crate::dns::resolve(dns)?;

			// Create the bridge and its firewall rules while holding the host's setup lock, because every server on the host shares them.
			let _lock = host::lock_setup()?;
			create_bridge(firewall, BRIDGE_NAME, gateway_ip(), &dns)?;

			Ok(dns)
		})
		.clone()
}

pub fn create_bridge(
	firewall: crate::Firewall,
	name: &str,
	ip: Ipv4Addr,
	dns: &[Ipv4Addr],
) -> tg::Result<()> {
	let mut netlink = Netlink::new()?;
	if !netlink.link_exists(name)? {
		netlink.link_add_bridge(name)?;
	}
	host::disable_ipv6(name)?;
	netlink.addr_replace_v4(name, ip, 16)?;
	netlink.link_set_up(name)?;
	host::enable_ipv4_forwarding()?;
	host::enable_route_localnet(name)?;
	host::setup_bridge_networking(firewall, name, ip, dns)?;
	Ok(())
}

/// Generate a random suffix for the names of a pair, which keeps the names within the kernel's limit and unique across the servers on the host.
fn interface_name_suffix() -> String {
	const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
	let length = libc::IFNAMSIZ - 1 - HOST_INTERFACE_NAME_PREFIX.len();
	(0..length)
		.map(|_| char::from(ALPHABET[rand::random_range(0..ALPHABET.len())]))
		.collect()
}

fn isolate_bridge_port(interface: &str) -> tg::Result<()> {
	let path = format!("/sys/class/net/{interface}/brport/isolated");
	std::fs::write(&path, "1\n")
		.map_err(|error| tg::error!(!error, %interface, %path, "failed to isolate a bridge port"))
}

fn gateway_ip() -> Ipv4Addr {
	Ipv4Addr::new(172, 18, 0, 1)
}

#[must_use]
pub fn guest_ip_min() -> Ipv4Addr {
	Ipv4Addr::new(172, 18, 0, 4)
}

#[must_use]
pub fn guest_ip_max() -> Ipv4Addr {
	Ipv4Addr::new(172, 18, 255, 255)
}
