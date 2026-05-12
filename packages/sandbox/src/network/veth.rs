use {
	crate::{
		netlink::Netlink,
		network::{host, ip},
	},
	std::{net::Ipv4Addr, os::fd::AsRawFd as _},
	tangram_client::prelude::*,
	tokio::io::{AsyncReadExt, AsyncWriteExt},
};

pub(crate) const BRIDGE_NAME: &str = "tangram0";

pub(crate) struct Network {
	bridge_name: String,
	gateway_ip: Ipv4Addr,
	guest: ip::Lease,
	_port_forwarding_rules: Vec<host::IptablesRuleGuard>,
}

#[derive(Debug)]
pub(crate) struct Pair {
	pub(crate) guest_name: String,
	pub(crate) guest_pipe: Option<std::os::unix::net::UnixStream>,
	pub(crate) host_pipe: tokio::net::UnixStream,
	pub(crate) netlink: Netlink,
}

impl Network {
	pub(crate) fn new(guest: ip::Lease, ports: &[tg::sandbox::Port]) -> tg::Result<Self> {
		let bridge_name = BRIDGE_NAME.to_owned();
		let port_forwarding_rules =
			host::add_port_forwarding_rules(&bridge_name, guest.addr, ports)?;
		Ok(Self {
			_port_forwarding_rules: port_forwarding_rules,
			bridge_name,
			gateway_ip: gateway_ip(),
			guest,
		})
	}

	pub(crate) fn bridge_name(&self) -> &str {
		&self.bridge_name
	}

	pub(crate) fn gateway_ip(&self) -> Ipv4Addr {
		self.gateway_ip
	}

	pub(crate) fn guest_ip(&self) -> Ipv4Addr {
		self.guest.addr
	}
}

impl Pair {
	pub(crate) fn new(id: &tg::sandbox::Id, bridge_name: &str) -> tg::Result<Self> {
		let id_str = id.to_string();
		if id_str.len() < 9 {
			return Err(tg::error!(%id, "the sandbox id is too short"));
		}
		let truncated = &id_str[..9];
		let host_name = format!("tg-vh-{truncated}");
		let guest_name = format!("tg-vc-{truncated}");

		let mut netlink = Netlink::new()?;
		netlink.link_add_veth_pair(&host_name, &guest_name)?;
		netlink.link_set_master(&host_name, bridge_name)?;
		netlink.link_set_up(&host_name)?;

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

		Ok(Self {
			guest_name,
			guest_pipe: Some(guest_pipe),
			host_pipe,
			netlink,
		})
	}

	pub(crate) async fn connect(&mut self, child: libc::pid_t) -> tg::Result<()> {
		self.host_pipe
			.read_u8()
			.await
			.map_err(|error| tg::error!(!error, "child process failed"))?;
		let pid = u32::try_from(child)
			.map_err(|error| tg::error!(!error, "the child pid does not fit in a u32"))?;
		self.netlink.link_set_netns_pid(&self.guest_name, pid)?;
		self.host_pipe
			.write_u8(0)
			.await
			.map_err(|error| tg::error!(!error, "child process failed"))?;
		Ok(())
	}
}

pub(crate) fn setup() -> tg::Result<()> {
	static SETUP: std::sync::OnceLock<tg::Result<()>> = std::sync::OnceLock::new();
	SETUP
		.get_or_init(|| {
			if let Err(error) = host::cleanup_persistent_rules(Some(BRIDGE_NAME)) {
				tracing::warn!(%error, "failed to clean up persistent sandbox rules");
			}
			create_bridge(BRIDGE_NAME, gateway_ip())
		})
		.clone()
}

pub(crate) fn create_bridge(name: &str, ip: Ipv4Addr) -> tg::Result<()> {
	let mut netlink = Netlink::new()?;
	if !netlink.link_exists(name)? {
		netlink.link_add_bridge(name)?;
	}
	netlink.addr_replace_v4(name, ip, 16)?;
	netlink.link_set_up(name)?;
	host::enable_ipv4_forwarding()?;
	ensure_bridge_iptables_rules(name, ip)?;
	Ok(())
}

fn gateway_ip() -> Ipv4Addr {
	Ipv4Addr::new(172, 18, 0, 1)
}

pub(crate) fn guest_ip_min() -> Ipv4Addr {
	Ipv4Addr::new(172, 18, 0, 4)
}

pub(crate) fn guest_ip_max() -> Ipv4Addr {
	Ipv4Addr::new(172, 18, 255, 255)
}

fn ensure_bridge_iptables_rules(bridge: &str, addr: Ipv4Addr) -> tg::Result<()> {
	let octets = addr.octets();
	let subnet = Ipv4Addr::new(octets[0], octets[1], 0, 0);
	let cidr = format!("{subnet}/16");
	host::get_or_set_iptables_rule(
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
	host::get_or_set_iptables_rule(&[], &["FORWARD", "-i", bridge, "-j", "ACCEPT"])?;
	host::get_or_set_iptables_rule(
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
