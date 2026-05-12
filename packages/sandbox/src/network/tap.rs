use {
	crate::{
		netlink::Netlink,
		network::{host, ip},
	},
	std::{
		net::Ipv4Addr,
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
	},
	tangram_client::prelude::*,
};

const TUNSETIFF: libc::c_ulong = 0x4004_54ca;

pub(crate) struct Network {
	guest: ip::Lease,
	host: ip::Lease,
	_port_forwarding_rules: Vec<host::IptablesRuleGuard>,
}

#[derive(Debug)]
pub(crate) struct Device {
	fd: OwnedFd,
	guest_ip: Ipv4Addr,
	host_ip: Ipv4Addr,
	mac: String,
	name: String,
	netlink: Netlink,
	netmask: Ipv4Addr,
}

impl Network {
	pub(crate) fn new(
		host: ip::Lease,
		guest: ip::Lease,
		ports: &[tg::sandbox::Port],
	) -> tg::Result<Self> {
		let prefix = format!("{}+", host::TAP_INTERFACE_NAME_PREFIX);
		let port_forwarding_rules = host::add_port_forwarding_rules(&prefix, guest.addr, ports)?;
		Ok(Self {
			_port_forwarding_rules: port_forwarding_rules,
			guest,
			host,
		})
	}

	pub(crate) fn guest_ip(&self) -> Ipv4Addr {
		self.guest.addr
	}

	pub(crate) fn host_ip(&self) -> Ipv4Addr {
		self.host.addr
	}
}

impl Device {
	pub(crate) fn new(id: &str, host_ip: Ipv4Addr, guest_ip: Ipv4Addr) -> tg::Result<Self> {
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

		host::enable_ipv4_forwarding()?;
		setup()?;

		// Clear FD_CLOEXEC so the fd survives exec() into cloud-hypervisor.
		let raw = fd.as_raw_fd();
		let flags = unsafe { libc::fcntl(raw, libc::F_GETFD) };
		if flags < 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(!error, "failed to get tap fd flags"));
		}
		if unsafe { libc::fcntl(raw, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(!error, "failed to clear O_CLOEXEC on tap fd"));
		}

		Ok(Self {
			fd,
			guest_ip,
			host_ip,
			mac,
			name,
			netlink,
			netmask,
		})
	}

	pub(crate) fn cloud_hypervisor_arg(&self) -> String {
		format!("fd={},mac={}", self.fd.as_raw_fd(), self.mac)
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

impl Drop for Device {
	fn drop(&mut self) {
		if let Err(error) = self.netlink.link_delete(&self.name) {
			tracing::error!(%error, "failed to clean up the tap");
		}
	}
}

pub(crate) fn setup() -> tg::Result<()> {
	static SETUP: std::sync::OnceLock<tg::Result<()>> = std::sync::OnceLock::new();
	SETUP
		.get_or_init(|| {
			if let Err(error) = host::cleanup_persistent_rules(None) {
				tracing::warn!(%error, "failed to clean up persistent sandbox rules");
			}
			ensure_iptables_rules()
		})
		.clone()
}

fn ensure_iptables_rules() -> tg::Result<()> {
	let prefix = format!("{}+", host::TAP_INTERFACE_NAME_PREFIX);

	// This rule makes guest VM traffic appear to come from the host IP, which prevents networks such as Wi-Fi access points from rejecting the packets.
	host::get_or_set_iptables_rule(
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
	host::get_or_set_iptables_rule(&[], &["FORWARD", "-i", prefix.as_str(), "-j", "ACCEPT"])?;

	// Allow response packets to come back to the guest. Two rules instead of one to avoid outside connections from reaching the guest.
	host::get_or_set_iptables_rule(
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

fn open_tap(name: &str) -> tg::Result<OwnedFd> {
	let fd = unsafe { libc::open(c"/dev/net/tun".as_ptr(), libc::O_RDWR | libc::O_NONBLOCK) };
	if fd < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to open /dev/net/tun"));
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
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "TUNSETIFF failed"));
	}
	Ok(tap)
}

fn tap_name(id: &str) -> String {
	let (_, id) = id.split_at(3);
	format!("{}{}", host::TAP_INTERFACE_NAME_PREFIX, id)
}
