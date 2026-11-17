use {
	std::{
		net::Ipv4Addr,
		os::unix::ffi::OsStrExt as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

mod iptables;
mod nft;

pub const TAP_INTERFACE_NAME_PREFIX: &str = "tg-";

#[derive(Debug)]
#[allow(dead_code)]
pub enum FirewallRuleGuard {
	Iptables(iptables::IptablesRuleGuard),
	Nft(nft::FirewallRuleGuard),
}

/// Lock the setup of the bridge, the tap interfaces, and their firewall rules, which every server on the host shares.
pub fn lock_setup() -> tg::Result<std::fs::File> {
	let path = Path::new(crate::lock::DIRECTORY).join("setup.lock");
	crate::lock::lock(&path)
}

pub fn enable_ipv4_forwarding() -> tg::Result<()> {
	std::fs::write("/proc/sys/net/ipv4/ip_forward", "1\n")
		.map_err(|error| tg::error!(!error, "failed to enable ipv4 forwarding"))
}

pub fn enable_route_localnet(interface: &str) -> tg::Result<()> {
	let path = format!("/proc/sys/net/ipv4/conf/{interface}/route_localnet");
	std::fs::write(&path, "1\n")
		.map_err(|error| tg::error!(!error, %path, "failed to enable route_localnet"))
}

pub fn disable_ipv6(interface: &str) -> tg::Result<()> {
	let path = format!("/proc/sys/net/ipv6/conf/{interface}/disable_ipv6");
	match std::fs::write(&path, "1\n") {
		Ok(()) => Ok(()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(error) => Err(tg::error!(!error, %path, "failed to disable ipv6")),
	}
}

pub fn setup_tap_networking(backend: crate::Firewall) -> tg::Result<()> {
	match backend {
		crate::Firewall::Iptables => iptables::setup_tap_networking(),
		crate::Firewall::Nft => nft::setup_tap_networking(),
	}
}

pub fn setup_bridge_networking(
	backend: crate::Firewall,
	bridge: &str,
	addr: Ipv4Addr,
	dns: &[Ipv4Addr],
) -> tg::Result<()> {
	match backend {
		crate::Firewall::Iptables => iptables::setup_bridge_networking(bridge, addr, dns),
		crate::Firewall::Nft => nft::setup_bridge_networking(bridge, addr, dns),
	}
}

pub fn add_port_forwarding_rules(
	backend: crate::Firewall,
	index: u64,
	identity: &std::path::Path,
	out_interface: &str,
	host_ip: Ipv4Addr,
	guest_ip: Ipv4Addr,
	ports: &[tg::sandbox::Port],
) -> tg::Result<Vec<FirewallRuleGuard>> {
	match backend {
		crate::Firewall::Iptables => iptables::add_port_forwarding_rules(
			index,
			identity,
			out_interface,
			host_ip,
			guest_ip,
			ports,
		)
		.map(|rules| rules.into_iter().map(FirewallRuleGuard::Iptables).collect()),
		crate::Firewall::Nft => {
			nft::add_port_forwarding_rules(index, identity, out_interface, host_ip, guest_ip, ports)
				.map(|rules| rules.into_iter().map(FirewallRuleGuard::Nft).collect())
		},
	}
}

/// Hash the path of a server's directory, which identifies the server's firewall rules.
fn hash_identity(identity: &Path) -> String {
	let identity = canonicalize_identity(identity);
	let mut hash = 0xcbf2_9ce4_8422_2325_u64;
	for byte in identity.as_os_str().as_bytes() {
		hash ^= u64::from(*byte);
		hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
	}
	format!("{hash:016x}")
}

fn canonicalize_identity(identity: &Path) -> PathBuf {
	std::fs::canonicalize(identity).unwrap_or_else(|_| identity.to_owned())
}
