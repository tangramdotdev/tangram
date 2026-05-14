use {std::net::Ipv4Addr, tangram_client::prelude::*};

mod iptables;
mod nft;

pub(crate) const TAP_INTERFACE_NAME_PREFIX: &str = "tg-";

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum FirewallRuleGuard {
	Iptables(iptables::IptablesRuleGuard),
	Nft(nft::FirewallRuleGuard),
}

pub(crate) fn enable_ipv4_forwarding() -> tg::Result<()> {
	std::fs::write("/proc/sys/net/ipv4/ip_forward", "1\n")
		.map_err(|error| tg::error!(!error, "failed to enable ipv4 forwarding"))
}

pub(crate) fn enable_route_localnet(interface: &str) -> tg::Result<()> {
	let path = format!("/proc/sys/net/ipv4/conf/{interface}/route_localnet");
	std::fs::write(&path, "1\n")
		.map_err(|error| tg::error!(!error, %path, "failed to enable route_localnet"))
}

pub(crate) fn setup_tap_networking(backend: crate::Firewall) -> tg::Result<()> {
	match backend {
		crate::Firewall::Iptables => iptables::setup_tap_networking(),
		crate::Firewall::Nft => nft::setup_tap_networking(),
	}
}

pub(crate) fn setup_bridge_networking(
	backend: crate::Firewall,
	bridge: &str,
	addr: Ipv4Addr,
) -> tg::Result<()> {
	match backend {
		crate::Firewall::Iptables => iptables::setup_bridge_networking(bridge, addr),
		crate::Firewall::Nft => nft::setup_bridge_networking(bridge, addr),
	}
}

pub(crate) fn add_port_forwarding_rules(
	backend: crate::Firewall,
	id: &tg::sandbox::Id,
	identity: &std::path::Path,
	out_interface: &str,
	host_ip: Ipv4Addr,
	guest_ip: Ipv4Addr,
	ports: &[tg::sandbox::Port],
) -> tg::Result<Vec<FirewallRuleGuard>> {
	match backend {
		crate::Firewall::Iptables => iptables::add_port_forwarding_rules(
			id,
			identity,
			out_interface,
			host_ip,
			guest_ip,
			ports,
		)
		.map(|rules| {
			rules
				.into_iter()
				.map(FirewallRuleGuard::Iptables)
				.collect()
		}),
		crate::Firewall::Nft => nft::add_port_forwarding_rules(
			id,
			identity,
			out_interface,
			host_ip,
			guest_ip,
			ports,
		)
		.map(|rules| rules.into_iter().map(FirewallRuleGuard::Nft).collect()),
	}
}
