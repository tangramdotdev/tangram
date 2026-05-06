use {std::net::Ipv4Addr, tangram_client::prelude::*};

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub(crate) use self::linux::{Bridge, Tap};

/// Create or update a bridge interface with the given name and IPv4 address.
///
/// Returns an error on unsupported platforms (macOS).
pub fn create_bridge(name: &str, ip: Ipv4Addr) -> tg::Result<()> {
	#[cfg(target_os = "linux")]
	{
		self::linux::create_bridge(name, ip)
	}
	#[cfg(not(target_os = "linux"))]
	{
		let _ = (name, ip);
		Err(tg::error!(
			"bridge networking is not supported on this platform"
		))
	}
}

/// Best-effort cleanup of host-wide iptables rules left behind by previous
/// runs of the server. Returns an error on unsupported platforms (macOS).
pub fn cleanup_persistent_rules(bridge: Option<&str>) -> tg::Result<()> {
	#[cfg(target_os = "linux")]
	{
		self::linux::cleanup_persistent_rules(bridge)
	}
	#[cfg(not(target_os = "linux"))]
	{
		let _ = bridge;
		Err(tg::error!(
			"bridge networking is not supported on this platform"
		))
	}
}
