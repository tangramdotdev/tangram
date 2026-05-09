#[cfg(target_os = "linux")]
use std::net::Ipv4Addr;

#[cfg(target_os = "linux")]
pub(crate) mod host;
#[cfg(target_os = "linux")]
pub(crate) mod passt;
#[cfg(target_os = "linux")]
pub(crate) mod pasta;
#[cfg(target_os = "linux")]
pub(crate) mod tap;
#[cfg(target_os = "linux")]
pub(crate) mod veth;

pub mod ip;

pub(crate) enum Network {
	Host,

	#[cfg(target_os = "linux")]
	Passt(self::passt::Network),

	#[cfg(target_os = "linux")]
	Pasta(self::pasta::Network),

	#[cfg(target_os = "linux")]
	Tap(self::tap::Network),

	#[cfg(target_os = "linux")]
	Veth(self::veth::Network),
}

impl Network {
	#[cfg(target_os = "linux")]
	#[expect(clippy::match_same_arms)]
	pub(crate) fn guest_ip(&self) -> Option<Ipv4Addr> {
		match self {
			Self::Host => None,
			Self::Passt(network) => Some(network.guest_ip()),
			Self::Pasta(_) => None,
			Self::Tap(network) => Some(network.guest_ip()),
			Self::Veth(network) => Some(network.guest_ip()),
		}
	}

	#[cfg(target_os = "linux")]
	#[expect(clippy::match_same_arms)]
	pub(crate) fn host_ip(&self) -> Option<Ipv4Addr> {
		match self {
			Self::Host => None,
			Self::Passt(network) => Some(network.host_ip()),
			Self::Pasta(_) => None,
			Self::Tap(network) => Some(network.host_ip()),
			Self::Veth(_) => None,
		}
	}

	#[cfg(target_os = "linux")]
	pub(crate) fn kind(&self) -> &'static str {
		match self {
			Self::Host => "host",
			Self::Passt(_) => "passt",
			Self::Pasta(_) => "pasta",
			Self::Tap(_) => "tap",
			Self::Veth(_) => "veth",
		}
	}
}

#[cfg(target_os = "linux")]
pub(crate) fn root() -> bool {
	(unsafe { libc::geteuid() }) == 0
}
