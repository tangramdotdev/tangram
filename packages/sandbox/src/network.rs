use std::net::Ipv4Addr;

pub(crate) mod host;
pub(crate) mod passt;
pub(crate) mod pasta;
pub(crate) mod tap;
pub(crate) mod veth;

pub mod ip;

pub(crate) enum Network {
	Host,

	Passt(self::passt::Network),

	Pasta(self::pasta::Network),

	Tap(self::tap::Network),

	Veth(self::veth::Network),
}

impl Network {
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

pub(crate) fn root() -> bool {
	(unsafe { libc::geteuid() }) == 0
}
