mod dns;

pub(crate) mod host;
pub mod ip;
pub(crate) mod linux;
pub(crate) mod passt;
pub(crate) mod pasta;
pub(crate) mod tap;
pub(crate) mod veth;

pub(crate) enum Network {
	Host,

	Passt(self::passt::Network),

	Pasta(self::pasta::Network),

	Tap(self::tap::Network),

	Veth(self::veth::Network),
}

pub(crate) fn root() -> bool {
	(unsafe { libc::geteuid() }) == 0
}
