#[cfg(target_os = "linux")]
mod dns;

#[cfg(target_os = "linux")]
pub mod host;
#[cfg(target_os = "linux")]
pub mod ip;
#[cfg(target_os = "linux")]
pub mod netlink;
#[cfg(target_os = "linux")]
pub mod passt;
#[cfg(target_os = "linux")]
pub mod pasta;
#[cfg(target_os = "linux")]
pub mod tap;
#[cfg(target_os = "linux")]
pub mod veth;

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	derive_more::Display,
	derive_more::FromStr,
	serde::Deserialize,
	serde::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum Firewall {
	Iptables,

	#[default]
	Nft,
}

#[cfg(target_os = "linux")]
pub enum Network {
	Host,

	Passt(self::passt::Network),

	Pasta(self::pasta::Network),

	Tap(self::tap::Network),

	Veth(self::veth::Network),
}

#[cfg(target_os = "linux")]
#[must_use]
pub fn root() -> bool {
	(unsafe { libc::geteuid() }) == 0
}
