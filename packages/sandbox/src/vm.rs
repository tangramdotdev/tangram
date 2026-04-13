use std::net::Ipv4Addr;

mod spawn;

pub(crate) use self::spawn::spawn;

pub mod init;
pub mod run;

#[derive(Clone, Debug)]
pub struct Network {
	pub dns_servers: Vec<Ipv4Addr>,
	pub gateway_ip: Ipv4Addr,
	pub guest_ip: Ipv4Addr,
	pub netmask: Ipv4Addr,
}

pub const HOST_VSOCK_CID: u32 = 2;
