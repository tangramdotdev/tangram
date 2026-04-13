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

pub const VMADDR_CID_HOST: u32 = 2;
pub const VMADDR_CID_ANY: u32 = u32::MAX;
