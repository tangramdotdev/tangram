mod amount;
mod builder;
mod cpu;
mod data;
mod id;
mod isolation;
mod mount;
mod network;
mod resource;

pub use {
	self::{
		amount::Amount,
		builder::Builder,
		cpu::Cpu,
		data::{Data, ResourceUsage, Usage},
		handle::{Options, Sandbox as Handle},
		id::Id,
		isolation::Isolation,
		mount::Mount,
		network::{Bridge, Network, Port, Protocol as PortProtocol, Range as PortRange},
		resource::Resource,
		status::Status,
	},
	crate::process::Source,
};

pub mod control;
pub mod create;
pub mod destroy;
pub mod get;
pub mod handle;
pub mod list;
pub mod processes;
pub mod status;

#[derive(Clone, Copy, Debug)]
pub struct Config {
	pub ttl: std::time::Duration,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			ttl: std::time::Duration::from_mins(5),
		}
	}
}
