mod amount;
mod builder;
mod cpu;
mod data;
mod mount;
mod network;
mod resource;

pub use self::{
	amount::Amount,
	builder::Builder,
	cpu::Cpu,
	data::{Data, ResourceUsage, Usage},
	handle::{Options, Sandbox as Handle},
	id::Id,
	mount::Mount,
	network::{Bridge, Network, Port, Protocol as PortProtocol, Range as PortRange},
	outcome::Outcome,
	resource::Resource,
	status::Status,
};

pub mod control;
pub mod create;
pub mod destroy;
pub mod get;
pub mod handle;
pub mod id;
pub mod list;
pub mod outcome;
pub mod processes;
pub mod start;
pub mod status;
pub mod stop;
pub mod wait;

/// The header with which a runner's request claims to act for one of its sandboxes.
pub const SANDBOX_HEADER: &str = "x-tg-sandbox";

#[derive(Clone, Copy, Debug, Default)]
pub struct Config {
	pub time_to_live: Option<std::time::Duration>,
	pub time_to_stop: Option<std::time::Duration>,
}
