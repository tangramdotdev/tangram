pub use self::{
	client::{Client, Command, Message, PidMessage, SpawnMessage, Stdio, WaitMessage},
	server::Server,
};

pub type Error = std::io::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod client;
pub mod server;
