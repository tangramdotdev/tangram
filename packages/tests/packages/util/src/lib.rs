#![allow(
	clippy::disallowed_methods,
	reason = "the tests spawn processes that do not execute files that this process writes"
)]

#[cfg(feature = "instance")]
mod client;
mod config;
#[cfg(feature = "instance")]
mod semaphore;

#[cfg(feature = "instance")]
pub use self::instance::Instance;
pub use self::{config::Config, temp::Temp};

#[cfg(feature = "cli")]
pub mod cli;
#[cfg(feature = "database")]
pub mod database;
#[cfg(feature = "instance")]
pub mod instance;
pub mod quota;
#[cfg(feature = "server")]
pub mod server;
pub mod temp;
