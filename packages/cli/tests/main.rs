#![allow(
	clippy::disallowed_methods,
	reason = "the tests spawn processes that do not execute files that this process writes"
)]

mod attach;
mod grants;
mod instance;
mod lifecycle;
mod log;
#[cfg(target_os = "linux")]
mod process;
mod runner;
#[cfg(all(
	feature = "foundationdb",
	feature = "nats",
	feature = "postgres",
	feature = "scylla"
))]
mod spawn;
