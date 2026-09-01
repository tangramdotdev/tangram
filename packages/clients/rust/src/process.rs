use {
	crate::prelude::*,
	std::{path::PathBuf, time::Duration},
};

pub use self::{
	availability::Availability,
	build::{build, build_with_handle},
	data::Data,
	debug::Debug,
	env::env,
	exec::{exec, exec_with_handle},
	handle::{Options, Process as Handle},
	id::Id,
	metadata::Metadata,
	run::{run, run_with_handle},
	signal::Signal,
	spawn::{spawn, spawn_with_handle},
	state::State,
	status::Status,
	stdio::Stdio,
	tty::Tty,
	wait::Wait,
};

pub mod availability;
pub mod build;
pub mod cancel;
pub mod children;
pub mod control;
pub mod data;
pub mod debug;
pub mod env;
pub mod exec;
pub mod get;
pub mod handle;
pub mod id;
pub mod metadata;
pub mod put;
pub mod run;
pub mod signal;
pub mod spawn;
pub mod state;
pub mod status;
pub mod stdio;
pub mod touch;
pub mod tty;
pub mod wait;

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub args: tg::value::Array,
	pub cache_location: Option<tg::location::Arg>,
	pub cached: Option<bool>,
	pub checksum: Option<tg::Checksum>,
	pub command: Option<tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::Command>>>,
	pub cpu: Option<u64>,
	pub cwd: Option<PathBuf>,
	pub debug: Option<tg::Either<bool, tg::process::Debug>>,
	pub env: tg::value::Map,
	pub executable: Option<tg::command::Executable>,
	pub host: Option<String>,
	pub location: Option<tg::location::Arg>,
	pub memory: Option<u64>,
	pub mounts: Vec<tg::sandbox::Mount>,
	pub name: Option<String>,
	pub network: Option<tg::sandbox::Network>,
	pub owner: Option<tg::Principal>,
	pub ports: Vec<tg::sandbox::Port>,
	pub public: bool,
	pub retry: bool,
	pub sandbox: Option<tg::process::SandboxArg>,
	pub stderr: tg::process::Stdio,
	pub stdin: tg::process::Stdio,
	pub stdout: tg::process::Stdio,
	pub tty: Option<tg::Either<bool, tg::process::Tty>>,
	pub user: Option<String>,
}

#[derive(Clone, Debug)]
pub enum SandboxArg {
	Arg(tg::process::SandboxCreateArg),
	Bool(bool),
	Id(tg::sandbox::Id),
}

#[derive(Clone, Debug, Default)]
pub struct SandboxCreateArg {
	pub cpu: Option<u64>,
	pub hostname: Option<String>,
	pub isolation: Option<tg::sandbox::Isolation>,
	pub location: Option<tg::location::Arg>,
	pub memory: Option<u64>,
	pub mounts: Vec<tg::sandbox::Mount>,
	pub network: Option<tg::sandbox::Network>,
	pub owner: Option<tg::Principal>,
	pub ttl: Option<Option<Duration>>,
}
