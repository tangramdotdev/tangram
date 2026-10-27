use {
	crate::prelude::*,
	std::{path::PathBuf, time::Duration},
};

pub use self::{
	attach::{attach, attach_with_instance},
	availability::Availability,
	build::{build, build_with_instance},
	checksum::Checksum,
	data::Data,
	debug::Debug,
	env::env,
	exec::{exec, exec_with_instance},
	handle::{Options, Process as Handle},
	id::Id,
	lease::Lease,
	metadata::Metadata,
	nonce::Nonce,
	outcome::Outcome,
	run::{run, run_with_instance},
	signal::Signal,
	source::Source,
	spawn::{spawn, spawn_with_instance},
	state::State,
	status::Status,
	stdio::Stdio,
	storage::Storage,
	tty::Tty,
};

pub mod attach;
pub mod availability;
pub mod build;
pub mod cancel;
pub mod checksum;
pub mod children;
pub mod control;
pub mod data;
pub mod debug;
pub mod env;
pub mod exec;
pub mod get;
pub mod handle;
pub mod id;
pub mod lease;
pub mod log;
pub mod metadata;
pub mod nonce;
pub mod outcome;
pub mod put;
pub mod run;
pub mod signal;
pub mod source;
pub mod spawn;
pub mod state;
pub mod status;
pub mod stdio;
pub mod storage;
pub mod touch;
pub mod tty;
pub mod wait;

/// The header with which a runner's request claims to act for one of its processes.
pub const PROCESS_HEADER: &str = "x-tg-process";

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub args: tg::value::Array,
	pub cache_location: Option<tg::location::Arg>,
	pub cached: Option<bool>,
	pub checksum: Option<tg::Checksum>,
	pub command: Option<tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::Command>>>,
	pub cpu: Option<tg::sandbox::Cpu>,
	pub cwd: Option<PathBuf>,
	pub debug: Option<tg::Either<bool, tg::process::Debug>>,
	pub env: tg::value::Map,
	pub executable: Option<tg::command::Executable>,
	pub host: Option<String>,
	pub location: Option<tg::location::Arg>,
	pub memory: Option<tg::sandbox::Resource<u64>>,
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

#[derive(Clone, Copy, Debug, Default)]
pub struct Config {
	pub attach: tg::process::attach::Config,
}

#[derive(Clone, Debug)]
pub enum SandboxArg {
	Arg(tg::process::SandboxCreateArg),
	Bool(bool),
	Referent(tg::Referent<tg::sandbox::Id>),
}

#[derive(Clone, Debug, Default)]
pub struct SandboxCreateArg {
	pub cpu: Option<tg::sandbox::Cpu>,
	pub hostname: Option<String>,
	pub location: Option<tg::location::Arg>,
	pub memory: Option<tg::sandbox::Resource<u64>>,
	pub mounts: Vec<tg::sandbox::Mount>,
	pub network: Option<tg::sandbox::Network>,
	pub owner: Option<tg::Principal>,
	pub storage: Option<tg::sandbox::Resource<u64>>,
	pub ttl: Option<Option<Duration>>,
}

impl Config {
	pub(crate) fn validate(self) -> tg::Result<Self> {
		self.attach
			.validate()
			.map_err(|source| tg::error!(!source, "invalid process attach configuration"))?;
		Ok(self)
	}
}
