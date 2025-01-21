use crate as tg;
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug)]
pub struct Builder {
	args: Vec<tg::Value>,
	checksum: Option<tg::Checksum>,
	cwd: Option<PathBuf>,
	env: BTreeMap<String, tg::Value>,
	executable: Option<tg::command::Executable>,
	host: String,
	sandbox: Option<tg::command::Sandbox>,
}

impl Builder {
	#[must_use]
	pub fn new(host: impl Into<String>) -> Self {
		Self {
			args: Vec::new(),
			checksum: None,
			cwd: None,
			env: BTreeMap::new(),
			executable: None,
			host: host.into(),
			sandbox: None,
		}
	}

	#[must_use]
	pub fn with_object(object: &tg::command::Object) -> Self {
		Self {
			args: object.args.clone(),
			checksum: object.checksum.clone(),
			cwd: object.cwd.clone(),
			env: object.env.clone(),
			executable: object.executable.clone(),
			host: object.host.clone(),
			sandbox: object.sandbox.clone(),
		}
	}

	#[must_use]
	pub fn args(mut self, args: Vec<tg::Value>) -> Self {
		self.args = args;
		self
	}

	#[must_use]
	pub fn checksum(mut self, checksum: impl Into<Option<tg::Checksum>>) -> Self {
		self.checksum = checksum.into();
		self
	}

	#[must_use]
	pub fn cwd(mut self, cwd: impl Into<Option<PathBuf>>) -> Self {
		self.cwd = cwd.into();
		self
	}

	#[must_use]
	pub fn env(mut self, env: BTreeMap<String, tg::Value>) -> Self {
		self.env = env;
		self
	}

	#[must_use]
	pub fn executable(mut self, executable: impl Into<Option<tg::command::Executable>>) -> Self {
		self.executable = executable.into();
		self
	}

	#[must_use]
	pub fn host(mut self, host: String) -> Self {
		self.host = host;
		self
	}

	#[must_use]
	pub fn sandbox(mut self, sandbox: impl Into<Option<tg::command::Sandbox>>) -> Self {
		self.sandbox = sandbox.into();
		self
	}

	#[must_use]
	pub fn build(self) -> tg::Command {
		tg::Command::with_object(tg::command::Object {
			args: self.args,
			checksum: self.checksum,
			cwd: self.cwd,
			env: self.env,
			executable: self.executable,
			host: self.host,
			sandbox: self.sandbox,
		})
	}
}
