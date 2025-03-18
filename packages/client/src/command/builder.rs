use crate as tg;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Builder {
	args: Vec<tg::Value>,
	env: BTreeMap<String, tg::Value>,
	executable: Option<tg::command::Executable>,
	host: String,
	mounts: Vec<tg::command::Mount>,
}

impl Builder {
	#[must_use]
	pub fn new(host: impl Into<String>) -> Self {
		Self {
			args: Vec::new(),
			env: BTreeMap::new(),
			executable: None,
			host: host.into(),
			mounts: Vec::new(),
		}
	}

	#[must_use]
	pub fn with_object(object: &tg::command::Object) -> Self {
		Self {
			args: object.args.clone(),
			env: object.env.clone(),
			executable: object.executable.clone(),
			host: object.host.clone(),
			mounts: object.mounts.clone(),
		}
	}

	#[must_use]
	pub fn args(mut self, args: Vec<tg::Value>) -> Self {
		self.args = args;
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

	pub fn mount(mut self, mount: tg::command::Mount) -> tg::Result<Self> {
		self.mounts.push(mount);
		Ok(self)
	}

	#[must_use]
	pub fn build(self) -> tg::Command {
		tg::Command::with_object(tg::command::Object {
			args: self.args,
			env: self.env,
			executable: self.executable,
			host: self.host,
			mounts: self.mounts,
		})
	}
}
