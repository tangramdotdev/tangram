use crate as tg;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Builder {
	args: Vec<tg::Value>,
	checksum: Option<tg::Checksum>,
	env: BTreeMap<String, tg::Value>,
	executable: Option<tg::target::Executable>,
	host: String,
}

impl Builder {
	#[must_use]
	pub fn new(host: impl Into<String>) -> Self {
		Self {
			args: Vec::new(),
			checksum: None,
			env: BTreeMap::new(),
			executable: None,
			host: host.into(),
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
	pub fn env(mut self, env: BTreeMap<String, tg::Value>) -> Self {
		self.env = env;
		self
	}

	#[must_use]
	pub fn executable(mut self, executable: impl Into<Option<tg::target::Executable>>) -> Self {
		self.executable = executable.into();
		self
	}

	#[must_use]
	pub fn host(mut self, host: String) -> Self {
		self.host = host;
		self
	}

	#[must_use]
	pub fn build(self) -> tg::Target {
		tg::Target::with_object(tg::target::Object {
			args: self.args,
			checksum: self.checksum,
			env: self.env,
			executable: self.executable,
			host: self.host,
		})
	}
}

impl From<&tg::target::Object> for Builder {
	fn from(value: &tg::target::Object) -> Self {
		Self {
			args: value.args.clone(),
			checksum: value.checksum.clone(),
			env: value.env.clone(),
			executable: value.executable.clone(),
			host: value.host.clone(),
		}
	}
}
