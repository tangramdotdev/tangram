use {
	crate::prelude::*,
	std::{collections::BTreeMap, path::PathBuf},
};

#[derive(Clone, Debug)]
pub struct Builder {
	args: Vec<tg::Value>,
	cwd: Option<PathBuf>,
	env: BTreeMap<String, tg::Value>,
	executable: tg::command::Executable,
	host: String,
	mounts: Vec<tg::command::Mount>,
	stdin: Option<tg::Blob>,
	user: Option<String>,
}

impl Builder {
	#[must_use]
	pub fn new(host: impl Into<String>, executable: impl Into<tg::command::Executable>) -> Self {
		Self {
			args: Vec::new(),
			cwd: None,
			env: BTreeMap::new(),
			executable: executable.into(),
			host: host.into(),
			mounts: Vec::new(),
			stdin: None,
			user: None,
		}
	}

	#[must_use]
	pub fn with_object(object: &tg::command::Object) -> Self {
		Self {
			args: object.args.clone(),
			cwd: object.cwd.clone(),
			env: object.env.clone(),
			executable: object.executable.clone(),
			host: object.host.clone(),
			mounts: object.mounts.clone(),
			stdin: object.stdin.clone(),
			user: object.user.clone(),
		}
	}

	#[must_use]
	pub fn arg(mut self, arg: tg::Value) -> Self {
		self.args.push(arg);
		self
	}

	#[must_use]
	pub fn args(mut self, args: impl IntoIterator<Item = tg::Value>) -> Self {
		self.args.extend(args);
		self
	}

	#[must_use]
	pub fn cwd(mut self, cwd: impl Into<Option<PathBuf>>) -> Self {
		self.cwd = cwd.into();
		self
	}

	#[must_use]
	pub fn env(mut self, env: impl IntoIterator<Item = (String, tg::Value)>) -> Self {
		self.env.extend(env);
		self
	}

	#[must_use]
	pub fn executable(mut self, executable: impl Into<tg::command::Executable>) -> Self {
		self.executable = executable.into();
		self
	}

	#[must_use]
	pub fn host(mut self, host: String) -> Self {
		self.host = host;
		self
	}

	#[must_use]
	pub fn mount(mut self, mount: tg::command::Mount) -> Self {
		self.mounts.push(mount);
		self
	}

	#[must_use]
	pub fn mounts(mut self, mounts: impl IntoIterator<Item = tg::command::Mount>) -> Self {
		self.mounts.extend(mounts);
		self
	}

	#[must_use]
	pub fn stdin(mut self, stdin: impl Into<Option<tg::Blob>>) -> Self {
		self.stdin = stdin.into();
		self
	}

	#[must_use]
	pub fn user(mut self, user: impl Into<Option<String>>) -> Self {
		self.user = user.into();
		self
	}

	#[must_use]
	pub fn build(self) -> tg::Command {
		tg::Command::with_object(tg::command::Object {
			args: self.args,
			cwd: self.cwd,
			env: self.env,
			executable: self.executable,
			host: self.host,
			mounts: self.mounts,
			stdin: self.stdin,
			user: self.user,
		})
	}
}
