use {
	crate::prelude::*,
	std::{
		collections::BTreeMap,
		future::{IntoFuture, Ready, ready},
		path::PathBuf,
	},
};

#[derive(Clone, Debug, Default)]
pub struct Builder {
	args: Vec<tg::Value>,
	cwd: Option<PathBuf>,
	env: BTreeMap<String, tg::Value>,
	executable: Option<tg::command::Executable>,
	host: Option<String>,
	stdin: Option<tg::Blob>,
	user: Option<String>,
}

impl Builder {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn with_object(object: &tg::command::Object) -> Self {
		Self {
			args: object.args.clone(),
			cwd: object.cwd.clone(),
			env: object.env.clone(),
			executable: Some(object.executable.clone()),
			host: Some(object.host.clone()),
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
		self.executable = Some(executable.into());
		self
	}

	#[must_use]
	pub fn host(mut self, host: impl Into<String>) -> Self {
		self.host = Some(host.into());
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

	pub fn finish(self) -> tg::Result<tg::Command> {
		let executable = self
			.executable
			.ok_or_else(|| tg::error!("cannot create a command without an executable"))?;
		let host = self.host.unwrap_or_else(|| tg::host::current().to_owned());
		Ok(tg::Command::with_object(tg::command::Object {
			args: self.args,
			cwd: self.cwd,
			env: self.env,
			executable,
			host,
			stdin: self.stdin,
			user: self.user,
		}))
	}
}

impl IntoFuture for Builder {
	type Output = tg::Result<tg::Command>;
	type IntoFuture = Ready<Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		ready(self.finish())
	}
}
