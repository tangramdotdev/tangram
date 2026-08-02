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
	args: Vec<tg::command::Value>,
	cwd: Option<PathBuf>,
	env: BTreeMap<String, tg::command::Value>,
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
	pub fn with_spawn_arg(arg: tg::process::spawn::CommandArg) -> Self {
		Self {
			args: arg.args,
			cwd: arg.cwd,
			env: arg.env,
			executable: Some(arg.executable),
			host: arg.host,
			stdin: arg.stdin,
			user: arg.user,
		}
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
	pub fn arg(mut self, arg: impl Into<tg::command::Value>) -> Self {
		self.args.push(arg.into());
		self
	}

	#[must_use]
	pub fn args<A>(mut self, args: impl IntoIterator<Item = A>) -> Self
	where
		A: Into<tg::command::Value>,
	{
		self.args.extend(args.into_iter().map(Into::into));
		self
	}

	#[must_use]
	pub fn cwd(mut self, cwd: impl Into<Option<PathBuf>>) -> Self {
		self.cwd = cwd.into();
		self
	}

	#[must_use]
	pub fn env(mut self, env: impl IntoIterator<Item = (String, tg::command::Value)>) -> Self {
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

	pub fn build(self) -> tg::Result<tg::Command> {
		let mut arg = self.build_spawn_arg()?;
		let host = arg
			.host
			.take()
			.unwrap_or_else(|| tg::host::current().to_owned());
		let object = arg.into_object(host);
		Ok(tg::Command::with_object(object))
	}

	pub fn build_spawn_arg(self) -> tg::Result<tg::process::spawn::CommandArg> {
		let executable = self
			.executable
			.ok_or_else(|| tg::error!("cannot create a command without an executable"))?;
		Ok(tg::process::spawn::CommandArg {
			args: self.args,
			cwd: self.cwd,
			env: self.env,
			executable,
			host: self.host,
			stdin: self.stdin,
			user: self.user,
		})
	}

	pub fn finish(self) -> tg::Result<tg::Command> {
		self.build()
	}
}

impl IntoFuture for Builder {
	type Output = tg::Result<tg::Command>;
	type IntoFuture = Ready<Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		ready(self.build())
	}
}
