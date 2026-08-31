use {
	crate::prelude::*,
	std::{
		collections::BTreeMap,
		future::{IntoFuture, Ready, ready},
		path::PathBuf,
	},
};

#[cfg(test)]
mod tests;

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

	pub fn try_with_spawn_arg(arg: tg::process::spawn::CommandArg) -> tg::Result<Self> {
		let args = arg
			.args
			.into_iter()
			.map(tg::command::Value::try_from_data)
			.collect::<tg::Result<_>>()?;
		let env = arg
			.env
			.into_iter()
			.map(|(key, value)| Ok((key, tg::command::Value::try_from_data(value)?)))
			.collect::<tg::Result<_>>()?;
		let tg::Referent { node, options } = arg.executable;
		let executable = tg::command::Executable::try_from_data(node)?;
		if let Some(artifact) = &executable.artifact {
			artifact.state().set_location(options.location);
			artifact.state().set_tokens(options.tokens);
		}
		let stdin = arg.stdin.map(tg::Blob::with_referent);
		let builder = Self {
			args,
			cwd: arg.cwd,
			env,
			executable: Some(executable),
			host: arg.host,
			stdin,
			user: arg.user,
		};

		Ok(builder)
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
		let executable = self
			.executable
			.ok_or_else(|| tg::error!("cannot create a command without an executable"))?;
		let host = self.host.unwrap_or_else(|| tg::host::current().to_owned());
		let object = tg::command::Object {
			args: self.args,
			cwd: self.cwd,
			env: self.env,
			executable,
			host,
			stdin: self.stdin,
			user: self.user,
		};
		Ok(tg::Command::with_object(object))
	}

	pub fn build_spawn_arg(self) -> tg::Result<tg::process::spawn::CommandArg> {
		let executable = self
			.executable
			.ok_or_else(|| tg::error!("cannot create a command without an executable"))?;
		let options = executable
			.artifact
			.as_ref()
			.map_or_else(tg::referent::Options::default, |artifact| {
				artifact.to_referent().options
			});
		let executable = tg::Referent::new(executable.to_data(), options);
		Ok(tg::process::spawn::CommandArg {
			args: self.args.iter().map(tg::command::Value::to_data).collect(),
			cwd: self.cwd,
			env: self
				.env
				.iter()
				.map(|(key, value)| (key.clone(), value.to_data()))
				.collect(),
			executable,
			host: self.host,
			stdin: self.stdin.as_ref().map(tg::Blob::to_referent),
			user: self.user,
		})
	}

	#[must_use]
	pub fn objects(&self) -> Vec<tg::Object> {
		std::iter::empty()
			.chain(
				self.executable
					.iter()
					.flat_map(tg::command::Executable::objects),
			)
			.chain(self.args.iter().flat_map(tg::command::Value::objects))
			.chain(self.env.values().flat_map(tg::command::Value::objects))
			.chain(self.stdin.iter().cloned().map(Into::into))
			.collect()
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
