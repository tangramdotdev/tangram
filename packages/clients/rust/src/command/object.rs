use {super::Data, crate::prelude::*, std::path::PathBuf};

#[derive(Clone, Debug)]
pub struct Command {
	pub args: tg::value::Array,
	pub cwd: Option<PathBuf>,
	pub env: tg::value::Map,
	pub executable: tg::command::Executable,
	pub host: String,
	pub stdin: Option<tg::Blob>,
	pub user: Option<String>,
}

#[derive(Clone, Debug, derive_more::From, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Executable {
	Artifact(ArtifactExecutable),
	Module(ModuleExecutable),
	Path(PathExecutable),
}

#[derive(Clone, Debug)]
pub struct ArtifactExecutable {
	pub artifact: tg::Artifact,
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct ModuleExecutable {
	pub module: tg::Module,
	pub export: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PathExecutable {
	pub path: PathBuf,
}

impl Command {
	#[must_use]
	pub fn to_data(&self) -> Data {
		let args = self.args.iter().map(tg::Value::to_data).collect();
		let cwd = self.cwd.clone();
		let env = self
			.env
			.iter()
			.map(|(key, value)| {
				let key = key.clone();
				let value = value.to_data();
				(key, value)
			})
			.collect();
		let executable = self.executable.to_data();
		let host = self.host.clone();
		let stdin = self.stdin.as_ref().map(tg::Blob::id);
		let user = self.user.clone();
		Data {
			args,
			cwd,
			env,
			executable,
			host,
			stdin,
			user,
		}
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let args = data
			.args
			.into_iter()
			.map(TryInto::try_into)
			.collect::<tg::Result<_>>()?;
		let cwd = data.cwd;
		let env = data
			.env
			.into_iter()
			.map(|(key, data)| Ok::<_, tg::Error>((key, data.try_into()?)))
			.collect::<tg::Result<_>>()?;
		let executable = tg::command::object::Executable::try_from_data(data.executable)?;
		let host = data.host;
		let stdin = data.stdin.map(tg::Blob::with_id);
		let user = data.user;
		Ok(Self {
			args,
			cwd,
			env,
			executable,
			host,
			stdin,
			user,
		})
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		std::iter::empty()
			.chain(self.executable.objects())
			.chain(self.args.iter().flat_map(tg::Value::objects))
			.chain(self.env.values().flat_map(tg::Value::objects))
			.collect()
	}
}

impl Executable {
	#[must_use]
	pub fn to_data(&self) -> tg::command::data::Executable {
		match self {
			tg::command::Executable::Artifact(executable) => {
				let executable = executable.to_data();
				tg::command::data::Executable::Artifact(executable)
			},
			tg::command::Executable::Module(executable) => {
				let executable = executable.to_data();
				tg::command::data::Executable::Module(executable)
			},
			tg::command::Executable::Path(executable) => {
				let executable = executable.to_data();
				tg::command::data::Executable::Path(executable)
			},
		}
	}

	pub fn try_from_data(data: tg::command::data::Executable) -> tg::Result<Self> {
		match data {
			tg::command::data::Executable::Artifact(executable) => {
				let executable =
					tg::command::object::ArtifactExecutable::try_from_data(executable)?;
				Ok(Self::Artifact(executable))
			},
			tg::command::data::Executable::Module(executable) => {
				let executable = tg::command::object::ModuleExecutable::try_from_data(executable)?;
				Ok(Self::Module(executable))
			},
			tg::command::data::Executable::Path(executable) => {
				let executable = tg::command::object::PathExecutable::try_from_data(executable)?;
				Ok(Self::Path(executable))
			},
		}
	}

	#[must_use]
	pub fn objects(&self) -> Vec<tg::Object> {
		match self {
			Self::Artifact(executable) => executable.objects(),
			Self::Module(executable) => executable.objects(),
			Self::Path(_) => vec![],
		}
	}
}

impl ArtifactExecutable {
	#[must_use]
	pub fn to_data(&self) -> tg::command::data::ArtifactExecutable {
		tg::command::data::ArtifactExecutable {
			artifact: self.artifact.id(),
			path: self.path.clone(),
		}
	}

	pub fn try_from_data(data: tg::command::data::ArtifactExecutable) -> tg::Result<Self> {
		let artifact = tg::Artifact::with_id(data.artifact);
		let path = data.path;
		Ok(Self { artifact, path })
	}

	#[must_use]
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		[self.artifact.clone().into()].into()
	}
}

impl ModuleExecutable {
	#[must_use]
	pub fn to_data(&self) -> tg::command::data::ModuleExecutable {
		let module = self.module.to_data();
		let export = self.export.clone();
		tg::command::data::ModuleExecutable { module, export }
	}

	pub fn try_from_data(data: tg::command::data::ModuleExecutable) -> tg::Result<Self> {
		let executable = Self {
			module: data.module.try_into()?,
			export: data.export,
		};
		Ok(executable)
	}

	#[must_use]
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		self.module.children()
	}
}

impl PathExecutable {
	#[must_use]
	pub fn to_data(&self) -> tg::command::data::PathExecutable {
		tg::command::data::PathExecutable {
			path: self.path.clone(),
		}
	}

	pub fn try_from_data(data: tg::command::data::PathExecutable) -> tg::Result<Self> {
		let path = data.path;
		Ok(Self { path })
	}

	#[must_use]
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		vec![]
	}
}

impl From<tg::File> for Executable {
	fn from(value: tg::File) -> Self {
		Self::Artifact(ArtifactExecutable {
			artifact: value.into(),
			path: None,
		})
	}
}
