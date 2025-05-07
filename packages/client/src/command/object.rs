use super::Data;
use crate as tg;
use itertools::Itertools as _;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Command {
	pub args: tg::value::Array,
	pub cwd: Option<PathBuf>,
	pub env: tg::value::Map,
	pub executable: tg::command::Executable,
	pub host: String,
	pub mounts: Option<Vec<tg::command::Mount>>,
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
	pub subpath: Option<PathBuf>,
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

#[derive(Clone, Debug)]
pub struct Mount {
	pub source: tg::Artifact,
	pub target: PathBuf,
}

impl Command {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		std::iter::empty()
			.chain(self.executable.object())
			.chain(self.args.iter().flat_map(tg::Value::objects))
			.chain(self.env.values().flat_map(tg::Value::objects))
			.chain(
				self.mounts
					.as_deref()
					.unwrap_or_default()
					.iter()
					.flat_map(tg::command::Mount::object),
			)
			.collect()
	}
}

impl Executable {
	#[must_use]
	pub fn object(&self) -> Vec<tg::Object> {
		match self {
			Self::Artifact(executable) => executable.objects(),
			Self::Module(executable) => executable.objects(),
			Self::Path(_) => vec![],
		}
	}
}

impl ArtifactExecutable {
	#[must_use]
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		[self.artifact.clone().into()].into()
	}
}

impl ModuleExecutable {
	#[must_use]
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		self.module.objects()
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<tg::command::data::ModuleExecutable>
	where
		H: tg::Handle,
	{
		let module = self.module.data(handle).await?;
		let export = self.export.clone();
		let data = tg::command::data::ModuleExecutable { module, export };
		Ok(data)
	}
}

impl TryFrom<Data> for Command {
	type Error = tg::Error;

	fn try_from(data: Data) -> Result<Self, Self::Error> {
		let args = data.args.into_iter().map(TryInto::try_into).try_collect()?;
		let cwd = data.cwd;
		let env = data
			.env
			.into_iter()
			.map(|(key, data)| Ok::<_, tg::Error>((key, data.try_into()?)))
			.try_collect()?;
		let executable = data.executable.into();
		let host = data.host;
		let mounts = data
			.mounts
			.map(|mounts| mounts.into_iter().map(Into::into).collect());
		let stdin = data.stdin.map(tg::Blob::with_id);
		let user = data.user;
		Ok(Self {
			args,
			cwd,
			env,
			executable,
			host,
			mounts,
			stdin,
			user,
		})
	}
}

impl From<tg::command::data::Executable> for Executable {
	fn from(data: tg::command::data::Executable) -> Self {
		match data {
			tg::command::data::Executable::Artifact(executable) => {
				Self::Artifact(executable.into())
			},
			tg::command::data::Executable::Module(executable) => Self::Module(executable.into()),
			tg::command::data::Executable::Path(executable) => Self::Path(executable.into()),
		}
	}
}

impl From<tg::command::data::ArtifactExecutable> for ArtifactExecutable {
	fn from(data: tg::command::data::ArtifactExecutable) -> Self {
		let artifact = tg::Artifact::with_id(data.artifact);
		let subpath = data.subpath;
		Self { artifact, subpath }
	}
}

impl From<tg::command::data::ModuleExecutable> for ModuleExecutable {
	fn from(data: tg::command::data::ModuleExecutable) -> Self {
		Self {
			module: data.module.into(),
			export: data.export,
		}
	}
}

impl From<tg::command::data::PathExecutable> for PathExecutable {
	fn from(data: tg::command::data::PathExecutable) -> Self {
		let path = data.path;
		Self { path }
	}
}

impl From<tg::File> for Executable {
	fn from(value: tg::File) -> Self {
		Self::Artifact(ArtifactExecutable {
			artifact: value.into(),
			subpath: None,
		})
	}
}

impl Mount {
	pub async fn data<H>(&self, handle: &H) -> tg::Result<tg::command::data::Mount>
	where
		H: tg::Handle,
	{
		let source = self.source.id(handle).await?;
		let target = self.target.clone();
		let mount = tg::command::data::Mount { source, target };
		Ok(mount)
	}

	#[must_use]
	pub fn object(&self) -> Vec<tg::Object> {
		[self.source.clone().into()].into()
	}
}

impl std::str::FromStr for Mount {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let s = if let Some((s, ro)) = s.split_once(',') {
			if ro == "ro" {
				s
			} else if ro == "rw" {
				return Err(tg::error!("cannot mount artifacts read-write"));
			} else {
				return Err(tg::error!("unknown option: {ro:#?}"));
			}
		} else {
			s
		};
		let (source, target) = s
			.split_once(':')
			.ok_or_else(|| tg::error!("expected a target path"))?;
		let target = PathBuf::from(target);
		if !target.is_absolute() {
			return Err(tg::error!(%target = target.display(), "expected an absolute path"));
		}
		let id = source.parse()?;
		let source = tg::Artifact::with_id(id);
		Ok(Self { source, target })
	}
}

impl From<tg::command::data::Mount> for Mount {
	fn from(data: tg::command::data::Mount) -> Self {
		let source = tg::Artifact::with_id(data.source);
		let target = data.target;
		Self { source, target }
	}
}
