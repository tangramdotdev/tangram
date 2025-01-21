use super::Data;
use crate as tg;
use itertools::Itertools as _;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Command {
	pub args: tg::value::Array,
	pub checksum: Option<tg::Checksum>,
	pub cwd: Option<PathBuf>,
	pub env: tg::value::Map,
	pub executable: Option<tg::command::Executable>,
	pub host: String,
	pub sandbox: Option<Sandbox>,
}

#[derive(Clone, Debug, derive_more::From, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Executable {
	Artifact(tg::Artifact),
	Module(Module),
}

#[derive(Clone, Debug)]
pub struct Module {
	pub kind: tg::module::Kind,
	pub referent: tg::Referent<tg::Object>,
}

#[derive(Clone, Debug)]
pub struct Sandbox {
	pub filesystem: bool,
	pub network: bool,
}

impl Command {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		std::iter::empty()
			.chain(
				self.executable
					.iter()
					.flat_map(tg::command::Executable::object),
			)
			.chain(self.args.iter().flat_map(tg::Value::objects))
			.chain(self.env.values().flat_map(tg::Value::objects))
			.collect()
	}
}

impl Executable {
	#[must_use]
	pub fn object(&self) -> Vec<tg::Object> {
		match self {
			Self::Artifact(artifact) => [artifact.clone().into()].into(),
			Self::Module(module) => module.objects(),
		}
	}
}

impl Module {
	#[must_use]
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		vec![self.referent.item.clone()]
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<tg::command::data::Module>
	where
		H: tg::Handle,
	{
		let kind = self.kind;
		let item = self.referent.item.id(handle).await?;
		let path = self.referent.path.clone();
		let subpath = self.referent.subpath.clone();
		let tag = self.referent.tag.clone();
		let referent = tg::Referent {
			item,
			path,
			subpath,
			tag,
		};
		let data = tg::command::data::Module { kind, referent };
		Ok(data)
	}
}

impl TryFrom<Data> for Command {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let args = data.args.into_iter().map(TryInto::try_into).try_collect()?;
		let checksum = data.checksum;
		let cwd = data.cwd;
		let env = data
			.env
			.into_iter()
			.map(|(key, data)| Ok::<_, tg::Error>((key, data.try_into()?)))
			.try_collect()?;
		let executable = data.executable.map(TryInto::try_into).transpose()?;
		let host = data.host;
		let sandbox = data.sandbox.map(TryInto::try_into).transpose()?;
		Ok(Self {
			args,
			checksum,
			cwd,
			env,
			executable,
			host,
			sandbox,
		})
	}
}

impl TryFrom<tg::command::data::Executable> for Executable {
	type Error = tg::Error;

	fn try_from(data: tg::command::data::Executable) -> std::result::Result<Self, Self::Error> {
		match data {
			tg::command::data::Executable::Artifact(id) => {
				Ok(Self::Artifact(tg::Artifact::with_id(id)))
			},
			tg::command::data::Executable::Module(module) => Ok(Self::Module(module.try_into()?)),
		}
	}
}

impl TryFrom<tg::command::data::Module> for Module {
	type Error = tg::Error;

	fn try_from(data: tg::command::data::Module) -> std::result::Result<Self, Self::Error> {
		let kind = data.kind;
		let item = tg::Object::with_id(data.referent.item);
		let path = data.referent.path;
		let subpath = data.referent.subpath;
		let tag = data.referent.tag;
		let referent = tg::Referent {
			item,
			path,
			subpath,
			tag,
		};
		let module = Self { kind, referent };
		Ok(module)
	}
}

impl From<tg::File> for Executable {
	fn from(value: tg::File) -> Self {
		Self::Artifact(value.into())
	}
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.kind)?;
		if let Some(tag) = &self.referent.tag {
			write!(f, ":{tag}")?;
		} else {
			write!(f, ":{}", self.referent.item)?;
		}
		if let Some(subpath) = &self.referent.subpath {
			write!(f, ":{}", subpath.display())?;
		}
		Ok(())
	}
}

impl TryFrom<tg::command::data::Sandbox> for Sandbox {
	type Error = tg::Error;

	fn try_from(data: tg::command::data::Sandbox) -> std::result::Result<Self, Self::Error> {
		Ok(Sandbox {
			filesystem: data.filesystem,
			network: data.network,
		})
	}
}
