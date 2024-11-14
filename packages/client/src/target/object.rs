use super::Data;
use crate as tg;
use itertools::Itertools as _;

#[derive(Clone, Debug)]
pub struct Target {
	pub args: tg::value::Array,
	pub checksum: Option<tg::Checksum>,
	pub env: tg::value::Map,
	pub executable: Option<tg::target::Executable>,
	pub host: String,
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

impl Target {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		std::iter::empty()
			.chain(
				self.executable
					.iter()
					.flat_map(tg::target::Executable::object),
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

	pub async fn data<H>(&self, handle: &H) -> tg::Result<tg::target::data::Module>
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
		let data = tg::target::data::Module { kind, referent };
		Ok(data)
	}
}

impl TryFrom<Data> for Target {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let args = data.args.into_iter().map(TryInto::try_into).try_collect()?;
		let checksum = data.checksum;
		let env = data
			.env
			.into_iter()
			.map(|(key, data)| Ok::<_, tg::Error>((key, data.try_into()?)))
			.try_collect()?;
		let executable = data.executable.map(TryInto::try_into).transpose()?;
		let host = data.host;
		Ok(Self {
			args,
			checksum,
			env,
			executable,
			host,
		})
	}
}

impl TryFrom<tg::target::data::Executable> for Executable {
	type Error = tg::Error;

	fn try_from(data: tg::target::data::Executable) -> std::result::Result<Self, Self::Error> {
		match data {
			tg::target::data::Executable::Artifact(id) => {
				Ok(Self::Artifact(tg::Artifact::with_id(id)))
			},
			tg::target::data::Executable::Module(module) => Ok(Self::Module(module.try_into()?)),
		}
	}
}

impl TryFrom<tg::target::data::Module> for Module {
	type Error = tg::Error;

	fn try_from(data: tg::target::data::Module) -> std::result::Result<Self, Self::Error> {
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
