use crate as tg;
use std::{collections::BTreeSet, path::PathBuf};

pub use self::data::Data;

#[derive(Clone, Debug)]
pub struct Module {
	pub kind: Kind,
	pub referent: tg::Referent<Item>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Kind {
	Js,
	Ts,
	Dts,
	Object,
	Blob,
	Artifact,
	Leaf,
	Branch,
	Directory,
	File,
	Symlink,
	Graph,
	Target,
}

#[derive(Clone, Debug, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Item {
	Path(PathBuf),
	Object(tg::Object),
}

pub mod data {
	use super::Kind;
	use crate as tg;
	use std::path::PathBuf;

	#[derive(
		Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
	)]
	pub struct Data {
		pub kind: Kind,
		pub referent: tg::Referent<Item>,
	}

	#[derive(
		Clone,
		Debug,
		Eq,
		Hash,
		Ord,
		PartialEq,
		PartialOrd,
		serde_with::DeserializeFromStr,
		serde_with::SerializeDisplay,
	)]
	pub enum Item {
		Path(PathBuf),
		Object(tg::object::Id),
	}
}

impl Module {
	#[must_use]
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		match &self.referent.item {
			Item::Path(_) => vec![],
			Item::Object(object) => vec![object.clone()],
		}
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		let kind = self.kind;
		let item = match &self.referent.item {
			Item::Path(path) => data::Item::Path(path.clone()),
			Item::Object(object) => data::Item::Object(object.id(handle).await?),
		};
		let subpath = self.referent.subpath.clone();
		let tag = self.referent.tag.clone();
		let referent = tg::Referent { item, subpath, tag };
		let data = Data { kind, referent };
		Ok(data)
	}
}

impl Data {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match &self.referent.item {
			data::Item::Path(_) => [].into(),
			data::Item::Object(object) => [object.clone()].into(),
		}
	}
}

impl TryFrom<Data> for Module {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let kind = data.kind;
		let item = match data.referent.item {
			data::Item::Path(path) => Item::Path(path),
			data::Item::Object(object) => Item::Object(tg::Object::with_id(object)),
		};
		let subpath = data.referent.subpath;
		let tag = data.referent.tag;
		let referent = tg::Referent { item, subpath, tag };
		let module = Self { kind, referent };
		Ok(module)
	}
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.kind)?;
		if let Some(tag) = &self.referent.tag {
			write!(f, ":{tag}")?;
		} else {
			match &self.referent.item {
				Item::Path(path) => {
					write!(f, ":{}", path.display())?;
				},
				Item::Object(object) => {
					write!(f, ":{object}")?;
				},
			}
		}
		if let Some(subpath) = &self.referent.subpath {
			write!(f, ":{}", subpath.display())?;
		}
		Ok(())
	}
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Js => write!(f, "js"),
			Self::Ts => write!(f, "ts"),
			Self::Dts => write!(f, "dts"),
			Self::Object => write!(f, "object"),
			Self::Artifact => write!(f, "artifact"),
			Self::Blob => write!(f, "blob"),
			Self::Leaf => write!(f, "leaf"),
			Self::Branch => write!(f, "branch"),
			Self::Directory => write!(f, "directory"),
			Self::File => write!(f, "file"),
			Self::Symlink => write!(f, "symlink"),
			Self::Graph => write!(f, "graph"),
			Self::Target => write!(f, "target"),
		}
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"js" => Ok(Self::Js),
			"ts" => Ok(Self::Ts),
			"dts" => Ok(Self::Dts),
			"object" => Ok(Self::Object),
			"artifact" => Ok(Self::Artifact),
			"blob" => Ok(Self::Blob),
			"leaf" => Ok(Self::Leaf),
			"branch" => Ok(Self::Branch),
			"directory" => Ok(Self::Directory),
			"file" => Ok(Self::File),
			"symlink" => Ok(Self::Symlink),
			"graph" => Ok(Self::Graph),
			"target" => Ok(Self::Target),
			_ => Err(tg::error!(%kind = s, "invalid kind")),
		}
	}
}

impl std::fmt::Display for Item {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Path(path) => {
				if path
					.components()
					.next()
					.is_some_and(|component| matches!(component, std::path::Component::Normal(_)))
				{
					write!(f, "./")?;
				}
				write!(f, "{}", path.display())?;
			},
			Self::Object(object) => {
				write!(f, "{object}")?;
			},
		}
		Ok(())
	}
}

impl std::fmt::Display for Data {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.kind)?;
		if let Some(tag) = &self.referent.tag {
			write!(f, ":{tag}")?;
		} else {
			match &self.referent.item {
				data::Item::Path(path) => {
					write!(f, ":{}", path.display())?;
				},
				data::Item::Object(object) => {
					write!(f, ":{object}")?;
				},
			}
		}
		if let Some(subpath) = &self.referent.subpath {
			write!(f, ":{}", subpath.display())?;
		}
		Ok(())
	}
}

impl std::fmt::Display for data::Item {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Path(path) => {
				if path
					.components()
					.next()
					.is_some_and(|component| matches!(component, std::path::Component::Normal(_)))
				{
					write!(f, "./")?;
				}
				write!(f, "{}", path.display())?;
			},
			Self::Object(object) => {
				write!(f, "{object}")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for data::Item {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.starts_with('.') || s.starts_with('/') {
			Ok(Self::Path(s.into()))
		} else {
			Ok(Self::Object(s.parse()?))
		}
	}
}
