use crate as tg;
use std::path::PathBuf;

pub use self::{data::Module as Data, import::Import};

pub mod data;
pub mod import;

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
	Artifact,
	Blob,

	Directory,
	File,
	Symlink,
	Graph,
	Command,
}

#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Item {
	Path(PathBuf),
	Object(tg::Object),
}

impl Module {
	#[must_use]
	pub fn children(&self) -> Vec<tg::object::Handle> {
		match &self.referent.item {
			Item::Path(_) => vec![],
			Item::Object(object) => vec![object.clone()],
		}
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		let kind = self.kind;
		let referent = self.referent.clone().map(|item| match item {
			Item::Path(path) => tg::module::data::Item::Path(path),
			Item::Object(object) => tg::module::data::Item::Object(object.id()),
		});
		tg::module::Data { kind, referent }
	}
}

impl From<tg::module::Data> for Module {
	fn from(value: tg::module::Data) -> Self {
		Self {
			kind: value.kind,
			referent: value.referent.map(|item| match item {
				tg::module::data::Item::Path(path) => tg::module::Item::Path(path),
				tg::module::data::Item::Object(object) => {
					tg::module::Item::Object(tg::Object::with_id(object))
				},
			}),
		}
	}
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.module(self)?;
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
			Self::Directory => write!(f, "directory"),
			Self::File => write!(f, "file"),
			Self::Symlink => write!(f, "symlink"),
			Self::Graph => write!(f, "graph"),
			Self::Command => write!(f, "command"),
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
			"directory" => Ok(Self::Directory),
			"file" => Ok(Self::File),
			"symlink" => Ok(Self::Symlink),
			"graph" => Ok(Self::Graph),
			"command" => Ok(Self::Command),
			_ => Err(tg::error!(%kind = s, "invalid kind")),
		}
	}
}
