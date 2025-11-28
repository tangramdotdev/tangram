use {crate::prelude::*, std::path::PathBuf};

pub use self::{data::Module as Data, import::Import};

pub mod data;
pub mod import;
pub mod load;
pub mod resolve;

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
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
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
	Edge(tg::graph::Edge<tg::Object>),
	Path(PathBuf),
}

impl Module {
	#[must_use]
	pub fn children(&self) -> Vec<tg::object::Handle> {
		match &self.referent.item {
			Item::Edge(edge) => edge.children(),
			Item::Path(_) => vec![],
		}
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		let kind = self.kind;
		let referent = self.referent.clone().map(|item| match item {
			Item::Edge(edge) => tg::module::data::Item::Edge(edge.to_data()),
			Item::Path(path) => tg::module::data::Item::Path(path),
		});
		tg::module::Data { kind, referent }
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let kind = data.kind;
		let referent = data.referent.try_map(|item| {
			let item = match item {
				tg::module::data::Item::Edge(edge) => {
					let edge = tg::graph::Edge::try_from_data(edge)?;
					tg::module::Item::Edge(edge)
				},
				tg::module::data::Item::Path(path) => tg::module::Item::Path(path),
			};
			Ok::<_, tg::Error>(item)
		})?;
		let module = Self { kind, referent };
		Ok(module)
	}
}

impl TryFrom<tg::module::Data> for Module {
	type Error = tg::Error;

	fn try_from(value: tg::module::Data) -> Result<Self, Self::Error> {
		Self::try_from_data(value)
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
			_ => Err(tg::error!(kind = %s, "invalid kind")),
		}
	}
}
