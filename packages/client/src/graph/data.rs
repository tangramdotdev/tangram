use crate::{self as tg, util::serde::is_false};
use bytes::Bytes;
use std::{
	collections::{BTreeMap, BTreeSet},
	path::PathBuf,
};
use tangram_either::Either;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Graph {
	pub nodes: Vec<tg::graph::data::Node>,
}

#[derive(
	Clone, Debug, serde::Deserialize, serde::Serialize, derive_more::TryUnwrap, derive_more::Unwrap,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Node {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Directory {
	pub entries: BTreeMap<String, Either<usize, tg::artifact::Id>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct File {
	pub contents: tg::blob::Id,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub dependencies: BTreeMap<tg::Reference, tg::Referent<Either<usize, tg::object::Id>>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub executable: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Symlink {
	Target {
		target: PathBuf,
	},

	Artifact {
		artifact: Either<usize, tg::artifact::Id>,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		subpath: Option<PathBuf>,
	},
}

impl Graph {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		serde_json::to_vec(self)
			.map(Into::into)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		serde_json::from_reader(bytes.into().as_ref())
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		let mut children = BTreeSet::new();
		for node in &self.nodes {
			match node {
				tg::graph::data::Node::Directory(tg::graph::data::Directory { entries }) => {
					for either in entries.values() {
						if let Either::Right(id) = either {
							children.insert(id.clone().into());
						}
					}
				},
				tg::graph::data::Node::File(tg::graph::data::File {
					contents,
					dependencies,
					..
				}) => {
					children.insert(contents.clone().into());
					for referent in dependencies.values() {
						if let Either::Right(id) = &referent.item {
							children.insert(id.clone());
						}
					}
				},
				tg::graph::data::Node::Symlink(symlink) => {
					if let tg::graph::data::Symlink::Artifact {
						artifact: Either::Right(id),
						..
					} = symlink
					{
						children.insert(id.clone().into());
					}
				},
			}
		}
		children
	}
}

impl Node {
	#[must_use]
	pub fn kind(&self) -> tg::artifact::Kind {
		match self {
			Self::Directory(_) => tg::artifact::Kind::Directory,
			Self::File(_) => tg::artifact::Kind::File,
			Self::Symlink(_) => tg::artifact::Kind::Symlink,
		}
	}
}
