use super::Data;
use crate as tg;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use itertools::Itertools as _;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_either::Either;

#[derive(Clone, Debug)]
pub struct Graph {
	pub nodes: Vec<Node>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Node {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
}

#[derive(Clone, Debug)]
pub enum Kind {
	Directory,
	File,
	Symlink,
}

#[derive(Clone, Debug)]
pub struct Directory {
	pub entries: BTreeMap<String, Either<usize, tg::Artifact>>,
}

#[derive(Clone, Debug)]
pub struct File {
	pub contents: tg::Blob,
	pub dependencies: BTreeMap<tg::Reference, tg::Referent<Either<usize, tg::Object>>>,
	pub executable: bool,
}

#[derive(Clone, Debug)]
pub struct Symlink {
	pub artifact: Option<Either<usize, tg::Artifact>>,
	pub subpath: Option<PathBuf>,
}

impl Graph {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		let mut children = Vec::new();
		for node in &self.nodes {
			match node {
				Node::Directory(tg::graph::object::Directory { entries }) => {
					for either in entries.values() {
						if let Either::Right(id) = either {
							children.push(id.clone().into());
						}
					}
				},
				Node::File(tg::graph::object::File {
					contents,
					dependencies,
					..
				}) => {
					children.push(contents.clone().into());
					for referent in dependencies.values() {
						if let Either::Right(id) = &referent.item {
							children.push(id.clone());
						}
					}
				},
				Node::Symlink(tg::graph::object::Symlink { artifact, .. }) => {
					if let Some(Either::Right(id)) = artifact {
						children.push(id.clone().into());
					}
				},
			}
		}
		children
	}
}

impl Node {
	pub async fn data<H>(&self, handle: &H) -> tg::Result<tg::graph::data::Node>
	where
		H: tg::Handle,
	{
		match self {
			Self::Directory(tg::graph::object::Directory { entries }) => {
				let entries = entries
					.iter()
					.map(|(name, either)| async move {
						let artifact = match either {
							Either::Left(index) => Either::Left(*index),
							Either::Right(artifact) => Either::Right(artifact.id(handle).await?),
						};
						Ok::<_, tg::Error>((name.clone(), artifact))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?;
				Ok(tg::graph::data::Node::Directory(
					tg::graph::data::Directory { entries },
				))
			},

			Self::File(tg::graph::object::File {
				contents,
				dependencies,
				executable,
			}) => {
				let contents = contents.id(handle).await?;
				let dependencies = dependencies
					.iter()
					.map(|(reference, referent)| async move {
						let item = match &referent.item {
							Either::Left(index) => Either::Left(*index),
							Either::Right(object) => Either::Right(object.id(handle).await?),
						};
						let referent = tg::Referent {
							item,
							path: referent.path.clone(),
							subpath: referent.subpath.clone(),
							tag: referent.tag.clone(),
						};
						Ok::<_, tg::Error>((reference.clone(), referent))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?;
				let executable = *executable;
				Ok(tg::graph::data::Node::File(tg::graph::data::File {
					contents,
					dependencies,
					executable,
				}))
			},

			Self::Symlink(tg::graph::object::Symlink { artifact, subpath }) => {
				let artifact = if let Some(artifact) = artifact {
					Some(match artifact {
						Either::Left(index) => Either::Left(*index),
						Either::Right(artifact) => Either::Right(artifact.id(handle).await?),
					})
				} else {
					None
				};
				let subpath = subpath.clone();
				Ok(tg::graph::data::Node::Symlink(tg::graph::data::Symlink {
					artifact,
					subpath,
				}))
			},
		}
	}

	#[must_use]
	pub fn kind(&self) -> tg::artifact::Kind {
		match self {
			Self::Directory(_) => tg::artifact::Kind::Directory,
			Self::File(_) => tg::artifact::Kind::File,
			Self::Symlink(_) => tg::artifact::Kind::Symlink,
		}
	}
}

impl TryFrom<Data> for Graph {
	type Error = tg::Error;

	fn try_from(value: Data) -> std::result::Result<Self, Self::Error> {
		let nodes = value
			.nodes
			.into_iter()
			.map(TryInto::try_into)
			.try_collect()?;
		Ok(Self { nodes })
	}
}

impl TryFrom<tg::graph::data::Node> for Node {
	type Error = tg::Error;

	fn try_from(value: tg::graph::data::Node) -> std::result::Result<Self, Self::Error> {
		match value {
			tg::graph::data::Node::Directory(tg::graph::data::Directory { entries }) => {
				let entries = entries
					.into_iter()
					.map(|(name, either)| (name, either.map_right(tg::Artifact::with_id)))
					.collect();
				let directory = tg::graph::object::Directory { entries };
				let node = Node::Directory(directory);
				Ok(node)
			},
			tg::graph::data::Node::File(tg::graph::data::File {
				contents,
				dependencies,
				executable,
			}) => {
				let contents = tg::Blob::with_id(contents);
				let dependencies = dependencies
					.into_iter()
					.map(|(reference, referent)| {
						let referent = tg::Referent {
							item: referent.item.map_right(tg::Object::with_id),
							path: referent.path.clone(),
							subpath: referent.subpath,
							tag: referent.tag,
						};
						(reference, referent)
					})
					.collect();
				let file = tg::graph::object::File {
					contents,
					dependencies,
					executable,
				};
				let node = Node::File(file);
				Ok(node)
			},
			tg::graph::data::Node::Symlink(tg::graph::data::Symlink { artifact, subpath }) => {
				let artifact = artifact.map(|either| either.map_right(tg::Artifact::with_id));
				let symlink = tg::graph::object::Symlink { artifact, subpath };
				let node = Node::Symlink(symlink);
				Ok(node)
			},
		}
	}
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Directory => write!(f, "directory"),
			Self::File => write!(f, "file"),
			Self::Symlink => write!(f, "symlink"),
		}
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"directory" => Ok(Self::Directory),
			"file" => Ok(Self::File),
			"symlink" => Ok(Self::Symlink),
			_ => Err(tg::error!(%kind = s, "invalid kind")),
		}
	}
}
