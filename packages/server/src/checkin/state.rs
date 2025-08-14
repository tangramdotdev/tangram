use crate::lock::Lock;
use bytes::Bytes;
use std::{
	path::{Path, PathBuf},
	sync::Arc,
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_ignore as ignore;

pub struct State {
	pub arg: tg::checkin::Arg,
	pub artifacts_path: PathBuf,
	pub destructive_fixup_sender: Option<std::sync::mpsc::Sender<(PathBuf, std::fs::Metadata)>>,
	pub graph: Graph,
	pub graph_objects: Vec<GraphObject>,
	pub lock: Option<Lock>,
	pub ignorer: Option<ignore::Ignorer>,
	pub progress: crate::progress::Handle<tg::checkin::Output>,
}

pub struct GraphObject {
	pub id: tg::graph::Id,
	pub data: tg::graph::Data,
	pub bytes: Bytes,
}

#[derive(Clone, Debug, Default)]
pub struct Graph {
	pub nodes: im::Vector<Node>,
	pub paths: im::HashMap<PathBuf, usize, fnv::FnvBuildHasher>,
	pub roots: im::OrdMap<usize, Vec<usize>>,
}

#[derive(Clone, Debug)]
pub struct Node {
	pub artifacts_entry: Option<tg::object::Id>,
	pub lock_index: Option<usize>,
	pub metadata: Option<std::fs::Metadata>,
	pub object: Option<Object>,
	pub parent: Option<usize>,
	pub path: Option<Arc<PathBuf>>,
	pub root: Option<usize>,
	pub tag: Option<tg::Tag>,
	pub variant: Variant,
}

#[derive(Clone, Debug, derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
pub enum Variant {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
	Object,
}

#[derive(Clone, Debug)]
pub struct Directory {
	pub entries: Vec<(String, usize)>,
}

#[derive(Clone, Debug)]
pub struct File {
	pub blob: Option<Blob>,
	pub executable: bool,
	pub dependencies: Vec<(
		tg::Reference,
		Option<tg::Referent<Either<tg::object::Id, usize>>>,
	)>,
}

#[derive(Clone, Debug)]
pub enum Blob {
	Create(crate::blob::create::Blob),
	Id(tg::blob::Id),
}

#[derive(Clone, Debug)]
pub struct Symlink {
	pub artifact: Option<Either<tg::artifact::Id, usize>>,
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct Object {
	pub bytes: Option<Bytes>,
	pub data: Option<tg::object::Data>,
	pub id: tg::object::Id,
}

impl Graph {
	// Given a referrer and referent, find the "path" that corresponds to it.
	pub fn referent_path(&self, referrer: usize, referent: usize) -> Option<PathBuf> {
		// Get the path of the referrer.
		let mut referrer_path = self.nodes[referrer].path.as_deref()?.as_ref();

		// If the referrer is a module, use its parent.
		if tg::package::is_module_path(referrer_path) {
			referrer_path = referrer_path.parent()?;
		}

		// Get the path of the referent.
		let referent_path = self.nodes[referent].path.as_deref()?.as_ref();

		// Skip any imports of self.
		if referent_path == referrer_path {
			return None;
		}

		// Compute the relative path.
		tg::util::path::diff(referrer_path, referent_path).ok()
	}
}

impl Node {
	pub fn path(&self) -> &Path {
		self.path.as_deref().unwrap()
	}

	pub fn edges(&self) -> Vec<usize> {
		match &self.variant {
			Variant::Directory(directory) => {
				directory.entries.iter().map(|(_, node)| *node).collect()
			},
			Variant::File(file) => file
				.dependencies
				.iter()
				.filter_map(|(_, dependency)| dependency.as_ref()?.item.as_ref().right().copied())
				.collect(),
			Variant::Symlink(symlink) => symlink
				.artifact
				.as_ref()
				.and_then(|either| either.as_ref().right().copied())
				.into_iter()
				.collect(),
			Variant::Object => Vec::new(),
		}
	}
}

impl petgraph::visit::GraphBase for Graph {
	type EdgeId = (usize, usize);

	type NodeId = usize;
}

impl petgraph::visit::IntoNodeIdentifiers for &Graph {
	type NodeIdentifiers = std::ops::Range<usize>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.nodes.len()
	}
}

impl petgraph::visit::NodeIndexable for Graph {
	fn from_index(&self, index: usize) -> Self::NodeId {
		index
	}

	fn node_bound(&self) -> usize {
		self.nodes.len()
	}

	fn to_index(&self, id: Self::NodeId) -> usize {
		id
	}
}

impl petgraph::visit::IntoNeighbors for &Graph {
	type Neighbors = std::vec::IntoIter<usize>;

	fn neighbors(self, id: Self::NodeId) -> Self::Neighbors {
		self.nodes[id].edges().into_iter()
	}
}
