use {
	crate::write::Output,
	bytes::Bytes,
	indexmap::IndexMap,
	smallvec::SmallVec,
	std::{
		collections::{BTreeMap, HashMap},
		path::PathBuf,
	},
	tangram_client as tg,
	tangram_either::Either,
	tangram_ignore as ignore,
};

pub struct State {
	pub arg: tg::checkin::Arg,
	pub artifacts_path: Option<PathBuf>,
	pub blobs: HashMap<tg::blob::Id, Output, tg::id::BuildHasher>,
	pub fixup_sender: Option<std::sync::mpsc::Sender<FixupMessage>>,
	pub graph: Graph,
	pub ignorer: Option<ignore::Ignorer>,
	pub lock: Option<tg::graph::Data>,
	pub objects: Option<IndexMap<tg::object::Id, Object>>,
	pub progress: crate::progress::Handle<tg::checkin::Output>,
	pub root_path: PathBuf,
}

pub struct FixupMessage {
	pub path: PathBuf,
	pub metadata: std::fs::Metadata,
}

#[derive(Clone, Debug, Default)]
pub struct Graph {
	pub nodes: im::Vector<Node>,
	pub paths: im::HashMap<PathBuf, usize, fnv::FnvBuildHasher>,
}

#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug)]
pub struct Node {
	pub lock_node: Option<usize>,
	pub object_id: Option<tg::object::Id>,
	pub referrers: SmallVec<[usize; 1]>,
	pub path: Option<PathBuf>,
	pub path_metadata: Option<std::fs::Metadata>,
	pub variant: Variant,
}

#[derive(Clone, Debug, derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
pub enum Variant {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
}

#[derive(Clone, Debug)]
pub struct Directory {
	pub entries: BTreeMap<String, tg::graph::data::Edge<tg::artifact::Id>>,
}

#[derive(Clone, Debug)]
pub struct File {
	pub contents: Option<Either<crate::write::Output, tg::blob::Id>>,
	pub dependencies:
		BTreeMap<tg::Reference, Option<tg::Referent<tg::graph::data::Edge<tg::object::Id>>>>,
	pub executable: bool,
}

#[derive(Clone, Debug)]
pub struct Symlink {
	pub artifact: Option<tg::graph::data::Edge<tg::artifact::Id>>,
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct Object {
	pub bytes: Option<Bytes>,
	pub cache_reference: Option<crate::store::CacheReference>,
	pub complete: bool,
	pub data: Option<tg::object::Data>,
	pub id: tg::object::Id,
	pub metadata: Option<tg::object::Metadata>,
	pub size: u64,
}

impl Graph {
	// Given a referrer and referent, find the "path" that corresponds to it.
	pub fn referent_path(&self, referrer: usize, referent: usize) -> Option<PathBuf> {
		// Get the path of the referrer.
		let mut referrer_path = self.nodes[referrer].path.as_deref()?;

		// If the referrer is a module, use its parent.
		if tg::package::is_module_path(referrer_path) {
			referrer_path = referrer_path.parent()?;
		}

		// Get the path of the referent.
		let referent_path = self.nodes[referent].path.as_deref()?;

		// Skip any imports of self.
		if referent_path == referrer_path {
			return None;
		}

		// Compute the relative path.
		tangram_util::path::diff(referrer_path, referent_path).ok()
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
		match &self.nodes[id].variant {
			Variant::Directory(directory) => directory
				.entries
				.values()
				.filter_map(|edge| {
					edge.try_unwrap_reference_ref()
						.ok()
						.and_then(|reference| reference.graph.is_none().then_some(reference.node))
				})
				.collect::<Vec<_>>()
				.into_iter(),
			Variant::File(file) => file
				.dependencies
				.values()
				.filter_map(|option| {
					option
						.as_ref()
						.map(|referent| &referent.item)
						.and_then(|edge| {
							edge.try_unwrap_reference_ref().ok().and_then(|reference| {
								reference.graph.is_none().then_some(reference.node)
							})
						})
				})
				.collect::<Vec<_>>()
				.into_iter(),
			Variant::Symlink(symlink) => symlink
				.artifact
				.iter()
				.filter_map(|edge| {
					edge.try_unwrap_reference_ref()
						.ok()
						.and_then(|reference| reference.graph.is_none().then_some(reference.node))
				})
				.collect::<Vec<_>>()
				.into_iter(),
		}
	}
}
