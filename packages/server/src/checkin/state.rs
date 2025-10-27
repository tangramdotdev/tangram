use {
	bytes::Bytes,
	smallvec::SmallVec,
	std::{collections::BTreeMap, path::PathBuf},
	tangram_client as tg, tangram_ignore as ignore,
};

#[derive(Debug)]
pub struct State {
	pub arg: tg::checkin::Arg,
	pub artifacts_path: Option<PathBuf>,
	pub fixup_sender: Option<std::sync::mpsc::Sender<FixupMessage>>,
	pub graph: Graph,
	pub ignorer: Option<ignore::Ignorer>,
	pub lock: Option<tg::graph::Data>,
	pub objects: Objects,
	pub progress: crate::progress::Handle<tg::checkin::Output>,
	pub root_path: PathBuf,
}

#[derive(Debug)]
pub struct FixupMessage {
	pub path: PathBuf,
	pub metadata: std::fs::Metadata,
}

#[derive(Clone, Debug, Default)]
pub struct Graph {
	pub ids: im::HashMap<tg::object::Id, usize, tg::id::BuildHasher>,
	pub next: usize,
	pub nodes: im::HashMap<usize, Node, fnv::FnvBuildHasher>,
	pub paths: im::HashMap<PathBuf, usize, fnv::FnvBuildHasher>,
}

#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug)]
pub struct Node {
	pub complete: bool,
	pub id: Option<tg::object::Id>,
	pub lock_node: Option<usize>,
	pub metadata: Option<tg::object::Metadata>,
	pub path: Option<PathBuf>,
	pub path_metadata: Option<std::fs::Metadata>,
	pub referrers: SmallVec<[usize; 1]>,
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
	pub contents: Option<tg::blob::Id>,
	pub contents_metadata: Option<tg::object::Metadata>,
	pub dependencies:
		BTreeMap<tg::Reference, Option<tg::Referent<tg::graph::data::Edge<tg::object::Id>>>>,
	pub executable: bool,
}

#[derive(Clone, Debug)]
pub struct Symlink {
	pub artifact: Option<tg::graph::data::Edge<tg::artifact::Id>>,
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, Default)]
pub struct Objects {
	lookup: im::HashMap<tg::object::Id, usize, tg::id::BuildHasher>,
	map: im::OrdMap<usize, (tg::object::Id, Object)>,
	next: usize,
}

#[derive(Clone, Debug)]
pub struct Object {
	pub bytes: Option<Bytes>,
	pub cache_reference: Option<crate::store::CacheReference>,
	pub cache_reference_range: Option<CacheReferenceRange>,
	pub complete: bool,
	pub data: Option<tg::object::Data>,
	pub id: tg::object::Id,
	pub metadata: Option<tg::object::Metadata>,
	pub size: u64,
}

#[derive(Clone, Debug)]
pub struct CacheReferenceRange {
	pub position: u64,
	pub length: u64,
}

impl Objects {
	pub fn get(&self, id: &tg::object::Id) -> Option<&Object> {
		let index = self.lookup.get(id)?;
		let (_, object) = self.map.get(index)?;
		Some(object)
	}

	pub fn get_mut(&mut self, id: &tg::object::Id) -> Option<&mut Object> {
		let index = self.lookup.get(id)?;
		let (_, object) = self.map.get_mut(index)?;
		Some(object)
	}

	pub fn insert(&mut self, id: tg::object::Id, object: Object) {
		let index = if let Some(index) = self.lookup.get(&id).copied() {
			index
		} else {
			let index = self.next;
			self.next += 1;
			index
		};
		self.lookup.insert(id.clone(), index);
		self.map.insert(index, (id, object));
	}

	pub fn remove(&mut self, id: &tg::object::Id) -> Option<Object> {
		if let Some(index) = self.lookup.get(id).copied() {
			self.lookup.remove(id).unwrap();
			let (_, object) = self.map.remove(&index).unwrap();
			Some(object)
		} else {
			None
		}
	}

	pub fn values(&self) -> impl Iterator<Item = &Object> {
		self.map.values().map(|(_, object)| object)
	}
}

impl std::iter::Extend<(tg::object::Id, Object)> for Objects {
	fn extend<I: IntoIterator<Item = (tg::object::Id, Object)>>(&mut self, iter: I) {
		for (id, object) in iter {
			self.insert(id, object);
		}
	}
}

impl petgraph::visit::GraphBase for Graph {
	type EdgeId = (usize, usize);

	type NodeId = usize;
}

impl petgraph::visit::IntoNodeIdentifiers for &Graph {
	type NodeIdentifiers = std::vec::IntoIter<usize>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		let mut keys = self.nodes.keys().copied().collect::<Vec<_>>();
		keys.sort_unstable();
		keys.into_iter()
	}
}

impl petgraph::visit::IntoNeighbors for &Graph {
	type Neighbors = std::vec::IntoIter<usize>;

	fn neighbors(self, id: Self::NodeId) -> Self::Neighbors {
		let Some(node) = self.nodes.get(&id) else {
			return Vec::new().into_iter();
		};
		match &node.variant {
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

impl petgraph::visit::NodeIndexable for &Graph {
	fn node_bound(&self) -> usize {
		self.next
	}

	fn to_index(&self, id: Self::NodeId) -> usize {
		id
	}

	fn from_index(&self, index: usize) -> Self::NodeId {
		index
	}
}
