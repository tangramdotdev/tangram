use {
	smallvec::SmallVec,
	std::{collections::BTreeMap, path::PathBuf},
	tangram_client as tg,
	tangram_either::Either,
};

#[derive(Clone, Debug, Default)]
pub struct Graph {
	pub ids: im::HashMap<tg::object::Id, usize, tg::id::BuildHasher>,
	pub next: usize,
	pub nodes: im::OrdMap<usize, Node>,
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
	pub contents:
		Option<Either<crate::write::Output, (tg::blob::Id, Option<tg::object::Metadata>)>>,
	pub dependencies:
		BTreeMap<tg::Reference, Option<tg::Referent<tg::graph::data::Edge<tg::object::Id>>>>,
	pub executable: bool,
}

#[derive(Clone, Debug)]
pub struct Symlink {
	pub artifact: Option<tg::graph::data::Edge<tg::artifact::Id>>,
	pub path: Option<PathBuf>,
}

impl petgraph::visit::GraphBase for Graph {
	type EdgeId = (usize, usize);

	type NodeId = usize;
}

impl<'a> petgraph::visit::IntoNodeIdentifiers for &'a Graph {
	type NodeIdentifiers = std::iter::Copied<im::ordmap::Keys<'a, usize, Node>>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		self.nodes.keys().copied()
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
