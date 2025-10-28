use {
	smallvec::SmallVec,
	std::{collections::BTreeMap, path::PathBuf},
	tangram_client as tg,
	tangram_either::Either,
	tangram_util::iter::Ext as _,
};

#[derive(Clone, Debug, Default)]
pub struct Graph {
	pub ids: im::HashMap<tg::object::Id, usize, tg::id::BuildHasher>,
	pub next: usize,
	pub nodes: im::OrdMap<usize, Box<Node>>,
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
	pub contents: Option<
		Either<Box<crate::write::Output>, (tg::blob::Id, bool, Option<tg::object::Metadata>)>,
	>,
	pub dependencies:
		BTreeMap<tg::Reference, Option<tg::Referent<tg::graph::data::Edge<tg::object::Id>>>>,
	pub executable: bool,
}

#[derive(Clone, Debug)]
pub struct Symlink {
	pub artifact: Option<tg::graph::data::Edge<tg::artifact::Id>>,
	pub path: Option<PathBuf>,
}

pub struct Petgraph<'a> {
	pub graph: &'a Graph,
	pub next: usize,
}

impl petgraph::visit::GraphBase for Petgraph<'_> {
	type EdgeId = (usize, usize);

	type NodeId = usize;
}

impl<'a> petgraph::visit::IntoNodeIdentifiers for &'a Petgraph<'a> {
	type NodeIdentifiers = Box<dyn Iterator<Item = usize> + 'a>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		self.graph
			.nodes
			.range(self.next..)
			.map(|(index, _)| index)
			.copied()
			.boxed()
	}
}

impl<'a> petgraph::visit::IntoNeighbors for &'a Petgraph<'a> {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;

	fn neighbors(self, id: Self::NodeId) -> Self::Neighbors {
		let Some(node) = self.graph.nodes.get(&id) else {
			return std::iter::empty().boxed();
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
				.boxed(),
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
				.boxed(),
			Variant::Symlink(symlink) => symlink
				.artifact
				.iter()
				.filter_map(|edge| {
					edge.try_unwrap_reference_ref()
						.ok()
						.and_then(|reference| reference.graph.is_none().then_some(reference.node))
				})
				.boxed(),
		}
	}
}

impl petgraph::visit::NodeIndexable for &Petgraph<'_> {
	fn node_bound(&self) -> usize {
		self.graph.next
	}

	fn to_index(&self, id: Self::NodeId) -> usize {
		id
	}

	fn from_index(&self, index: usize) -> Self::NodeId {
		index
	}
}
