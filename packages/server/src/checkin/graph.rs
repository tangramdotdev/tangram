use {
	smallvec::SmallVec,
	std::{
		collections::{BTreeMap, HashSet},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
	tangram_util::{collections::path_trie::PathTrie, iter::Ext as _},
};

#[derive(Clone, Debug, Default)]
pub struct Graph {
	pub artifacts: im::HashMap<tg::artifact::Id, usize, tg::id::BuildHasher>,
	pub ids: im::HashMap<tg::object::Id, SmallVec<[usize; 1]>, tg::id::BuildHasher>,
	pub next: usize,
	pub nodes: im::OrdMap<usize, Box<Node>>,
	pub paths: PathTrie<usize>,
}

#[expect(clippy::struct_field_names)]
#[derive(Clone, Debug)]
pub struct Node {
	pub artifact: Option<tg::artifact::Id>,
	pub edge: Option<tg::graph::data::Edge<tg::object::Id>>,
	pub id: Option<tg::object::Id>,
	pub lock_node: Option<usize>,
	pub metadata: Option<tg::object::Metadata>,
	pub path: Option<PathBuf>,
	pub path_metadata: Option<std::fs::Metadata>,
	pub referrers: SmallVec<[usize; 1]>,
	pub solvable: bool,
	pub solved: bool,
	pub stored: crate::object::stored::Output,
	pub variant: Variant,
}

impl Graph {
	pub fn clean(&mut self, root: &Path) -> HashSet<PathBuf, fnv::FnvBuildHasher> {
		// Get nodes with no referrers.
		let root = self.paths.get(root).unwrap();
		let mut queue = self
			.nodes
			.iter()
			.filter(|(index, node)| *index != root && node.referrers.is_empty())
			.map(|(index, _)| *index)
			.collect::<Vec<_>>();

		let mut removed_paths = HashSet::default();
		let mut visited = HashSet::<usize, fnv::FnvBuildHasher>::default();
		while let Some(index) = queue.pop() {
			if !visited.insert(index) {
				continue;
			}

			// Remove the node.
			let node = self.nodes.remove(&index).unwrap();
			tracing::trace!(path = ?node.path, artifact = ?node.artifact, edge = ?node.edge.as_ref().map(ToString::to_string), "cleaned");
			if let Some(artifact) = &node.artifact {
				self.artifacts.remove(artifact).unwrap();
			}
			if let Some(edge) = &node.edge
				&& let Some(id) = edge.try_unwrap_object_ref().ok()
				&& let Some(nodes) = self.ids.get_mut(id)
			{
				nodes.retain(|i| *i != index);
				if nodes.is_empty() {
					self.ids.remove(id);
				}
			}
			if let Some(path) = &node.path {
				self.paths.remove(path);
			}

			// Remove the node from its children's referrers and enqueue its children with no more referrers.
			for child_index in node.children() {
				if let Some(child) = self.nodes.get_mut(&child_index) {
					child.referrers.retain(|index_| *index_ != index);
					if child.referrers.is_empty() {
						queue.push(child_index);
					}
				}
			}

			// Add to the removed paths set.
			if let Some(path) = node.path {
				removed_paths.insert(path);
			}
		}

		removed_paths
	}

	pub fn unsolve(&mut self) {
		let mut queue = Vec::new();
		let indices = self.nodes.keys().copied().collect::<Vec<_>>();
		for index in indices {
			let node = self.nodes.get_mut(&index).unwrap();
			if let Variant::File(file) = &mut node.variant {
				let mut marked = false;
				for (reference, option) in &mut file.dependencies {
					if reference.is_solvable() {
						marked = true;
						option.take();
					}
				}
				if marked {
					queue.push(index);
				}
			}
		}
		let mut visited = HashSet::<usize, fnv::FnvBuildHasher>::default();
		while let Some(index) = queue.pop() {
			if !visited.insert(index) {
				continue;
			}
			let node = self.nodes.get_mut(&index).unwrap();
			node.solved = false;
			for index in &node.referrers {
				queue.push(*index);
			}
		}
	}
}

impl Node {
	/// Extract all child node indices from this node's variant.
	pub fn children(&self) -> Vec<usize> {
		let mut children = Vec::new();
		match &self.variant {
			Variant::Directory(directory) => {
				for edge in directory.entries.values() {
					if let Ok(reference) = edge.try_unwrap_reference_ref()
						&& reference.graph.is_none()
					{
						children.push(reference.index);
					}
				}
			},
			Variant::File(file) => {
				for referent in file.dependencies.values().flatten() {
					if let Some(edge) = &referent.item
						&& let Ok(reference) = edge.try_unwrap_reference_ref()
						&& reference.graph.is_none()
					{
						children.push(reference.index);
					}
				}
			},
			Variant::Symlink(symlink) => {
				if let Some(edge) = &symlink.artifact
					&& let Ok(reference) = edge.try_unwrap_reference_ref()
					&& reference.graph.is_none()
				{
					children.push(reference.index);
				}
			},
		}
		children
	}
}

#[derive(Clone, Debug, derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
pub enum Variant {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
}

impl Variant {
	#[must_use]
	pub fn kind(&self) -> tg::artifact::Kind {
		match self {
			Self::Directory(_) => tg::artifact::Kind::Directory,
			Self::File(_) => tg::artifact::Kind::File,
			Self::Symlink(_) => tg::artifact::Kind::Symlink,
		}
	}
}

#[derive(Clone, Debug)]
pub struct Directory {
	pub entries: BTreeMap<String, tg::graph::data::Edge<tg::artifact::Id>>,
}

#[derive(Clone, Debug)]
pub struct File {
	pub contents: Option<Contents>,
	pub dependencies: BTreeMap<tg::Reference, Option<tg::graph::data::Dependency>>,
	pub executable: bool,
}

#[derive(Clone, Debug)]
pub enum Contents {
	Write(Box<crate::write::Output>),
	Id {
		id: tg::blob::Id,
		stored: crate::object::stored::Output,
		metadata: Option<tg::object::Metadata>,
	},
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
		let next = self.next;
		match &node.variant {
			Variant::Directory(directory) => directory
				.entries
				.values()
				.filter_map(move |edge| {
					edge.try_unwrap_reference_ref()
						.ok()
						.and_then(|reference| reference.graph.is_none().then_some(reference.index))
						.filter(|&index| index >= next)
				})
				.boxed(),
			Variant::File(file) => file
				.dependencies
				.values()
				.filter_map(move |option| {
					option
						.as_ref()
						.and_then(|referent| referent.item.as_ref())
						.and_then(|edge| {
							edge.try_unwrap_reference_ref().ok().and_then(|reference| {
								reference.graph.is_none().then_some(reference.index)
							})
						})
						.filter(|&index| index >= next)
				})
				.boxed(),
			Variant::Symlink(symlink) => symlink
				.artifact
				.iter()
				.filter_map(move |edge| {
					edge.try_unwrap_reference_ref()
						.ok()
						.and_then(|reference| reference.graph.is_none().then_some(reference.index))
						.filter(|&index| index >= next)
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
