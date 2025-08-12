use indexmap::IndexMap;
use smallvec::SmallVec;
use tangram_client as tg;
use tangram_either::Either;

#[derive(Default)]
pub struct Graph {
	nodes: IndexMap<Id, Node, fnv::FnvBuildHasher>,
}

pub type Id = Either<tg::process::Id, tg::object::Id>;

struct Node {
	children: Vec<usize>,
	complete: bool,
	parents: SmallVec<[usize; 1]>,
}

impl Graph {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn update(&mut self, parent: Option<Id>, id: Id, complete: bool) -> (bool, bool) {
		// Get or insert the node and set its complete flag.
		let mut inserted = false;
		let index = if let Some((index, _, node)) = self.nodes.get_full_mut(&id) {
			node.complete |= complete;
			index
		} else {
			inserted = true;
			let node = Node {
				complete,
				parents: SmallVec::new(),
				children: Vec::new(),
			};
			let index = self.nodes.len();
			self.nodes.insert(id, node);
			index
		};

		// If the parent is set, then update the node and its parent.
		if let Some(parent) = parent {
			// Get the parent index and node.
			let (parent_index, _, parent_node) = self.nodes.get_full_mut(&parent).unwrap();

			// Add the node as a child of the parent.
			if !parent_node.children.contains(&index) {
				parent_node.children.push(index);
			}

			// Add the parent to the node.
			let (_, node) = self.nodes.get_index_mut(index).unwrap();
			if !node.parents.contains(&parent_index) {
				node.parents.push(parent_index);
			}
		}

		// Attempt to find a complete ancestor and then mark all nodes on the path to that ancestor complete.
		let mut stack = vec![vec![index]];
		let path = loop {
			let Some(path) = stack.pop() else {
				break None;
			};
			let index = *path.last().unwrap();
			let (_, node) = self.nodes.get_index_mut(index).unwrap();
			if node.complete {
				break Some(path);
			}
			for parent in &node.parents {
				let mut path = path.clone();
				assert!(!path.contains(parent));
				path.push(*parent);
				stack.push(path);
			}
		};
		let complete = path.is_some();
		if let Some(path) = path {
			for index in path {
				let (_, node) = self.nodes.get_index_mut(index).unwrap();
				node.complete = true;
			}
		}

		(inserted, complete)
	}
}
