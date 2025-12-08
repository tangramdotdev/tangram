use {indexmap::IndexMap, smallvec::SmallVec, tangram_client::prelude::*};

#[derive(Default)]
pub struct Graph {
	nodes: IndexMap<Id, Node, fnv::FnvBuildHasher>,
}

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Id {
	Process(tg::process::Id),
	Object(tg::object::Id),
}

#[derive(Debug, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
pub enum Node {
	Process(ProcessNode),
	Object(ObjectNode),
}

#[derive(Clone, Debug, Default)]
pub struct ProcessNode {
	pub children: Option<Vec<usize>>,
	pub parents: SmallVec<[usize; 1]>,
	pub stored: Option<crate::process::stored::Output>,
}

#[derive(Clone, Debug, Default)]
pub struct ObjectNode {
	pub children: Option<Vec<usize>>,
	pub parents: SmallVec<[usize; 1]>,
	pub stored: Option<crate::object::stored::Output>,
}

impl Node {
	pub fn parents(&self) -> &SmallVec<[usize; 1]> {
		match self {
			Node::Process(node) => &node.parents,
			Node::Object(node) => &node.parents,
		}
	}

	pub fn parents_mut(&mut self) -> &mut SmallVec<[usize; 1]> {
		match self {
			Node::Process(node) => &mut node.parents,
			Node::Object(node) => &mut node.parents,
		}
	}

	#[expect(dead_code)]
	pub fn children(&self) -> Option<&Vec<usize>> {
		match self {
			Node::Process(node) => node.children.as_ref(),
			Node::Object(node) => node.children.as_ref(),
		}
	}

	pub fn children_mut(&mut self) -> &mut Option<Vec<usize>> {
		match self {
			Node::Process(node) => &mut node.children,
			Node::Object(node) => &mut node.children,
		}
	}
}

impl Graph {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn update_process(
		&mut self,
		id: &tg::process::Id,
		parent: Option<Id>,
		stored: Option<&crate::process::stored::Output>,
	) -> (bool, Option<crate::process::stored::Output>) {
		let entry = self.nodes.entry(id.clone().into());
		let inserted = matches!(entry, indexmap::map::Entry::Vacant(_));
		let index = entry.index();
		entry.or_insert_with(|| Node::Process(ProcessNode::default()));

		if let Some(stored) = stored {
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.stored = Some(stored.clone());
		}
		if let Some(parent) = parent {
			// Get the parent index and node.
			let (parent_index, _, parent_node) = self.nodes.get_full_mut(&parent).unwrap();

			// Add the node as a child of the parent.
			if let Some(children) = parent_node.children_mut().as_mut()
				&& !children.contains(&index)
			{
				children.push(index);
			}

			// Add the parent to the node.
			let (_, node) = self.nodes.get_index_mut(index).unwrap();
			if !node.parents_mut().contains(&parent_index) {
				node.parents_mut().push(parent_index);
			}
		}

		if let Some(path) = self.find_process_ancestor(index, |stored| stored.subtree) {
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.stored.get_or_insert_default().subtree = true;
			self.propagate_process_field(&path, |stored| stored.subtree = true);
		}

		if let Some(path) = self.find_process_ancestor(index, |stored| stored.subtree_command) {
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.stored.get_or_insert_default().subtree_command = true;
			self.propagate_process_field(&path, |stored| stored.subtree_command = true);
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.stored.get_or_insert_default().node_command = true;
			self.propagate_process_field(&path, |stored| stored.node_command = true);
		}

		if let Some(path) = self.find_process_ancestor(index, |stored| stored.subtree_output) {
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.stored.get_or_insert_default().subtree_output = true;
			self.propagate_process_field(&path, |stored| stored.subtree_output = true);
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.stored.get_or_insert_default().node_output = true;
			self.propagate_process_field(&path, |stored| stored.node_output = true);
		}

		let stored = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_process_ref()
			.stored
			.clone();

		(inserted, stored)
	}

	fn find_process_ancestor<F>(&self, index: usize, f: F) -> Option<Vec<usize>>
	where
		F: Fn(&crate::process::stored::Output) -> bool,
	{
		let mut stack = vec![vec![index]];
		loop {
			let Some(path) = stack.pop() else {
				break None;
			};
			let index = *path.last().unwrap();
			let node = self.nodes.get_index(index).unwrap().1.unwrap_process_ref();
			let stored = node.stored.as_ref().is_some_and(&f);
			if stored {
				break Some(path);
			}
			for parent in &node.parents {
				let mut path = path.clone();
				assert!(!path.contains(parent));
				path.push(*parent);
				stack.push(path);
			}
		}
	}

	fn propagate_process_field<F>(&mut self, path: &[usize], f: F)
	where
		F: Fn(&mut crate::process::stored::Output),
	{
		for &index in path {
			let (_, node) = self.nodes.get_index_mut(index).unwrap();
			if let Node::Process(process) = node {
				let stored = process.stored.get_or_insert_with(Default::default);
				f(stored);
			}
		}
	}

	pub fn update_object(
		&mut self,
		id: &tg::object::Id,
		parent: Option<Id>,
		kind: Option<crate::sync::queue::ObjectKind>,
		stored: Option<crate::object::stored::Output>,
	) -> (bool, Option<crate::object::stored::Output>) {
		let entry = self.nodes.entry(id.clone().into());
		let inserted = matches!(entry, indexmap::map::Entry::Vacant(_));
		let index = entry.index();
		entry.or_insert_with(|| Node::Object(ObjectNode::default()));

		if let Some(stored) = stored {
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_object_mut();
			node.stored = Some(stored);
		}

		if let Some(parent) = parent {
			// Get the parent index and node.
			let (parent_index, _, parent_node) = self.nodes.get_full_mut(&parent).unwrap();

			// Add the node as a child of the parent.
			if let Some(children) = parent_node.children_mut().as_mut()
				&& !children.contains(&index)
			{
				children.push(index);
			}

			// Add the parent to the node.
			let (_, node) = self.nodes.get_index_mut(index).unwrap();
			if !node.parents_mut().contains(&parent_index) {
				node.parents_mut().push(parent_index);
			}
		}

		// Search for a stored ancestor and propagate stored.
		let path = self.find_object_ancestor(index, kind);
		if let Some(path) = path {
			for index in path {
				let (_, node) = self.nodes.get_index_mut(index).unwrap();
				match node {
					Node::Process(process) => {
						let stored = process.stored.get_or_insert_with(Default::default);
						match kind {
							Some(crate::sync::queue::ObjectKind::Command) => {
								stored.subtree_command = true;
								stored.node_command = true;
							},
							Some(crate::sync::queue::ObjectKind::Output) => {
								stored.subtree_output = true;
								stored.node_output = true;
							},
							None => {
								stored.subtree_command = true;
								stored.subtree_output = true;
								stored.node_command = true;
								stored.node_output = true;
							},
						}
					},
					Node::Object(object) => {
						object.stored = Some(crate::object::stored::Output { subtree: true });
					},
				}
			}
		}

		let stored = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_object_ref()
			.stored
			.clone();

		(inserted, stored)
	}

	fn find_object_ancestor(
		&self,
		index: usize,
		kind: Option<crate::sync::queue::ObjectKind>,
	) -> Option<Vec<usize>> {
		let mut stack = vec![vec![index]];
		loop {
			let Some(path) = stack.pop() else {
				break None;
			};
			let index = *path.last().unwrap();
			let node = self.nodes.get_index(index).unwrap().1;
			let stored = match node {
				Node::Process(process) => {
					process.stored.as_ref().is_some_and(|stored| match kind {
						Some(crate::sync::queue::ObjectKind::Command) => {
							stored.subtree_command || (path.len() == 2 && stored.node_command)
						},
						Some(crate::sync::queue::ObjectKind::Output) => {
							stored.subtree_output || (path.len() == 2 && stored.node_output)
						},
						None => stored.subtree_command && stored.subtree_output,
					})
				},
				Node::Object(object) => object.stored.as_ref().is_some_and(|stored| stored.subtree),
			};
			if stored {
				break Some(path);
			}
			for parent in node.parents() {
				let mut path = path.clone();
				assert!(!path.contains(parent));
				path.push(*parent);
				stack.push(path);
			}
		}
	}
}
