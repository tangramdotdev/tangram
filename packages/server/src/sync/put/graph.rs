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
	pub complete: Option<crate::process::complete::Output>,
	pub parents: SmallVec<[usize; 1]>,
}

#[derive(Clone, Debug, Default)]
pub struct ObjectNode {
	pub children: Option<Vec<usize>>,
	pub complete: Option<bool>,
	pub parents: SmallVec<[usize; 1]>,
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
		complete: Option<&crate::process::complete::Output>,
	) -> (bool, Option<crate::process::complete::Output>) {
		let entry = self.nodes.entry(id.clone().into());
		let inserted = matches!(entry, indexmap::map::Entry::Vacant(_));
		let index = entry.index();
		entry.or_insert_with(|| Node::Process(ProcessNode::default()));

		if let Some(complete) = complete {
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.complete = Some(complete.clone());
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

		if let Some(path) = self.find_complete_process_ancestor(index, |complete| complete.children)
		{
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.complete.get_or_insert_default().children = true;
			self.propagate_process_field(&path, |complete| complete.children = true);
		}

		if let Some(path) =
			self.find_complete_process_ancestor(index, |complete| complete.children_commands)
		{
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.complete.get_or_insert_default().children_commands = true;
			self.propagate_process_field(&path, |complete| complete.children_commands = true);
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.complete.get_or_insert_default().command = true;
			self.propagate_process_field(&path, |complete| complete.command = true);
		}

		if let Some(path) =
			self.find_complete_process_ancestor(index, |complete| complete.children_outputs)
		{
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.complete.get_or_insert_default().children_outputs = true;
			self.propagate_process_field(&path, |complete| complete.children_outputs = true);
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.complete.get_or_insert_default().output = true;
			self.propagate_process_field(&path, |complete| complete.output = true);
		}

		let complete = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_process_ref()
			.complete
			.clone();

		(inserted, complete)
	}

	fn find_complete_process_ancestor<F>(&self, index: usize, f: F) -> Option<Vec<usize>>
	where
		F: Fn(&crate::process::complete::Output) -> bool,
	{
		let mut stack = vec![vec![index]];
		loop {
			let Some(path) = stack.pop() else {
				break None;
			};
			let index = *path.last().unwrap();
			let node = self.nodes.get_index(index).unwrap().1.unwrap_process_ref();
			let complete = node.complete.as_ref().is_some_and(&f);
			if complete {
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
		F: Fn(&mut crate::process::complete::Output),
	{
		for &index in path {
			let (_, node) = self.nodes.get_index_mut(index).unwrap();
			if let Node::Process(process) = node {
				let complete = process.complete.get_or_insert_with(Default::default);
				f(complete);
			}
		}
	}

	pub fn update_object(
		&mut self,
		id: &tg::object::Id,
		parent: Option<Id>,
		kind: Option<crate::sync::queue::ObjectKind>,
		complete: Option<bool>,
	) -> (bool, Option<bool>) {
		let entry = self.nodes.entry(id.clone().into());
		let inserted = matches!(entry, indexmap::map::Entry::Vacant(_));
		let index = entry.index();
		entry.or_insert_with(|| Node::Object(ObjectNode::default()));

		if let Some(complete) = complete {
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_object_mut();
			node.complete = Some(complete);
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

		// Search for a complete ancestor and propagate completeness.
		let path = self.find_complete_object_ancestor(index, kind);
		if let Some(path) = path {
			for index in path {
				let (_, node) = self.nodes.get_index_mut(index).unwrap();
				match node {
					Node::Process(process) => {
						let complete = process.complete.get_or_insert_with(Default::default);
						match kind {
							Some(crate::sync::queue::ObjectKind::Command) => {
								complete.children_commands = true;
								complete.command = true;
							},
							Some(crate::sync::queue::ObjectKind::Output) => {
								complete.children_outputs = true;
								complete.output = true;
							},
							None => {
								complete.children_commands = true;
								complete.children_outputs = true;
								complete.command = true;
								complete.output = true;
							},
						}
					},
					Node::Object(object) => {
						object.complete = Some(true);
					},
				}
			}
		}

		let complete = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_object_ref()
			.complete;

		(inserted, complete)
	}

	fn find_complete_object_ancestor(
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
			let complete = match node {
				Node::Process(process) => {
					process
						.complete
						.as_ref()
						.is_some_and(|complete| match kind {
							Some(crate::sync::queue::ObjectKind::Command) => {
								complete.children_commands || (path.len() == 2 && complete.command)
							},
							Some(crate::sync::queue::ObjectKind::Output) => {
								complete.children_outputs || (path.len() == 2 && complete.output)
							},
							None => complete.children_commands && complete.children_outputs,
						})
				},
				Node::Object(object) => object.complete.is_some_and(|complete| complete),
			};
			if complete {
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
