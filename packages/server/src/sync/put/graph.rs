use {indexmap::IndexMap, smallvec::SmallVec, tangram_client::prelude::*};

#[derive(Default)]
pub struct Graph {
	arg: tg::sync::Arg,
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
	pub complete: bool,
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
		complete: Option<crate::process::complete::Output>,
	) -> (bool, bool) {
		let entry = self.nodes.entry(id.clone().into());
		let inserted = matches!(entry, indexmap::map::Entry::Vacant(_));
		let index = entry.index();
		entry.or_insert_with(|| Node::Process(ProcessNode::default()));

		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_process_mut();
		if let Some(complete) = complete {
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

		// Attempt to find a complete ancestor and then mark all nodes on the path to that ancestor complete.
		let mut stack = vec![vec![index]];
		let path = loop {
			let Some(path) = stack.pop() else {
				break None;
			};
			let index = *path.last().unwrap();
			let node = self.nodes.get_index(index).unwrap().1.unwrap_process_ref();
			let complete = node.complete.as_ref().is_some_and(|complete| {
				if self.arg.recursive {
					complete.children
						&& (!self.arg.commands || complete.children_commands)
						&& (!self.arg.outputs || complete.children_outputs)
				} else {
					(!self.arg.commands || complete.command)
						&& (!self.arg.outputs || complete.output)
				}
			});
			if complete {
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
				if let Node::Process(process) = node {
					let complete = process.complete.get_or_insert_with(Default::default);
					if self.arg.recursive {
						complete.children = true;
						if self.arg.commands {
							complete.children_commands = true;
						}
						if self.arg.outputs {
							complete.children_outputs = true;
						}
					} else {
						if self.arg.commands {
							complete.command = true;
						}
						if self.arg.outputs {
							complete.output = true;
						}
					}
				}
			}
		}

		(inserted, complete)
	}

	pub fn update_object(
		&mut self,
		id: &tg::object::Id,
		parent: Option<Id>,
		complete: bool,
	) -> (bool, bool) {
		let entry = self.nodes.entry(id.clone().into());
		let inserted = matches!(entry, indexmap::map::Entry::Vacant(_));
		let index = entry.index();
		entry.or_insert_with(|| Node::Object(ObjectNode::default()));

		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_object_mut();
		node.complete |= complete;

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

		// Attempt to find a complete ancestor and then mark all nodes on the path to that ancestor complete.
		let mut stack = vec![vec![index]];
		let path = loop {
			let Some(path) = stack.pop() else {
				break None;
			};
			let index = *path.last().unwrap();
			let node = self.nodes.get_index(index).unwrap().1;
			let complete = match node {
				Node::Process(process) => {
					process.complete.as_ref().is_some_and(|complete| {
						if self.arg.recursive {
							complete.children
								&& (!self.arg.commands || complete.children_commands)
								&& (!self.arg.outputs || complete.children_outputs)
						} else {
							(!self.arg.commands || complete.command)
								&& (!self.arg.outputs || complete.output)
						}
					})
				},
				Node::Object(object) => object.complete,
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
		};
		let complete = path.is_some();
		if let Some(path) = path {
			for index in path {
				let (_, node) = self.nodes.get_index_mut(index).unwrap();
				match node {
					Node::Process(process) => {
						let complete = process.complete.get_or_insert_with(Default::default);
						if self.arg.recursive {
							complete.children = true;
							if self.arg.commands {
								complete.children_commands = true;
							}
							if self.arg.outputs {
								complete.children_outputs = true;
							}
						} else {
							if self.arg.commands {
								complete.command = true;
							}
							if self.arg.outputs {
								complete.output = true;
							}
						}
					},
					Node::Object(object) => {
						object.complete = true;
					},
				}
			}
		}

		(inserted, complete)
	}
}
