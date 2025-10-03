use {indexmap::IndexMap, smallvec::SmallVec, tangram_client as tg, tangram_either::Either};

pub struct Graph {
	arg: tg::sync::Arg,
	nodes: IndexMap<Either<tg::process::Id, tg::object::Id>, Node, fnv::FnvBuildHasher>,
}

struct Node {
	children: Vec<usize>,
	parents: SmallVec<[usize; 1]>,
	inner: Option<NodeInner>,
}

enum NodeInner {
	Process(ProcessNode),
	Object(ObjectNode),
}

struct ProcessNode {
	complete: crate::process::complete::Output,
}

struct ObjectNode {
	complete: bool,
}

impl Graph {
	pub fn new(arg: tg::sync::Arg) -> Self {
		Self {
			arg,
			nodes: IndexMap::default(),
		}
	}

	pub fn update(
		&mut self,
		parent: Option<Either<tg::process::Id, tg::object::Id>>,
		id: Either<tg::process::Id, tg::object::Id>,
		complete: Either<crate::process::complete::Output, bool>,
	) -> (bool, Either<crate::process::complete::Output, bool>) {
		// Get or insert the node and set its complete flag.
		let mut inserted = false;
		let index = if let Some((index, _, node)) = self.nodes.get_full_mut(&id) {
			match complete {
				Either::Left(process_complete) => {
					if let Some(NodeInner::Process(process_node)) = &mut node.inner {
						process_node.complete.children |= process_complete.children;
						process_node.complete.children_commands |=
							process_complete.children_commands;
						process_node.complete.children_outputs |= process_complete.children_outputs;
						process_node.complete.command |= process_complete.command;
						process_node.complete.output |= process_complete.output;
					} else {
						node.inner = Some(NodeInner::Process(ProcessNode {
							complete: process_complete,
						}));
					}
				},
				Either::Right(object_complete) => {
					if let Some(NodeInner::Object(object_node)) = &mut node.inner {
						object_node.complete |= object_complete;
					} else if object_complete {
						node.inner = Some(NodeInner::Object(ObjectNode {
							complete: object_complete,
						}));
					}
				},
			}
			index
		} else {
			inserted = true;
			let inner = match complete {
				Either::Left(process_complete) => Some(NodeInner::Process(ProcessNode {
					complete: process_complete,
				})),
				Either::Right(true) => Some(NodeInner::Object(ObjectNode { complete: true })),
				Either::Right(false) => None,
			};
			let node = Node {
				parents: SmallVec::new(),
				children: Vec::new(),
				inner,
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
			let complete = match &node.inner {
				Some(NodeInner::Process(process)) => {
					if self.arg.recursive {
						process.complete.children
							&& (!self.arg.commands || process.complete.children_commands)
							&& (!self.arg.outputs || process.complete.children_outputs)
					} else {
						(!self.arg.commands || process.complete.command)
							&& (!self.arg.outputs || process.complete.output)
					}
				},
				Some(NodeInner::Object(object)) => object.complete,
				None => false,
			};
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
		if let Some(path) = path {
			for index in path {
				let (_, node) = self.nodes.get_index_mut(index).unwrap();
				match &mut node.inner {
					Some(NodeInner::Process(process)) => {
						if self.arg.recursive {
							process.complete.children = true;
							if self.arg.commands {
								process.complete.children_commands = true;
							}
							if self.arg.outputs {
								process.complete.children_outputs = true;
							}
						} else {
							if self.arg.commands {
								process.complete.command = true;
							}
							if self.arg.outputs {
								process.complete.output = true;
							}
						}
					},
					Some(NodeInner::Object(object)) => {
						object.complete = true;
					},
					None => {},
				}
			}
		}

		// Get the final completeness of the node.
		let (_, node) = self.nodes.get_index(index).unwrap();
		let complete = match &node.inner {
			Some(NodeInner::Process(process)) => Either::Left(process.complete.clone()),
			Some(NodeInner::Object(object)) => Either::Right(object.complete),
			None => Either::Right(false),
		};

		(inserted, complete)
	}
}
