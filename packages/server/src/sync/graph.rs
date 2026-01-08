use {
	indexmap::IndexMap,
	petgraph::visit::IntoNeighbors as _,
	smallvec::SmallVec,
	std::collections::{BTreeSet, HashSet},
	tangram_client::prelude::*,
	tangram_util::iter::Ext as _,
};

#[derive(Default)]
pub struct Graph {
	pub get_end_received: bool,
	pub local_roots: HashSet<Id, fnv::FnvBuildHasher>,
	pub nodes: IndexMap<Id, Node, fnv::FnvBuildHasher>,
	pub remote_roots: HashSet<Id, fnv::FnvBuildHasher>,
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
	Object(tg::object::Id),
	Process(tg::process::Id),
}

#[derive(Debug, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
pub enum Node {
	Object(ObjectNode),
	Process(ProcessNode),
}

#[derive(Clone, Debug, Default)]
pub struct ObjectNode {
	pub children: Option<Vec<usize>>,
	pub local_stored: Option<crate::object::stored::Output>,
	pub marked: bool,
	pub metadata: Option<tg::object::Metadata>,
	pub parents: SmallVec<[usize; 1]>,
	pub remote_stored: Option<crate::object::stored::Output>,
	pub requested: Option<Requested>,
}

#[derive(Clone, Debug, Default)]
pub struct ProcessNode {
	pub children: Option<Vec<usize>>,
	pub local_stored: Option<crate::process::stored::Output>,
	pub marked: bool,
	pub metadata: Option<tg::process::Metadata>,
	pub objects: Option<Vec<(usize, crate::index::message::ProcessObjectKind)>>,
	pub parents: SmallVec<[usize; 1]>,
	pub remote_stored: Option<crate::process::stored::Output>,
	pub requested: Option<Requested>,
}

#[derive(Clone, Debug, Default)]
pub struct Requested {
	pub eager: bool,
}

impl Graph {
	pub fn new(
		local_roots: &[tg::Either<tg::object::Id, tg::process::Id>],
		remote_roots: &[tg::Either<tg::object::Id, tg::process::Id>],
	) -> Self {
		let local_roots = local_roots
			.iter()
			.map(|id| match id {
				tg::Either::Left(id) => Id::Object(id.clone()),
				tg::Either::Right(id) => Id::Process(id.clone()),
			})
			.collect();
		let remote_roots = remote_roots
			.iter()
			.map(|id| match id {
				tg::Either::Left(id) => Id::Object(id.clone()),
				tg::Either::Right(id) => Id::Process(id.clone()),
			})
			.collect();
		Graph {
			nodes: IndexMap::default(),
			local_roots,
			remote_roots,
			get_end_received: false,
		}
	}

	pub fn mark_get_end_received(&mut self) {
		self.get_end_received = true;
	}

	pub fn update_object_local(
		&mut self,
		id: &tg::object::Id,
		data: Option<&tg::object::Data>,
		stored: Option<crate::object::stored::Output>,
		metadata: Option<tg::object::Metadata>,
		marked: Option<bool>,
		requested: Option<Requested>,
	) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_insert_with(|| Node::Object(ObjectNode::default()));

		let children = if let Some(data) = data {
			let mut children = BTreeSet::new();
			data.children(&mut children);
			let children: Vec<usize> = children
				.into_iter()
				.map(|child| {
					let child_entry = self.nodes.entry(child.into());
					let child_index = child_entry.index();
					let child_node =
						child_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
					child_node.unwrap_object_mut().parents.push(index);
					child_index
				})
				.collect();
			Some(children)
		} else {
			None
		};

		let computed_stored = children.as_ref().map(|children| {
			children.iter().all(|child| {
				self.nodes
					.get_index(*child)
					.unwrap()
					.1
					.unwrap_object_ref()
					.local_stored
					.as_ref()
					.is_some_and(|stored| stored.subtree)
			})
		});

		let old_stored = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_object_ref()
			.local_stored
			.as_ref()
			.is_some_and(|stored| stored.subtree);

		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_object_mut();

		if let Some(children) = children {
			node.children = Some(children);
			node.local_stored = Some(crate::object::stored::Output {
				subtree: computed_stored.unwrap(),
			});
		}

		if let Some(stored) = stored {
			node.local_stored = Some(stored);
		}

		if let Some(mut metadata) = metadata {
			if let Some(existing) = &node.metadata {
				metadata.merge(existing);
			}
			node.metadata = Some(metadata);
		}

		if let Some(marked) = marked {
			node.marked = marked;
		}

		if let Some(requested) = requested {
			node.requested = Some(requested);
		}

		let new_stored = node
			.local_stored
			.as_ref()
			.is_some_and(|stored| stored.subtree);
		if !old_stored && new_stored {
			let mut stack: Vec<usize> = node.parents.iter().copied().collect();
			while let Some(parent_index) = stack.pop() {
				if let Some(parents) = self.try_propagate_local_stored(parent_index) {
					stack.extend(parents);
				}
			}
		}
	}

	pub fn update_process_local(
		&mut self,
		id: &tg::process::Id,
		data: Option<&tg::process::Data>,
		stored: Option<crate::process::stored::Output>,
		metadata: Option<tg::process::Metadata>,
		marked: Option<bool>,
		requested: Option<Requested>,
	) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_insert_with(|| Node::Process(ProcessNode::default()));

		let children = if let Some(data) = data {
			data.children.as_ref().map(|children| {
				children
					.iter()
					.map(|child| {
						let child_entry = self.nodes.entry(child.item.clone().into());
						let child_index = child_entry.index();
						let child_node =
							child_entry.or_insert_with(|| Node::Process(ProcessNode::default()));
						child_node.unwrap_process_mut().parents.push(index);
						child_index
					})
					.collect::<Vec<_>>()
			})
		} else {
			None
		};

		let objects = if let Some(data) = data {
			let mut objects: Vec<(usize, crate::index::message::ProcessObjectKind)> = Vec::new();

			let command: tg::object::Id = data.command.clone().into();
			let command_entry = self.nodes.entry(command.into());
			let command_index = command_entry.index();
			let command_node = command_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
			command_node.unwrap_object_mut().parents.push(index);
			objects.push((
				command_index,
				crate::index::message::ProcessObjectKind::Command,
			));

			if let Some(error) = &data.error {
				match error {
					tg::Either::Left(error_data) => {
						let mut error_children = BTreeSet::new();
						error_data.children(&mut error_children);
						for object_id in error_children {
							let object_entry = self.nodes.entry(object_id.into());
							let object_index = object_entry.index();
							let object_node =
								object_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
							object_node.unwrap_object_mut().parents.push(index);
							objects.push((
								object_index,
								crate::index::message::ProcessObjectKind::Error,
							));
						}
					},
					tg::Either::Right(error_id) => {
						let error_entry = self
							.nodes
							.entry(tg::object::Id::from(error_id.clone()).into());
						let error_index = error_entry.index();
						let error_node =
							error_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
						error_node.unwrap_object_mut().parents.push(index);
						objects
							.push((error_index, crate::index::message::ProcessObjectKind::Error));
					},
				}
			}

			if let Some(log) = data.log.clone() {
				let log_entry = self.nodes.entry(tg::object::Id::from(log).into());
				let log_index = log_entry.index();
				let log_node = log_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
				log_node.unwrap_object_mut().parents.push(index);
				objects.push((log_index, crate::index::message::ProcessObjectKind::Log));
			}

			if let Some(output) = &data.output {
				let mut output_children = BTreeSet::new();
				output.children(&mut output_children);
				for object_id in output_children {
					let object_entry = self.nodes.entry(object_id.into());
					let object_index = object_entry.index();
					let object_node =
						object_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
					object_node.unwrap_object_mut().parents.push(index);
					objects.push((
						object_index,
						crate::index::message::ProcessObjectKind::Output,
					));
				}
			}

			Some(objects)
		} else {
			None
		};

		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_process_mut();
		let node_old_stored = node.local_stored.clone();

		if let (Some(children), Some(objects)) = (&children, &objects) {
			let new_stored = self.compute_process_local_stored(children, objects);
			let merged = Self::merge_process_stored(node_old_stored.as_ref(), new_stored);
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.local_stored = Some(merged);
		}

		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_process_mut();

		if let Some(children) = children {
			node.children = Some(children);
		}

		if let Some(stored) = stored {
			let merged = Self::merge_process_stored(node.local_stored.as_ref(), stored);
			node.local_stored = Some(merged);
		}

		if let Some(mut metadata) = metadata {
			if let Some(existing) = &node.metadata {
				metadata.merge(existing);
			}
			node.metadata = Some(metadata);
		}

		if let Some(objects) = objects {
			node.objects = Some(objects);
		}

		if let Some(marked) = marked {
			node.marked = marked;
		}

		if let Some(requested) = requested {
			node.requested = Some(requested);
		}

		if Self::should_propagate_process_stored(
			node_old_stored.as_ref(),
			node.local_stored.as_ref(),
		) {
			let mut stack: Vec<usize> = node.parents.iter().copied().collect();
			while let Some(parent_index) = stack.pop() {
				if let Some(parents) = self.try_propagate_local_stored(parent_index) {
					stack.extend(parents);
				}
			}
		}
	}

	pub fn update_object_remote(
		&mut self,
		id: &tg::object::Id,
		parent: Option<Id>,
		kind: Option<crate::sync::queue::ObjectKind>,
		stored: Option<&crate::object::stored::Output>,
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
			node.remote_stored = Some(stored.clone());
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

		if stored.is_some() {
			let path = self.find_object_remote_ancestor(index, kind);
			if let Some(path) = path {
				for index in path {
					let (_, node) = self.nodes.get_index_mut(index).unwrap();
					match node {
						Node::Object(object) => {
							object.remote_stored =
								Some(crate::object::stored::Output { subtree: true });
						},
						Node::Process(process) => {
							let stored = process.remote_stored.get_or_insert_with(Default::default);
							match kind {
								Some(crate::sync::queue::ObjectKind::Command) => {
									stored.node_command = true;
									stored.subtree_command = true;
								},
								Some(crate::sync::queue::ObjectKind::Error) => {
									stored.node_error = true;
									stored.subtree_error = true;
								},
								Some(crate::sync::queue::ObjectKind::Log) => {
									stored.node_log = true;
									stored.subtree_log = true;
								},
								Some(crate::sync::queue::ObjectKind::Output) => {
									stored.node_output = true;
									stored.subtree_output = true;
								},
								None => {
									stored.node_command = true;
									stored.node_error = true;
									stored.node_log = true;
									stored.node_output = true;
									stored.subtree_command = true;
									stored.subtree_error = true;
									stored.subtree_log = true;
									stored.subtree_output = true;
								},
							}
						},
					}
				}
			}
		}

		let remote_stored = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_object_ref()
			.remote_stored
			.clone();

		(inserted, remote_stored)
	}

	pub fn update_process_remote(
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
			node.remote_stored = Some(stored.clone());
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

		if stored.is_none() {
			let stored = self
				.nodes
				.get_index(index)
				.unwrap()
				.1
				.unwrap_process_ref()
				.remote_stored
				.clone();
			return (inserted, stored);
		}

		if let Some(path) = self.find_process_remote_ancestor(index, |stored| stored.subtree) {
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.remote_stored.get_or_insert_default().subtree = true;
			self.propagate_process_remote_field(&path, |stored| stored.subtree = true);
		}

		if let Some(path) =
			self.find_process_remote_ancestor(index, |stored| stored.subtree_command)
		{
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.remote_stored.get_or_insert_default().subtree_command = true;
			self.propagate_process_remote_field(&path, |stored| stored.subtree_command = true);
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.remote_stored.get_or_insert_default().node_command = true;
			self.propagate_process_remote_field(&path, |stored| stored.node_command = true);
		}

		if let Some(path) = self.find_process_remote_ancestor(index, |stored| stored.subtree_output)
		{
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.remote_stored.get_or_insert_default().subtree_output = true;
			self.propagate_process_remote_field(&path, |stored| stored.subtree_output = true);
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.remote_stored.get_or_insert_default().node_output = true;
			self.propagate_process_remote_field(&path, |stored| stored.node_output = true);
		}

		let stored = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_process_ref()
			.remote_stored
			.clone();

		(inserted, stored)
	}

	pub fn get_process_local_stored(
		&self,
		id: &tg::process::Id,
	) -> Option<&crate::process::stored::Output> {
		self.nodes
			.get(&Id::Process(id.clone()))
			.and_then(|node| node.unwrap_process_ref().local_stored.as_ref())
	}

	pub fn get_object_requested(&self, id: &tg::object::Id) -> Option<Requested> {
		self.nodes
			.get(&Id::Object(id.clone()))
			.and_then(|node| node.unwrap_object_ref().requested.clone())
	}

	pub fn get_process_requested(&self, id: &tg::process::Id) -> Option<Requested> {
		self.nodes
			.get(&Id::Process(id.clone()))
			.and_then(|node| node.unwrap_process_ref().requested.clone())
	}

	pub fn end_local(&self, arg: &tg::sync::Arg) -> bool {
		self.local_roots.iter().all(|root| {
			let Some(node) = self.nodes.get(root) else {
				return false;
			};
			match node {
				Node::Object(node) => node
					.local_stored
					.as_ref()
					.is_some_and(|stored| stored.subtree),
				Node::Process(node) => node.local_stored.as_ref().is_some_and(|stored| {
					if arg.recursive {
						stored.subtree
							&& (!arg.commands || stored.subtree_command)
							&& (!arg.errors || stored.subtree_error)
							&& (!arg.logs || stored.subtree_log)
							&& (!arg.outputs || stored.subtree_output)
					} else {
						(!arg.commands || stored.node_command)
							&& (!arg.errors || stored.node_error)
							&& (!arg.logs || stored.node_log)
							&& (!arg.outputs || stored.node_output)
					}
				}),
			}
		})
	}

	pub fn end_remote(&self, arg: &tg::sync::Arg) -> bool {
		if !self.get_end_received {
			return false;
		}
		self.remote_roots.iter().all(|root| {
			let Some(node) = self.nodes.get(root) else {
				return false;
			};
			match node {
				Node::Object(node) => node
					.remote_stored
					.as_ref()
					.is_some_and(|stored| stored.subtree),
				Node::Process(node) => node.remote_stored.as_ref().is_some_and(|stored| {
					if arg.recursive {
						stored.subtree
							&& (!arg.commands || stored.subtree_command)
							&& (!arg.errors || stored.subtree_error)
							&& (!arg.logs || stored.subtree_log)
							&& (!arg.outputs || stored.subtree_output)
					} else {
						(!arg.commands || stored.node_command)
							&& (!arg.errors || stored.node_error)
							&& (!arg.logs || stored.node_log)
							&& (!arg.outputs || stored.node_output)
					}
				}),
			}
		})
	}

	fn compute_process_local_stored(
		&self,
		children: &[usize],
		objects: &[(usize, crate::index::message::ProcessObjectKind)],
	) -> crate::process::stored::Output {
		let mut stored = crate::process::stored::Output {
			node_command: true,
			node_error: true,
			node_log: true,
			node_output: true,
			subtree: true,
			subtree_command: true,
			subtree_error: true,
			subtree_log: true,
			subtree_output: true,
		};
		for child_index in children {
			let child_stored = self
				.nodes
				.get_index(*child_index)
				.and_then(|(_, node)| node.try_unwrap_process_ref().ok()?.local_stored.as_ref());
			if let Some(child_stored) = child_stored {
				stored.subtree = stored.subtree && child_stored.subtree;
				stored.subtree_command = stored.subtree_command && child_stored.subtree_command;
				stored.subtree_log = stored.subtree_log && child_stored.subtree_log;
				stored.subtree_output = stored.subtree_output && child_stored.subtree_output;
			} else {
				stored.subtree = false;
				stored.subtree_command = false;
				stored.subtree_log = false;
				stored.subtree_output = false;
			}
		}
		for (object_index, object_kind) in objects {
			let object_stored = self
				.nodes
				.get_index(*object_index)
				.and_then(|(_, node)| node.try_unwrap_object_ref().ok()?.local_stored.as_ref())
				.is_some_and(|s| s.subtree);
			match object_kind {
				crate::index::message::ProcessObjectKind::Command => {
					stored.node_command = stored.node_command && object_stored;
					stored.subtree_command = stored.subtree_command && object_stored;
				},
				crate::index::message::ProcessObjectKind::Error => {
					stored.node_error = stored.node_error && object_stored;
					stored.subtree_error = stored.subtree_error && object_stored;
				},
				crate::index::message::ProcessObjectKind::Log => {
					stored.node_log = stored.node_log && object_stored;
					stored.subtree_log = stored.subtree_log && object_stored;
				},
				crate::index::message::ProcessObjectKind::Output => {
					stored.node_output = stored.node_output && object_stored;
					stored.subtree_output = stored.subtree_output && object_stored;
				},
			}
		}
		stored
	}

	fn try_propagate_local_stored(&mut self, index: usize) -> Option<SmallVec<[usize; 1]>> {
		let (_, node) = self.nodes.get_index(index)?;
		match node {
			Node::Object(_) => self.try_propagate_object_local_stored(index),
			Node::Process(_) => self.try_propagate_process_local_stored(index),
		}
	}

	fn try_propagate_object_local_stored(&mut self, index: usize) -> Option<SmallVec<[usize; 1]>> {
		let (children, parents) = self.nodes.get_index(index).and_then(|(_, node)| {
			let node = node.try_unwrap_object_ref().ok()?;
			if node.local_stored.as_ref().is_some_and(|s| s.subtree) {
				return None;
			}
			let children = node.children.as_ref()?.clone();
			Some((children, node.parents.clone()))
		})?;

		let all_children_stored = children.iter().all(|child_index| {
			self.nodes
				.get_index(*child_index)
				.and_then(|(_, node)| node.try_unwrap_object_ref().ok()?.local_stored.as_ref())
				.is_some_and(|s| s.subtree)
		});
		if all_children_stored {
			if let Some((_, node)) = self.nodes.get_index_mut(index)
				&& let Ok(node) = node.try_unwrap_object_mut()
			{
				node.local_stored = Some(crate::object::stored::Output { subtree: true });
			}
			Some(parents)
		} else {
			None
		}
	}

	fn try_propagate_process_local_stored(&mut self, index: usize) -> Option<SmallVec<[usize; 1]>> {
		let (old_stored, children, objects, parents) =
			self.nodes.get_index(index).and_then(|(_, node)| {
				let node = node.try_unwrap_process_ref().ok()?;
				let children = node.children.clone().unwrap_or_default();
				let objects = node.objects.as_ref()?.clone();
				Some((
					node.local_stored.clone(),
					children,
					objects,
					node.parents.clone(),
				))
			})?;
		let new_stored = self.compute_process_local_stored(&children, &objects);
		let merged_stored = Self::merge_process_stored(old_stored.as_ref(), new_stored);
		if Self::should_propagate_process_stored(old_stored.as_ref(), Some(&merged_stored)) {
			if let Some((_, node)) = self.nodes.get_index_mut(index)
				&& let Ok(process) = node.try_unwrap_process_mut()
			{
				process.local_stored = Some(merged_stored);
			}
			Some(parents)
		} else {
			None
		}
	}

	fn should_propagate_process_stored(
		old: Option<&crate::process::stored::Output>,
		new: Option<&crate::process::stored::Output>,
	) -> bool {
		let Some(old) = old else {
			return new.is_some();
		};
		let Some(new) = new else {
			return false;
		};
		(!old.node_command && new.node_command)
			|| (!old.node_error && new.node_error)
			|| (!old.node_log && new.node_log)
			|| (!old.node_output && new.node_output)
			|| (!old.subtree && new.subtree)
			|| (!old.subtree_command && new.subtree_command)
			|| (!old.subtree_error && new.subtree_error)
			|| (!old.subtree_log && new.subtree_log)
			|| (!old.subtree_output && new.subtree_output)
	}

	fn merge_process_stored(
		old: Option<&crate::process::stored::Output>,
		new: crate::process::stored::Output,
	) -> crate::process::stored::Output {
		let Some(old) = old else {
			return new;
		};
		crate::process::stored::Output {
			subtree: old.subtree || new.subtree,
			subtree_command: old.subtree_command || new.subtree_command,
			subtree_error: old.subtree_error || new.subtree_error,
			subtree_log: old.subtree_log || new.subtree_log,
			subtree_output: old.subtree_output || new.subtree_output,
			node_command: old.node_command || new.node_command,
			node_error: old.node_error || new.node_error,
			node_log: old.node_log || new.node_log,
			node_output: old.node_output || new.node_output,
		}
	}

	fn find_object_remote_ancestor(
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
				Node::Object(object) => object
					.remote_stored
					.as_ref()
					.is_some_and(|stored| stored.subtree),
				Node::Process(process) => {
					process
						.remote_stored
						.as_ref()
						.is_some_and(|stored| match kind {
							Some(crate::sync::queue::ObjectKind::Command) => {
								stored.subtree_command || (path.len() == 2 && stored.node_command)
							},
							Some(crate::sync::queue::ObjectKind::Error) => {
								stored.subtree_error || (path.len() == 2 && stored.node_error)
							},
							Some(crate::sync::queue::ObjectKind::Log) => {
								stored.subtree_log || (path.len() == 2 && stored.node_log)
							},
							Some(crate::sync::queue::ObjectKind::Output) => {
								stored.subtree_output || (path.len() == 2 && stored.node_output)
							},
							None => stored.subtree_command && stored.subtree_output,
						})
				},
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

	fn find_process_remote_ancestor<F>(&self, index: usize, f: F) -> Option<Vec<usize>>
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
			let stored = node.remote_stored.as_ref().is_some_and(&f);
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

	fn propagate_process_remote_field<F>(&mut self, path: &[usize], f: F)
	where
		F: Fn(&mut crate::process::stored::Output),
	{
		for &index in path {
			let (_, node) = self.nodes.get_index_mut(index).unwrap();
			if let Node::Process(process) = node {
				let stored = process.remote_stored.get_or_insert_with(Default::default);
				f(stored);
			}
		}
	}
}

impl Node {
	pub fn parents(&self) -> &SmallVec<[usize; 1]> {
		match self {
			Node::Object(node) => &node.parents,
			Node::Process(node) => &node.parents,
		}
	}

	pub fn parents_mut(&mut self) -> &mut SmallVec<[usize; 1]> {
		match self {
			Node::Object(node) => &mut node.parents,
			Node::Process(node) => &mut node.parents,
		}
	}

	pub fn children(&self) -> Option<&Vec<usize>> {
		match self {
			Node::Object(node) => node.children.as_ref(),
			Node::Process(node) => node.children.as_ref(),
		}
	}

	pub fn children_mut(&mut self) -> &mut Option<Vec<usize>> {
		match self {
			Node::Object(node) => &mut node.children,
			Node::Process(node) => &mut node.children,
		}
	}

	pub fn marked(&self) -> bool {
		match self {
			Node::Object(node) => node.marked,
			Node::Process(node) => node.marked,
		}
	}
}

impl petgraph::visit::GraphBase for Graph {
	type EdgeId = (usize, usize);

	type NodeId = usize;
}

impl petgraph::visit::IntoNodeIdentifiers for &Graph {
	type NodeIdentifiers = std::ops::Range<Self::NodeId>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.nodes.len()
	}
}

impl petgraph::visit::NodeIndexable for Graph {
	fn node_bound(&self) -> usize {
		self.nodes.len()
	}

	fn to_index(&self, id: Self::NodeId) -> usize {
		id
	}

	fn from_index(&self, index: usize) -> Self::NodeId {
		index
	}
}

impl<'a> petgraph::visit::IntoNeighbors for &'a Graph {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;

	fn neighbors(self, id: Self::NodeId) -> Self::Neighbors {
		let (_, node) = self.nodes.get_index(id).unwrap();
		match &node {
			Node::Object(node) => node.children.iter().flatten().copied().boxed(),
			Node::Process(node) => std::iter::empty()
				.chain(node.children.iter().flatten())
				.chain(node.objects.iter().flatten().map(|(id, _)| id))
				.copied()
				.boxed(),
		}
	}
}

impl<'a> petgraph::visit::IntoNeighborsDirected for &'a Graph {
	type NeighborsDirected = Box<dyn Iterator<Item = usize> + 'a>;

	fn neighbors_directed(
		self,
		id: Self::NodeId,
		direction: petgraph::Direction,
	) -> Self::NeighborsDirected {
		match direction {
			petgraph::Direction::Outgoing => self.neighbors(id),
			petgraph::Direction::Incoming => {
				let (_, node) = self.nodes.get_index(id).unwrap();
				match node {
					Node::Object(node) => node.parents.iter().copied().boxed(),
					Node::Process(node) => node.parents.iter().copied().boxed(),
				}
			},
		}
	}
}

impl petgraph::visit::Visitable for Graph {
	type Map = HashSet<Self::NodeId>;

	fn visit_map(&self) -> Self::Map {
		HashSet::with_capacity(self.nodes.len())
	}

	fn reset_map(&self, map: &mut Self::Map) {
		map.clear();
		map.reserve(self.nodes.len());
	}
}
