use {
	indexmap::IndexMap,
	petgraph::visit::IntoNeighbors as _,
	smallvec::SmallVec,
	std::collections::{BTreeSet, HashMap, HashSet, VecDeque},
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Parent {
	Object(usize),
	Process(usize),
	ProcessObject {
		index: usize,
		kind: crate::sync::queue::ObjectKind,
	},
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct PermissionState {
	index: usize,
	permission: tg::grant::Permission,
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
	pub local_permissions: Option<tg::grant::permission::Set>,
	pub local_stored: Option<tangram_index::object::Stored>,
	pub local_visible: Option<tangram_index::object::Stored>,
	pub marked: bool,
	pub metadata: Option<tg::object::Metadata>,
	pub parents: SmallVec<[Parent; 1]>,
	pub remote_requested: bool,
	pub remote_stored: Option<tangram_index::object::Stored>,
	pub requested: Option<Requested>,
}

#[derive(Clone, Debug, Default)]
pub struct ProcessNode {
	pub children: Option<Vec<usize>>,
	pub data: Option<tg::process::Data>,
	pub local_permissions: Option<tg::grant::permission::Set>,
	pub local_stored: Option<tangram_index::process::Stored>,
	pub local_visible: Option<tangram_index::process::Stored>,
	pub marked: bool,
	pub metadata: Option<tg::process::Metadata>,
	pub objects: Option<Vec<(usize, tangram_index::process::object::Kind)>>,
	pub parents: SmallVec<[Parent; 1]>,
	pub remote_requested: bool,
	pub remote_stored: Option<tangram_index::process::Stored>,
	pub requested: Option<Requested>,
}

#[derive(Clone, Debug, Default)]
pub struct Requested {
	pub eager: bool,
}

pub struct UpdateObjectLocalArg<'a> {
	pub data: Option<&'a tg::object::Data>,
	pub id: &'a tg::object::Id,
	pub marked: Option<bool>,
	pub metadata: Option<tg::object::Metadata>,
	pub permissions: Option<tg::grant::permission::Set>,
	pub requested: Option<Requested>,
	pub stored: Option<tangram_index::object::Stored>,
}

pub struct UpdateProcessLocalArg<'a> {
	pub data: Option<&'a tg::process::Data>,
	pub id: &'a tg::process::Id,
	pub marked: Option<bool>,
	pub metadata: Option<tg::process::Metadata>,
	pub permissions: Option<tg::grant::permission::Set>,
	pub requested: Option<Requested>,
	pub stored: Option<tangram_index::process::Stored>,
}

// Remaining grant sync work: seed graph grant state from sent items and authorized touches, propagate grants separately from stored state, emit reduced grants in the final sync index batch, and add focused authorization, stored, and grant propagation tests.

impl Graph {
	pub fn new(
		local_roots: &[tg::MaybeWithToken<tg::Either<tg::object::Id, tg::process::Id>>],
		remote_roots: &[tg::MaybeWithToken<tg::Either<tg::object::Id, tg::process::Id>>],
	) -> Self {
		let local_roots = local_roots
			.iter()
			.map(|id| match id.as_ref().map_right(|id| &id.id).into_inner() {
				tg::Either::Left(id) => Id::Object(id.clone()),
				tg::Either::Right(id) => Id::Process(id.clone()),
			})
			.collect();
		let remote_roots = remote_roots
			.iter()
			.map(|id| match id.as_ref().map_right(|id| &id.id).into_inner() {
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

	pub fn update_object_local(&mut self, update: UpdateObjectLocalArg) {
		let UpdateObjectLocalArg {
			data,
			id,
			marked,
			metadata,
			permissions,
			requested,
			stored,
		} = update;
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
					let parent = Parent::Object(index);
					if !child_node.unwrap_object_ref().parents.contains(&parent) {
						child_node.unwrap_object_mut().parents.push(parent);
					}
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

		let old_stored = self.object_local_stored(index);
		let old_visible = self.object_local_visible(index);
		let computed_visible = children.as_ref().is_some_and(|children| {
			children
				.iter()
				.all(|index| self.object_local_visible(*index))
		});

		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_object_mut();

		if let Some(children) = children {
			node.children = Some(children);
			node.local_stored = Some(tangram_index::object::Stored {
				subtree: computed_stored.unwrap(),
			});
		}

		if let Some(stored) = stored {
			node.local_stored = Some(stored);
		}

		if let Some(permissions) = permissions {
			Self::merge_local_permissions(&mut node.local_permissions, permissions);
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

		let visible =
			Self::compute_object_visible(node.local_stored.as_ref(), node.local_permissions)
				|| (node
					.local_stored
					.as_ref()
					.is_some_and(|stored| stored.subtree)
					&& computed_visible);
		let visible = node
			.local_visible
			.as_ref()
			.is_some_and(|visible| visible.subtree)
			|| visible;
		node.local_visible = Some(tangram_index::object::Stored { subtree: visible });

		let new_stored = self.object_local_stored(index);
		let new_visible = self.object_local_visible(index);
		if (!old_stored && new_stored) || (!old_visible && new_visible) {
			let mut stack: Vec<usize> = self
				.nodes
				.get_index(index)
				.unwrap()
				.1
				.parents()
				.iter()
				.map(Parent::index)
				.collect();
			while let Some(parent_index) = stack.pop() {
				if let Some(parents) = self.try_propagate_local_stored(parent_index) {
					stack.extend(parents);
				}
			}
		}
	}

	pub fn update_process_local(&mut self, update: UpdateProcessLocalArg) {
		let UpdateProcessLocalArg {
			data,
			id,
			marked,
			metadata,
			permissions,
			requested,
			stored,
		} = update;
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_insert_with(|| Node::Process(ProcessNode::default()));

		let children = if let Some(data) = data {
			data.children.as_ref().map(|children| {
				children
					.iter()
					.map(|child| {
						let child = child.process.item.clone();
						let child_entry = self.nodes.entry(child.into());
						let child_index = child_entry.index();
						let child_node =
							child_entry.or_insert_with(|| Node::Process(ProcessNode::default()));
						let parent = Parent::Process(index);
						if !child_node.unwrap_process_ref().parents.contains(&parent) {
							child_node.unwrap_process_mut().parents.push(parent);
						}
						child_index
					})
					.collect::<Vec<_>>()
			})
		} else {
			None
		};

		let objects = if let Some(data) = data {
			let mut objects: Vec<(usize, tangram_index::process::object::Kind)> = Vec::new();

			let command: tg::object::Id = data.command.clone().into();
			let command_entry = self.nodes.entry(command.into());
			let command_index = command_entry.index();
			let command_node = command_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
			let parent = Parent::ProcessObject {
				index,
				kind: crate::sync::queue::ObjectKind::Command,
			};
			if !command_node.unwrap_object_ref().parents.contains(&parent) {
				command_node.unwrap_object_mut().parents.push(parent);
			}
			objects.push((command_index, tangram_index::process::object::Kind::Command));

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
							let parent = Parent::ProcessObject {
								index,
								kind: crate::sync::queue::ObjectKind::Error,
							};
							if !object_node.unwrap_object_ref().parents.contains(&parent) {
								object_node.unwrap_object_mut().parents.push(parent);
							}
							objects
								.push((object_index, tangram_index::process::object::Kind::Error));
						}
					},
					tg::Either::Right(error_id) => {
						let error_id = error_id.clone().map_right(|error| error.id).into_inner();
						let error_entry = self.nodes.entry(tg::object::Id::from(error_id).into());
						let error_index = error_entry.index();
						let error_node =
							error_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
						let parent = Parent::ProcessObject {
							index,
							kind: crate::sync::queue::ObjectKind::Error,
						};
						if !error_node.unwrap_object_ref().parents.contains(&parent) {
							error_node.unwrap_object_mut().parents.push(parent);
						}
						objects.push((error_index, tangram_index::process::object::Kind::Error));
					},
				}
			}

			if let Some(log) = data
				.log
				.clone()
				.map(|log| log.map_right(|log| log.id).into_inner())
			{
				let log_entry = self.nodes.entry(tg::object::Id::from(log).into());
				let log_index = log_entry.index();
				let log_node = log_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
				let parent = Parent::ProcessObject {
					index,
					kind: crate::sync::queue::ObjectKind::Log,
				};
				if !log_node.unwrap_object_ref().parents.contains(&parent) {
					log_node.unwrap_object_mut().parents.push(parent);
				}
				objects.push((log_index, tangram_index::process::object::Kind::Log));
			}

			if let Some(output) = &data.output {
				let mut output_children = BTreeSet::new();
				output.children(&mut output_children);
				for object_id in output_children {
					let object_entry = self.nodes.entry(object_id.into());
					let object_index = object_entry.index();
					let object_node =
						object_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
					let parent = Parent::ProcessObject {
						index,
						kind: crate::sync::queue::ObjectKind::Output,
					};
					if !object_node.unwrap_object_ref().parents.contains(&parent) {
						object_node.unwrap_object_mut().parents.push(parent);
					}
					objects.push((object_index, tangram_index::process::object::Kind::Output));
				}
			}

			Some(objects)
		} else {
			None
		};
		let node_old_stored = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_process_ref()
			.local_stored
			.clone();
		let node_old_visible = self.process_local_visible(index);

		let computed_stored = if let (Some(children), Some(objects)) = (&children, &objects) {
			Some(self.compute_process_local_stored(children, objects))
		} else {
			None
		};
		let computed_visible = if let (Some(children), Some(objects)) = (&children, &objects) {
			Some(self.compute_process_local_visible(children, objects))
		} else {
			None
		};

		{
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();

			if let Some(children) = children {
				node.children = Some(children);
			}

			if let Some(data) = data {
				node.data = Some(data.clone());
			}

			if let Some(stored) = stored {
				let merged = Self::merge_process_stored(node.local_stored.as_ref(), stored);
				node.local_stored = Some(merged);
			}

			if let Some(permissions) = permissions {
				Self::merge_local_permissions(&mut node.local_permissions, permissions);
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

			if let Some(computed_stored) = computed_stored {
				let merged_stored =
					Self::merge_process_stored(node.local_stored.as_ref(), computed_stored);
				node.local_stored = Some(merged_stored);
			}

			let visible_from_permissions = Self::compute_process_visible_from_permissions(
				node.local_stored.as_ref(),
				node.local_permissions,
			);
			let computed_visible = computed_visible
				.map_or(visible_from_permissions.clone(), |visible| {
					Self::merge_process_visible(Some(&visible_from_permissions), visible)
				});
			let merged_visible =
				Self::merge_process_visible(node.local_visible.as_ref(), computed_visible);
			node.local_visible = Some(merged_visible);
		}

		let node_new_stored = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_process_ref()
			.local_stored
			.clone();
		let node_new_visible = self.process_local_visible(index);
		if Self::should_propagate_process_stored(node_old_stored.as_ref(), node_new_stored.as_ref())
			|| Self::should_propagate_process_visible(
				Some(&node_old_visible),
				Some(&node_new_visible),
			) {
			let mut stack: Vec<usize> = self
				.nodes
				.get_index(index)
				.unwrap()
				.1
				.parents()
				.iter()
				.map(Parent::index)
				.collect();
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
		stored: Option<&tangram_index::object::Stored>,
	) -> (bool, Option<tangram_index::object::Stored>) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_insert_with(|| Node::Object(ObjectNode::default()));
		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_object_mut();
		let inserted = stored.is_none() && !node.remote_requested;
		if stored.is_none() {
			node.remote_requested = true;
		}

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
			let parent = match parent {
				Id::Object(_) => Parent::Object(parent_index),
				Id::Process(_) => Parent::ProcessObject {
					index: parent_index,
					kind: kind.unwrap(),
				},
			};

			// Add the node as a child of the parent.
			match parent_node {
				Node::Object(node) => {
					if let Some(children) = node.children.as_mut()
						&& !children.contains(&index)
					{
						children.push(index);
					}
				},
				Node::Process(node) => {
					let Parent::ProcessObject { kind, .. } = parent else {
						unreachable!();
					};
					let kind = Self::process_object_kind(kind);
					if let Some(objects) = node.objects.as_mut()
						&& !objects.iter().any(|(object, object_kind)| {
							*object == index
								&& std::mem::discriminant(object_kind)
									== std::mem::discriminant(&kind)
						}) {
						objects.push((index, kind));
					}
				},
			}

			// Add the parent to the node.
			let (_, node) = self.nodes.get_index_mut(index).unwrap();
			if !node.parents_mut().contains(&parent) {
				node.parents_mut().push(parent);
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
								Some(tangram_index::object::Stored { subtree: true });
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
		stored: Option<&tangram_index::process::Stored>,
	) -> (bool, Option<tangram_index::process::Stored>) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_insert_with(|| Node::Process(ProcessNode::default()));
		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_process_mut();
		let inserted = stored.is_none() && !node.remote_requested;
		if stored.is_none() {
			node.remote_requested = true;
		}

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
			let parent = Parent::Process(parent_index);

			// Add the node as a child of the parent.
			if let Some(children) = parent_node.children_mut().as_mut()
				&& !children.contains(&index)
			{
				children.push(index);
			}

			// Add the parent to the node.
			let (_, node) = self.nodes.get_index_mut(index).unwrap();
			if !node.parents_mut().contains(&parent) {
				node.parents_mut().push(parent);
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

		if let Some(path) = self.find_process_remote_ancestor(index, |stored| stored.subtree_error)
		{
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.remote_stored.get_or_insert_default().subtree_error = true;
			self.propagate_process_remote_field(&path, |stored| stored.subtree_error = true);
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.remote_stored.get_or_insert_default().node_error = true;
			self.propagate_process_remote_field(&path, |stored| stored.node_error = true);
		}

		if let Some(path) = self.find_process_remote_ancestor(index, |stored| stored.subtree_log) {
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.remote_stored.get_or_insert_default().subtree_log = true;
			self.propagate_process_remote_field(&path, |stored| stored.subtree_log = true);
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.remote_stored.get_or_insert_default().node_log = true;
			self.propagate_process_remote_field(&path, |stored| stored.node_log = true);
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
	) -> Option<&tangram_index::process::Stored> {
		self.nodes
			.get(&Id::Process(id.clone()))
			.and_then(|node| node.unwrap_process_ref().local_stored.as_ref())
	}

	pub fn get_process_local_visible(
		&self,
		id: &tg::process::Id,
	) -> tangram_index::process::Stored {
		self.nodes
			.get_index_of(&Id::Process(id.clone()))
			.map(|index| self.process_local_visible(index))
			.unwrap_or_default()
	}

	pub fn get_object_local_visible(&self, id: &tg::object::Id) -> tangram_index::object::Stored {
		tangram_index::object::Stored {
			subtree: self
				.nodes
				.get_index_of(&Id::Object(id.clone()))
				.is_some_and(|index| self.object_local_visible(index)),
		}
	}

	pub fn get_object_local_permissions(
		&mut self,
		id: &tg::object::Id,
		required: tg::grant::permission::Set,
	) -> tg::grant::permission::Set {
		let Some(index) = self.nodes.get_index_of(&Id::Object(id.clone())) else {
			return tg::grant::permission::Set::Object(tg::grant::permission::object::Set::empty());
		};
		self.get_local_permissions(index, required)
	}

	pub fn get_process_local_permissions(
		&mut self,
		id: &tg::process::Id,
		required: tg::grant::permission::Set,
	) -> tg::grant::permission::Set {
		let Some(index) = self.nodes.get_index_of(&Id::Process(id.clone())) else {
			return tg::grant::permission::Set::Process(
				tg::grant::permission::process::Set::empty(),
			);
		};
		self.get_local_permissions(index, required)
	}

	pub fn update_object_local_permissions(
		&mut self,
		id: &tg::object::Id,
		permissions: tg::grant::permission::Set,
	) {
		let permissions = Self::normalize_permissions(permissions);
		let update = UpdateObjectLocalArg {
			data: None,
			id,
			marked: None,
			metadata: None,
			permissions: Some(permissions),
			requested: None,
			stored: None,
		};
		self.update_object_local(update);
	}

	pub fn update_process_local_permissions(
		&mut self,
		id: &tg::process::Id,
		permissions: tg::grant::permission::Set,
	) {
		let permissions = Self::normalize_permissions(permissions);
		let update = UpdateProcessLocalArg {
			data: None,
			id,
			marked: None,
			metadata: None,
			permissions: Some(permissions),
			requested: None,
			stored: None,
		};
		self.update_process_local(update);
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
				Node::Object(_) => {
					self.object_local_visible(self.nodes.get_index_of(root).unwrap())
				},
				Node::Process(_) => {
					let visible =
						self.process_local_visible(self.nodes.get_index_of(root).unwrap());
					if arg.recursive {
						visible.subtree
							&& (!arg.commands || visible.subtree_command)
							&& (!arg.errors || visible.subtree_error)
							&& (!arg.logs || visible.subtree_log)
							&& (!arg.outputs || visible.subtree_output)
					} else {
						(!arg.commands || visible.node_command)
							&& (!arg.errors || visible.node_error)
							&& (!arg.logs || visible.node_log)
							&& (!arg.outputs || visible.node_output)
					}
				},
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

	fn get_local_permissions(
		&mut self,
		index: usize,
		required: tg::grant::permission::Set,
	) -> tg::grant::permission::Set {
		let permissions = self
			.nodes
			.get_index(index)
			.and_then(|(_, node)| node.local_permissions())
			.map_or_else(|| required.empty_like(), Self::normalize_permissions);
		if permissions.contains(required) {
			return permissions;
		}

		let mut predecessors = HashMap::new();
		let mut queue = VecDeque::new();
		let mut visited = HashSet::new();
		for permission in required
			.iter()
			.filter(|permission| !permissions.contains(*permission))
		{
			let state = PermissionState { index, permission };
			queue.push_back(state);
			visited.insert(state);
		}

		while let Some(state) = queue.pop_front() {
			let permissions = self
				.nodes
				.get_index(state.index)
				.and_then(|(_, node)| node.local_permissions())
				.map(Self::normalize_permissions);
			if permissions.is_some_and(|permissions| permissions.contains(state.permission)) {
				self.cache_local_permission_path(state, &predecessors);
				let permissions = self
					.nodes
					.get_index(index)
					.and_then(|(_, node)| node.local_permissions())
					.map_or_else(|| required.empty_like(), Self::normalize_permissions);
				if permissions.contains(required) {
					return permissions;
				}
				continue;
			}

			let parents = self.nodes.get_index(state.index).unwrap().1.parents();
			for &parent in parents {
				let Some(permission) = Self::parent_required_permission(parent, state.permission)
				else {
					continue;
				};
				let parent_state = PermissionState {
					index: parent.index(),
					permission,
				};
				if visited.insert(parent_state) {
					predecessors.insert(parent_state, (state, parent));
					queue.push_back(parent_state);
				}
			}
		}

		self.nodes
			.get_index(index)
			.and_then(|(_, node)| node.local_permissions())
			.map_or_else(|| required.empty_like(), Self::normalize_permissions)
	}

	fn cache_local_permission_path(
		&mut self,
		mut state: PermissionState,
		predecessors: &HashMap<PermissionState, (PermissionState, Parent)>,
	) {
		while let Some(&(child, parent)) = predecessors.get(&state) {
			let permission = Self::derive_child_permission(parent, state.permission);
			self.update_local_permission(child.index, permission);
			state = child;
		}
	}

	fn update_local_permission(&mut self, index: usize, permission: tg::grant::Permission) {
		let id = self.nodes.get_index(index).unwrap().0.clone();
		let permissions = tg::grant::permission::Set::from_permission(permission);
		match id {
			Id::Object(id) => self.update_object_local_permissions(&id, permissions),
			Id::Process(id) => self.update_process_local_permissions(&id, permissions),
		}
	}

	fn parent_required_permission(
		parent: Parent,
		permission: tg::grant::Permission,
	) -> Option<tg::grant::Permission> {
		match parent {
			Parent::Object(_) => match permission {
				tg::grant::Permission::Object(_) => Some(tg::grant::Permission::Object(
					tg::grant::permission::object::Permission::Subtree,
				)),
				_ => None,
			},
			Parent::Process(_) => match permission {
				tg::grant::Permission::Process(
					tg::grant::permission::process::Permission::Write,
				) => None,
				tg::grant::Permission::Process(permission) => {
					Some(tg::grant::Permission::Process(permission.to_subtree()))
				},
				_ => None,
			},
			Parent::ProcessObject { kind, .. } => match permission {
				tg::grant::Permission::Object(_) => Some(tg::grant::Permission::Process(
					Self::process_object_permission(kind),
				)),
				_ => None,
			},
		}
	}

	fn derive_child_permission(
		parent: Parent,
		permission: tg::grant::Permission,
	) -> tg::grant::Permission {
		match parent {
			Parent::Object(_) => match permission {
				tg::grant::Permission::Object(
					tg::grant::permission::object::Permission::Subtree,
				) => permission,
				_ => unreachable!(),
			},
			Parent::Process(_) => match permission {
				tg::grant::Permission::Process(
					tg::grant::permission::process::Permission::Subtree
					| tg::grant::permission::process::Permission::SubtreeCommand
					| tg::grant::permission::process::Permission::SubtreeError
					| tg::grant::permission::process::Permission::SubtreeLog
					| tg::grant::permission::process::Permission::SubtreeOutput,
				) => permission,
				_ => unreachable!(),
			},
			Parent::ProcessObject { .. } => {
				tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree)
			},
		}
	}

	fn normalize_permissions(
		mut permissions: tg::grant::permission::Set,
	) -> tg::grant::permission::Set {
		let implied = permissions
			.iter()
			.filter_map(|permission| match permission {
				tg::grant::Permission::Object(
					tg::grant::permission::object::Permission::Subtree,
				) => Some(tg::grant::Permission::Object(
					tg::grant::permission::object::Permission::Node,
				)),
				tg::grant::Permission::Process(permission) => match permission {
					tg::grant::permission::process::Permission::Subtree
					| tg::grant::permission::process::Permission::Write => Some(tg::grant::Permission::Process(
						tg::grant::permission::process::Permission::Node,
					)),
					tg::grant::permission::process::Permission::SubtreeCommand => {
						Some(tg::grant::Permission::Process(
							tg::grant::permission::process::Permission::NodeCommand,
						))
					},
					tg::grant::permission::process::Permission::SubtreeError => {
						Some(tg::grant::Permission::Process(
							tg::grant::permission::process::Permission::NodeError,
						))
					},
					tg::grant::permission::process::Permission::SubtreeLog => {
						Some(tg::grant::Permission::Process(
							tg::grant::permission::process::Permission::NodeLog,
						))
					},
					tg::grant::permission::process::Permission::SubtreeOutput => {
						Some(tg::grant::Permission::Process(
							tg::grant::permission::process::Permission::NodeOutput,
						))
					},
					_ => None,
				},
				_ => None,
			})
			.collect::<Vec<_>>();
		for permission in implied {
			permissions.insert(tg::grant::permission::Set::from_permission(permission));
		}
		permissions
	}

	fn process_object_kind(
		kind: crate::sync::queue::ObjectKind,
	) -> tangram_index::process::object::Kind {
		match kind {
			crate::sync::queue::ObjectKind::Command => {
				tangram_index::process::object::Kind::Command
			},
			crate::sync::queue::ObjectKind::Error => tangram_index::process::object::Kind::Error,
			crate::sync::queue::ObjectKind::Log => tangram_index::process::object::Kind::Log,
			crate::sync::queue::ObjectKind::Output => tangram_index::process::object::Kind::Output,
		}
	}

	fn process_object_permission(
		kind: crate::sync::queue::ObjectKind,
	) -> tg::grant::permission::process::Permission {
		match kind {
			crate::sync::queue::ObjectKind::Command => {
				tg::grant::permission::process::Permission::NodeCommand
			},
			crate::sync::queue::ObjectKind::Error => {
				tg::grant::permission::process::Permission::NodeError
			},
			crate::sync::queue::ObjectKind::Log => {
				tg::grant::permission::process::Permission::NodeLog
			},
			crate::sync::queue::ObjectKind::Output => {
				tg::grant::permission::process::Permission::NodeOutput
			},
		}
	}

	fn compute_process_local_stored(
		&self,
		children: &[usize],
		objects: &[(usize, tangram_index::process::object::Kind)],
	) -> tangram_index::process::Stored {
		let mut stored = tangram_index::process::Stored {
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
				stored.subtree_error = stored.subtree_error && child_stored.subtree_error;
				stored.subtree_log = stored.subtree_log && child_stored.subtree_log;
				stored.subtree_output = stored.subtree_output && child_stored.subtree_output;
			} else {
				stored.subtree = false;
				stored.subtree_command = false;
				stored.subtree_error = false;
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
				tangram_index::process::object::Kind::Command => {
					stored.node_command = stored.node_command && object_stored;
					stored.subtree_command = stored.subtree_command && object_stored;
				},
				tangram_index::process::object::Kind::Error => {
					stored.node_error = stored.node_error && object_stored;
					stored.subtree_error = stored.subtree_error && object_stored;
				},
				tangram_index::process::object::Kind::Log => {
					stored.node_log = stored.node_log && object_stored;
					stored.subtree_log = stored.subtree_log && object_stored;
				},
				tangram_index::process::object::Kind::Output => {
					stored.node_output = stored.node_output && object_stored;
					stored.subtree_output = stored.subtree_output && object_stored;
				},
			}
		}
		stored
	}

	fn compute_process_local_visible(
		&self,
		children: &[usize],
		objects: &[(usize, tangram_index::process::object::Kind)],
	) -> tangram_index::process::Stored {
		let mut visible = tangram_index::process::Stored {
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
			let child_visible = self.process_local_visible(*child_index);
			visible.subtree = visible.subtree && child_visible.subtree;
			visible.subtree_command = visible.subtree_command && child_visible.subtree_command;
			visible.subtree_error = visible.subtree_error && child_visible.subtree_error;
			visible.subtree_log = visible.subtree_log && child_visible.subtree_log;
			visible.subtree_output = visible.subtree_output && child_visible.subtree_output;
		}
		for (object_index, object_kind) in objects {
			let object_visible = self.object_local_visible(*object_index);
			match object_kind {
				tangram_index::process::object::Kind::Command => {
					visible.node_command = visible.node_command && object_visible;
					visible.subtree_command = visible.subtree_command && object_visible;
				},
				tangram_index::process::object::Kind::Error => {
					visible.node_error = visible.node_error && object_visible;
					visible.subtree_error = visible.subtree_error && object_visible;
				},
				tangram_index::process::object::Kind::Log => {
					visible.node_log = visible.node_log && object_visible;
					visible.subtree_log = visible.subtree_log && object_visible;
				},
				tangram_index::process::object::Kind::Output => {
					visible.node_output = visible.node_output && object_visible;
					visible.subtree_output = visible.subtree_output && object_visible;
				},
			}
		}
		visible
	}

	fn object_local_stored(&self, index: usize) -> bool {
		self.nodes
			.get_index(index)
			.and_then(|(_, node)| node.try_unwrap_object_ref().ok()?.local_stored.as_ref())
			.is_some_and(|stored| stored.subtree)
	}

	fn object_local_visible(&self, index: usize) -> bool {
		self.nodes
			.get_index(index)
			.and_then(|(_, node)| node.try_unwrap_object_ref().ok()?.local_visible.as_ref())
			.is_some_and(|visible| visible.subtree)
	}

	fn process_local_visible(&self, index: usize) -> tangram_index::process::Stored {
		self.nodes
			.get_index(index)
			.and_then(|(_, node)| node.try_unwrap_process_ref().ok()?.local_visible.clone())
			.unwrap_or_default()
	}

	fn compute_object_visible(
		stored: Option<&tangram_index::object::Stored>,
		permissions: Option<tg::grant::permission::Set>,
	) -> bool {
		stored.is_some_and(|stored| stored.subtree)
			&& permissions.is_some_and(|permissions| {
				permissions.contains(tg::grant::Permission::Object(
					tg::grant::permission::object::Permission::Subtree,
				))
			})
	}

	fn compute_process_visible_from_permissions(
		stored: Option<&tangram_index::process::Stored>,
		permissions: Option<tg::grant::permission::Set>,
	) -> tangram_index::process::Stored {
		let Some(stored) = stored else {
			return tangram_index::process::Stored::default();
		};
		tangram_index::process::Stored {
			node_command: stored.node_command
				&& Self::contains_process_permission(
					permissions,
					tg::grant::permission::process::Permission::NodeCommand,
				),
			node_error: stored.node_error
				&& Self::contains_process_permission(
					permissions,
					tg::grant::permission::process::Permission::NodeError,
				),
			node_log: stored.node_log
				&& Self::contains_process_permission(
					permissions,
					tg::grant::permission::process::Permission::NodeLog,
				),
			node_output: stored.node_output
				&& Self::contains_process_permission(
					permissions,
					tg::grant::permission::process::Permission::NodeOutput,
				),
			subtree: stored.subtree
				&& Self::contains_process_permission(
					permissions,
					tg::grant::permission::process::Permission::Subtree,
				),
			subtree_command: stored.subtree_command
				&& Self::contains_process_permission(
					permissions,
					tg::grant::permission::process::Permission::SubtreeCommand,
				),
			subtree_error: stored.subtree_error
				&& Self::contains_process_permission(
					permissions,
					tg::grant::permission::process::Permission::SubtreeError,
				),
			subtree_log: stored.subtree_log
				&& Self::contains_process_permission(
					permissions,
					tg::grant::permission::process::Permission::SubtreeLog,
				),
			subtree_output: stored.subtree_output
				&& Self::contains_process_permission(
					permissions,
					tg::grant::permission::process::Permission::SubtreeOutput,
				),
		}
	}

	fn contains_process_permission(
		permissions: Option<tg::grant::permission::Set>,
		permission: tg::grant::permission::process::Permission,
	) -> bool {
		permissions.is_some_and(|permissions| {
			permissions.contains(tg::grant::Permission::Process(permission))
		})
	}

	pub fn object_permissions_for_stored(
		stored: &tangram_index::object::Stored,
	) -> Option<tg::grant::permission::Set> {
		stored.subtree.then(|| {
			tg::grant::permission::Set::from_permission(tg::grant::Permission::Object(
				tg::grant::permission::object::Permission::Subtree,
			))
		})
	}

	pub fn process_permissions_for_stored(
		stored: &tangram_index::process::Stored,
	) -> Option<tg::grant::permission::Set> {
		let mut permissions =
			tg::grant::permission::Set::Process(tg::grant::permission::process::Set::empty());
		let mut insert = |permission| {
			permissions.insert(tg::grant::permission::Set::from_permission(
				tg::grant::Permission::Process(permission),
			));
		};
		if stored.node_command {
			insert(tg::grant::permission::process::Permission::NodeCommand);
		}
		if stored.node_error {
			insert(tg::grant::permission::process::Permission::NodeError);
		}
		if stored.node_log {
			insert(tg::grant::permission::process::Permission::NodeLog);
		}
		if stored.node_output {
			insert(tg::grant::permission::process::Permission::NodeOutput);
		}
		if stored.subtree {
			insert(tg::grant::permission::process::Permission::Subtree);
		}
		if stored.subtree_command {
			insert(tg::grant::permission::process::Permission::SubtreeCommand);
		}
		if stored.subtree_error {
			insert(tg::grant::permission::process::Permission::SubtreeError);
		}
		if stored.subtree_log {
			insert(tg::grant::permission::process::Permission::SubtreeLog);
		}
		if stored.subtree_output {
			insert(tg::grant::permission::process::Permission::SubtreeOutput);
		}
		(!permissions.is_empty()).then_some(permissions)
	}

	pub fn process_visible_any(visible: &tangram_index::process::Stored) -> bool {
		visible.node_command
			|| visible.node_error
			|| visible.node_log
			|| visible.node_output
			|| visible.subtree
			|| visible.subtree_command
			|| visible.subtree_error
			|| visible.subtree_log
			|| visible.subtree_output
	}

	fn merge_local_permissions(
		existing: &mut Option<tg::grant::permission::Set>,
		permissions: tg::grant::permission::Set,
	) {
		if permissions.is_empty() {
			return;
		}
		match existing {
			Some(existing) if existing.same_kind(permissions) => existing.insert(permissions),
			Some(_) | None => *existing = Some(permissions),
		}
	}

	fn try_propagate_local_stored(&mut self, index: usize) -> Option<SmallVec<[usize; 1]>> {
		let (_, node) = self.nodes.get_index(index)?;
		match node {
			Node::Object(_) => self.try_propagate_object_local_stored(index),
			Node::Process(_) => self.try_propagate_process_local_stored(index),
		}
	}

	fn try_propagate_object_local_stored(&mut self, index: usize) -> Option<SmallVec<[usize; 1]>> {
		let (old_stored, old_visible, children, parents) =
			self.nodes.get_index(index).and_then(|(_, node)| {
				let node = node.try_unwrap_object_ref().ok()?;
				if node
					.local_stored
					.as_ref()
					.is_some_and(|stored| stored.subtree)
					&& node
						.local_visible
						.as_ref()
						.is_some_and(|visible| visible.subtree)
				{
					return None;
				}
				let children = node.children.as_ref()?.clone();
				Some((
					self.object_local_stored(index),
					self.object_local_visible(index),
					children,
					node.parents.iter().map(Parent::index).collect(),
				))
			})?;

		let all_children_stored = children.iter().all(|child_index| {
			self.nodes
				.get_index(*child_index)
				.and_then(|(_, node)| node.try_unwrap_object_ref().ok()?.local_stored.as_ref())
				.is_some_and(|s| s.subtree)
		});
		if all_children_stored
			&& let Some((_, node)) = self.nodes.get_index_mut(index)
			&& let Ok(node) = node.try_unwrap_object_mut()
		{
			node.local_stored = Some(tangram_index::object::Stored { subtree: true });
		}

		let all_children_visible = children
			.iter()
			.all(|child_index| self.object_local_visible(*child_index));
		if self.object_local_stored(index)
			&& all_children_visible
			&& let Some((_, node)) = self.nodes.get_index_mut(index)
			&& let Ok(node) = node.try_unwrap_object_mut()
		{
			node.local_visible = Some(tangram_index::object::Stored { subtree: true });
		}

		let new_stored = self.object_local_stored(index);
		let new_visible = self.object_local_visible(index);
		((!old_stored && new_stored) || (!old_visible && new_visible)).then_some(parents)
	}

	fn try_propagate_process_local_stored(&mut self, index: usize) -> Option<SmallVec<[usize; 1]>> {
		let (old_stored, old_visible, children, objects, parents) =
			self.nodes.get_index(index).and_then(|(_, node)| {
				let node = node.try_unwrap_process_ref().ok()?;
				let children = node.children.clone().unwrap_or_default();
				let objects = node.objects.as_ref()?.clone();
				Some((
					node.local_stored.clone(),
					self.process_local_visible(index),
					children,
					objects,
					node.parents.iter().map(Parent::index).collect(),
				))
			})?;
		let new_stored = self.compute_process_local_stored(&children, &objects);
		let merged_stored = Self::merge_process_stored(old_stored.as_ref(), new_stored);
		let stored_improved =
			Self::should_propagate_process_stored(old_stored.as_ref(), Some(&merged_stored));

		let new_visible = self.compute_process_local_visible(&children, &objects);
		let merged_visible = self
			.nodes
			.get_index(index)
			.and_then(|(_, node)| node.try_unwrap_process_ref().ok()?.local_visible.as_ref())
			.map_or(new_visible.clone(), |old| {
				Self::merge_process_visible(Some(old), new_visible)
			});
		let visible_improved =
			Self::should_propagate_process_visible(Some(&old_visible), Some(&merged_visible));

		if stored_improved
			&& let Some((_, node)) = self.nodes.get_index_mut(index)
			&& let Ok(process) = node.try_unwrap_process_mut()
		{
			process.local_stored = Some(merged_stored);
		}

		if visible_improved
			&& let Some((_, node)) = self.nodes.get_index_mut(index)
			&& let Ok(process) = node.try_unwrap_process_mut()
		{
			process.local_visible = Some(merged_visible);
		}

		if stored_improved || visible_improved {
			return Some(parents);
		}
		None
	}

	fn should_propagate_process_stored(
		old: Option<&tangram_index::process::Stored>,
		new: Option<&tangram_index::process::Stored>,
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

	fn should_propagate_process_visible(
		old: Option<&tangram_index::process::Stored>,
		new: Option<&tangram_index::process::Stored>,
	) -> bool {
		Self::should_propagate_process_stored(old, new)
	}

	fn merge_process_stored(
		old: Option<&tangram_index::process::Stored>,
		new: tangram_index::process::Stored,
	) -> tangram_index::process::Stored {
		let Some(old) = old else {
			return new;
		};
		tangram_index::process::Stored {
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

	fn merge_process_visible(
		old: Option<&tangram_index::process::Stored>,
		new: tangram_index::process::Stored,
	) -> tangram_index::process::Stored {
		Self::merge_process_stored(old, new)
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
				let parent = parent.index();
				let mut path = path.clone();
				assert!(!path.contains(&parent));
				path.push(parent);
				stack.push(path);
			}
		}
	}

	fn find_process_remote_ancestor<F>(&self, index: usize, f: F) -> Option<Vec<usize>>
	where
		F: Fn(&tangram_index::process::Stored) -> bool,
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
				let parent = parent.index();
				let mut path = path.clone();
				assert!(!path.contains(&parent));
				path.push(parent);
				stack.push(path);
			}
		}
	}

	fn propagate_process_remote_field<F>(&mut self, path: &[usize], f: F)
	where
		F: Fn(&mut tangram_index::process::Stored),
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

impl Parent {
	#[must_use]
	pub fn index(&self) -> usize {
		match self {
			Self::Object(index) | Self::Process(index) | Self::ProcessObject { index, .. } => {
				*index
			},
		}
	}
}

impl Node {
	fn local_permissions(&self) -> Option<tg::grant::permission::Set> {
		match self {
			Self::Object(node) => node.local_permissions,
			Self::Process(node) => node.local_permissions,
		}
	}

	pub fn parents(&self) -> &SmallVec<[Parent; 1]> {
		match self {
			Node::Object(node) => &node.parents,
			Node::Process(node) => &node.parents,
		}
	}

	pub fn parents_mut(&mut self) -> &mut SmallVec<[Parent; 1]> {
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
					Node::Object(node) => node.parents.iter().map(Parent::index).boxed(),
					Node::Process(node) => node.parents.iter().map(Parent::index).boxed(),
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
