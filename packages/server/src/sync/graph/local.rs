use {
	super::{
		Graph, Node, Parent,
		state::{Aspects, Facts, Published},
	},
	tangram_client::prelude::*,
};

impl Graph {
	pub(super) fn insert_local_edge(&mut self, parent: Parent, child: usize) {
		let node = self.nodes.get_index_mut(child).unwrap().1;
		if !node.parents_mut().insert(parent) {
			return;
		}
		if matches!(parent, Parent::Node(_)) {
			return;
		}

		// Account for facts already published by the child; pending changes will follow through the queue.
		let facts = Self::published_local_facts(node);
		self.update_local_dependency(parent, None, &facts);
		let permissions = self
			.nodes
			.get_index(parent.index())
			.unwrap()
			.1
			.local_permissions();
		if let Some(permissions) = permissions {
			self.inherit_local_permissions(parent, child, permissions);
		}
	}

	#[must_use]
	fn published_local_facts(node: &Node) -> Published {
		match node {
			Node::Object(node) => Published {
				node: node.state.propagated.clone(),
				objects: Aspects::default(),
			},
			Node::Process(node) => node.state.propagated.clone(),
			_ => unreachable!(),
		}
	}

	fn update_local_dependency(
		&mut self,
		parent: Parent,
		old: Option<&Published>,
		new: &Published,
	) {
		let node = self.nodes.get_index_mut(parent.index()).unwrap().1;
		let update =
			|dependencies: &mut super::state::Dependencies, old: Option<&Facts>, new: &Facts| {
				if let Some(old) = old {
					dependencies.update(old, new);
				} else {
					dependencies.insert(new);
				}
			};
		match parent {
			Parent::Node(_) => return,
			Parent::Object(_) => update(
				&mut node.unwrap_object_mut().state.dependencies,
				old.map(|facts| &facts.node),
				&new.node,
			),
			Parent::Process(_) => {
				let state = &mut node.unwrap_process_mut().state;
				update(&mut state.children, old.map(|facts| &facts.node), &new.node);
				update(
					&mut state.subtree_objects.command,
					old.map(|facts| &facts.objects.command),
					&new.objects.command,
				);
				update(
					&mut state.subtree_objects.error,
					old.map(|facts| &facts.objects.error),
					&new.objects.error,
				);
				update(
					&mut state.subtree_objects.log,
					old.map(|facts| &facts.objects.log),
					&new.objects.log,
				);
				update(
					&mut state.subtree_objects.output,
					old.map(|facts| &facts.objects.output),
					&new.objects.output,
				);
			},
			Parent::ProcessObject { kind, .. } => {
				let state = &mut node.unwrap_process_mut().state;
				update(
					state.objects.aspect_mut(kind),
					old.map(|facts| &facts.node),
					&new.node,
				);
				update(
					state.subtree_objects.aspect_mut(kind),
					old.map(|facts| &facts.node),
					&new.node,
				);
			},
		}
		self.queue_local(parent.index());
	}

	pub(super) fn queue_local(&mut self, index: usize) {
		if self.local_queued.insert(index) {
			self.local_queue.push_back(index);
		}
	}

	fn inherit_local_permissions(
		&mut self,
		parent: Parent,
		child: usize,
		permissions: tg::authorization::permission::Set,
	) {
		let mut inherited = permissions.empty_like();
		for permission in permissions.iter() {
			let inheritable = match (parent, permission) {
				(
					Parent::Object(_),
					tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					),
				) => true,
				(Parent::Process(_), tg::authorization::Permission::Process(permission)) => {
					permission == permission.to_subtree()
				},
				_ => false,
			};
			if inheritable {
				inherited.insert(tg::authorization::permission::Set::from_permission(
					permission,
				));
			}
		}
		if inherited.is_empty() {
			return;
		}
		let node = self.nodes.get_index_mut(child).unwrap().1;
		let permissions = match node {
			Node::Object(node) => &mut node.local_permissions,
			Node::Process(node) => &mut node.local_permissions,
			_ => unreachable!(),
		};
		let previous = *permissions;
		Self::merge_local_permissions(permissions, inherited);
		if *permissions != previous {
			self.queue_local(child);
		}
	}

	pub(super) fn propagate_local(&mut self) {
		while let Some(index) = self.local_queue.pop_front() {
			self.local_queued.remove(&index);
			let node = self.nodes.get_index_mut(index).unwrap().1;
			let old = Self::published_local_facts(node);
			Self::compute_local_state(node);
			Self::publish_local_facts(node);
			let new = Self::published_local_facts(node);
			let permissions = node.local_permissions();
			let (children, propagated_permissions) = match node {
				Node::Object(node) => (
					&node.remote_children,
					&mut node.state.propagated_permissions,
				),
				Node::Process(node) => (
					&node.remote_children,
					&mut node.state.propagated_permissions,
				),
				_ => unreachable!(),
			};
			let permissions = permissions.and_then(|permissions| {
				let mut change = permissions;
				if let Some(previous) = *propagated_permissions {
					change.remove(previous);
				}
				*propagated_permissions = Some(permissions);
				(!change.is_empty()).then_some(change)
			});
			let children = permissions.map(|_| children.iter().copied().collect::<Vec<_>>());
			let parents = (old != new).then(|| node.parents().iter().copied().collect::<Vec<_>>());

			// Send each new permission down and each newly established aggregate fact up.
			if let Some(permissions) = permissions {
				let parent = match self.nodes.get_index(index).unwrap().1 {
					Node::Object(_) => Parent::Object(index),
					Node::Process(_) => Parent::Process(index),
					_ => unreachable!(),
				};
				for child in children.unwrap() {
					self.inherit_local_permissions(parent, child, permissions);
				}
			}
			if let Some(parents) = parents {
				for parent in parents {
					self.update_local_dependency(parent, Some(&old), &new);
				}
			}
			self.update_local_end(index);
		}
	}

	fn compute_local_state(node: &mut Node) {
		match node {
			Node::Object(node) => {
				let children = node.state.dependencies.facts();
				if node.children.is_some() {
					if node.marked || node.local_storage.is_some() {
						node.local_storage.get_or_insert_default().subtree |= children.storage;
					}
					if let Some(metadata) = &mut node.metadata {
						let subtree = tg::object::metadata::Subtree {
							count: children.metadata.count.map(|count| count + 1),
							depth: children.metadata.depth.map(|depth| depth + 1),
							size: children.metadata.size.map(|size| size + metadata.node.size),
							solvable: children
								.metadata
								.solvable
								.map(|solvable| solvable || metadata.node.solvable),
							solved: children
								.metadata
								.solved
								.map(|solved| solved && metadata.node.solved),
						};
						metadata.subtree.merge(&subtree);
					}
				}
				let availability = Self::compute_object_availability(
					node.local_storage.as_ref(),
					node.local_permissions,
				) || (node.children.is_some()
					&& children.availability
					&& node
						.local_storage
						.as_ref()
						.is_some_and(|storage| storage.subtree));
				node.local_availability.get_or_insert_default().subtree |= availability;
			},
			Node::Process(node) => {
				if node.objects.is_some() {
					let children_known = node.children.is_some();
					let children = node.state.children.facts();
					let mut objects = node.state.objects.map(super::state::Dependencies::facts);
					let mut subtree_objects = node
						.state
						.subtree_objects
						.map(super::state::Dependencies::facts);
					// The log metadata remains unknown until compaction; storage covers only existing object references.
					if node
						.data
						.as_ref()
						.is_some_and(crate::Session::process_log_needs_compaction)
					{
						objects.log.metadata = tg::object::metadata::Subtree::default();
						subtree_objects.log.metadata = tg::object::metadata::Subtree::default();
					}
					let storage = tangram_index::process::Storage {
						node_command: objects.command.storage,
						node_error: objects.error.storage,
						node_log: objects.log.storage,
						node_output: objects.output.storage,
						subtree: children_known && children.storage,
						subtree_command: children_known && subtree_objects.command.storage,
						subtree_error: children_known && subtree_objects.error.storage,
						subtree_log: children_known && subtree_objects.log.storage,
						subtree_output: children_known && subtree_objects.output.storage,
					};
					node.local_storage.get_or_insert_default().merge(&storage);
					let availability = tg::process::Availability {
						node_command: objects.command.availability,
						node_error: objects.error.availability,
						node_log: objects.log.availability,
						node_output: objects.output.availability,
						subtree: children_known && children.availability,
						subtree_command: children_known && subtree_objects.command.availability,
						subtree_error: children_known && subtree_objects.error.availability,
						subtree_log: children_known && subtree_objects.log.availability,
						subtree_output: children_known && subtree_objects.output.availability,
					};
					node.local_availability
						.get_or_insert_default()
						.merge(&availability);
					// The process metadata aggregates numeric object facts; solvability belongs to objects.
					let metadata = |facts: &Facts| tg::object::metadata::Subtree {
						count: facts.metadata.count,
						depth: facts.metadata.depth,
						size: facts.metadata.size,
						solvable: None,
						solved: None,
					};
					let metadata = tg::process::Metadata {
						node: tg::process::metadata::Node {
							command: metadata(&objects.command),
							error: metadata(&objects.error),
							log: metadata(&objects.log),
							output: metadata(&objects.output),
						},
						subtree: if children_known {
							tg::process::metadata::Subtree {
								command: metadata(&subtree_objects.command),
								count: children.metadata.count.map(|count| count + 1),
								depth: Some(1),
								error: metadata(&subtree_objects.error),
								log: metadata(&subtree_objects.log),
								output: metadata(&subtree_objects.output),
							}
						} else {
							tg::process::metadata::Subtree::default()
						},
					};
					node.metadata.get_or_insert_default().merge(&metadata);
				}
				let availability = Self::compute_process_availability_from_permissions(
					node.local_storage.as_ref(),
					node.local_permissions,
				);
				node.local_availability
					.get_or_insert_default()
					.merge(&availability);
			},
			_ => unreachable!(),
		}
	}
	fn publish_local_facts(node: &mut Node) {
		match node {
			Node::Object(node) => {
				let facts = Facts {
					availability: node
						.local_availability
						.as_ref()
						.is_some_and(|availability| availability.subtree),
					metadata: node
						.metadata
						.as_ref()
						.map(|metadata| metadata.subtree.clone())
						.unwrap_or_default(),
					storage: node
						.local_storage
						.as_ref()
						.is_some_and(|storage| storage.subtree),
				};
				node.state.propagated = facts;
			},
			Node::Process(node) => {
				let availability = node.local_availability.clone().unwrap_or_default();
				let storage = node.local_storage.clone().unwrap_or_default();
				let metadata = node.metadata.clone().unwrap_or_default();
				let core = Facts {
					availability: availability.subtree,
					metadata: tg::object::metadata::Subtree {
						count: metadata.subtree.count,
						..Default::default()
					},
					storage: storage.subtree,
				};
				let objects = Aspects {
					command: Facts {
						availability: availability.subtree_command,
						metadata: metadata.subtree.command,
						storage: storage.subtree_command,
					},
					error: Facts {
						availability: availability.subtree_error,
						metadata: metadata.subtree.error,
						storage: storage.subtree_error,
					},
					log: Facts {
						availability: availability.subtree_log,
						metadata: metadata.subtree.log,
						storage: storage.subtree_log,
					},
					output: Facts {
						availability: availability.subtree_output,
						metadata: metadata.subtree.output,
						storage: storage.subtree_output,
					},
				};
				node.state.propagated = Published {
					node: core,
					objects,
				};
			},
			_ => unreachable!(),
		}
	}
}
