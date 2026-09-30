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
			self.queue_local(child);
			return;
		}

		// Account for facts already published by the child; pending changes will follow through the queue.
		let facts = Self::published_local_facts(node);
		self.queue_local(child);
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
					&mut state.subtree_objects.command_objects,
					old.map(|facts| &facts.objects.command_objects),
					&new.objects.command_objects,
				);
				update(
					&mut state.subtree_objects.error_objects,
					old.map(|facts| &facts.objects.error_objects),
					&new.objects.error_objects,
				);
				update(
					&mut state.subtree_objects.log_objects,
					old.map(|facts| &facts.objects.log_objects),
					&new.objects.log_objects,
				);
				update(
					&mut state.subtree_objects.output_objects,
					old.map(|facts| &facts.objects.output_objects),
					&new.objects.output_objects,
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
		let mut changed = std::collections::BTreeSet::new();
		while let Some(index) = self.local_queue.pop_front() {
			self.local_queued.remove(&index);
			if self.control.is_some() {
				changed.insert(self.nodes.get_index(index).unwrap().0.clone());
			}
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
		if let Some(control) = self
			.control
			.as_ref()
			.and_then(tokio::sync::mpsc::WeakUnboundedSender::upgrade)
			&& !changed.is_empty()
		{
			control
				.send(super::super::control::Event::Nodes(
					changed.into_iter().collect(),
				))
				.ok();
		}
	}

	fn compute_local_state(node: &mut Node) {
		match node {
			Node::Object(node) => {
				let children = node.state.dependencies.facts();
				if node.children.is_some() {
					if let Some(storage) = &mut node.local_storage {
						storage.subtree |= children.storage;
					}
					if children.permissions
						&& node.local_permissions.is_some_and(|permissions| {
							permissions.contains(tg::authorization::permission::Set::Object(
								tg::authorization::permission::object::Set::NODE,
							))
						}) {
						Self::merge_local_permissions(
							&mut node.local_permissions,
							tg::authorization::permission::Set::Object(
								tg::authorization::permission::object::Set::SUBTREE,
							),
						);
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
				);
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
						objects.log_objects.metadata = tg::object::metadata::Subtree::default();
						subtree_objects.log_objects.metadata =
							tg::object::metadata::Subtree::default();
					}
					let storage = tangram_index::process::Storage {
						node_command_objects: objects.command_objects.storage,
						node_error_objects: objects.error_objects.storage,
						node_log_objects: objects.log_objects.storage,
						node_output_objects: objects.output_objects.storage,
						subtree: children_known && children.storage,
						subtree_command_objects: children_known
							&& subtree_objects.command_objects.storage,
						subtree_error_objects: children_known
							&& subtree_objects.error_objects.storage,
						subtree_log_objects: children_known && subtree_objects.log_objects.storage,
						subtree_output_objects: children_known
							&& subtree_objects.output_objects.storage,
					};
					if let Some(local_storage) = &mut node.local_storage {
						local_storage.merge(&storage);
					}
					if Self::contains_process_permission(
						node.local_permissions,
						tg::authorization::permission::process::Permission::Node,
					) {
						use tg::authorization::permission::process::Set;
						let mut permissions = Set::empty();
						for (proven, permission) in [
							(
								objects.command_objects.permissions,
								Set::NODE_COMMAND_OBJECTS,
							),
							(objects.error_objects.permissions, Set::NODE_ERROR_OBJECTS),
							(objects.log_objects.permissions, Set::NODE_LOG_OBJECTS),
							(objects.output_objects.permissions, Set::NODE_OUTPUT_OBJECTS),
							(children_known && children.permissions, Set::SUBTREE),
							(
								children_known && subtree_objects.command_objects.permissions,
								Set::SUBTREE_COMMAND_OBJECTS,
							),
							(
								children_known && subtree_objects.error_objects.permissions,
								Set::SUBTREE_ERROR_OBJECTS,
							),
							(
								children_known && subtree_objects.log_objects.permissions,
								Set::SUBTREE_LOG_OBJECTS,
							),
							(
								children_known && subtree_objects.output_objects.permissions,
								Set::SUBTREE_OUTPUT_OBJECTS,
							),
						] {
							if proven {
								permissions.insert(permission);
							}
						}
						Self::merge_local_permissions(
							&mut node.local_permissions,
							tg::authorization::permission::Set::Process(permissions),
						);
					}
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
							command_objects: metadata(&objects.command_objects),
							error_objects: metadata(&objects.error_objects),
							log_objects: metadata(&objects.log_objects),
							output_objects: metadata(&objects.output_objects),
						},
						subtree: if children_known {
							tg::process::metadata::Subtree {
								command_objects: metadata(&subtree_objects.command_objects),
								count: children.metadata.count.map(|count| count + 1),
								depth: Some(1),
								error_objects: metadata(&subtree_objects.error_objects),
								log_objects: metadata(&subtree_objects.log_objects),
								output_objects: metadata(&subtree_objects.output_objects),
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
					metadata: node
						.metadata
						.as_ref()
						.map(|metadata| metadata.subtree.clone())
						.unwrap_or_default(),
					permissions: node.local_permissions.is_some_and(|permissions| {
						permissions.contains(tg::authorization::permission::Set::Object(
							tg::authorization::permission::object::Set::SUBTREE,
						))
					}),
					storage: node
						.local_storage
						.as_ref()
						.is_some_and(|storage| storage.subtree),
				};
				node.state.propagated = facts;
			},
			Node::Process(node) => {
				let storage = node.local_storage.clone().unwrap_or_default();
				let metadata = node.metadata.clone().unwrap_or_default();
				let core = Facts {
					metadata: tg::object::metadata::Subtree {
						count: metadata.subtree.count,
						..Default::default()
					},
					permissions: Self::contains_process_permission(
						node.local_permissions,
						tg::authorization::permission::process::Permission::Subtree,
					),
					storage: storage.subtree,
				};
				let objects = Aspects {
					command_objects: Facts {
						metadata: metadata.subtree.command_objects,
						permissions: Self::contains_process_permission(
							node.local_permissions,
							tg::authorization::permission::process::Permission::SubtreeCommandObjects,
						),
						storage: storage.subtree_command_objects,
					},
					error_objects: Facts {
						metadata: metadata.subtree.error_objects,
						permissions: Self::contains_process_permission(
							node.local_permissions,
							tg::authorization::permission::process::Permission::SubtreeErrorObjects,
						),
						storage: storage.subtree_error_objects,
					},
					log_objects: Facts {
						metadata: metadata.subtree.log_objects,
						permissions: Self::contains_process_permission(
							node.local_permissions,
							tg::authorization::permission::process::Permission::SubtreeLogObjects,
						),
						storage: storage.subtree_log_objects,
					},
					output_objects: Facts {
						metadata: metadata.subtree.output_objects,
						permissions: Self::contains_process_permission(
							node.local_permissions,
							tg::authorization::permission::process::Permission::SubtreeOutputObjects,
						),
						storage: storage.subtree_output_objects,
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
