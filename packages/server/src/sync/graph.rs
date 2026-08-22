use {
	indexmap::{IndexMap, IndexSet},
	petgraph::visit::IntoNeighbors as _,
	smallvec::SmallVec,
	std::collections::{BTreeSet, HashMap, HashSet, VecDeque},
	tangram_client::prelude::*,
	tangram_util::iter::Ext as _,
};

pub struct Graph {
	pub get_end_received: bool,
	local_pending_roots: usize,
	pub local_roots: HashSet<tg::Id, fnv::FnvBuildHasher>,
	local_selectors: HashSet<tg::Specifier, fnv::FnvBuildHasher>,
	pub nodes: IndexMap<tg::Id, Node, fnv::FnvBuildHasher>,
	process_children: bool,
	process_commands: bool,
	process_errors: bool,
	process_logs: bool,
	process_outputs: bool,
	remote_pending_roots: usize,
	pub remote_roots: HashSet<tg::Id, fnv::FnvBuildHasher>,
	remote_selectors: HashMap<tg::Specifier, RemoteSelector, fnv::FnvBuildHasher>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Parent {
	Node(usize),
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
	permission: tg::authorization::Permission,
}

#[derive(Debug, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
pub enum Node {
	Group(DatabaseNode),
	Object(ObjectNode),
	Organization(DatabaseNode),
	Process(ProcessNode),
	Sandbox(DatabaseNode),
	Tag(DatabaseNode),
	User(DatabaseNode),
}

#[derive(Clone, Debug, Default)]
pub struct DatabaseNode {
	pub children: Option<Vec<usize>>,
	local_end: bool,
	pub local_message: Option<tg::sync::PutNodeMessage>,
	pub local_requested: bool,
	pub parents: IndexSet<Parent, fnv::FnvBuildHasher>,
	remote_descendants: Descendants,
	remote_end: bool,
	remote_missing: bool,
	remote_pending_children: Option<usize>,
	pub remote_requested: bool,
	remote_selectors: BTreeSet<tg::Selector<tg::Id>>,
	pub remote_sent: bool,
	pub token: Option<tg::authorization::Token>,
}

#[derive(Clone, Debug, Default)]
pub struct ObjectNode {
	pub children: Option<Vec<usize>>,
	local_end: bool,
	pub local_permissions: Option<tg::authorization::permission::Set>,
	pub local_stored: Option<tangram_index::object::Stored>,
	pub local_visible: Option<tangram_index::object::Stored>,
	pub marked: bool,
	pub metadata: Option<tg::object::Metadata>,
	pub parents: IndexSet<Parent, fnv::FnvBuildHasher>,
	remote_children: HashSet<usize, fnv::FnvBuildHasher>,
	remote_descendants: Descendants,
	remote_end: bool,
	pub remote_missing: bool,
	remote_pending_children: Option<usize>,
	pub remote_requested: bool,
	pub remote_sent: bool,
	pub remote_stored: Option<tangram_index::object::Stored>,
	pub requested: Option<Requested>,
	pub token: Option<tg::authorization::Token>,
}

#[derive(Clone, Debug, Default)]
pub struct ProcessNode {
	pub children: Option<Vec<usize>>,
	pub data: Option<tg::process::Data>,
	local_end: bool,
	pub local_permissions: Option<tg::authorization::permission::Set>,
	pub local_stored: Option<tangram_index::process::Stored>,
	pub local_visible: Option<tangram_index::process::Stored>,
	pub marked: bool,
	pub metadata: Option<tg::process::Metadata>,
	pub objects: Option<Vec<(usize, tangram_index::process::object::Kind)>>,
	pub parents: IndexSet<Parent, fnv::FnvBuildHasher>,
	remote_children: HashSet<usize, fnv::FnvBuildHasher>,
	remote_descendants: Descendants,
	remote_end: bool,
	remote_missing: bool,
	remote_objects: HashSet<(usize, crate::sync::queue::ObjectKind), fnv::FnvBuildHasher>,
	remote_pending_children: Option<usize>,
	remote_pending_commands: usize,
	remote_pending_errors: usize,
	remote_pending_logs: usize,
	remote_pending_outputs: usize,
	remote_propagated_stored: tangram_index::process::Stored,
	pub remote_requested: bool,
	remote_sent: bool,
	pub remote_stored: Option<tangram_index::process::Stored>,
	pub requested: Option<Requested>,
	pub token: Option<tg::authorization::Token>,
}

#[derive(Clone, Debug, Default)]
struct Descendants {
	eager: bool,
	requested: bool,
	sent: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RemoteAction {
	pub descendants: bool,
	pub send: bool,
}

#[derive(Clone, Debug)]
pub struct RemoteSelector {
	pub descendants: bool,
	pub eager: bool,
	pub token: Option<tg::authorization::Token>,
}

#[derive(Clone, Debug, Default)]
pub struct Requested {
	pub eager: bool,
}

pub struct Authorization {
	pub permissions: tg::authorization::permission::Set,
	pub token: Option<tg::authorization::Token>,
}

pub struct UpdateObjectLocalArg<'a> {
	pub data: Option<&'a tg::object::Data>,
	pub id: &'a tg::object::Id,
	pub marked: Option<bool>,
	pub metadata: Option<tg::object::Metadata>,
	pub permissions: Option<tg::authorization::permission::Set>,
	pub requested: Option<Requested>,
	pub stored: Option<tangram_index::object::Stored>,
}

pub struct UpdateProcessLocalArg<'a> {
	pub data: Option<&'a tg::process::Data>,
	pub id: &'a tg::process::Id,
	pub marked: Option<bool>,
	pub metadata: Option<tg::process::Metadata>,
	pub permissions: Option<tg::authorization::permission::Set>,
	pub requested: Option<Requested>,
	pub stored: Option<tangram_index::process::Stored>,
}

impl Graph {
	#[must_use]
	pub fn new(arg: &tg::sync::Arg) -> Self {
		// Create the graph.
		let mut graph = Graph {
			get_end_received: false,
			local_pending_roots: 0,
			local_roots: HashSet::default(),
			local_selectors: arg
				.get
				.iter()
				.filter_map(|node| match &node.node {
					tg::Selector::Id(_) => None,
					tg::Selector::Specifier(specifier) => Some(specifier.clone()),
				})
				.collect(),
			nodes: IndexMap::default(),
			process_children: arg.process_children,
			process_commands: arg.process_commands,
			process_errors: arg.process_errors,
			process_logs: arg.process_logs,
			process_outputs: arg.process_outputs,
			remote_pending_roots: 0,
			remote_roots: HashSet::default(),
			remote_selectors: HashMap::default(),
		};

		// Add the roots.
		for root in &arg.get {
			let tg::Selector::Id(id) = &root.node else {
				continue;
			};
			graph.insert_local_root(id.clone());
		}
		for root in &arg.put {
			graph.insert_remote_root(root.node.clone());
		}

		// Add the root tokens.
		for root in &arg.get {
			let Some(token) = root.options.tokens.local().cloned() else {
				continue;
			};
			let tg::Selector::Id(id) = &root.node else {
				continue;
			};
			graph.update_root_token(id, token);
		}
		for root in &arg.put {
			let Some(token) = root.options.tokens.local().cloned() else {
				continue;
			};
			graph.update_root_token(&root.node, token);
		}

		graph
	}

	fn update_root_token(&mut self, id: &tg::Id, token: tg::authorization::Token) {
		match id.kind() {
			tg::id::Kind::Process => {
				self.update_process_token(&id.clone().try_into().unwrap(), token);
			},
			tg::id::Kind::Group
			| tg::id::Kind::Organization
			| tg::id::Kind::Sandbox
			| tg::id::Kind::Tag
			| tg::id::Kind::User => self.update_node_token(id, token),
			_ if tg::object::Id::try_from(id.clone()).is_ok() => {
				self.update_object_token(&id.clone().try_into().unwrap(), token);
			},
			_ => {},
		}
	}

	pub fn mark_get_end_received(&mut self) {
		self.get_end_received = true;
	}

	pub fn insert_local_root(&mut self, id: tg::Id) -> bool {
		if !self.local_roots.insert(id.clone()) {
			return false;
		}
		let entry = self.nodes.entry(id);
		let index = entry.index();
		entry.or_insert_with_key(Node::for_id);
		let local_end = self.compute_local_end(index);
		self.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.set_local_end(local_end);
		if !local_end {
			self.local_pending_roots += 1;
		}

		true
	}

	pub fn insert_local_selector(&mut self, specifier: tg::Specifier) -> bool {
		self.local_selectors.insert(specifier)
	}

	pub fn insert_remote_root(&mut self, id: tg::Id) {
		let remote_end = self.nodes.get(&id).is_some_and(Node::remote_end);
		if self.remote_roots.insert(id) && !remote_end {
			self.remote_pending_roots += 1;
		}
	}

	pub fn insert_remote_selector(
		&mut self,
		descendants: bool,
		eager: bool,
		specifier: tg::Specifier,
		token: Option<tg::authorization::Token>,
	) -> bool {
		let Some(request) = self.remote_selectors.get_mut(&specifier) else {
			let request = RemoteSelector {
				descendants,
				eager,
				token,
			};
			self.remote_selectors.insert(specifier, request);

			return true;
		};
		request.descendants |= descendants;
		request.eager |= eager;
		if let Some(token) = token {
			request.token.get_or_insert(token);
		}

		false
	}

	pub fn resolve_local_selector(&mut self, specifier: &tg::Specifier, id: tg::Id) -> bool {
		if !self.local_selectors.remove(specifier) {
			return false;
		}
		self.insert_local_root(id);
		true
	}

	pub fn resolve_local_selector_missing(&mut self, specifier: &tg::Specifier) -> bool {
		self.local_selectors.remove(specifier)
	}

	pub fn resolve_remote_selector(&mut self, specifier: &tg::Specifier) -> Option<RemoteSelector> {
		self.remote_selectors.remove(specifier)
	}

	#[must_use]
	pub fn has_local_node(&self, id: &tg::Id) -> bool {
		self.local_roots.contains(id)
			|| self.nodes.get(id).is_some_and(|node| match node {
				Node::Group(node)
				| Node::Organization(node)
				| Node::Sandbox(node)
				| Node::Tag(node)
				| Node::User(node) => node.local_message.is_some() || node.local_requested,
				Node::Object(_) | Node::Process(_) => false,
			})
	}

	#[must_use]
	pub fn has_local_selector(&self, specifier: &tg::Specifier) -> bool {
		self.local_selectors.contains(specifier)
	}

	pub fn local_messages(&self) -> Vec<tg::sync::PutNodeMessage> {
		self.nodes
			.values()
			.filter_map(|node| match node {
				Node::Group(node)
				| Node::Organization(node)
				| Node::Sandbox(node)
				| Node::Tag(node)
				| Node::User(node) => node.local_message.clone(),
				Node::Object(_) | Node::Process(_) => None,
			})
			.collect()
	}

	pub fn update_node_local_message(
		&mut self,
		message: tg::sync::PutNodeMessage,
	) -> tg::Result<()> {
		let id = match &message {
			tg::sync::PutNodeMessage::Group(message) => message.id.clone().into(),
			tg::sync::PutNodeMessage::Object(_) | tg::sync::PutNodeMessage::Process(_) => {
				return Err(tg::error!("invalid sync node kind"));
			},
			tg::sync::PutNodeMessage::Organization(message) => message.id.clone().into(),
			tg::sync::PutNodeMessage::Sandbox(message) => message.id.clone().into(),
			tg::sync::PutNodeMessage::Tag(message) => message.id.clone().into(),
			tg::sync::PutNodeMessage::User(message) => message.id.clone().into(),
		};
		let entry = self.nodes.entry(id);
		let index = entry.index();
		let node = entry.or_insert_with_key(Node::for_id).unwrap_database_mut();
		if node.local_message.is_some() {
			return Err(tg::error!("received the node more than once"));
		}
		node.local_message = Some(message);
		self.update_local_end(index);

		Ok(())
	}

	pub fn update_node_local_requested(
		&mut self,
		id: &tg::Id,
		token: Option<tg::authorization::Token>,
	) -> bool {
		let node = self
			.nodes
			.entry(id.clone())
			.or_insert_with(|| Node::for_id(id));
		let node = node.unwrap_database_mut();
		if let Some(token) = token {
			node.token.get_or_insert(token);
		}
		let inserted = !node.local_requested;
		node.local_requested = true;

		inserted
	}

	pub fn update_database_node_remote(
		&mut self,
		descendants: bool,
		id: &tg::Id,
		selector: tg::Selector<tg::Id>,
		token: Option<tg::authorization::Token>,
	) -> RemoteAction {
		let entry = self.nodes.entry(id.clone());
		let index = entry.index();
		let node = entry
			.or_insert_with(|| Node::for_id(id))
			.unwrap_database_mut();
		if let Some(token) = token {
			node.token.get_or_insert(token);
		}
		let enqueue_descendants = node.remote_descendants.request(descendants, false);
		let send = !node.remote_requested && (!node.remote_sent || node.remote_missing);
		if !node.remote_sent || node.remote_missing {
			node.remote_selectors.insert(selector);
		}
		if send {
			node.remote_requested = true;
		}
		if send && node.remote_sent {
			node.remote_pending_children = None;
			node.remote_sent = false;
		}
		if enqueue_descendants || send {
			self.update_remote_end(index);
		}

		RemoteAction {
			descendants: enqueue_descendants,
			send,
		}
	}

	pub fn update_node_remote(
		&mut self,
		descendants: bool,
		id: &tg::Id,
		token: Option<tg::authorization::Token>,
	) -> RemoteAction {
		let entry = self.nodes.entry(id.clone());
		let index = entry.index();
		let node = entry
			.or_insert_with(|| Node::for_id(id))
			.unwrap_database_mut();
		if let Some(token) = token {
			node.token.get_or_insert(token);
		}
		let enqueue_descendants = node.remote_descendants.request(descendants, false);
		let send = !node.remote_requested && (!node.remote_sent || node.remote_missing);
		if send {
			node.remote_requested = true;
		}
		if send && node.remote_sent {
			node.remote_pending_children = None;
			node.remote_sent = false;
		}
		if enqueue_descendants || send {
			self.update_remote_end(index);
		}

		RemoteAction {
			descendants: enqueue_descendants,
			send,
		}
	}

	pub fn finish_database_node_remote_found(&mut self, id: &tg::Id) {
		self.nodes
			.get_mut(id)
			.unwrap()
			.unwrap_database_mut()
			.remote_selectors
			.clear();
		self.finish_node_remote_found(id);
	}

	pub fn finish_database_node_remote_missing(
		&mut self,
		id: &tg::Id,
	) -> BTreeSet<tg::Selector<tg::Id>> {
		let selectors = {
			let node = self.nodes.get_mut(id).unwrap().unwrap_database_mut();
			std::mem::take(&mut node.remote_selectors)
		};
		self.finish_node_remote_missing(id);

		selectors
	}

	pub fn finish_node_remote_descendants(&mut self, id: &tg::Id, children: &[tg::Id]) {
		let index = self.nodes.get_index_of(id).unwrap();

		// Collect the children.
		let mut child_indices = HashSet::<usize, fnv::FnvBuildHasher>::default();
		let children = children
			.iter()
			.filter_map(|child| {
				let entry = self.nodes.entry(child.clone());
				let child_index = entry.index();
				let child_node = entry.or_insert_with(|| Node::for_id(child));
				let parent = Parent::Node(index);
				child_node.parents_mut().insert(parent);
				child_indices.insert(child_index).then_some(child_index)
			})
			.collect::<Vec<_>>();

		// Update the node.
		let remote_pending_children = self.count_remote_pending(&children);
		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_database_mut();
		node.children = Some(children);
		node.remote_descendants.finish(false);
		node.remote_pending_children = Some(remote_pending_children);

		// Update the End state.
		self.update_remote_end(index);
	}

	pub fn finish_node_remote_found(&mut self, id: &tg::Id) {
		let index = self.nodes.get_index_of(id).unwrap();
		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_database_mut();
		node.remote_missing = false;
		node.remote_requested = false;
		node.remote_sent = true;
		self.update_remote_end(index);
	}

	pub fn finish_node_remote_missing(&mut self, id: &tg::Id) {
		let index = self.nodes.get_index_of(id).unwrap();
		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_database_mut();
		node.remote_descendants.finish(false);
		node.remote_missing = true;
		node.remote_pending_children = Some(0);
		node.remote_requested = false;
		node.remote_sent = true;
		self.update_remote_end(index);
	}

	pub fn update_node_token(&mut self, id: &tg::Id, token: tg::authorization::Token) {
		let node = self
			.nodes
			.entry(id.clone())
			.or_insert_with(|| Node::for_id(id));
		node.unwrap_database_mut().token.get_or_insert(token);
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

		// Collect the children.
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
					child_node.unwrap_object_mut().parents.insert(parent);
					child_index
				})
				.collect();
			Some(children)
		} else {
			None
		};

		// Compute the derived state.
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
		let remote_pending_children = children
			.as_ref()
			.map(|children| self.count_remote_pending(children));

		// Update the node.
		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_object_mut();

		if let Some(children) = children {
			node.remote_children.extend(children.iter().copied());
			node.children = Some(children);
			node.local_stored = Some(tangram_index::object::Stored {
				subtree: computed_stored.unwrap(),
			});
			node.remote_pending_children = remote_pending_children;
		}

		if let Some(stored) = stored {
			match &mut node.local_stored {
				Some(local_stored) => local_stored.merge(&stored),
				None => node.local_stored = Some(stored),
			}
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

		// Update the local End state and propagate the local stored and visible state.
		self.update_local_end(index);
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
					self.update_local_end(parent_index);
					stack.extend(parents);
				}
			}
		}

		// Update the remote End state.
		self.update_remote_end(index);
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

		// Collect the children.
		let children = if let Some(data) = data {
			data.children.as_ref().map(|children| {
				let mut child_indices = HashSet::<usize, fnv::FnvBuildHasher>::default();
				children
					.iter()
					.filter_map(|child| {
						let child = child.process.node.clone();
						let child_entry = self.nodes.entry(child.into());
						let child_index = child_entry.index();
						let child_node =
							child_entry.or_insert_with(|| Node::Process(ProcessNode::default()));
						let parent = Parent::Process(index);
						child_node.unwrap_process_mut().parents.insert(parent);
						child_indices.insert(child_index).then_some(child_index)
					})
					.collect::<Vec<_>>()
			})
		} else {
			None
		};

		// Collect the objects.
		let objects = if let Some(data) = data {
			let mut objects: Vec<(usize, tangram_index::process::object::Kind)> = Vec::new();

			let command: tg::object::Id = data.command.node.clone().into();
			let command_entry = self.nodes.entry(command.into());
			let command_index = command_entry.index();
			let command_node = command_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
			let parent = Parent::ProcessObject {
				index,
				kind: crate::sync::queue::ObjectKind::Command,
			};
			command_node.unwrap_object_mut().parents.insert(parent);
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
							object_node.unwrap_object_mut().parents.insert(parent);
							objects
								.push((object_index, tangram_index::process::object::Kind::Error));
						}
					},
					tg::Either::Right(error_id) => {
						let error_id = error_id.node.clone();
						let error_entry = self.nodes.entry(tg::object::Id::from(error_id).into());
						let error_index = error_entry.index();
						let error_node =
							error_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
						let parent = Parent::ProcessObject {
							index,
							kind: crate::sync::queue::ObjectKind::Error,
						};
						error_node.unwrap_object_mut().parents.insert(parent);
						objects.push((error_index, tangram_index::process::object::Kind::Error));
					},
				}
			}

			if let Some(log) = data.log.clone().map(|log| log.node) {
				let log_entry = self.nodes.entry(tg::object::Id::from(log).into());
				let log_index = log_entry.index();
				let log_node = log_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
				let parent = Parent::ProcessObject {
					index,
					kind: crate::sync::queue::ObjectKind::Log,
				};
				log_node.unwrap_object_mut().parents.insert(parent);
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
					object_node.unwrap_object_mut().parents.insert(parent);
					objects.push((object_index, tangram_index::process::object::Kind::Output));
				}
			}

			Some(objects)
		} else {
			None
		};

		// Get the current local state.
		let node_old_stored = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_process_ref()
			.local_stored
			.clone();
		let node_old_visible = self.process_local_visible(index);

		// Compute the derived state.
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
		let remote_pending_children = children
			.as_ref()
			.map(|children| self.count_remote_pending(children));
		let remote_pending_objects = objects
			.as_ref()
			.map(|objects| self.count_process_remote_pending(objects));

		// Update the node.
		let mut inserted_remote_children = Vec::new();
		{
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();

			if let Some(children) = children {
				for &child in &children {
					if node.remote_children.insert(child) {
						inserted_remote_children.push(child);
					}
				}
				node.children = Some(children);
				node.remote_pending_children = remote_pending_children;
			}

			if let Some(data) = data {
				let mut data = data.clone();
				if data.children.is_none() {
					data.children = node.data.as_ref().and_then(|data| data.children.clone());
				}
				node.data = Some(data);
			}

			if let Some(stored) = stored {
				match &mut node.local_stored {
					Some(local_stored) => local_stored.merge(&stored),
					None => node.local_stored = Some(stored),
				}
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
				node.remote_objects.extend(
					objects
						.iter()
						.map(|(index, kind)| (*index, Self::process_object_remote_kind(*kind))),
				);
				node.objects = Some(objects);
				let (commands, errors, logs, outputs) = remote_pending_objects.unwrap();
				node.remote_pending_commands = commands;
				node.remote_pending_errors = errors;
				node.remote_pending_logs = logs;
				node.remote_pending_outputs = outputs;
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

		// Update the local End state and propagate the local stored and visible state.
		self.update_local_end(index);
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
					self.update_local_end(parent_index);
					stack.extend(parents);
				}
			}
		}

		// Propagate the remote stored state and update the End state.
		let mut end_indices = self.inherit_process_remote_stored(index, &inserted_remote_children);
		let stored_indices = std::iter::once(index)
			.chain(end_indices.iter().copied())
			.collect::<Vec<_>>();
		end_indices.extend(self.propagate_process_remote_stored(stored_indices));
		end_indices.push(index);
		self.update_remote_ends(end_indices);
	}

	pub fn update_object_remote(
		&mut self,
		descendants: bool,
		id: &tg::object::Id,
		parent: Option<tg::Id>,
		kind: Option<crate::sync::queue::ObjectKind>,
		stored: Option<&tangram_index::object::Stored>,
	) -> (RemoteAction, Option<tangram_index::object::Stored>) {
		// Get or create the node.
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_insert_with(|| Node::Object(ObjectNode::default()));
		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_object_mut();

		// Update the remote state.
		let action = if stored.is_none() {
			let complete = node
				.remote_stored
				.as_ref()
				.is_some_and(|stored| stored.subtree);
			let enqueue_descendants = node.remote_descendants.request(descendants, complete);
			let send =
				!complete && !node.remote_missing && !node.remote_requested && !node.remote_sent;
			if send {
				node.remote_requested = true;
			}
			RemoteAction {
				descendants: enqueue_descendants,
				send,
			}
		} else {
			RemoteAction::default()
		};
		if !action.descendants && !action.send && parent.is_none() && stored.is_none() {
			return (action, node.remote_stored.clone());
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

		// Add the parent edge.
		let parent_index = if let Some(parent) = parent {
			let remote_end = self.nodes.get_index(index).unwrap().1.remote_end();
			let (parent_index, _, parent_node) = self.nodes.get_full_mut(&parent).unwrap();
			let parent = if parent.kind() == tg::id::Kind::Process {
				Parent::ProcessObject {
					index: parent_index,
					kind: kind.unwrap(),
				}
			} else {
				Parent::Object(parent_index)
			};

			let dependency_inserted = match parent_node {
				Node::Group(_)
				| Node::Organization(_)
				| Node::Sandbox(_)
				| Node::Tag(_)
				| Node::User(_) => false,
				Node::Object(node) => {
					let remote_child_inserted = node.remote_children.insert(index);
					if remote_child_inserted && let Some(children) = node.children.as_mut() {
						children.push(index);
						if !remote_end {
							*node.remote_pending_children.as_mut().unwrap() += 1;
						}
						true
					} else {
						false
					}
				},
				Node::Process(node) => {
					let Parent::ProcessObject { kind, .. } = parent else {
						unreachable!();
					};
					let remote_object_inserted = node.remote_objects.insert((index, kind));
					let dependency_inserted =
						if remote_object_inserted && let Some(objects) = node.objects.as_mut() {
							let kind = Self::process_object_kind(kind);
							objects.push((index, kind));
							true
						} else {
							false
						};
					if dependency_inserted && !remote_end {
						match parent {
							Parent::ProcessObject {
								kind: crate::sync::queue::ObjectKind::Command,
								..
							} => node.remote_pending_commands += 1,
							Parent::ProcessObject {
								kind: crate::sync::queue::ObjectKind::Error,
								..
							} => node.remote_pending_errors += 1,
							Parent::ProcessObject {
								kind: crate::sync::queue::ObjectKind::Log,
								..
							} => node.remote_pending_logs += 1,
							Parent::ProcessObject {
								kind: crate::sync::queue::ObjectKind::Output,
								..
							} => node.remote_pending_outputs += 1,
							Parent::Node(_) | Parent::Object(_) | Parent::Process(_) => {
								unreachable!()
							},
						}
					}
					dependency_inserted
				},
			};

			let (_, node) = self.nodes.get_index_mut(index).unwrap();
			node.parents_mut().insert(parent);

			dependency_inserted.then_some(parent_index)
		} else {
			None
		};

		// Update the End state.
		let mut end_indices = vec![index];
		end_indices.extend(parent_index);
		self.update_remote_ends(end_indices);

		// Get the remote stored state.
		let remote_stored = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_object_ref()
			.remote_stored
			.clone();

		(action, remote_stored)
	}

	pub fn finish_object_remote_descendants(&mut self, id: &tg::object::Id, eager: bool) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		let node = entry
			.or_insert_with(|| Node::Object(ObjectNode::default()))
			.unwrap_object_mut();
		node.remote_descendants.finish(eager);
		node.remote_pending_children.get_or_insert(0);
		self.update_remote_end(index);
	}

	pub fn update_object_remote_missing(&mut self, id: &tg::object::Id) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		let node = entry
			.or_insert_with(|| Node::Object(ObjectNode::default()))
			.unwrap_object_mut();
		node.remote_descendants.finish(false);
		node.remote_missing = true;
		node.remote_requested = false;
		self.update_remote_end(index);
	}

	pub fn update_object_remote_sent(&mut self, id: &tg::object::Id) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		let node = entry
			.or_insert_with(|| Node::Object(ObjectNode::default()))
			.unwrap_object_mut();
		node.remote_missing = false;
		node.remote_requested = false;
		node.remote_sent = true;
		self.update_remote_end(index);
	}

	pub fn update_process_remote(
		&mut self,
		descendants: bool,
		id: &tg::process::Id,
		parent: Option<tg::Id>,
		stored: Option<&tangram_index::process::Stored>,
	) -> (RemoteAction, Option<tangram_index::process::Stored>) {
		let complete = self
			.nodes
			.get(&tg::Id::from(id.clone()))
			.and_then(|node| node.unwrap_process_ref().remote_stored.as_ref())
			.is_some_and(|stored| self.process_remote_stored_complete(stored));

		// Get or create the node.
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_insert_with(|| Node::Process(ProcessNode::default()));
		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_process_mut();

		// Update the remote state.
		let action = if stored.is_none() {
			let enqueue_descendants = node.remote_descendants.request(descendants, complete);
			let send =
				!complete && !node.remote_missing && !node.remote_requested && !node.remote_sent;
			if send {
				node.remote_requested = true;
			}
			RemoteAction {
				descendants: enqueue_descendants,
				send,
			}
		} else {
			RemoteAction::default()
		};
		if !action.descendants && !action.send && parent.is_none() && stored.is_none() {
			return (action, node.remote_stored.clone());
		}

		if let Some(stored) = stored {
			let mut stored = stored.clone();
			Self::normalize_process_remote_stored(&mut stored);
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			let stored = Self::merge_process_stored(node.remote_stored.as_ref(), stored);
			node.remote_stored = Some(stored);
		}

		// Add the parent edge.
		let parent = if let Some(parent) = parent {
			let parent_index = self.nodes.get_index_of(&parent).unwrap();
			let (dependency_inserted, remote_child_inserted) =
				self.insert_process_remote_child(parent_index, index);
			let parent = Parent::Process(parent_index);
			let (_, node) = self.nodes.get_index_mut(index).unwrap();
			node.parents_mut().insert(parent);

			Some((dependency_inserted, parent_index, remote_child_inserted))
		} else {
			None
		};

		// Propagate the stored state and update the End state.
		let mut end_indices = Vec::new();
		let mut stored_indices = vec![index];
		if let Some((dependency_inserted, parent_index, remote_child_inserted)) = parent {
			if dependency_inserted {
				end_indices.push(parent_index);
			}
			if remote_child_inserted {
				let inherited = self.inherit_process_remote_stored(parent_index, &[index]);
				stored_indices.extend(inherited.iter().copied());
				end_indices.extend(inherited);
			}
		}
		end_indices.push(index);
		end_indices.extend(self.propagate_process_remote_stored(stored_indices));
		self.update_remote_ends(end_indices);

		// Get the remote stored state.
		let stored = self
			.nodes
			.get_index(index)
			.unwrap()
			.1
			.unwrap_process_ref()
			.remote_stored
			.clone();

		(action, stored)
	}

	pub fn finish_process_remote_descendants(&mut self, id: &tg::process::Id, eager: bool) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		let node = entry
			.or_insert_with(|| Node::Process(ProcessNode::default()))
			.unwrap_process_mut();
		node.remote_descendants.finish(eager);
		self.update_remote_end(index);
	}

	pub fn update_process_remote_missing(&mut self, id: &tg::process::Id) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		let node = entry
			.or_insert_with(|| Node::Process(ProcessNode::default()))
			.unwrap_process_mut();
		node.remote_descendants.finish(false);
		node.remote_missing = true;
		node.remote_requested = false;
		self.update_remote_end(index);
	}

	pub fn update_process_remote_sent(&mut self, id: &tg::process::Id) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		let node = entry
			.or_insert_with(|| Node::Process(ProcessNode::default()))
			.unwrap_process_mut();
		node.remote_missing = false;
		node.remote_requested = false;
		node.remote_sent = true;
		node.remote_stored.get_or_insert_default();
		self.update_remote_end(index);
	}

	pub fn get_process_local_stored(
		&self,
		id: &tg::process::Id,
	) -> Option<&tangram_index::process::Stored> {
		self.nodes
			.get(&tg::Id::from(id.clone()))
			.and_then(|node| node.unwrap_process_ref().local_stored.as_ref())
	}

	pub fn get_process_local_visible(
		&self,
		id: &tg::process::Id,
	) -> tangram_index::process::Stored {
		self.nodes
			.get_index_of(&tg::Id::from(id.clone()))
			.map(|index| self.process_local_visible(index))
			.unwrap_or_default()
	}

	pub fn get_object_local_visible(&self, id: &tg::object::Id) -> tangram_index::object::Stored {
		tangram_index::object::Stored {
			subtree: self
				.nodes
				.get_index_of(&tg::Id::from(id.clone()))
				.is_some_and(|index| self.object_local_visible(index)),
		}
	}

	pub fn get_object_local_authorization(
		&mut self,
		id: &tg::object::Id,
		required: tg::authorization::permission::Set,
	) -> Authorization {
		let Some(index) = self.nodes.get_index_of(&tg::Id::from(id.clone())) else {
			let permissions = tg::authorization::permission::Set::Object(
				tg::authorization::permission::object::Set::empty(),
			);
			return Authorization {
				permissions,
				token: None,
			};
		};
		self.get_local_authorization(index, required)
	}

	pub fn get_process_local_authorization(
		&mut self,
		id: &tg::process::Id,
		required: tg::authorization::permission::Set,
	) -> Authorization {
		let Some(index) = self.nodes.get_index_of(&tg::Id::from(id.clone())) else {
			let permissions = tg::authorization::permission::Set::Process(
				tg::authorization::permission::process::Set::empty(),
			);
			return Authorization {
				permissions,
				token: None,
			};
		};
		self.get_local_authorization(index, required)
	}

	pub fn update_object_token(&mut self, id: &tg::object::Id, token: tg::authorization::Token) {
		let node = self
			.nodes
			.entry(id.clone().into())
			.or_insert_with(|| Node::Object(ObjectNode::default()));
		node.unwrap_object_mut().token.get_or_insert(token);
	}

	pub fn update_process_token(&mut self, id: &tg::process::Id, token: tg::authorization::Token) {
		let node = self
			.nodes
			.entry(id.clone().into())
			.or_insert_with(|| Node::Process(ProcessNode::default()));
		node.unwrap_process_mut().token.get_or_insert(token);
	}

	pub fn update_object_local_permissions(
		&mut self,
		id: &tg::object::Id,
		permissions: tg::authorization::permission::Set,
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
		permissions: tg::authorization::permission::Set,
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
			.get(&tg::Id::from(id.clone()))
			.and_then(|node| node.unwrap_object_ref().requested.clone())
	}

	pub fn get_process_requested(&self, id: &tg::process::Id) -> Option<Requested> {
		self.nodes
			.get(&tg::Id::from(id.clone()))
			.and_then(|node| node.unwrap_process_ref().requested.clone())
	}

	#[must_use]
	pub fn object_remote_stored(&self, id: &tg::object::Id) -> bool {
		self.nodes
			.get(&tg::Id::from(id.clone()))
			.and_then(|node| node.unwrap_object_ref().remote_stored.as_ref())
			.is_some_and(|stored| stored.subtree)
	}

	#[must_use]
	pub fn process_remote_stored(&self, id: &tg::process::Id) -> bool {
		self.nodes
			.get(&tg::Id::from(id.clone()))
			.and_then(|node| node.unwrap_process_ref().remote_stored.as_ref())
			.is_some_and(|stored| self.process_remote_stored_complete(stored))
	}

	#[must_use]
	pub fn end_local(&self) -> bool {
		self.local_pending_roots == 0 && self.local_selectors.is_empty()
	}

	#[must_use]
	pub fn end_remote(&self) -> bool {
		if !self.get_end_received {
			return false;
		}

		self.remote_pending_roots == 0 && self.remote_selectors.is_empty()
	}

	fn get_local_authorization(
		&mut self,
		index: usize,
		required: tg::authorization::permission::Set,
	) -> Authorization {
		let permissions = self
			.nodes
			.get_index(index)
			.and_then(|(_, node)| node.local_permissions())
			.map_or_else(|| required.empty_like(), Self::normalize_permissions);
		if permissions.contains(required) {
			let token = self
				.nodes
				.get_index(index)
				.and_then(|(_, node)| node.token())
				.cloned();
			return Authorization { permissions, token };
		}

		let mut predecessors = HashMap::new();
		let mut queue = VecDeque::new();
		let mut token = self
			.nodes
			.get_index(index)
			.and_then(|(_, node)| node.token())
			.cloned();
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
			if token.is_none() {
				token = self
					.nodes
					.get_index(state.index)
					.and_then(|(_, node)| node.token())
					.cloned();
			}
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
					return Authorization { permissions, token };
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

		let permissions = self
			.nodes
			.get_index(index)
			.and_then(|(_, node)| node.local_permissions())
			.map_or_else(|| required.empty_like(), Self::normalize_permissions);

		Authorization { permissions, token }
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

	fn update_local_permission(&mut self, index: usize, permission: tg::authorization::Permission) {
		let id = self.nodes.get_index(index).unwrap().0.clone();
		let permissions = tg::authorization::permission::Set::from_permission(permission);
		match id.kind() {
			tg::id::Kind::Process => {
				self.update_process_local_permissions(&id.try_into().unwrap(), permissions);
			},
			_ if tg::object::Id::try_from(id.clone()).is_ok() => {
				self.update_object_local_permissions(&id.try_into().unwrap(), permissions);
			},
			_ => (),
		}
	}

	fn parent_required_permission(
		parent: Parent,
		permission: tg::authorization::Permission,
	) -> Option<tg::authorization::Permission> {
		match parent {
			Parent::Node(_) => None,
			Parent::Object(_) => match permission {
				tg::authorization::Permission::Object(_) => {
					Some(tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					))
				},
				_ => None,
			},
			Parent::Process(_) => match permission {
				tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Write,
				) => None,
				tg::authorization::Permission::Process(permission) => Some(
					tg::authorization::Permission::Process(permission.to_subtree()),
				),
				_ => None,
			},
			Parent::ProcessObject { kind, .. } => match permission {
				tg::authorization::Permission::Object(_) => Some(
					tg::authorization::Permission::Process(Self::process_object_permission(kind)),
				),
				_ => None,
			},
		}
	}

	fn derive_child_permission(
		parent: Parent,
		permission: tg::authorization::Permission,
	) -> tg::authorization::Permission {
		match parent {
			Parent::Node(_) => unreachable!(),
			Parent::Object(_) => match permission {
				tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				) => permission,
				_ => unreachable!(),
			},
			Parent::Process(_) => match permission {
				tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Read
					| tg::authorization::permission::process::Permission::Subtree
					| tg::authorization::permission::process::Permission::SubtreeCommand
					| tg::authorization::permission::process::Permission::SubtreeError
					| tg::authorization::permission::process::Permission::SubtreeLog
					| tg::authorization::permission::process::Permission::SubtreeOutput,
				) => permission,
				_ => unreachable!(),
			},
			Parent::ProcessObject { .. } => tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			),
		}
	}

	fn normalize_permissions(
		mut permissions: tg::authorization::permission::Set,
	) -> tg::authorization::permission::Set {
		let implied = permissions
			.iter()
			.filter_map(|permission| match permission {
				tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				) => Some(tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Node,
				)),
				tg::authorization::Permission::Process(permission) => match permission {
					tg::authorization::permission::process::Permission::Subtree
					| tg::authorization::permission::process::Permission::Write => {
						Some(tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::Node,
						))
					},
					tg::authorization::permission::process::Permission::SubtreeCommand => {
						Some(tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::NodeCommand,
						))
					},
					tg::authorization::permission::process::Permission::SubtreeError => {
						Some(tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::NodeError,
						))
					},
					tg::authorization::permission::process::Permission::SubtreeLog => {
						Some(tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::NodeLog,
						))
					},
					tg::authorization::permission::process::Permission::SubtreeOutput => {
						Some(tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::NodeOutput,
						))
					},
					_ => None,
				},
				_ => None,
			})
			.collect::<Vec<_>>();
		for permission in implied {
			permissions.insert(tg::authorization::permission::Set::from_permission(
				permission,
			));
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

	#[must_use]
	fn process_object_remote_kind(
		kind: tangram_index::process::object::Kind,
	) -> crate::sync::queue::ObjectKind {
		match kind {
			tangram_index::process::object::Kind::Command => {
				crate::sync::queue::ObjectKind::Command
			},
			tangram_index::process::object::Kind::Error => crate::sync::queue::ObjectKind::Error,
			tangram_index::process::object::Kind::Log => crate::sync::queue::ObjectKind::Log,
			tangram_index::process::object::Kind::Output => crate::sync::queue::ObjectKind::Output,
		}
	}

	fn process_object_permission(
		kind: crate::sync::queue::ObjectKind,
	) -> tg::authorization::permission::process::Permission {
		match kind {
			crate::sync::queue::ObjectKind::Command => {
				tg::authorization::permission::process::Permission::NodeCommand
			},
			crate::sync::queue::ObjectKind::Error => {
				tg::authorization::permission::process::Permission::NodeError
			},
			crate::sync::queue::ObjectKind::Log => {
				tg::authorization::permission::process::Permission::NodeLog
			},
			crate::sync::queue::ObjectKind::Output => {
				tg::authorization::permission::process::Permission::NodeOutput
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
		permissions: Option<tg::authorization::permission::Set>,
	) -> bool {
		stored.is_some_and(|stored| stored.subtree)
			&& permissions.is_some_and(|permissions| {
				permissions.contains(tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				))
			})
	}

	fn compute_process_visible_from_permissions(
		stored: Option<&tangram_index::process::Stored>,
		permissions: Option<tg::authorization::permission::Set>,
	) -> tangram_index::process::Stored {
		let Some(stored) = stored else {
			return tangram_index::process::Stored::default();
		};
		tangram_index::process::Stored {
			node_command: stored.node_command
				&& Self::contains_process_permission(
					permissions,
					tg::authorization::permission::process::Permission::NodeCommand,
				),
			node_error: stored.node_error
				&& Self::contains_process_permission(
					permissions,
					tg::authorization::permission::process::Permission::NodeError,
				),
			node_log: stored.node_log
				&& Self::contains_process_permission(
					permissions,
					tg::authorization::permission::process::Permission::NodeLog,
				),
			node_output: stored.node_output
				&& Self::contains_process_permission(
					permissions,
					tg::authorization::permission::process::Permission::NodeOutput,
				),
			subtree: stored.subtree
				&& Self::contains_process_permission(
					permissions,
					tg::authorization::permission::process::Permission::Subtree,
				),
			subtree_command: stored.subtree_command
				&& Self::contains_process_permission(
					permissions,
					tg::authorization::permission::process::Permission::SubtreeCommand,
				),
			subtree_error: stored.subtree_error
				&& Self::contains_process_permission(
					permissions,
					tg::authorization::permission::process::Permission::SubtreeError,
				),
			subtree_log: stored.subtree_log
				&& Self::contains_process_permission(
					permissions,
					tg::authorization::permission::process::Permission::SubtreeLog,
				),
			subtree_output: stored.subtree_output
				&& Self::contains_process_permission(
					permissions,
					tg::authorization::permission::process::Permission::SubtreeOutput,
				),
		}
	}

	fn contains_process_permission(
		permissions: Option<tg::authorization::permission::Set>,
		permission: tg::authorization::permission::process::Permission,
	) -> bool {
		permissions.is_some_and(|permissions| {
			permissions.contains(tg::authorization::Permission::Process(permission))
		})
	}

	pub fn object_permissions_for_stored(
		stored: &tangram_index::object::Stored,
	) -> Option<tg::authorization::permission::Set> {
		stored.subtree.then(|| {
			tg::authorization::permission::Set::from_permission(
				tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				),
			)
		})
	}

	pub fn process_permissions_for_stored(
		stored: &tangram_index::process::Stored,
	) -> Option<tg::authorization::permission::Set> {
		let mut permissions = tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::empty(),
		);
		let mut insert = |permission| {
			permissions.insert(tg::authorization::permission::Set::from_permission(
				tg::authorization::Permission::Process(permission),
			));
		};
		if stored.node_command {
			insert(tg::authorization::permission::process::Permission::NodeCommand);
		}
		if stored.node_error {
			insert(tg::authorization::permission::process::Permission::NodeError);
		}
		if stored.node_log {
			insert(tg::authorization::permission::process::Permission::NodeLog);
		}
		if stored.node_output {
			insert(tg::authorization::permission::process::Permission::NodeOutput);
		}
		if stored.subtree {
			insert(tg::authorization::permission::process::Permission::Subtree);
		}
		if stored.subtree_command {
			insert(tg::authorization::permission::process::Permission::SubtreeCommand);
		}
		if stored.subtree_error {
			insert(tg::authorization::permission::process::Permission::SubtreeError);
		}
		if stored.subtree_log {
			insert(tg::authorization::permission::process::Permission::SubtreeLog);
		}
		if stored.subtree_output {
			insert(tg::authorization::permission::process::Permission::SubtreeOutput);
		}
		(!permissions.is_empty()).then_some(permissions)
	}

	#[must_use]
	pub fn process_visible(&self, visible: &tangram_index::process::Stored) -> bool {
		if self.process_children {
			visible.subtree
				&& (!self.process_commands || visible.subtree_command)
				&& (!self.process_errors || visible.subtree_error)
				&& (!self.process_logs || visible.subtree_log)
				&& (!self.process_outputs || visible.subtree_output)
		} else {
			(!self.process_commands || visible.node_command)
				&& (!self.process_errors || visible.node_error)
				&& (!self.process_logs || visible.node_log)
				&& (!self.process_outputs || visible.node_output)
		}
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

	fn insert_process_remote_child(&mut self, parent: usize, child: usize) -> (bool, bool) {
		let remote_end = self.nodes.get_index(child).unwrap().1.remote_end();
		let node = self
			.nodes
			.get_index_mut(parent)
			.unwrap()
			.1
			.unwrap_process_mut();
		let remote_child_inserted = node.remote_children.insert(child);
		if !remote_child_inserted {
			return (false, false);
		}
		let Some(children) = node.children.as_mut() else {
			return (false, true);
		};
		children.push(child);
		if !remote_end {
			*node.remote_pending_children.as_mut().unwrap() += 1;
		}

		(true, true)
	}

	#[must_use]
	fn count_remote_pending(&self, children: &[usize]) -> usize {
		children
			.iter()
			.filter(|index| !self.nodes.get_index(**index).unwrap().1.remote_end())
			.count()
	}

	#[must_use]
	fn count_process_remote_pending(
		&self,
		objects: &[(usize, tangram_index::process::object::Kind)],
	) -> (usize, usize, usize, usize) {
		let mut pending = (0, 0, 0, 0);
		for &(index, kind) in objects {
			if self.nodes.get_index(index).unwrap().1.remote_end() {
				continue;
			}
			match kind {
				tangram_index::process::object::Kind::Command => pending.0 += 1,
				tangram_index::process::object::Kind::Error => pending.1 += 1,
				tangram_index::process::object::Kind::Log => pending.2 += 1,
				tangram_index::process::object::Kind::Output => pending.3 += 1,
			}
		}

		pending
	}

	#[must_use]
	fn process_remote_stored_complete(&self, stored: &tangram_index::process::Stored) -> bool {
		let children = !self.process_children || stored.subtree;
		let command = !self.process_commands
			|| if self.process_children {
				stored.subtree_command
			} else {
				stored.node_command
			};
		let error = !self.process_errors
			|| if self.process_children {
				stored.subtree_error
			} else {
				stored.node_error
			};
		let log = !self.process_logs
			|| if self.process_children {
				stored.subtree_log
			} else {
				stored.node_log
			};
		let output = !self.process_outputs
			|| if self.process_children {
				stored.subtree_output
			} else {
				stored.node_output
			};

		children && command && error && log && output
	}

	fn update_local_end(&mut self, index: usize) {
		let (id, node) = self.nodes.get_index(index).unwrap();
		if !self.local_roots.contains(id) {
			return;
		}
		let old_local_end = node.local_end();
		let local_end = self.compute_local_end(index);
		if old_local_end == local_end {
			return;
		}
		self.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.set_local_end(local_end);
		Self::apply_root_end_transition(&mut self.local_pending_roots, local_end);
	}

	#[must_use]
	fn compute_local_end(&self, index: usize) -> bool {
		let (_, node) = self.nodes.get_index(index).unwrap();
		match node {
			Node::Group(node)
			| Node::Organization(node)
			| Node::Sandbox(node)
			| Node::Tag(node)
			| Node::User(node) => node.local_message.is_some(),
			Node::Object(_) => self.object_local_visible(index),
			Node::Process(_) => self.process_visible(&self.process_local_visible(index)),
		}
	}

	fn update_remote_end(&mut self, index: usize) {
		self.update_remote_ends([index]);
	}

	fn update_remote_ends(&mut self, indices: impl IntoIterator<Item = usize>) {
		// Seed the queue.
		let mut queue = VecDeque::new();
		let mut queued = HashSet::new();
		for index in indices {
			if queued.insert(index) {
				queue.push_back(index);
			}
		}

		// Propagate the End transitions.
		while let Some(index) = queue.pop_front() {
			queued.remove(&index);
			let remote_end = self.compute_remote_end(index);
			let (id, node) = self.nodes.get_index(index).unwrap();
			if node.remote_end() == remote_end {
				continue;
			}
			let remote_root = self.remote_roots.contains(id);
			let parents = node
				.parents()
				.iter()
				.copied()
				.collect::<SmallVec<[Parent; 1]>>();
			self.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.set_remote_end(remote_end);
			if remote_root {
				Self::apply_root_end_transition(&mut self.remote_pending_roots, remote_end);
			}

			// Update each parent from this edge alone, without rescanning its other children.
			for parent in parents {
				if self.update_remote_pending(parent, remote_end) {
					let parent = parent.index();
					if queued.insert(parent) {
						queue.push_back(parent);
					}
				}
			}
		}
	}

	fn apply_root_end_transition(pending: &mut usize, end: bool) {
		if end {
			*pending = pending.checked_sub(1).unwrap();
		} else {
			*pending += 1;
		}
	}

	#[must_use]
	fn compute_remote_end(&self, index: usize) -> bool {
		// Get the node.
		let (_, node) = self.nodes.get_index(index).unwrap();

		// Compute the End state.
		match node {
			Node::Group(node)
			| Node::Organization(node)
			| Node::Sandbox(node)
			| Node::Tag(node)
			| Node::User(node) => {
				!node.remote_requested
					&& node.remote_sent
					&& (node.remote_missing
						|| !node.remote_descendants.requested
						|| (node.remote_descendants.sent
							&& node.remote_pending_children == Some(0)))
			},
			Node::Object(node) => {
				node.remote_missing
					|| node
						.remote_stored
						.as_ref()
						.is_some_and(|stored| stored.subtree)
					|| (node.remote_sent
						&& (!node.remote_descendants.requested
							|| !node.remote_descendants.eager
							|| (node.remote_descendants.sent
								&& node.remote_pending_children == Some(0))))
			},
			Node::Process(node) => {
				if node.remote_missing {
					return true;
				}
				let Some(stored) = node.remote_stored.as_ref() else {
					return false;
				};
				if self.process_remote_stored_complete(stored) {
					return true;
				}
				if !node.remote_descendants.requested
					|| (!node.remote_descendants.eager && node.remote_sent)
				{
					return node.remote_sent;
				}
				if !node.remote_descendants.sent {
					return false;
				}
				let children_end = !self.process_children
					|| stored.subtree
					|| node.remote_pending_children == Some(0);
				let stored_command = if self.process_children {
					stored.subtree_command
				} else {
					stored.node_command
				};
				let command_end =
					!self.process_commands || stored_command || node.remote_pending_commands == 0;
				let stored_error = if self.process_children {
					stored.subtree_error
				} else {
					stored.node_error
				};
				let error_end =
					!self.process_errors || stored_error || node.remote_pending_errors == 0;
				let stored_log = if self.process_children {
					stored.subtree_log
				} else {
					stored.node_log
				};
				let log_end = !self.process_logs || stored_log || node.remote_pending_logs == 0;
				let stored_output = if self.process_children {
					stored.subtree_output
				} else {
					stored.node_output
				};
				let output_end =
					!self.process_outputs || stored_output || node.remote_pending_outputs == 0;

				children_end && command_end && error_end && log_end && output_end
			},
		}
	}

	fn update_remote_pending(&mut self, parent: Parent, child_remote_end: bool) -> bool {
		// Get the pending count.
		let (_, node) = self.nodes.get_index_mut(parent.index()).unwrap();
		let pending = match (parent, node) {
			(
				Parent::Node(_),
				Node::Group(node)
				| Node::Organization(node)
				| Node::Sandbox(node)
				| Node::Tag(node)
				| Node::User(node),
			) => node.remote_pending_children.as_mut(),
			(Parent::Object(_), Node::Object(node)) => node.remote_pending_children.as_mut(),
			(Parent::Process(_), Node::Process(node)) => node.remote_pending_children.as_mut(),
			(
				Parent::ProcessObject {
					kind: crate::sync::queue::ObjectKind::Command,
					..
				},
				Node::Process(node),
			) if node.objects.is_some() => Some(&mut node.remote_pending_commands),
			(
				Parent::ProcessObject {
					kind: crate::sync::queue::ObjectKind::Error,
					..
				},
				Node::Process(node),
			) if node.objects.is_some() => Some(&mut node.remote_pending_errors),
			(
				Parent::ProcessObject {
					kind: crate::sync::queue::ObjectKind::Log,
					..
				},
				Node::Process(node),
			) if node.objects.is_some() => Some(&mut node.remote_pending_logs),
			(
				Parent::ProcessObject {
					kind: crate::sync::queue::ObjectKind::Output,
					..
				},
				Node::Process(node),
			) if node.objects.is_some() => Some(&mut node.remote_pending_outputs),
			(
				Parent::Node(_)
				| Parent::Object(_)
				| Parent::Process(_)
				| Parent::ProcessObject { .. },
				Node::Group(_)
				| Node::Object(_)
				| Node::Organization(_)
				| Node::Process(_)
				| Node::Sandbox(_)
				| Node::Tag(_)
				| Node::User(_),
			) => None,
		};

		// Apply the End transition.
		let Some(pending) = pending else {
			return false;
		};
		if child_remote_end {
			*pending = pending.checked_sub(1).unwrap();
		} else {
			*pending += 1;
		}

		true
	}

	fn merge_local_permissions(
		existing: &mut Option<tg::authorization::permission::Set>,
		permissions: tg::authorization::permission::Set,
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
			Node::Group(_)
			| Node::Organization(_)
			| Node::Sandbox(_)
			| Node::Tag(_)
			| Node::User(_) => None,
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

	fn inherit_process_remote_stored(&mut self, parent: usize, children: &[usize]) -> Vec<usize> {
		let Some(stored) = self.nodes.get_index(parent).and_then(|(_, node)| {
			let node = node.try_unwrap_process_ref().ok()?;
			let stored = node.remote_stored.as_ref()?;
			let stored = Self::process_remote_stored_for_child(stored);
			Self::process_stored_any(&stored).then_some(stored)
		}) else {
			return Vec::new();
		};
		children
			.iter()
			.copied()
			.filter(|&child| self.merge_process_remote_stored_at(child, &stored))
			.collect()
	}

	fn propagate_process_remote_stored(
		&mut self,
		indices: impl IntoIterator<Item = usize>,
	) -> Vec<usize> {
		// Seed the queue.
		let mut changed_children = Vec::new();
		let mut queue = VecDeque::new();
		let mut queued = HashSet::<usize, fnv::FnvBuildHasher>::default();
		for index in indices {
			if queued.insert(index) {
				queue.push_back(index);
			}
		}

		// Propagate each new stored field.
		while let Some(index) = queue.pop_front() {
			queued.remove(&index);
			let Some(stored) = self.take_process_remote_stored_delta(index) else {
				continue;
			};
			let children = self
				.nodes
				.get_index(index)
				.unwrap()
				.1
				.unwrap_process_ref()
				.remote_children
				.iter()
				.copied()
				.collect::<Vec<_>>();
			for child in children {
				if !self.merge_process_remote_stored_at(child, &stored) {
					continue;
				}
				changed_children.push(child);
				if queued.insert(child) {
					queue.push_back(child);
				}
			}
		}

		changed_children
	}

	fn merge_process_remote_stored_at(
		&mut self,
		index: usize,
		stored: &tangram_index::process::Stored,
	) -> bool {
		let Some((_, node)) = self.nodes.get_index_mut(index) else {
			return false;
		};
		let Ok(node) = node.try_unwrap_process_mut() else {
			return false;
		};
		let merged = Self::merge_process_stored(node.remote_stored.as_ref(), stored.clone());
		if !Self::should_propagate_process_stored(node.remote_stored.as_ref(), Some(&merged)) {
			return false;
		}
		node.remote_stored = Some(merged);

		true
	}

	fn take_process_remote_stored_delta(
		&mut self,
		index: usize,
	) -> Option<tangram_index::process::Stored> {
		let (_, node) = self.nodes.get_index_mut(index)?;
		let node = node.try_unwrap_process_mut().ok()?;
		let stored = node.remote_stored.as_ref()?;
		let propagated = &mut node.remote_propagated_stored;

		// Compute the fields that have not been propagated.
		let delta = tangram_index::process::Stored {
			node_command: false,
			node_error: false,
			node_log: false,
			node_output: false,
			subtree: stored.subtree && !propagated.subtree,
			subtree_command: stored.subtree_command && !propagated.subtree_command,
			subtree_error: stored.subtree_error && !propagated.subtree_error,
			subtree_log: stored.subtree_log && !propagated.subtree_log,
			subtree_output: stored.subtree_output && !propagated.subtree_output,
		};

		// Record the delta before walking the children so each stored field crosses each edge once.
		propagated.subtree |= delta.subtree;
		propagated.subtree_command |= delta.subtree_command;
		propagated.subtree_error |= delta.subtree_error;
		propagated.subtree_log |= delta.subtree_log;
		propagated.subtree_output |= delta.subtree_output;

		// Derive the stored state for the children.
		let stored = Self::process_remote_stored_for_child(&delta);

		Self::process_stored_any(&stored).then_some(stored)
	}

	#[must_use]
	fn process_remote_stored_for_child(
		stored: &tangram_index::process::Stored,
	) -> tangram_index::process::Stored {
		tangram_index::process::Stored {
			node_command: stored.subtree_command,
			node_error: stored.subtree_error,
			node_log: stored.subtree_log,
			node_output: stored.subtree_output,
			subtree: stored.subtree,
			subtree_command: stored.subtree_command,
			subtree_error: stored.subtree_error,
			subtree_log: stored.subtree_log,
			subtree_output: stored.subtree_output,
		}
	}

	#[must_use]
	fn process_stored_any(stored: &tangram_index::process::Stored) -> bool {
		stored.node_command
			|| stored.node_error
			|| stored.node_log
			|| stored.node_output
			|| stored.subtree
			|| stored.subtree_command
			|| stored.subtree_error
			|| stored.subtree_log
			|| stored.subtree_output
	}

	fn normalize_process_remote_stored(stored: &mut tangram_index::process::Stored) {
		stored.node_command |= stored.subtree_command;
		stored.node_error |= stored.subtree_error;
		stored.node_log |= stored.subtree_log;
		stored.node_output |= stored.subtree_output;
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
}

impl Descendants {
	fn request(&mut self, enabled: bool, complete: bool) -> bool {
		let enqueue = enabled && !self.requested && !complete;
		self.requested |= enabled;
		if enqueue {
			self.sent = false;
		}

		enqueue
	}

	fn finish(&mut self, eager: bool) {
		self.eager |= eager;
		self.sent = true;
	}
}

impl Parent {
	#[must_use]
	pub fn index(&self) -> usize {
		match self {
			Self::Node(index)
			| Self::Object(index)
			| Self::Process(index)
			| Self::ProcessObject { index, .. } => *index,
		}
	}
}

impl Node {
	fn for_id(id: &tg::Id) -> Self {
		match id.kind() {
			tg::id::Kind::Group => Self::Group(DatabaseNode::default()),
			tg::id::Kind::Organization => Self::Organization(DatabaseNode::default()),
			tg::id::Kind::Process => Self::Process(ProcessNode::default()),
			tg::id::Kind::Sandbox => Self::Sandbox(DatabaseNode::default()),
			tg::id::Kind::Tag => Self::Tag(DatabaseNode::default()),
			tg::id::Kind::User => Self::User(DatabaseNode::default()),
			_ if tg::object::Id::try_from(id.clone()).is_ok() => {
				Self::Object(ObjectNode::default())
			},
			_ => unreachable!(),
		}
	}

	#[must_use]
	fn local_end(&self) -> bool {
		match self {
			Self::Group(node)
			| Self::Organization(node)
			| Self::Sandbox(node)
			| Self::Tag(node)
			| Self::User(node) => node.local_end,
			Self::Object(node) => node.local_end,
			Self::Process(node) => node.local_end,
		}
	}

	fn set_local_end(&mut self, local_end: bool) {
		match self {
			Self::Group(node)
			| Self::Organization(node)
			| Self::Sandbox(node)
			| Self::Tag(node)
			| Self::User(node) => node.local_end = local_end,
			Self::Object(node) => node.local_end = local_end,
			Self::Process(node) => node.local_end = local_end,
		}
	}

	fn local_permissions(&self) -> Option<tg::authorization::permission::Set> {
		match self {
			Self::Group(_)
			| Self::Organization(_)
			| Self::Sandbox(_)
			| Self::Tag(_)
			| Self::User(_) => None,
			Self::Object(node) => node.local_permissions,
			Self::Process(node) => node.local_permissions,
		}
	}

	#[must_use]
	fn remote_end(&self) -> bool {
		match self {
			Self::Group(node)
			| Self::Organization(node)
			| Self::Sandbox(node)
			| Self::Tag(node)
			| Self::User(node) => node.remote_end,
			Self::Object(node) => node.remote_end,
			Self::Process(node) => node.remote_end,
		}
	}

	fn set_remote_end(&mut self, remote_end: bool) {
		match self {
			Self::Group(node)
			| Self::Organization(node)
			| Self::Sandbox(node)
			| Self::Tag(node)
			| Self::User(node) => node.remote_end = remote_end,
			Self::Object(node) => node.remote_end = remote_end,
			Self::Process(node) => node.remote_end = remote_end,
		}
	}

	fn token(&self) -> Option<&tg::authorization::Token> {
		match self {
			Self::Group(node)
			| Self::Organization(node)
			| Self::Sandbox(node)
			| Self::Tag(node)
			| Self::User(node) => node.token.as_ref(),
			Self::Object(node) => node.token.as_ref(),
			Self::Process(node) => node.token.as_ref(),
		}
	}

	fn unwrap_database_mut(&mut self) -> &mut DatabaseNode {
		match self {
			Self::Group(node)
			| Self::Organization(node)
			| Self::Sandbox(node)
			| Self::Tag(node)
			| Self::User(node) => node,
			Self::Object(_) | Self::Process(_) => unreachable!(),
		}
	}

	#[must_use]
	pub fn parents(&self) -> &IndexSet<Parent, fnv::FnvBuildHasher> {
		match self {
			Node::Group(node)
			| Node::Organization(node)
			| Node::Sandbox(node)
			| Node::Tag(node)
			| Node::User(node) => &node.parents,
			Node::Object(node) => &node.parents,
			Node::Process(node) => &node.parents,
		}
	}

	#[must_use]
	pub fn parents_mut(&mut self) -> &mut IndexSet<Parent, fnv::FnvBuildHasher> {
		match self {
			Node::Group(node)
			| Node::Organization(node)
			| Node::Sandbox(node)
			| Node::Tag(node)
			| Node::User(node) => &mut node.parents,
			Node::Object(node) => &mut node.parents,
			Node::Process(node) => &mut node.parents,
		}
	}

	pub fn children(&self) -> Option<&Vec<usize>> {
		match self {
			Node::Group(node)
			| Node::Organization(node)
			| Node::Sandbox(node)
			| Node::Tag(node)
			| Node::User(node) => node.children.as_ref(),
			Node::Object(node) => node.children.as_ref(),
			Node::Process(node) => node.children.as_ref(),
		}
	}

	pub fn children_mut(&mut self) -> &mut Option<Vec<usize>> {
		match self {
			Node::Group(node)
			| Node::Organization(node)
			| Node::Sandbox(node)
			| Node::Tag(node)
			| Node::User(node) => &mut node.children,
			Node::Object(node) => &mut node.children,
			Node::Process(node) => &mut node.children,
		}
	}

	pub fn marked(&self) -> bool {
		match self {
			Node::Group(_)
			| Node::Organization(_)
			| Node::Sandbox(_)
			| Node::Tag(_)
			| Node::User(_) => false,
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
			Node::Group(node)
			| Node::Organization(node)
			| Node::Sandbox(node)
			| Node::Tag(node)
			| Node::User(node) => node.children.iter().flatten().copied().boxed(),
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
					Node::Group(node)
					| Node::Organization(node)
					| Node::Sandbox(node)
					| Node::Tag(node)
					| Node::User(node) => node.parents.iter().map(Parent::index).boxed(),
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
