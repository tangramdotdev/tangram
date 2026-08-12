use {
	crate::{
		Session,
		sync::{
			graph::Graph,
			put::State,
			queue::{self as raw, ObjectKind},
		},
	},
	futures::{StreamExt as _, TryStreamExt as _},
	std::sync::{
		Arc, Mutex,
		atomic::{AtomicUsize, Ordering},
	},
	tangram_client::prelude::*,
};

pub(super) struct Queue {
	database: async_channel::Sender<DatabaseNode>,
	graph: Arc<Mutex<Graph>>,
	object: async_channel::Sender<ObjectNode>,
	pending_nodes: AtomicUsize,
	process: async_channel::Sender<ProcessNode>,
	sandbox: async_channel::Sender<SandboxNode>,
}

pub(super) struct DatabaseNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::Id,
	pub selector: tg::Selector<tg::Id>,
	pub send: bool,
	pub token: Option<tg::authorization::Token>,
}

pub(super) struct ObjectNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::object::Id,
	pub kind: Option<ObjectKind>,
	pub send: bool,
	pub stored: bool,
}

pub(super) struct ProcessNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::process::Id,
	pub send: bool,
	pub stored: bool,
}

pub(super) struct SandboxNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::sandbox::Id,
	pub send: bool,
	pub token: Option<tg::authorization::Token>,
}

pub(super) struct SyncPutQueueArg {
	pub database_sender: tokio::sync::mpsc::Sender<super::database::Node>,
	pub index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectNode>,
	pub index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessNode>,
	pub queue_database_receiver: async_channel::Receiver<DatabaseNode>,
	pub queue_object_receiver: async_channel::Receiver<ObjectNode>,
	pub queue_process_receiver: async_channel::Receiver<ProcessNode>,
	pub queue_sandbox_receiver: async_channel::Receiver<SandboxNode>,
	pub sandbox_sender: tokio::sync::mpsc::Sender<super::sandbox::Node>,
	pub state: Arc<State>,
	pub store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectNode>,
	pub store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessNode>,
}

impl Queue {
	pub fn new(
		database: async_channel::Sender<DatabaseNode>,
		graph: Arc<Mutex<Graph>>,
		object: async_channel::Sender<ObjectNode>,
		process: async_channel::Sender<ProcessNode>,
		sandbox: async_channel::Sender<SandboxNode>,
	) -> Self {
		Self {
			database,
			graph,
			object,
			pending_nodes: AtomicUsize::new(0),
			process,
			sandbox,
		}
	}

	pub fn enqueue(
		&self,
		eager: bool,
		id: tg::Id,
		token: Option<tg::authorization::Token>,
	) -> tg::Result<()> {
		self.enqueue_with_descendants(true, eager, id, token)
	}

	pub fn enqueue_root_with_descendants(
		&self,
		descendants: bool,
		eager: bool,
		id: tg::Id,
		token: Option<tg::authorization::Token>,
	) -> tg::Result<()> {
		let mut graph = self.graph.lock().unwrap();
		graph.insert_remote_root(id.clone());
		self.enqueue_with_descendants_with_graph(&mut graph, descendants, eager, id, token)
	}

	pub fn enqueue_with_descendants(
		&self,
		descendants: bool,
		eager: bool,
		id: tg::Id,
		token: Option<tg::authorization::Token>,
	) -> tg::Result<()> {
		let mut graph = self.graph.lock().unwrap();
		self.enqueue_with_descendants_with_graph(&mut graph, descendants, eager, id, token)
	}

	pub fn enqueue_object(&self, node: raw::ObjectNode) -> tg::Result<()> {
		let mut graph = self.graph.lock().unwrap();
		self.enqueue_object_with_graph(&mut graph, node)
	}

	pub fn enqueue_objects(
		&self,
		nodes: impl IntoIterator<Item = raw::ObjectNode>,
	) -> tg::Result<()> {
		for node in nodes {
			self.enqueue_object(node)?;
		}

		Ok(())
	}

	pub fn enqueue_processes(
		&self,
		nodes: impl IntoIterator<Item = raw::ProcessNode>,
	) -> tg::Result<()> {
		for node in nodes {
			let mut graph = self.graph.lock().unwrap();
			self.enqueue_process_with_graph(&mut graph, node)?;
		}

		Ok(())
	}

	pub fn resolve(&self, specifier: &tg::Specifier, id: Option<tg::Id>) -> tg::Result<()> {
		let mut graph = self.graph.lock().unwrap();
		let request = graph
			.resolve_remote_selector(specifier)
			.ok_or_else(|| tg::error!(%specifier, "missing the selector request"))?;
		let Some(id) = id else {
			return Ok(());
		};
		graph.insert_remote_root(id.clone());
		let selector = tg::Selector::Specifier(specifier.clone());
		let node = raw::DatabaseNode {
			descendants: request.descendants,
			eager: request.eager,
			id,
			selector,
			token: request.token,
		};
		self.enqueue_database_with_graph(&mut graph, node)
	}

	pub fn close_if_end(&self) -> bool {
		let graph = self.graph.lock().unwrap();
		self.close_if_end_with_graph(&graph)
	}

	pub fn finish_node(&self) {
		let graph = self.graph.lock().unwrap();
		self.decrement_pending_nodes();
		self.close_if_end_with_graph(&graph);
	}

	fn close_if_end_with_graph(&self, graph: &Graph) -> bool {
		let end = graph.end_remote() && self.pending_nodes.load(Ordering::Relaxed) == 0;
		if end {
			self.database.close();
			self.object.close();
			self.process.close();
			self.sandbox.close();
		}

		end
	}

	fn decrement_pending_nodes(&self) {
		let pending_nodes = self.pending_nodes.fetch_sub(1, Ordering::Relaxed);
		assert!(pending_nodes > 0, "the pending node count must be positive");
	}

	fn enqueue_with_descendants_with_graph(
		&self,
		graph: &mut Graph,
		descendants: bool,
		eager: bool,
		id: tg::Id,
		token: Option<tg::authorization::Token>,
	) -> tg::Result<()> {
		match id.kind() {
			tg::id::Kind::Group
			| tg::id::Kind::Organization
			| tg::id::Kind::Tag
			| tg::id::Kind::User => {
				let selector = tg::Selector::Id(id.clone());
				let node = raw::DatabaseNode {
					descendants,
					eager,
					id,
					selector,
					token,
				};
				self.enqueue_database_with_graph(graph, node)?;
			},
			tg::id::Kind::Process => {
				let node = raw::ProcessNode {
					descendants,
					eager,
					id: id.try_into()?,
					parent: None,
					token,
				};
				self.enqueue_process_with_graph(graph, node)?;
			},
			tg::id::Kind::Sandbox => {
				let node = raw::SandboxNode {
					descendants,
					eager,
					id: id.try_into()?,
					token,
				};
				self.enqueue_sandbox_with_graph(graph, node)?;
			},
			_ => {
				let id = tg::object::Id::try_from(id)
					.map_err(|_| tg::error!("invalid sync node kind"))?;
				let node = raw::ObjectNode {
					descendants,
					eager,
					id,
					kind: None,
					parent: None,
					token,
				};
				self.enqueue_object_with_graph(graph, node)?;
			},
		}

		Ok(())
	}

	fn enqueue_database_with_graph(
		&self,
		graph: &mut Graph,
		node: raw::DatabaseNode,
	) -> tg::Result<()> {
		let action = graph.update_database_node_remote(
			node.descendants,
			&node.id,
			node.selector.clone(),
			node.token.clone(),
		);
		if !action.descendants && !action.send {
			return Ok(());
		}
		let node = DatabaseNode {
			descendants: action.descendants,
			eager: node.eager,
			id: node.id,
			selector: node.selector,
			send: action.send,
			token: node.token,
		};
		self.pending_nodes.fetch_add(1, Ordering::Relaxed);
		if self.database.force_send(node).is_err() {
			self.decrement_pending_nodes();
			return Err(tg::error!("failed to enqueue the database node"));
		}

		Ok(())
	}

	fn enqueue_object_with_graph(
		&self,
		graph: &mut Graph,
		node: raw::ObjectNode,
	) -> tg::Result<()> {
		if let Some(token) = &node.token {
			graph.update_object_token(&node.id, token.clone());
		}
		let parent = node.parent.clone();
		let (action, _) =
			graph.update_object_remote(node.descendants, &node.id, parent, node.kind, None);
		let stored = graph.object_remote_stored(&node.id);
		let skip = !action.descendants && !action.send && node.parent.is_none() && !stored;
		if skip {
			return Ok(());
		}
		let node = ObjectNode {
			descendants: action.descendants,
			eager: node.eager,
			id: node.id,
			kind: node.kind,
			send: action.send,
			stored,
		};
		self.pending_nodes.fetch_add(1, Ordering::Relaxed);
		if self.object.force_send(node).is_err() {
			self.decrement_pending_nodes();
			return Err(tg::error!("failed to enqueue the object"));
		}

		Ok(())
	}

	fn enqueue_process_with_graph(
		&self,
		graph: &mut Graph,
		node: raw::ProcessNode,
	) -> tg::Result<()> {
		if let Some(token) = &node.token {
			graph.update_process_token(&node.id, token.clone());
		}
		let parent = node.parent.clone().map(Into::into);
		let (action, _) = graph.update_process_remote(node.descendants, &node.id, parent, None);
		let stored = graph.process_remote_stored(&node.id);
		let skip = !action.descendants && !action.send && node.parent.is_none() && !stored;
		if skip {
			return Ok(());
		}
		let node = ProcessNode {
			descendants: action.descendants,
			eager: node.eager,
			id: node.id,
			send: action.send,
			stored,
		};
		self.pending_nodes.fetch_add(1, Ordering::Relaxed);
		if self.process.force_send(node).is_err() {
			self.decrement_pending_nodes();
			return Err(tg::error!("failed to enqueue the process"));
		}

		Ok(())
	}

	fn enqueue_sandbox_with_graph(
		&self,
		graph: &mut Graph,
		node: raw::SandboxNode,
	) -> tg::Result<()> {
		let id = node.id.clone().into();
		let action = graph.update_node_remote(node.descendants, &id, node.token.clone());
		if !action.descendants && !action.send {
			return Ok(());
		}
		let node = SandboxNode {
			descendants: action.descendants,
			eager: node.eager,
			id: node.id,
			send: action.send,
			token: node.token,
		};
		self.pending_nodes.fetch_add(1, Ordering::Relaxed);
		if self.sandbox.force_send(node).is_err() {
			self.decrement_pending_nodes();
			return Err(tg::error!("failed to enqueue the sandbox"));
		}

		Ok(())
	}
}

impl Session {
	#[tracing::instrument(err, level = "trace", name = "queue", ret, skip_all)]
	pub(super) async fn sync_put_queue(&self, arg: SyncPutQueueArg) -> tg::Result<()> {
		let SyncPutQueueArg {
			database_sender,
			index_object_sender,
			index_process_sender,
			queue_database_receiver,
			queue_object_receiver,
			queue_process_receiver,
			queue_sandbox_receiver,
			sandbox_sender,
			state,
			store_object_sender,
			store_process_sender,
		} = arg;

		// Create the database future.
		let database_future = queue_database_receiver.map(Ok).try_for_each(|node| {
			let database_sender = database_sender.clone();
			let session = self.clone();
			async move {
				crate::checkpoint!(
					session.server,
					"sync.put.queue.database",
					descendants = node.descendants,
					id = %node.id,
					selector = %node.selector,
				)
				.await;
				let node = super::database::Node {
					descendants: node.descendants,
					eager: node.eager,
					id: node.id,
					send: node.send,
					token: node.token,
				};
				database_sender
					.send(node)
					.await
					.map_err(|_| tg::error!("failed to send the node to the database task"))?;

				Ok(())
			}
		});

		// Create the objects future.
		let object_batch_size = self.server.config.sync.put.queue.object_batch_size;
		let object_batch_timeout = self.server.config.sync.put.queue.object_batch_timeout;
		let object_concurrency = self.server.config.sync.put.queue.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			queue_object_receiver,
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |nodes| {
			let session = self.clone();
			let state = state.clone();
			let index_object_sender = index_object_sender.clone();
			let store_object_sender = store_object_sender.clone();
			async move {
				session
					.sync_put_queue_object_batch(
						&state,
						nodes,
						index_object_sender,
						store_object_sender,
					)
					.await
			}
		});

		// Create the processes future.
		let process_batch_size = self.server.config.sync.put.queue.process_batch_size;
		let process_batch_timeout = self.server.config.sync.put.queue.process_batch_timeout;
		let process_concurrency = self.server.config.sync.put.queue.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			queue_process_receiver,
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |nodes| {
			let session = self.clone();
			let state = state.clone();
			let index_process_sender = index_process_sender.clone();
			let store_process_sender = store_process_sender.clone();
			async move {
				session
					.sync_put_queue_process_batch(
						&state,
						nodes,
						index_process_sender,
						store_process_sender,
					)
					.await
			}
		});

		// Create the sandboxes future.
		let sandboxes_future = queue_sandbox_receiver.map(Ok).try_for_each(|node| {
			let sandbox_sender = sandbox_sender.clone();
			async move {
				let node = super::sandbox::Node {
					descendants: node.descendants,
					eager: node.eager,
					id: node.id,
					send: node.send,
					token: node.token,
				};
				sandbox_sender
					.send(node)
					.await
					.map_err(|_| tg::error!("failed to send the sandbox to the sandbox task"))?;

				Ok(())
			}
		});

		// Join the futures.
		futures::try_join!(
			database_future,
			objects_future,
			processes_future,
			sandboxes_future
		)?;

		Ok(())
	}

	async fn sync_put_queue_object_batch(
		&self,
		state: &State,
		mut nodes: Vec<ObjectNode>,
		index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectNode>,
		store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectNode>,
	) -> tg::Result<()> {
		// Refresh the destination's stored state.
		for node in &mut nodes {
			node.stored = state.graph.lock().unwrap().object_remote_stored(&node.id);
			if node.stored {
				node.descendants = false;
				node.send = false;
			}
		}

		// Collect the objects requiring authorization.
		let required = Self::sync_put_object_permissions();
		let mut authorization_args = Vec::new();
		let mut authorization_positions = Vec::new();
		for (position, node) in nodes.iter().enumerate() {
			let requested = if node.descendants {
				required
			} else {
				Self::sync_put_object_node_permissions()
			};
			let authorization = state
				.graph
				.lock()
				.unwrap()
				.get_object_local_authorization(&node.id, requested);
			if authorization.permissions.contains(requested) {
				continue;
			}
			let resource = tg::Referent::with_node_and_token(node.id.clone(), authorization.token);
			authorization_args.push((resource, requested));
			authorization_positions.push(position);
		}

		// Authorize the objects.
		let outputs = self
			.authorize_batch(authorization_args)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the objects"))?;
		for (position, output) in std::iter::zip(authorization_positions, outputs) {
			if let Some(permissions) = output {
				state
					.graph
					.lock()
					.unwrap()
					.update_object_local_permissions(&nodes[position].id, permissions);
			}
		}

		// Route the objects.
		for node in nodes {
			let requested = Self::sync_put_object_node_permissions();
			let authorization = state
				.graph
				.lock()
				.unwrap()
				.get_object_local_authorization(&node.id, requested);
			if !authorization.permissions.contains(requested) {
				tracing::trace!(
					id = %node.id,
					principal = ?self.context.principal,
					permissions = ?authorization.permissions,
					"authorization denied"
				);
				if node.send {
					let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
						selector: tg::Selector::Id(node.id.clone().into()),
						token: None,
					});
					state.sender.send(Ok(message)).await.ok();
					state
						.graph
						.lock()
						.unwrap()
						.update_object_remote_missing(&node.id);
				}
				if node.descendants {
					state
						.graph
						.lock()
						.unwrap()
						.finish_object_remote_descendants(&node.id, node.eager);
				}
				state.queue.finish_node();
				continue;
			}
			if (!node.descendants && !node.send) || node.stored {
				let node = super::index::ObjectNode { id: node.id };
				index_object_sender
					.send(node)
					.await
					.map_err(|_| tg::error!("failed to send the object to the index task"))?;
				state.queue.finish_node();
			} else {
				let node = super::store::ObjectNode {
					descendants: node.descendants,
					eager: node.eager,
					id: node.id,
					kind: node.kind,
					send: node.send,
					token: authorization.token,
				};
				store_object_sender
					.send(node)
					.await
					.map_err(|_| tg::error!("failed to send the object to the store task"))?;
			}
		}

		state.queue.close_if_end();

		Ok(())
	}

	async fn sync_put_queue_process_batch(
		&self,
		state: &State,
		mut nodes: Vec<ProcessNode>,
		index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessNode>,
		store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessNode>,
	) -> tg::Result<()> {
		// Refresh the destination's stored state.
		for node in &mut nodes {
			node.stored = state.graph.lock().unwrap().process_remote_stored(&node.id);
			if node.stored {
				node.descendants = false;
				node.send = false;
			}
		}

		// Collect the processes requiring authorization.
		let required = Self::sync_put_process_permissions(&state.arg);
		let mut authorization_args = Vec::new();
		let mut authorization_positions = Vec::new();
		for (position, node) in nodes.iter().enumerate() {
			let requested = if node.descendants {
				required
			} else {
				Self::sync_put_process_node_permissions()
			};
			let authorization = state
				.graph
				.lock()
				.unwrap()
				.get_process_local_authorization(&node.id, requested);
			if authorization.permissions.contains(requested) {
				continue;
			}
			let resource = tg::Referent::with_node_and_token(node.id.clone(), authorization.token);
			authorization_args.push((resource, requested));
			authorization_positions.push(position);
		}

		// Authorize the processes.
		let outputs = self
			.authorize_batch(authorization_args)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the processes"))?;
		for (position, output) in std::iter::zip(authorization_positions, outputs) {
			if let Some(permissions) = output {
				state
					.graph
					.lock()
					.unwrap()
					.update_process_local_permissions(&nodes[position].id, permissions);
			}
		}

		// Route the processes.
		for node in nodes {
			let requested = Self::sync_put_process_node_permissions();
			let authorization = state
				.graph
				.lock()
				.unwrap()
				.get_process_local_authorization(&node.id, requested);
			if !authorization.permissions.contains(requested) {
				tracing::trace!(
					id = %node.id,
					principal = ?self.context.principal,
					permissions = ?authorization.permissions,
					"authorization denied"
				);
				if node.send {
					let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
						selector: tg::Selector::Id(node.id.clone().into()),
						token: None,
					});
					state.sender.send(Ok(message)).await.ok();
					state
						.graph
						.lock()
						.unwrap()
						.update_process_remote_missing(&node.id);
				}
				if node.descendants {
					state
						.graph
						.lock()
						.unwrap()
						.finish_process_remote_descendants(&node.id, node.eager);
				}
				state.queue.finish_node();
				continue;
			}
			if (!node.descendants && !node.send) || node.stored {
				let node = super::index::ProcessNode { id: node.id };
				index_process_sender
					.send(node)
					.await
					.map_err(|_| tg::error!("failed to send the process to the index task"))?;
				state.queue.finish_node();
			} else {
				let node = super::store::ProcessNode {
					descendants: node.descendants,
					eager: node.eager,
					id: node.id,
					send: node.send,
					token: authorization.token,
				};
				store_process_sender
					.send(node)
					.await
					.map_err(|_| tg::error!("failed to send the process to the store task"))?;
			}
		}

		state.queue.close_if_end();

		Ok(())
	}

	fn sync_put_object_node_permissions() -> tg::authorization::permission::Set {
		tg::authorization::permission::Set::from_permission(tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		))
	}

	fn sync_put_object_permissions() -> tg::authorization::permission::Set {
		let mut permissions = Self::sync_put_object_node_permissions();
		permissions.insert(tg::authorization::permission::Set::from_permission(
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			),
		));
		permissions
	}

	fn sync_put_process_node_permissions() -> tg::authorization::permission::Set {
		tg::authorization::permission::Set::from_permission(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		))
	}

	fn sync_put_process_permissions(arg: &tg::sync::Arg) -> tg::authorization::permission::Set {
		let mut permissions = Self::sync_put_process_node_permissions();
		let mut insert = |permission| {
			permissions.insert(tg::authorization::permission::Set::from_permission(
				tg::authorization::Permission::Process(permission),
			));
		};
		if arg.process_children {
			insert(tg::authorization::permission::process::Permission::Subtree);
		}
		for (enabled, node, subtree) in [
			(
				arg.process_commands,
				tg::authorization::permission::process::Permission::NodeCommand,
				tg::authorization::permission::process::Permission::SubtreeCommand,
			),
			(
				arg.process_errors,
				tg::authorization::permission::process::Permission::NodeError,
				tg::authorization::permission::process::Permission::SubtreeError,
			),
			(
				arg.process_logs,
				tg::authorization::permission::process::Permission::NodeLog,
				tg::authorization::permission::process::Permission::SubtreeLog,
			),
			(
				arg.process_outputs,
				tg::authorization::permission::process::Permission::NodeOutput,
				tg::authorization::permission::process::Permission::SubtreeOutput,
			),
		] {
			if enabled {
				insert(node);
				if arg.process_children {
					insert(subtree);
				}
			}
		}
		permissions
	}
}
