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
	database: async_channel::Sender<DatabaseItem>,
	graph: Arc<Mutex<Graph>>,
	object: async_channel::Sender<ObjectItem>,
	pending: AtomicUsize,
	process: async_channel::Sender<ProcessItem>,
	sandbox: async_channel::Sender<SandboxItem>,
}

pub(super) struct DatabaseItem {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::Id,
	pub selector: tg::Selector<tg::Id>,
	pub send: bool,
	pub token: Option<tg::grant::Token>,
}

pub(super) struct ObjectItem {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::object::Id,
	pub kind: Option<ObjectKind>,
	pub send: bool,
	pub stored: bool,
}

pub(super) struct ProcessItem {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::process::Id,
	pub send: bool,
	pub stored: bool,
}

pub(super) struct SandboxItem {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::sandbox::Id,
	pub send: bool,
	pub token: Option<tg::grant::Token>,
}

pub(super) struct SyncPutQueueArg {
	pub database_sender: tokio::sync::mpsc::Sender<super::database::Item>,
	pub index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectItem>,
	pub index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessItem>,
	pub queue_database_receiver: async_channel::Receiver<DatabaseItem>,
	pub queue_object_receiver: async_channel::Receiver<ObjectItem>,
	pub queue_process_receiver: async_channel::Receiver<ProcessItem>,
	pub queue_sandbox_receiver: async_channel::Receiver<SandboxItem>,
	pub sandbox_sender: tokio::sync::mpsc::Sender<super::sandbox::Item>,
	pub state: Arc<State>,
	pub store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectItem>,
	pub store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessItem>,
}

impl Queue {
	pub fn new(
		database: async_channel::Sender<DatabaseItem>,
		graph: Arc<Mutex<Graph>>,
		object: async_channel::Sender<ObjectItem>,
		process: async_channel::Sender<ProcessItem>,
		sandbox: async_channel::Sender<SandboxItem>,
	) -> Self {
		Self {
			database,
			graph,
			object,
			pending: AtomicUsize::new(0),
			process,
			sandbox,
		}
	}

	pub fn enqueue(
		&self,
		eager: bool,
		id: tg::Id,
		token: Option<tg::grant::Token>,
	) -> tg::Result<()> {
		self.enqueue_with_descendants(true, eager, id, token)
	}

	pub fn enqueue_root_with_descendants(
		&self,
		descendants: bool,
		eager: bool,
		id: tg::Id,
		token: Option<tg::grant::Token>,
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
		token: Option<tg::grant::Token>,
	) -> tg::Result<()> {
		let mut graph = self.graph.lock().unwrap();
		self.enqueue_with_descendants_with_graph(&mut graph, descendants, eager, id, token)
	}

	pub fn enqueue_object(&self, item: raw::ObjectItem) -> tg::Result<()> {
		let mut graph = self.graph.lock().unwrap();
		self.enqueue_object_with_graph(&mut graph, item)
	}

	pub fn enqueue_objects(
		&self,
		items: impl IntoIterator<Item = raw::ObjectItem>,
	) -> tg::Result<()> {
		for item in items {
			self.enqueue_object(item)?;
		}

		Ok(())
	}

	pub fn enqueue_processes(
		&self,
		items: impl IntoIterator<Item = raw::ProcessItem>,
	) -> tg::Result<()> {
		for item in items {
			let mut graph = self.graph.lock().unwrap();
			self.enqueue_process_with_graph(&mut graph, item)?;
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
		let item = raw::DatabaseItem {
			descendants: request.descendants,
			eager: request.eager,
			id,
			selector,
			token: request.token,
		};
		self.enqueue_database_with_graph(&mut graph, item)
	}

	pub fn close_if_end(&self) -> bool {
		let graph = self.graph.lock().unwrap();
		self.close_if_end_with_graph(&graph)
	}

	pub fn finish_item(&self) {
		let graph = self.graph.lock().unwrap();
		self.decrement_pending();
		self.close_if_end_with_graph(&graph);
	}

	fn close_if_end_with_graph(&self, graph: &Graph) -> bool {
		let end = graph.end_remote() && self.pending.load(Ordering::Relaxed) == 0;
		if end {
			self.database.close();
			self.object.close();
			self.process.close();
			self.sandbox.close();
		}

		end
	}

	fn decrement_pending(&self) {
		let pending = self.pending.fetch_sub(1, Ordering::Relaxed);
		assert!(pending > 0, "the pending item count must be positive");
	}

	fn enqueue_with_descendants_with_graph(
		&self,
		graph: &mut Graph,
		descendants: bool,
		eager: bool,
		id: tg::Id,
		token: Option<tg::grant::Token>,
	) -> tg::Result<()> {
		match id.kind() {
			tg::id::Kind::Group
			| tg::id::Kind::Organization
			| tg::id::Kind::Tag
			| tg::id::Kind::User => {
				let selector = tg::Selector::Id(id.clone());
				let item = raw::DatabaseItem {
					descendants,
					eager,
					id,
					selector,
					token,
				};
				self.enqueue_database_with_graph(graph, item)?;
			},
			tg::id::Kind::Process => {
				let item = raw::ProcessItem {
					descendants,
					eager,
					id: id.try_into()?,
					parent: None,
					token,
				};
				self.enqueue_process_with_graph(graph, item)?;
			},
			tg::id::Kind::Sandbox => {
				let item = raw::SandboxItem {
					descendants,
					eager,
					id: id.try_into()?,
					token,
				};
				self.enqueue_sandbox_with_graph(graph, item)?;
			},
			_ => {
				let id = tg::object::Id::try_from(id)
					.map_err(|_| tg::error!("invalid sync item kind"))?;
				let item = raw::ObjectItem {
					descendants,
					eager,
					id,
					kind: None,
					parent: None,
					token,
				};
				self.enqueue_object_with_graph(graph, item)?;
			},
		}

		Ok(())
	}

	fn enqueue_database_with_graph(
		&self,
		graph: &mut Graph,
		item: raw::DatabaseItem,
	) -> tg::Result<()> {
		let action = graph.update_database_item_remote(
			item.descendants,
			&item.id,
			item.selector.clone(),
			item.token.clone(),
		);
		if !action.descendants && !action.send {
			return Ok(());
		}
		let item = DatabaseItem {
			descendants: action.descendants,
			eager: item.eager,
			id: item.id,
			selector: item.selector,
			send: action.send,
			token: item.token,
		};
		self.pending.fetch_add(1, Ordering::Relaxed);
		if self.database.force_send(item).is_err() {
			self.decrement_pending();
			return Err(tg::error!("failed to enqueue the database item"));
		}

		Ok(())
	}

	fn enqueue_object_with_graph(
		&self,
		graph: &mut Graph,
		item: raw::ObjectItem,
	) -> tg::Result<()> {
		if let Some(token) = &item.token {
			graph.update_object_token(&item.id, token.clone());
		}
		let parent = item.parent.clone();
		let (action, _) =
			graph.update_object_remote(item.descendants, &item.id, parent, item.kind, None);
		let stored = graph.object_remote_stored(&item.id);
		let skip = !action.descendants && !action.send && item.parent.is_none() && !stored;
		if skip {
			return Ok(());
		}
		let item = ObjectItem {
			descendants: action.descendants,
			eager: item.eager,
			id: item.id,
			kind: item.kind,
			send: action.send,
			stored,
		};
		self.pending.fetch_add(1, Ordering::Relaxed);
		if self.object.force_send(item).is_err() {
			self.decrement_pending();
			return Err(tg::error!("failed to enqueue the object"));
		}

		Ok(())
	}

	fn enqueue_process_with_graph(
		&self,
		graph: &mut Graph,
		item: raw::ProcessItem,
	) -> tg::Result<()> {
		if let Some(token) = &item.token {
			graph.update_process_token(&item.id, token.clone());
		}
		let parent = item.parent.clone().map(Into::into);
		let (action, _) = graph.update_process_remote(item.descendants, &item.id, parent, None);
		let stored = graph.process_remote_stored(&item.id);
		let skip = !action.descendants && !action.send && item.parent.is_none() && !stored;
		if skip {
			return Ok(());
		}
		let item = ProcessItem {
			descendants: action.descendants,
			eager: item.eager,
			id: item.id,
			send: action.send,
			stored,
		};
		self.pending.fetch_add(1, Ordering::Relaxed);
		if self.process.force_send(item).is_err() {
			self.decrement_pending();
			return Err(tg::error!("failed to enqueue the process"));
		}

		Ok(())
	}

	fn enqueue_sandbox_with_graph(
		&self,
		graph: &mut Graph,
		item: raw::SandboxItem,
	) -> tg::Result<()> {
		let id = item.id.clone().into();
		let action = graph.update_item_remote(item.descendants, &id, item.token.clone());
		if !action.descendants && !action.send {
			return Ok(());
		}
		let item = SandboxItem {
			descendants: action.descendants,
			eager: item.eager,
			id: item.id,
			send: action.send,
			token: item.token,
		};
		self.pending.fetch_add(1, Ordering::Relaxed);
		if self.sandbox.force_send(item).is_err() {
			self.decrement_pending();
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
		let database_future = queue_database_receiver.map(Ok).try_for_each(|item| {
			let database_sender = database_sender.clone();
			let session = self.clone();
			async move {
				crate::checkpoint!(
					session.server,
					"sync.put.queue.database",
					descendants = item.descendants,
					id = %item.id,
					selector = %item.selector,
				)
				.await;
				let item = super::database::Item {
					descendants: item.descendants,
					eager: item.eager,
					id: item.id,
					send: item.send,
					token: item.token,
				};
				database_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the item to the database task"))?;

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
		.try_for_each_concurrent(object_concurrency, |items| {
			let session = self.clone();
			let state = state.clone();
			let index_object_sender = index_object_sender.clone();
			let store_object_sender = store_object_sender.clone();
			async move {
				session
					.sync_put_queue_object_batch(
						&state,
						items,
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
		.try_for_each_concurrent(process_concurrency, |items| {
			let session = self.clone();
			let state = state.clone();
			let index_process_sender = index_process_sender.clone();
			let store_process_sender = store_process_sender.clone();
			async move {
				session
					.sync_put_queue_process_batch(
						&state,
						items,
						index_process_sender,
						store_process_sender,
					)
					.await
			}
		});

		// Create the sandboxes future.
		let sandboxes_future = queue_sandbox_receiver.map(Ok).try_for_each(|item| {
			let sandbox_sender = sandbox_sender.clone();
			async move {
				let item = super::sandbox::Item {
					descendants: item.descendants,
					eager: item.eager,
					id: item.id,
					send: item.send,
					token: item.token,
				};
				sandbox_sender
					.send(item)
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
		mut items: Vec<ObjectItem>,
		index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectItem>,
		store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectItem>,
	) -> tg::Result<()> {
		// Refresh the destination's stored state.
		for item in &mut items {
			item.stored = state.graph.lock().unwrap().object_remote_stored(&item.id);
			if item.stored {
				item.descendants = false;
				item.send = false;
			}
		}

		// Collect the objects requiring authorization.
		let required = Self::sync_put_object_permissions();
		let mut authorization_args = Vec::new();
		let mut authorization_positions = Vec::new();
		for (position, item) in items.iter().enumerate() {
			let requested = if item.descendants {
				required
			} else {
				Self::sync_put_object_node_permissions()
			};
			let authorization = state
				.graph
				.lock()
				.unwrap()
				.get_object_local_authorization(&item.id, requested);
			if authorization.permissions.contains(requested) {
				continue;
			}
			let resource = tg::Referent::with_item_and_token(item.id.clone(), authorization.token);
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
					.update_object_local_permissions(&items[position].id, permissions);
			}
		}

		// Route the objects.
		for item in items {
			let requested = Self::sync_put_object_node_permissions();
			let authorization = state
				.graph
				.lock()
				.unwrap()
				.get_object_local_authorization(&item.id, requested);
			if !authorization.permissions.contains(requested) {
				tracing::trace!(
					id = %item.id,
					principal = ?self.context.principal,
					permissions = ?authorization.permissions,
					"authorization denied"
				);
				if item.send {
					let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
						selector: tg::Selector::Id(item.id.clone().into()),
						token: None,
					});
					state.sender.send(Ok(message)).await.ok();
					state
						.graph
						.lock()
						.unwrap()
						.update_object_remote_missing(&item.id);
				}
				if item.descendants {
					state
						.graph
						.lock()
						.unwrap()
						.finish_object_remote_descendants(&item.id, item.eager);
				}
				state.queue.finish_item();
				continue;
			}
			if (!item.descendants && !item.send) || item.stored {
				let item = super::index::ObjectItem { id: item.id };
				index_object_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the object to the index task"))?;
				state.queue.finish_item();
			} else {
				let item = super::store::ObjectItem {
					descendants: item.descendants,
					eager: item.eager,
					id: item.id,
					kind: item.kind,
					send: item.send,
					token: authorization.token,
				};
				store_object_sender
					.send(item)
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
		mut items: Vec<ProcessItem>,
		index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessItem>,
		store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessItem>,
	) -> tg::Result<()> {
		// Refresh the destination's stored state.
		for item in &mut items {
			item.stored = state.graph.lock().unwrap().process_remote_stored(&item.id);
			if item.stored {
				item.descendants = false;
				item.send = false;
			}
		}

		// Collect the processes requiring authorization.
		let required = Self::sync_put_process_permissions(&state.arg);
		let mut authorization_args = Vec::new();
		let mut authorization_positions = Vec::new();
		for (position, item) in items.iter().enumerate() {
			let requested = if item.descendants {
				required
			} else {
				Self::sync_put_process_node_permissions()
			};
			let authorization = state
				.graph
				.lock()
				.unwrap()
				.get_process_local_authorization(&item.id, requested);
			if authorization.permissions.contains(requested) {
				continue;
			}
			let resource = tg::Referent::with_item_and_token(item.id.clone(), authorization.token);
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
					.update_process_local_permissions(&items[position].id, permissions);
			}
		}

		// Route the processes.
		for item in items {
			let requested = Self::sync_put_process_node_permissions();
			let authorization = state
				.graph
				.lock()
				.unwrap()
				.get_process_local_authorization(&item.id, requested);
			if !authorization.permissions.contains(requested) {
				tracing::trace!(
					id = %item.id,
					principal = ?self.context.principal,
					permissions = ?authorization.permissions,
					"authorization denied"
				);
				if item.send {
					let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
						selector: tg::Selector::Id(item.id.clone().into()),
						token: None,
					});
					state.sender.send(Ok(message)).await.ok();
					state
						.graph
						.lock()
						.unwrap()
						.update_process_remote_missing(&item.id);
				}
				if item.descendants {
					state
						.graph
						.lock()
						.unwrap()
						.finish_process_remote_descendants(&item.id, item.eager);
				}
				state.queue.finish_item();
				continue;
			}
			if (!item.descendants && !item.send) || item.stored {
				let item = super::index::ProcessItem { id: item.id };
				index_process_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the process to the index task"))?;
				state.queue.finish_item();
			} else {
				let item = super::store::ProcessItem {
					descendants: item.descendants,
					eager: item.eager,
					id: item.id,
					send: item.send,
					token: authorization.token,
				};
				store_process_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the process to the store task"))?;
			}
		}

		state.queue.close_if_end();

		Ok(())
	}

	fn sync_put_object_node_permissions() -> tg::grant::permission::Set {
		tg::grant::permission::Set::from_permission(tg::grant::Permission::Object(
			tg::grant::permission::object::Permission::Node,
		))
	}

	fn sync_put_object_permissions() -> tg::grant::permission::Set {
		let mut permissions = Self::sync_put_object_node_permissions();
		permissions.insert(tg::grant::permission::Set::from_permission(
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree),
		));
		permissions
	}

	fn sync_put_process_node_permissions() -> tg::grant::permission::Set {
		tg::grant::permission::Set::from_permission(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::Node,
		))
	}

	fn sync_put_process_permissions(arg: &tg::sync::Arg) -> tg::grant::permission::Set {
		let mut permissions = Self::sync_put_process_node_permissions();
		let mut insert = |permission| {
			permissions.insert(tg::grant::permission::Set::from_permission(
				tg::grant::Permission::Process(permission),
			));
		};
		if arg.process_children {
			insert(tg::grant::permission::process::Permission::Subtree);
		}
		for (enabled, node, subtree) in [
			(
				arg.process_commands,
				tg::grant::permission::process::Permission::NodeCommand,
				tg::grant::permission::process::Permission::SubtreeCommand,
			),
			(
				arg.process_errors,
				tg::grant::permission::process::Permission::NodeError,
				tg::grant::permission::process::Permission::SubtreeError,
			),
			(
				arg.process_logs,
				tg::grant::permission::process::Permission::NodeLog,
				tg::grant::permission::process::Permission::SubtreeLog,
			),
			(
				arg.process_outputs,
				tg::grant::permission::process::Permission::NodeOutput,
				tg::grant::permission::process::Permission::SubtreeOutput,
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
