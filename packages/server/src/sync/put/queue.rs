use {
	crate::{
		Session,
		sync::{
			put::State,
			queue::{ObjectItem, ProcessItem},
		},
	},
	futures::{StreamExt as _, TryStreamExt as _},
	std::sync::Arc,
	tangram_client::prelude::*,
};

pub(super) struct SyncPutQueueArg {
	pub state: Arc<State>,
	pub queue_object_receiver: async_channel::Receiver<ObjectItem>,
	pub queue_process_receiver: async_channel::Receiver<ProcessItem>,
	pub index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectItem>,
	pub index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessItem>,
	pub store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectItem>,
	pub store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessItem>,
}

impl Session {
	#[tracing::instrument(err, level = "trace", name = "queue", ret, skip_all)]
	pub(super) async fn sync_put_queue(&self, arg: SyncPutQueueArg) -> tg::Result<()> {
		let SyncPutQueueArg {
			state,
			queue_object_receiver,
			queue_process_receiver,
			index_object_sender,
			index_process_sender,
			store_object_sender,
			store_process_sender,
		} = arg;
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

		// Join the objects and processes futures.
		futures::try_join!(objects_future, processes_future)?;

		Ok(())
	}

	async fn sync_put_queue_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
		index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectItem>,
		store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectItem>,
	) -> tg::Result<()> {
		// Update the graph.
		let mut statuses = Vec::with_capacity(items.len());
		for item in &items {
			let parent = item.parent.clone().map(|either| match either {
				tg::Either::Left(id) => crate::sync::graph::Id::Object(id),
				tg::Either::Right(id) => crate::sync::graph::Id::Process(id),
			});
			let (inserted, stored) = {
				let mut graph = state.graph.lock().unwrap();
				if let Some(token) = &item.token {
					graph.update_object_token(&item.id, token.clone());
				}
				graph.update_object_remote(&item.id, parent, item.kind, None)
			};
			let stored = stored.is_some_and(|stored| stored.subtree);
			statuses.push((inserted, stored));
		}

		// Collect the objects requiring authorization.
		let required = Self::sync_put_object_permissions();
		let mut authorization_args = Vec::new();
		let mut authorization_positions = Vec::new();
		for (position, (item, (inserted, _))) in std::iter::zip(&items, &statuses).enumerate() {
			let requested = if *inserted {
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
		let node = Self::sync_put_object_node_permissions();
		for (item, (inserted, stored)) in std::iter::zip(items, statuses) {
			let permissions = state
				.graph
				.lock()
				.unwrap()
				.get_object_local_authorization(&item.id, node)
				.permissions;
			if !permissions.contains(node) {
				let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage::Object(
					tg::sync::PutMissingObjectMessage { id: item.id },
				));
				state.sender.send(Ok(message)).await.ok();
				continue;
			}
			if !inserted || stored {
				let item = super::index::ObjectItem { id: item.id };
				index_object_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the object to the index task"))?;
			} else {
				let item = super::store::ObjectItem {
					eager: item.eager,
					id: item.id,
					kind: item.kind,
				};
				store_object_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the object to the store task"))?;
			}
		}

		if state.graph.lock().unwrap().end_remote(&state.arg) {
			state.queue.close();
		}

		Ok(())
	}

	async fn sync_put_queue_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
		index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessItem>,
		store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessItem>,
	) -> tg::Result<()> {
		// Update the graph.
		let mut statuses = Vec::with_capacity(items.len());
		for item in &items {
			let parent = item.parent.clone().map(crate::sync::graph::Id::Process);
			let (inserted, stored) = {
				let mut graph = state.graph.lock().unwrap();
				if let Some(token) = &item.token {
					graph.update_process_token(&item.id, token.clone());
				}
				graph.update_process_remote(&item.id, parent, None)
			};
			let stored = stored.is_some_and(|stored| {
				if state.arg.recursive {
					stored.subtree
						&& (!state.arg.commands || stored.subtree_command)
						&& (!state.arg.errors || stored.subtree_error)
						&& (!state.arg.logs || stored.subtree_log)
						&& (!state.arg.outputs || stored.subtree_output)
				} else {
					(!state.arg.commands || stored.node_command)
						&& (!state.arg.errors || stored.node_error)
						&& (!state.arg.logs || stored.node_log)
						&& (!state.arg.outputs || stored.node_output)
				}
			});
			statuses.push((inserted, stored));
		}

		// Collect the processes requiring authorization.
		let required = Self::sync_put_process_permissions(&state.arg);
		let mut authorization_args = Vec::new();
		let mut authorization_positions = Vec::new();
		for (position, (item, (inserted, _))) in std::iter::zip(&items, &statuses).enumerate() {
			let requested = if *inserted {
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
		let node = Self::sync_put_process_node_permissions();
		for (item, (inserted, stored)) in std::iter::zip(items, statuses) {
			let permissions = state
				.graph
				.lock()
				.unwrap()
				.get_process_local_authorization(&item.id, node)
				.permissions;
			if !permissions.contains(node) {
				let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage::Process(
					tg::sync::PutMissingProcessMessage { id: item.id },
				));
				state.sender.send(Ok(message)).await.ok();
				continue;
			}
			if !inserted || stored {
				let item = super::index::ProcessItem { id: item.id };
				index_process_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the process to the index task"))?;
			} else {
				let item = super::store::ProcessItem {
					eager: item.eager,
					id: item.id,
				};
				store_process_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the process to the store task"))?;
			}
		}

		if state.graph.lock().unwrap().end_remote(&state.arg) {
			state.queue.close();
		}

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
		if arg.recursive {
			insert(tg::grant::permission::process::Permission::Subtree);
		}
		for (enabled, node, subtree) in [
			(
				arg.commands,
				tg::grant::permission::process::Permission::NodeCommand,
				tg::grant::permission::process::Permission::SubtreeCommand,
			),
			(
				arg.errors,
				tg::grant::permission::process::Permission::NodeError,
				tg::grant::permission::process::Permission::SubtreeError,
			),
			(
				arg.logs,
				tg::grant::permission::process::Permission::NodeLog,
				tg::grant::permission::process::Permission::SubtreeLog,
			),
			(
				arg.outputs,
				tg::grant::permission::process::Permission::NodeOutput,
				tg::grant::permission::process::Permission::SubtreeOutput,
			),
		] {
			if enabled {
				insert(node);
				if arg.recursive {
					insert(subtree);
				}
			}
		}
		permissions
	}
}
