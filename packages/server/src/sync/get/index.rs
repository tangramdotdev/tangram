use {
	crate::sync::graph::{Graph, Node, UpdateObjectLocalArg, UpdateProcessLocalArg},
	crate::{Session, sync::get::State},
	futures::{StreamExt as _, TryStreamExt as _},
	num::ToPrimitive as _,
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_object_store::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ObjectItem {
	pub id: tg::object::Id,
	pub missing: bool,
}

pub struct ProcessItem {
	pub id: tg::process::Id,
	pub missing: bool,
}

impl Session {
	pub(super) async fn sync_get_index(
		&self,
		state: Arc<State>,
		index_object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
		index_process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
	) -> tg::Result<()> {
		// Create the objects future.
		let object_batch_size = self.server.config.sync.get.index.object_batch_size;
		let object_batch_timeout = self.server.config.sync.get.index.object_batch_timeout;
		let object_concurrency = self.server.config.sync.get.index.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(index_object_receiver),
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |items| {
			let session = self.clone();
			let state = state.clone();
			async move { session.sync_get_index_object_batch(&state, items).await }
		});

		// Create the processes future.
		let process_batch_size = self.server.config.sync.get.index.process_batch_size;
		let process_batch_timeout = self.server.config.sync.get.index.process_batch_timeout;
		let process_concurrency = self.server.config.sync.get.index.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(index_process_receiver),
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |items| {
			let session = self.clone();
			let state = state.clone();
			async move { session.sync_get_index_process_batch(&state, items).await }
		});

		// Join the objects and processes futures.
		futures::try_join!(objects_future, processes_future)?;

		Ok(())
	}

	pub(super) async fn sync_get_index_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		// Get the ids.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();

		// Touch the objects and get stored and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let outputs = self
			.server
			.index
			.touch_objects(&ids, touched_at, self.server.config.object.time_to_touch)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch and get object metadata"))?;
		let permissions = self.sync_get_authorize_objects(&ids, &outputs).await?;

		for ((item, output), permissions) in
			std::iter::zip(std::iter::zip(items, outputs), permissions)
		{
			// Update the graph.
			let arg = UpdateObjectLocalArg {
				data: None,
				id: &item.id,
				marked: None,
				metadata: output.as_ref().map(|object| object.metadata.clone()),
				permissions,
				requested: None,
				stored: output.as_ref().map(|object| object.stored.clone()),
			};
			state.graph.lock().unwrap().update_object_local(arg);
			let visible = state
				.graph
				.lock()
				.unwrap()
				.get_object_local_visible(&item.id);

			// If the object is visible, then send a stored message.
			if visible.subtree {
				let message = tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Object(
					tg::sync::GetStoredObjectMessage {
						id: item.id.clone(),
					},
				));
				state
					.sender
					.send(Ok(message))
					.await
					.map_err(|error| tg::error!(!error, "failed to send the stored message"))?;
			}

			if item.missing {
				// If the object is not stored, then error.
				if output.is_none() {
					return Err(tg::error!(id = %item.id, "failed to find the object"));
				}

				// If the object's subtree is not visible, then enqueue the children.
				if !visible.subtree {
					// Get the object.
					let bytes = self
						.try_get_object_local(&item.id, false)
						.await
						.map_err(
							|error| tg::error!(!error, id = %item.id, "failed to get the object locally"),
						)?
						.ok_or_else(|| tg::error!(id = %item.id, "expected the object to exist"))?
						.bytes;
					let data = tg::object::Data::deserialize(item.id.kind(), bytes).map_err(
						|error| tg::error!(!error, id = %item.id, "failed to deserialize the object"),
					)?;

					// Update the graph.
					let arg = UpdateObjectLocalArg {
						data: Some(&data),
						id: &item.id,
						marked: None,
						metadata: None,
						permissions: None,
						requested: None,
						stored: None,
					};
					state.graph.lock().unwrap().update_object_local(arg);

					// Enqueue the children.
					Self::sync_get_enqueue_object_children(state, &item.id, &data, None);
				}
			}
		}

		let end = state.graph.lock().unwrap().end_local(&state.arg);
		if end {
			state.queue.close();
		}

		Ok(())
	}

	pub(super) async fn sync_get_index_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
	) -> tg::Result<()> {
		// Get the ids.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();

		// Touch the processes and get stored and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let outputs = self
			.server
			.index
			.touch_processes(&ids, touched_at, self.server.config.process.time_to_touch)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch and get process metadata"))?;
		let permissions = self.sync_get_authorize_processes(&ids, &outputs).await?;

		for ((item, output), permissions) in
			std::iter::zip(std::iter::zip(items, outputs), permissions)
		{
			// Update the graph.
			let arg = UpdateProcessLocalArg {
				data: None,
				id: &item.id,
				marked: None,
				metadata: output.as_ref().map(|p| p.metadata.clone()),
				permissions,
				requested: None,
				stored: output.as_ref().map(|p| p.stored.clone()),
			};
			state.graph.lock().unwrap().update_process_local(arg);
			let visible = state
				.graph
				.lock()
				.unwrap()
				.get_process_local_visible(&item.id);

			// If the process is visible, then send a stored message.
			if Graph::process_visible_any(&visible) {
				let message = tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Process(
					tg::sync::GetStoredProcessMessage {
						id: item.id.clone(),
						node_command_stored: visible.node_command,
						node_error_stored: visible.node_error,
						node_log_stored: visible.node_log,
						node_output_stored: visible.node_output,
						subtree_command_stored: visible.subtree_command,
						subtree_error_stored: visible.subtree_error,
						subtree_log_stored: visible.subtree_log,
						subtree_output_stored: visible.subtree_output,
						subtree_stored: visible.subtree,
					},
				));
				state
					.sender
					.send(Ok(message))
					.await
					.map_err(|error| tg::error!(!error, "failed to send the stored message"))?;
			}

			if item.missing {
				// If the process is not stored, then error.
				if output.is_none() {
					return Err(tg::error!(id = %item.id, "failed to find the process"));
				}

				// Get the process.
				let data = self
					.try_get_process_local(&item.id, false)
					.await
					.map_err(
						|error| tg::error!(!error, id = %item.id, "failed to get the process locally"),
					)?
					.ok_or_else(|| tg::error!(id = %item.id, "expected the process to exist"))?
					.data;

				// Update the graph.
				let arg = UpdateProcessLocalArg {
					data: Some(&data),
					id: &item.id,
					marked: None,
					metadata: None,
					permissions: None,
					requested: None,
					stored: None,
				};
				state.graph.lock().unwrap().update_process_local(arg);

				// Enqueue the children.
				Self::sync_get_enqueue_process_children(state, &item.id, &data, Some(&visible));
			}
		}

		let end = state.graph.lock().unwrap().end_local(&state.arg);
		if end {
			state.queue.close();
		}

		Ok(())
	}

	pub(super) async fn sync_get_index_put(&self, graph: Arc<Mutex<Graph>>) -> tg::Result<()> {
		// Flush the store.
		self.server
			.object_store
			.flush()
			.await
			.map_err(|error| tg::error!(!error, "failed to flush the store"))?;

		// Create the index args.
		let (put_grant_args, put_object_args, put_process_args) = self
			.sync_get_index_create_args(&mut graph.lock().unwrap())
			.map_err(|error| tg::error!(!error, "failed to create the index args"))?;

		// Index the objects and processes.
		self.server
			.index
			.batch(tangram_index::batch::Arg {
				put_grants: put_grant_args,
				put_objects: put_object_args,
				put_processes: put_process_args,
				..Default::default()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to index the sync"))?;

		Ok(())
	}

	fn sync_get_index_create_args(
		&self,
		graph: &mut Graph,
	) -> tg::Result<(
		Vec<tangram_index::grant::put::Arg>,
		Vec<tangram_index::object::put::Arg>,
		Vec<tangram_index::process::put::Arg>,
	)> {
		// Get a reverse topological ordering using Tarjan's algorithm.
		let sccs = petgraph::algo::tarjan_scc(&*graph);
		for scc in &sccs {
			if scc.len() > 1 {
				return Err(tg::error!("the graph had a cycle"));
			}
		}
		let indices = sccs.into_iter().flatten().collect::<Vec<_>>();

		// Set stored and metadata.
		for index in indices.iter().copied() {
			let (_, node) = graph.nodes.get_index(index).unwrap();
			match node {
				Node::Object(node) => {
					let Some(children) = &node.children else {
						continue;
					};
					let Some(metadata) = &node.metadata else {
						continue;
					};
					let existing_metadata = metadata.clone();

					// Initialize the metadata.
					let mut metadata = tg::object::Metadata {
						node: metadata.node.clone(),
						subtree: tg::object::metadata::Subtree {
							count: Some(1),
							depth: Some(1),
							size: Some(metadata.node.size),
							solvable: Some(metadata.node.solvable),
							solved: Some(metadata.node.solved),
						},
					};

					// Handle each child.
					for child_index in children {
						let (_, child_node) = graph.nodes.get_index(*child_index).unwrap();
						let child_node = child_node
							.try_unwrap_object_ref()
							.ok()
							.ok_or_else(|| tg::error!("expected an object"))?;
						metadata.subtree.count = metadata
							.subtree
							.count
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.count),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.depth = metadata
							.subtree
							.depth
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.depth),
							)
							.map(|(a, b)| a.max(1 + b));
						metadata.subtree.size = metadata
							.subtree
							.size
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.size),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.solvable = metadata
							.subtree
							.solvable
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.solvable),
							)
							.map(|(a, b)| a || b);
						metadata.subtree.solved = metadata
							.subtree
							.solved
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.solved),
							)
							.map(|(a, b)| a && b);
					}

					// Merge the existing metadata.
					metadata.merge(&existing_metadata);

					// Update the node.
					let (_, node) = graph.nodes.get_index_mut(index).unwrap();
					let node = node.unwrap_object_mut();
					node.metadata = Some(metadata);
				},

				Node::Process(node) => {
					let Some(children) = &node.children else {
						continue;
					};
					let Some(objects) = &node.objects else {
						continue;
					};

					// Initialize the metadata.
					let mut metadata = tg::process::Metadata {
						node: tg::process::metadata::Node {
							command: tg::object::metadata::Subtree {
								count: None,
								depth: None,
								size: None,
								solvable: None,
								solved: None,
							},
							error: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
							log: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
							output: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
						},
						subtree: tg::process::metadata::Subtree {
							count: Some(1),
							command: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
							error: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
							log: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
							output: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
						},
					};

					// Handle the children.
					for child_index in children {
						let (_, child_node) = graph.nodes.get_index(*child_index).unwrap();
						let child_node =
							child_node.try_unwrap_process_ref().ok().ok_or_else(|| {
								tg::error!("all children of processes must be processes")
							})?;
						metadata.subtree.count = metadata
							.subtree
							.count
							.zip(child_node.metadata.as_ref().and_then(|m| m.subtree.count))
							.map(|(a, b)| a + b);

						// Aggregate child process's subtree command metadata.
						metadata.subtree.command.count = metadata
							.subtree
							.command
							.count
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.command.count),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.command.depth = metadata
							.subtree
							.command
							.depth
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.command.depth),
							)
							.map(|(a, b)| a.max(b));
						metadata.subtree.command.size = metadata
							.subtree
							.command
							.size
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.command.size),
							)
							.map(|(a, b)| a + b);

						// Aggregate the child process's subtree error metadata.
						metadata.subtree.error.count = metadata
							.subtree
							.error
							.count
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.error.count),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.error.depth = metadata
							.subtree
							.error
							.depth
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.error.depth),
							)
							.map(|(a, b)| a.max(b));
						metadata.subtree.error.size = metadata
							.subtree
							.error
							.size
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.error.size),
							)
							.map(|(a, b)| a + b);

						// Aggregate the child process's subtree log metadata.
						metadata.subtree.log.count = metadata
							.subtree
							.log
							.count
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.log.count),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.log.depth = metadata
							.subtree
							.log
							.depth
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.log.depth),
							)
							.map(|(a, b)| a.max(b));
						metadata.subtree.log.size = metadata
							.subtree
							.log
							.size
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.log.size),
							)
							.map(|(a, b)| a + b);

						// Aggregate the child process's subtree output metadata.
						metadata.subtree.output.count = metadata
							.subtree
							.output
							.count
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.output.count),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.output.depth = metadata
							.subtree
							.output
							.depth
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.output.depth),
							)
							.map(|(a, b)| a.max(b));
						metadata.subtree.output.size = metadata
							.subtree
							.output
							.size
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.output.size),
							)
							.map(|(a, b)| a + b);
					}

					// Handle the objects.
					for (object_index, object_kind) in objects {
						let (_, object_node) = graph.nodes.get_index(*object_index).unwrap();
						let object_node = object_node
							.try_unwrap_object_ref()
							.ok()
							.ok_or_else(|| tg::error!("expected an object"))?;
						match object_kind {
							tangram_index::process::object::Kind::Command => {
								metadata.node.command.count = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.count);
								metadata.node.command.depth = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.depth);
								metadata.node.command.size = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.size);

								metadata.subtree.command.count = metadata
									.subtree
									.command
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.subtree.command.depth = metadata
									.subtree
									.command
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.subtree.command.size = metadata
									.subtree
									.command
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);
							},

							tangram_index::process::object::Kind::Error => {
								metadata.node.error.count = metadata
									.node
									.error
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.node.error.depth = metadata
									.node
									.error
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.node.error.size = metadata
									.node
									.error
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);

								metadata.subtree.error.count = metadata
									.subtree
									.error
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.subtree.error.depth = metadata
									.subtree
									.error
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.subtree.error.size = metadata
									.subtree
									.error
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);
							},

							tangram_index::process::object::Kind::Log => {
								metadata.node.log.count = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.count);
								metadata.node.log.depth = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.depth);
								metadata.node.log.size = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.size);

								metadata.subtree.log.count = metadata
									.subtree
									.log
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.subtree.log.depth = metadata
									.subtree
									.log
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.subtree.log.size = metadata
									.subtree
									.log
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);
							},

							tangram_index::process::object::Kind::Output => {
								metadata.node.output.count = metadata
									.node
									.output
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.node.output.depth = metadata
									.node
									.output
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.node.output.size = metadata
									.node
									.output
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);

								metadata.subtree.output.count = metadata
									.subtree
									.output
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.subtree.output.depth = metadata
									.subtree
									.output
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.subtree.output.size = metadata
									.subtree
									.output
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);
							},
						}
					}

					// Merge the existing metadata.
					if let Some(existing) = &node.metadata {
						metadata.merge(existing);
					}

					// Update the node.
					let (_, node) = graph.nodes.get_index_mut(index).unwrap();
					let node_inner = node.unwrap_process_mut();
					node_inner.metadata = Some(metadata);
				},
			}
		}

		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Create the grant args.
		let mut put_grant_args = Vec::new();
		let grant_principal = match self.context.principal.as_ref() {
			Some(tg::Principal::Root) => None,
			Some(principal) => Some(tg::grant::Principal::from(principal.clone())),
			None => Some(tg::grant::Principal::Public),
		};
		if let Some(grant_principal) = grant_principal {
			let object_expires_at = touched_at
				+ self
					.server
					.config
					.object
					.grant_time_to_live
					.as_secs()
					.to_i64()
					.unwrap();
			let process_expires_at = touched_at
				+ self
					.server
					.config
					.process
					.grant_time_to_live
					.as_secs()
					.to_i64()
					.unwrap();
			let mut object_covered = vec![false; graph.nodes.len()];
			let mut process_covered =
				vec![tg::grant::permission::process::Set::empty(); graph.nodes.len()];
			for index in indices.iter().rev().copied() {
				let (id, node) = graph.nodes.get_index(index).unwrap();
				match node {
					Node::Object(node) => {
						let visible = node
							.local_visible
							.as_ref()
							.is_some_and(|visible| visible.subtree);
						let mut subtree = false;
						if node.marked && !object_covered[index] {
							let permission = if visible {
								tg::grant::permission::object::Permission::Subtree
							} else {
								tg::grant::permission::object::Permission::Node
							};
							subtree = visible;
							put_grant_args.push(tangram_index::grant::put::Arg {
								created_at: touched_at,
								creator: self.context.principal.clone(),
								expires_at: Some(object_expires_at),
								permissions: tg::grant::permission::Set::Object(
									tg::grant::permission::object::Set::from_permission(permission),
								),
								principal: grant_principal.clone(),
								resource: id.clone().unwrap_object().into(),
							});
						}
						let covered = object_covered[index] || subtree;
						if covered && let Some(children) = node.children.as_ref() {
							for child in children {
								object_covered[*child] = true;
							}
						}
					},
					Node::Process(node) => {
						let visible = node.local_visible.clone().unwrap_or_default();
						let mut permissions = if node.marked {
							Self::sync_get_index_process_grant_permissions(&visible)
						} else {
							tg::grant::permission::process::Set::empty()
						};
						Self::sync_get_index_remove_process_permissions_covered_by_ancestors(
							&mut permissions,
							process_covered[index],
						);
						if !permissions.is_empty() {
							put_grant_args.push(tangram_index::grant::put::Arg {
								created_at: touched_at,
								creator: self.context.principal.clone(),
								expires_at: Some(process_expires_at),
								permissions: tg::grant::permission::Set::Process(permissions),
								principal: grant_principal.clone(),
								resource: id.clone().unwrap_process().into(),
							});
						}
						let subtree_permissions =
							Self::sync_get_index_process_subtree_permissions(permissions);
						let mut covered = process_covered[index];
						covered.insert(subtree_permissions);
						if let Some(children) = node.children.as_ref() {
							for child in children {
								process_covered[*child].insert(covered);
							}
						}
					},
				}
			}
		}

		// Create the args.
		let mut put_object_args = Vec::new();
		let mut put_process_args = Vec::new();
		let mut visited = std::collections::HashSet::new();
		let mut stack = graph
			.nodes
			.iter()
			.enumerate()
			.filter_map(|(index, (_, node))| node.parents().is_empty().then_some(index))
			.collect::<Vec<_>>();
		while let Some(index) = stack.pop() {
			if !visited.insert(index) {
				continue;
			}
			let (id, node) = graph.nodes.get_index(index).unwrap();
			match node {
				Node::Object(node) => {
					let id = id.unwrap_object_ref().clone();
					if node.marked {
						let children = node
							.children
							.as_ref()
							.unwrap()
							.iter()
							.map(|index| {
								graph
									.nodes
									.get_index(*index)
									.unwrap()
									.0
									.clone()
									.unwrap_object()
							})
							.collect();
						let metadata = node.metadata.clone().unwrap();
						let stored = node.local_stored.clone().unwrap();
						let arg = tangram_index::object::put::Arg {
							cache_entry: None,
							children,
							id,
							metadata,
							stored,
							touched_at,
						};
						put_object_args.push(arg);
					}
					if let Some(children) = node.children.as_ref() {
						stack.extend(children.iter().copied());
					}
				},
				Node::Process(node) => {
					let id = id.unwrap_process_ref().clone();
					if node.marked {
						let children = node
							.children
							.as_ref()
							.unwrap()
							.iter()
							.map(|index| {
								graph
									.nodes
									.get_index(*index)
									.unwrap()
									.0
									.clone()
									.unwrap_process()
							})
							.collect();
						let stored = node.local_stored.clone().unwrap();
						let metadata = node.metadata.clone().unwrap();
						let objects = node
							.objects
							.as_ref()
							.unwrap()
							.iter()
							.copied()
							.map(|(index, kind)| {
								let id = graph
									.nodes
									.get_index(index)
									.unwrap()
									.0
									.clone()
									.unwrap_object();
								(id, kind)
							})
							.collect::<Vec<_>>();
						let mut command = None;
						let mut error = Vec::new();
						let mut log = None;
						let mut output = Vec::new();
						for (object, kind) in objects {
							match kind {
								tangram_index::process::object::Kind::Command => {
									command = Some(object);
								},
								tangram_index::process::object::Kind::Error => {
									error.push(object);
								},
								tangram_index::process::object::Kind::Log => {
									log = Some(object);
								},
								tangram_index::process::object::Kind::Output => {
									output.push(object);
								},
							}
						}
						let command =
							command.ok_or_else(|| tg::error!("expected the command to be set"))?;
						let arg = tangram_index::process::put::Arg {
							children: Some(children),
							command,
							error: Some((!error.is_empty()).then_some(error)),
							id,
							log: Some(log),
							metadata,
							output: Some((!output.is_empty()).then_some(output)),
							parent: None,
							sandbox: None,
							stored,
							touched_at,
						};
						put_process_args.push(arg);
					}
					if let Some(children) = node.children.as_ref() {
						stack.extend(children.iter().copied());
					}
					if let Some(objects) = node.objects.as_ref() {
						stack.extend(objects.iter().map(|(index, _)| *index));
					}
				},
			}
		}

		Ok((put_grant_args, put_object_args, put_process_args))
	}

	fn sync_get_index_process_grant_permissions(
		visible: &tangram_index::process::Stored,
	) -> tg::grant::permission::process::Set {
		let mut permissions = tg::grant::permission::process::Set::empty();
		if visible.subtree {
			permissions.insert(tg::grant::permission::process::Set::SUBTREE);
		} else {
			permissions.insert(tg::grant::permission::process::Set::NODE);
		}
		if visible.subtree_command {
			permissions.insert(tg::grant::permission::process::Set::SUBTREE_COMMAND);
		} else if visible.node_command {
			permissions.insert(tg::grant::permission::process::Set::NODE_COMMAND);
		}
		if visible.subtree_error {
			permissions.insert(tg::grant::permission::process::Set::SUBTREE_ERROR);
		} else if visible.node_error {
			permissions.insert(tg::grant::permission::process::Set::NODE_ERROR);
		}
		if visible.subtree_log {
			permissions.insert(tg::grant::permission::process::Set::SUBTREE_LOG);
		} else if visible.node_log {
			permissions.insert(tg::grant::permission::process::Set::NODE_LOG);
		}
		if visible.subtree_output {
			permissions.insert(tg::grant::permission::process::Set::SUBTREE_OUTPUT);
		} else if visible.node_output {
			permissions.insert(tg::grant::permission::process::Set::NODE_OUTPUT);
		}
		permissions
	}

	fn sync_get_index_remove_process_permissions_covered_by_ancestors(
		permissions: &mut tg::grant::permission::process::Set,
		covered: tg::grant::permission::process::Set,
	) {
		if covered.contains(tg::grant::permission::process::Set::SUBTREE) {
			permissions.remove(tg::grant::permission::process::Set::NODE);
			permissions.remove(tg::grant::permission::process::Set::SUBTREE);
		}
		if covered.contains(tg::grant::permission::process::Set::SUBTREE_COMMAND) {
			permissions.remove(tg::grant::permission::process::Set::NODE_COMMAND);
			permissions.remove(tg::grant::permission::process::Set::SUBTREE_COMMAND);
		}
		if covered.contains(tg::grant::permission::process::Set::SUBTREE_ERROR) {
			permissions.remove(tg::grant::permission::process::Set::NODE_ERROR);
			permissions.remove(tg::grant::permission::process::Set::SUBTREE_ERROR);
		}
		if covered.contains(tg::grant::permission::process::Set::SUBTREE_LOG) {
			permissions.remove(tg::grant::permission::process::Set::NODE_LOG);
			permissions.remove(tg::grant::permission::process::Set::SUBTREE_LOG);
		}
		if covered.contains(tg::grant::permission::process::Set::SUBTREE_OUTPUT) {
			permissions.remove(tg::grant::permission::process::Set::NODE_OUTPUT);
			permissions.remove(tg::grant::permission::process::Set::SUBTREE_OUTPUT);
		}
	}

	fn sync_get_index_process_subtree_permissions(
		permissions: tg::grant::permission::process::Set,
	) -> tg::grant::permission::process::Set {
		let mut subtree_permissions = tg::grant::permission::process::Set::empty();
		if permissions.contains(tg::grant::permission::process::Set::SUBTREE) {
			subtree_permissions.insert(tg::grant::permission::process::Set::SUBTREE);
		}
		if permissions.contains(tg::grant::permission::process::Set::SUBTREE_COMMAND) {
			subtree_permissions.insert(tg::grant::permission::process::Set::SUBTREE_COMMAND);
		}
		if permissions.contains(tg::grant::permission::process::Set::SUBTREE_ERROR) {
			subtree_permissions.insert(tg::grant::permission::process::Set::SUBTREE_ERROR);
		}
		if permissions.contains(tg::grant::permission::process::Set::SUBTREE_LOG) {
			subtree_permissions.insert(tg::grant::permission::process::Set::SUBTREE_LOG);
		}
		if permissions.contains(tg::grant::permission::process::Set::SUBTREE_OUTPUT) {
			subtree_permissions.insert(tg::grant::permission::process::Set::SUBTREE_OUTPUT);
		}
		subtree_permissions
	}
}
