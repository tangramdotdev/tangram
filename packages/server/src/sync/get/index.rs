use {
	crate::sync::graph::{Graph, Node},
	crate::{Server, sync::get::State},
	futures::{StreamExt as _, TryStreamExt as _},
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_store::prelude::*,
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

impl Server {
	pub(super) async fn sync_get_index(
		&self,
		state: Arc<State>,
		index_object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
		index_process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
	) -> tg::Result<()> {
		// Create the objects future.
		let object_batch_size = self.config.sync.get.index.object_batch_size;
		let object_batch_timeout = self.config.sync.get.index.object_batch_timeout;
		let object_concurrency = self.config.sync.get.index.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(index_object_receiver),
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |items| {
			let server = self.clone();
			let state = state.clone();
			async move { server.sync_get_index_object_batch(&state, items).await }
		});

		// Create the processes future.
		let process_batch_size = self.config.sync.get.index.process_batch_size;
		let process_batch_timeout = self.config.sync.get.index.process_batch_timeout;
		let process_concurrency = self.config.sync.get.index.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(index_process_receiver),
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |items| {
			let server = self.clone();
			let state = state.clone();
			async move { server.sync_get_index_process_batch(&state, items).await }
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
			.index
			.touch_objects(&ids, touched_at)
			.await
			.map_err(|source| tg::error!(!source, "failed to touch and get object metadata"))?;

		for (item, output) in std::iter::zip(items, outputs) {
			// Update the graph.
			state.graph.lock().unwrap().update_object_local(
				&item.id,
				None,
				output.as_ref().map(|object| object.stored.clone()),
				output.as_ref().map(|object| object.metadata.clone()),
				None,
				None,
			);

			// If the object is stored, then send a stored message.
			if output.as_ref().is_some_and(|object| object.stored.subtree) {
				let message = tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Object(
					tg::sync::GetStoredObjectMessage {
						id: item.id.clone(),
					},
				));
				state
					.sender
					.send(Ok(message))
					.await
					.map_err(|source| tg::error!(!source, "failed to send the stored message"))?;
			}

			if item.missing {
				// If the object is not stored, then error.
				if output.is_none() {
					return Err(tg::error!(id = %item.id, "failed to find the object"));
				}

				// If the object's subtree is not stored, then enqueue the children.
				let stored = output.as_ref().is_some_and(|object| object.stored.subtree);
				if !stored {
					// Get the object.
					let bytes = self
						.try_get_object_local(&item.id, false)
						.await
						.map_err(
							|source| tg::error!(!source, id = %item.id, "failed to get the object locally"),
						)?
						.ok_or_else(|| tg::error!(id = %item.id, "expected the object to exist"))?
						.bytes;
					let data = tg::object::Data::deserialize(item.id.kind(), bytes).map_err(
						|source| tg::error!(!source, id = %item.id, "failed to deserialize the object"),
					)?;

					// Update the graph.
					state.graph.lock().unwrap().update_object_local(
						&item.id,
						Some(&data),
						None,
						None,
						None,
						None,
					);

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
			.index
			.touch_processes(&ids, touched_at)
			.await
			.map_err(|source| tg::error!(!source, "failed to touch and get process metadata"))?;

		for (item, output) in std::iter::zip(items, outputs) {
			// Update the graph.
			state.graph.lock().unwrap().update_process_local(
				&item.id,
				None,
				output.as_ref().map(|p| p.stored.clone()),
				output.as_ref().map(|p| p.metadata.clone()),
				None,
				None,
			);

			// If the process is partially stored, then send a stored message.
			if let Some(stored) = output.as_ref().map(|p| &p.stored) {
				let message = tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Process(
					tg::sync::GetStoredProcessMessage {
						id: item.id.clone(),
						node_command_stored: stored.node_command,
						node_error_stored: stored.node_error,
						node_log_stored: stored.node_log,
						node_output_stored: stored.node_output,
						subtree_command_stored: stored.subtree_command,
						subtree_error_stored: stored.subtree_error,
						subtree_log_stored: stored.subtree_log,
						subtree_output_stored: stored.subtree_output,
						subtree_stored: stored.subtree,
					},
				));
				state
					.sender
					.send(Ok(message))
					.await
					.map_err(|source| tg::error!(!source, "failed to send the stored message"))?;
			}

			if item.missing {
				// If the process is not stored, then error.
				let Some(process) = output else {
					return Err(tg::error!(id = %item.id, "failed to find the process"));
				};

				// Get the process.
				let data = self
					.try_get_process_local(&item.id, false)
					.await
					.map_err(
						|source| tg::error!(!source, id = %item.id, "failed to get the process locally"),
					)?
					.ok_or_else(|| tg::error!(id = %item.id, "expected the process to exist"))?
					.data;

				// Update the graph.
				state.graph.lock().unwrap().update_process_local(
					&item.id,
					Some(&data),
					None,
					None,
					None,
					None,
				);

				// Enqueue the children.
				Self::sync_get_enqueue_process_children(
					state,
					&item.id,
					&data,
					Some(&process.stored),
				);
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
		self.store
			.flush()
			.await
			.map_err(|error| tg::error!(!error, "failed to flush the store"))?;

		// Create the index args.
		let (put_object_args, put_process_args) =
			Self::sync_get_index_create_args(&mut graph.lock().unwrap())
				.map_err(|source| tg::error!(!source, "failed to create the index args"))?;

		// Index the objects and processes.
		self.index
			.put(tangram_index::PutArg {
				objects: put_object_args,
				processes: put_process_args,
				..Default::default()
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to index the sync"))?;

		Ok(())
	}

	fn sync_get_index_create_args(
		graph: &mut Graph,
	) -> tg::Result<(
		Vec<tangram_index::PutObjectArg>,
		Vec<tangram_index::PutProcessArg>,
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
		for index in indices {
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
							tangram_index::ProcessObjectKind::Command => {
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

							tangram_index::ProcessObjectKind::Error => {
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

							tangram_index::ProcessObjectKind::Log => {
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

							tangram_index::ProcessObjectKind::Output => {
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

		// Create the args.
		let mut put_object_args = Vec::new();
		let mut put_process_args = Vec::new();
		let mut stack = graph
			.nodes
			.iter()
			.enumerate()
			.filter_map(|(index, (_, node))| node.parents().is_empty().then_some(index))
			.collect::<Vec<_>>();
		while let Some(index) = stack.pop() {
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
						let arg = tangram_index::PutObjectArg {
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
							.collect();
						let arg = tangram_index::PutProcessArg {
							children,
							id,
							metadata,
							objects,
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

		Ok((put_object_args, put_process_args))
	}
}
