use {
	super::graph::{Graph, Node},
	crate::{Server, index::message::ProcessObjectKind, sync::get::State},
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _},
	std::{collections::BTreeMap, sync::Arc},
	tangram_client::prelude::*,
	tangram_messenger::prelude::*,
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
			.try_touch_object_and_get_stored_and_metadata_batch(&ids, touched_at)
			.await?;

		for (item, output) in std::iter::zip(items, outputs) {
			// Update the graph.
			state.graph.lock().unwrap().update_object(
				&item.id,
				None,
				output.as_ref().map(|(stored, _)| stored.clone()),
				output.as_ref().map(|(_, metadata)| metadata.clone()),
				None,
			);

			// If the object is stored, then send a stored message.
			if output.as_ref().is_some_and(|(stored, _)| stored.subtree) {
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
				let stored = output.as_ref().is_some_and(|(stored, _)| stored.subtree);
				if !stored {
					let bytes = self
						.try_get_object_local(&item.id)
						.await?
						.ok_or_else(|| tg::error!("expected the object to exist"))?
						.bytes;
					let data = tg::object::Data::deserialize(item.id.kind(), bytes)?;
					Self::sync_get_enqueue_object_children(state, &item.id, &data, None);
				}
			}
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
			.try_touch_process_and_get_stored_and_metadata_batch(&ids, touched_at)
			.await?;

		for (item, output) in std::iter::zip(items, outputs) {
			// Update the graph.
			state.graph.lock().unwrap().update_process(
				&item.id,
				None,
				output.as_ref().map(|(stored, _)| stored.clone()),
				output.as_ref().map(|(_, metadata)| metadata.clone()),
				None,
			);

			// If the process is partially stored, then send a stored message.
			if let Some(stored) = output.as_ref().map(|(stored, _)| stored) {
				let message = tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Process(
					tg::sync::GetStoredProcessMessage {
						node_command_stored: stored.node_command,
						subtree_command_stored: stored.subtree_command,
						subtree_stored: stored.subtree,
						id: item.id.clone(),
						node_output_stored: stored.node_output,
						subtree_output_stored: stored.subtree_output,
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
				let Some((stored, _)) = output else {
					return Err(tg::error!(id = %item.id, "failed to find the process"));
				};

				// Enqueue the children as necessary.
				let data = self
					.try_get_process_local(&item.id)
					.await?
					.ok_or_else(|| tg::error!("expected the process to exist"))?
					.data;
				Self::sync_get_enqueue_process_children(state, &item.id, &data, Some(&stored));
			}
		}

		Ok(())
	}

	pub(super) async fn sync_get_index_publish(&self, state: Arc<State>) -> tg::Result<()> {
		// Flush the store.
		self.store
			.flush()
			.await
			.map_err(|error| tg::error!(!error, "failed to flush the store"))?;

		// Create the messages.
		let message_max_bytes = self.config.sync.get.index.message_max_bytes;
		let messages = Self::sync_get_index_create_messages(
			&mut state.graph.lock().unwrap(),
			message_max_bytes,
		)?;

		// Publish the messages.
		for messages in messages {
			self.messenger
				.stream_batch_publish("index".to_owned(), messages)
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the messages"))?
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the messages"))?;
		}

		Ok(())
	}

	fn sync_get_index_create_messages(
		graph: &mut Graph,
		message_max_bytes: usize,
	) -> tg::Result<Vec<Vec<Bytes>>> {
		// Get a topological ordering.
		let toposort = petgraph::algo::toposort(&*graph, None)
			.map_err(|_| tg::error!("failed to toposort the graph"))?;

		// Set stored and metadata.
		for index in toposort.into_iter().rev() {
			let (_, node) = graph.nodes.get_index(index).unwrap();
			match node {
				Node::Object(node) => {
					if node.stored.is_some() && node.metadata.is_some() {
						continue;
					}
					let Some(metadata) = &node.metadata else {
						continue;
					};

					// Initialize the stored output.
					let mut stored = true;

					// Initialize the metadata from existing node metadata.
					let mut metadata = tg::object::Metadata {
						node: tg::object::metadata::Node {
							size: metadata.node.size,
							solvable: metadata.node.solvable,
							solved: metadata.node.solved,
						},
						subtree: tg::object::metadata::Subtree {
							count: Some(1),
							depth: Some(1),
							size: Some(metadata.node.size),
							solvable: Some(metadata.node.solvable),
							solved: Some(metadata.node.solved),
						},
					};

					// Handle each child.
					if let Some(children) = &node.children {
						for child_index in children {
							let (_, child_node) = graph.nodes.get_index(*child_index).unwrap();
							let child_node = child_node
								.try_unwrap_object_ref()
								.ok()
								.ok_or_else(|| tg::error!("expected an object"))?;
							stored =
								stored && child_node.stored.clone().unwrap_or_default().subtree;
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
					} else {
						stored = false;
						metadata = tg::object::Metadata::default();
					}

					// Update the node.
					let (_, node) = graph.nodes.get_index_mut(index).unwrap();
					let node = node.unwrap_object_mut();
					node.stored = Some(crate::object::stored::Output { subtree: stored });
					node.metadata = Some(metadata);
				},

				Node::Process(node) => {
					if node.stored.is_some() && node.metadata.is_some() {
						continue;
					}

					// Initialize the stored output.
					let mut stored = crate::process::stored::Output {
						subtree: true,
						subtree_command: true,
						subtree_output: true,
						node_command: true,
						node_output: true,
					};

					// Initialize the metadata.
					let mut metadata = tg::process::Metadata {
						subtree: tg::process::metadata::Subtree {
							count: Some(1),
							command: tg::object::metadata::Subtree {
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
						node: tg::process::metadata::Node {
							command: tg::object::metadata::Subtree {
								count: None,
								depth: None,
								size: None,
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
					if let Some(children) = &node.children {
						for child_index in children {
							let (_, child_node) = graph.nodes.get_index(*child_index).unwrap();
							let child_node =
								child_node.try_unwrap_process_ref().ok().ok_or_else(|| {
									tg::error!("all children of processes must be processes")
								})?;
							stored.subtree = stored.subtree
								&& child_node
									.stored
									.as_ref()
									.is_some_and(|stored| stored.subtree);
							metadata.subtree.count = metadata
								.subtree
								.count
								.zip(child_node.metadata.as_ref().and_then(|m| m.subtree.count))
								.map(|(a, b)| a + b);
							stored.subtree_command = stored.subtree_command
								&& child_node
									.stored
									.as_ref()
									.is_some_and(|stored| stored.subtree_command);
							stored.subtree_output = stored.subtree_output
								&& child_node
									.stored
									.as_ref()
									.is_some_and(|stored| stored.subtree_output);
						}
					} else {
						stored = crate::process::stored::Output::default();
						metadata = tg::process::Metadata::default();
					}

					// Handle the objects.
					if let Some(objects) = &node.objects {
						for (object_index, object_kind) in objects {
							let (_, object_node) = graph.nodes.get_index(*object_index).unwrap();
							let object_node = object_node
								.try_unwrap_object_ref()
								.ok()
								.ok_or_else(|| tg::error!("expected an object"))?;
							match object_kind {
								ProcessObjectKind::Command => {
									stored.node_command =
										object_node.stored.clone().unwrap_or_default().subtree;
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

									stored.subtree_command = stored.subtree_command
										&& object_node.stored.clone().unwrap_or_default().subtree;
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
								ProcessObjectKind::Output => {
									stored.node_output = stored.node_output
										&& object_node.stored.clone().unwrap_or_default().subtree;
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

									stored.subtree_output = stored.subtree_output
										&& object_node.stored.clone().unwrap_or_default().subtree;
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
								_ => (),
							}
						}
					} else {
						stored = crate::process::stored::Output::default();
						metadata = tg::process::Metadata::default();
					}

					// Update the node.
					let (_, node) = graph.nodes.get_index_mut(index).unwrap();
					let node_inner = node.unwrap_process_mut();
					node_inner.stored = Some(stored);
					node_inner.metadata = Some(metadata);
				},
			}
		}

		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Create the messages.
		let mut messages = BTreeMap::new();
		let mut stack = graph
			.nodes
			.iter()
			.enumerate()
			.filter_map(|(index, (_, node))| node.parents().is_empty().then_some((index, 0)))
			.collect::<Vec<_>>();
		while let Some((index, level)) = stack.pop() {
			let (id, node) = graph.nodes.get_index(index).unwrap();
			if !node.marked() {
				continue;
			}
			match node {
				Node::Object(node) => {
					let id = id.unwrap_object_ref().clone();
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
					let message =
						crate::index::Message::PutObject(crate::index::message::PutObject {
							cache_entry: None,
							children,
							id,
							metadata: node.metadata.clone().unwrap(),
							stored: node.stored.clone().unwrap(),
							touched_at,
						});
					messages.entry(level).or_insert(Vec::new()).push(message);
					stack.extend(
						node.children
							.as_ref()
							.unwrap()
							.iter()
							.map(|index| (*index, level + 1)),
					);
				},
				Node::Process(node) => {
					let id = id.unwrap_process_ref().clone();
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
					let stored = node.stored.clone().unwrap();
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
					let message =
						crate::index::Message::PutProcess(crate::index::message::PutProcess {
							children,
							id,
							metadata,
							objects,
							stored,
							touched_at,
						});
					messages.entry(level).or_insert(Vec::new()).push(message);
					stack.extend(
						node.children
							.as_ref()
							.unwrap()
							.iter()
							.map(|index| (*index, level + 1)),
					);
					stack.extend(
						node.objects
							.as_ref()
							.unwrap()
							.iter()
							.map(|(index, _)| (*index, level + 1)),
					);
				},
			}
		}

		// Batch and serialize the messages.
		let mut batched = BTreeMap::new();
		for (level, messages) in messages {
			let mut batches = Vec::new();
			let mut messages = messages.into_iter().peekable();
			while let Some(message) = messages.next() {
				let message = message.serialize()?;
				let mut batch = message.to_vec();
				while let Some(message) = messages.peek() {
					let message = message.serialize()?;
					if batch.len() + message.len() <= message_max_bytes {
						messages.next().unwrap();
						batch.extend_from_slice(&message);
					} else {
						break;
					}
				}
				batches.push(batch.into());
			}
			batched.insert(level, batches);
		}
		let messages = batched.into_values().rev().collect();

		Ok(messages)
	}
}
