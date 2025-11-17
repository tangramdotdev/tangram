use {
	super::{Graph, NodeInner},
	crate::{Server, index::message::ProcessObjectKind, sync::get::State},
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _},
	std::{collections::BTreeMap, sync::Arc},
	tangram_client::prelude::*,
	tangram_messenger::prelude::*,
	tangram_store::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

const PROCESS_BATCH_SIZE: usize = 16;
const PROCESS_CONCURRENCY: usize = 8;
const OBJECT_BATCH_SIZE: usize = 16;
const OBJECT_CONCURRENCY: usize = 8;

const INDEX_MESSAGE_MAX_BYTES: usize = 1_000_000;

pub struct ProcessItem {
	pub id: tg::process::Id,
	pub missing: bool,
}

pub struct ObjectItem {
	pub id: tg::object::Id,
	pub missing: bool,
}

impl Server {
	pub(super) async fn sync_get_index_task(
		&self,
		state: Arc<State>,
		index_process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
		index_object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
	) -> tg::Result<()> {
		// Create the processes future.
		let processes_future = ReceiverStream::new(index_process_receiver)
			.ready_chunks(PROCESS_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(PROCESS_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move { server.sync_get_index_process_batch(&state, items).await }
			});

		// Create the objects future.
		let objects_future = ReceiverStream::new(index_object_receiver)
			.ready_chunks(OBJECT_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(OBJECT_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move { server.sync_get_index_object_batch(&state, items).await }
			});

		// Join the processes and objects futures.
		futures::try_join!(processes_future, objects_future)?;

		Ok(())
	}

	pub(super) async fn sync_get_index_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
	) -> tg::Result<()> {
		// Get the ids.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();

		// Touch the processes and get complete and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let outputs = self
			.try_touch_process_and_get_complete_and_metadata_batch(&ids, touched_at)
			.await?;

		// TODO update the graph.

		for (item, output) in std::iter::zip(items, outputs) {
			if let Some(complete) = output.as_ref().map(|(complete, _)| complete) {
				let message = tg::sync::GetMessage::Complete(
					tg::sync::GetCompleteMessage::Process(tg::sync::GetCompleteProcessMessage {
						command_complete: complete.command,
						children_commands_complete: complete.children_commands,
						children_complete: complete.children,
						id: item.id.clone(),
						output_complete: complete.output,
						children_outputs_complete: complete.children_outputs,
					}),
				);
				state
					.sender
					.send(Ok(message))
					.await
					.map_err(|source| tg::error!(!source, "failed to send the complete message"))?;
			}

			if item.missing {
				// If the process is not stored, then error.
				let Some((complete, _)) = output else {
					return Err(tg::error!(id = %item.id, "failed to find the process"));
				};

				// Enqueue the children as necessary.
				let data = self
					.try_get_process_local(&item.id)
					.await?
					.ok_or_else(|| tg::error!("expected the process to exist"))?
					.data;
				Self::sync_get_enqueue_process_children(state, &item.id, &data, &complete);
			}
		}

		Ok(())
	}

	pub(super) async fn sync_get_index_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		// Get the ids.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();

		// Touch the objects and get complete and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let outputs = self
			.try_touch_object_and_get_complete_and_metadata_batch(&ids, touched_at)
			.await?;

		// TODO update the graph.

		for (item, output) in std::iter::zip(items, outputs) {
			// If the object is complete, then send a complete message.
			if output.as_ref().is_some_and(|(complete, _)| *complete) {
				let message = tg::sync::GetMessage::Complete(tg::sync::GetCompleteMessage::Object(
					tg::sync::GetCompleteObjectMessage {
						id: item.id.clone(),
					},
				));
				state
					.sender
					.send(Ok(message))
					.await
					.map_err(|source| tg::error!(!source, "failed to send the complete message"))?;
			}

			if item.missing {
				// If the object is not stored, then error.
				if output.is_none() {
					return Err(tg::error!(id = %item.id, "failed to find the object"));
				}

				// If the object is not complete, then enqueue the children.
				let complete = output.as_ref().is_some_and(|(complete, _)| *complete);
				if !complete {
					let bytes = self
						.try_get_object_local(&item.id)
						.await?
						.ok_or_else(|| tg::error!("expected the object to exist"))?
						.bytes;
					let data = tg::object::Data::deserialize(item.id.kind(), bytes)?;
					Self::sync_get_enqueue_object_children(state, &item.id, &data);
				}
			}
		}

		Ok(())
	}

	pub(super) async fn sync_get_index(&self, state: Arc<State>) -> tg::Result<()> {
		// Flush the store.
		self.store
			.flush()
			.await
			.map_err(|error| tg::error!(!error, "failed to flush the store"))?;

		// Create the messages.
		let messages = Self::sync_get_index_create_messages(&mut state.graph.lock().unwrap())?;

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

	fn sync_get_index_create_messages(graph: &mut Graph) -> tg::Result<Vec<Vec<Bytes>>> {
		// Get a topological ordering.
		let toposort = petgraph::algo::toposort(&*graph, None)
			.map_err(|_| tg::error!("failed to toposort the graph"))?;

		// Set complete and metadata.
		for index in toposort.into_iter().rev() {
			let (_, node) = graph.nodes.get_index(index).unwrap();
			let Some(node_inner) = &node.inner else {
				continue;
			};
			match node_inner {
				NodeInner::Process(node) => {
					let mut complete = crate::process::complete::Output {
						children: true,
						children_commands: true,
						children_outputs: true,
						command: true,
						output: true,
					};
					let mut metadata = tg::process::Metadata {
						children: tg::process::metadata::Children { count: Some(1) },
						children_commands: tg::object::Metadata {
							count: Some(0),
							depth: Some(0),
							weight: Some(0),
						},
						children_outputs: tg::object::Metadata {
							count: Some(0),
							depth: Some(0),
							weight: Some(0),
						},
						command: tg::object::Metadata {
							count: None,
							depth: None,
							weight: None,
						},
						output: tg::object::Metadata {
							count: Some(0),
							depth: Some(0),
							weight: Some(0),
						},
					};
					for child_index in &node.children {
						let (_, child_node) = graph.nodes.get_index(*child_index).unwrap();
						let child_inner = if let Some(child_inner) = &child_node.inner {
							let child_inner =
								child_inner.try_unwrap_process_ref().ok().ok_or_else(|| {
									tg::error!("all children of processes must be processes")
								})?;
							Some(child_inner)
						} else {
							None
						};
						complete.children = complete.children
							&& child_inner.is_some_and(|inner| inner.complete.children);
						metadata.children.count = metadata
							.children
							.count
							.zip(child_inner.and_then(|inner| inner.metadata.children.count))
							.map(|(a, b)| a + b);
						complete.children_commands = complete.children_commands
							&& child_inner.is_some_and(|inner| inner.complete.children_commands);
						metadata.children_commands.count = metadata
							.children_commands
							.count
							.zip(
								child_inner
									.and_then(|inner| inner.metadata.children_commands.count),
							)
							.map(|(a, b)| a + b);
						metadata.children_commands.depth = metadata
							.children_commands
							.depth
							.zip(
								child_inner
									.and_then(|inner| inner.metadata.children_commands.depth),
							)
							.map(|(a, b)| a.max(b));
						metadata.children_commands.weight = metadata
							.children_commands
							.weight
							.zip(
								child_inner
									.and_then(|inner| inner.metadata.children_commands.weight),
							)
							.map(|(a, b)| a + b);
						metadata.children_outputs.count = metadata
							.children_outputs
							.count
							.zip(
								child_inner.and_then(|inner| inner.metadata.children_outputs.count),
							)
							.map(|(a, b)| a + b);
						metadata.children_outputs.depth = metadata
							.children_outputs
							.depth
							.zip(
								child_inner.and_then(|inner| inner.metadata.children_outputs.depth),
							)
							.map(|(a, b)| a.max(b));
						metadata.children_outputs.weight = metadata
							.children_outputs
							.weight
							.zip(
								child_inner
									.and_then(|inner| inner.metadata.children_outputs.weight),
							)
							.map(|(a, b)| a + b);
					}
					for (object_index, object_kind) in &node.objects {
						let (_, object_node) = graph.nodes.get_index(*object_index).unwrap();
						let object_inner = if let Some(object_inner) = &object_node.inner {
							let object_inner = object_inner
								.try_unwrap_object_ref()
								.ok()
								.ok_or_else(|| tg::error!("expected an object"))?;
							Some(object_inner)
						} else {
							None
						};
						match object_kind {
							ProcessObjectKind::Command => {
								complete.command = object_inner.is_some_and(|inner| inner.complete);
								metadata.command.count =
									object_inner.and_then(|inner| inner.metadata.count);
								metadata.command.depth =
									object_inner.and_then(|inner| inner.metadata.depth);
								metadata.command.weight =
									object_inner.and_then(|inner| inner.metadata.weight);

								complete.children_commands = complete.children_commands
									&& object_inner.is_some_and(|inner| inner.complete);
								metadata.children_commands.count = metadata
									.children_commands
									.count
									.zip(object_inner.and_then(|inner| inner.metadata.count))
									.map(|(a, b)| a + b);
								metadata.children_commands.depth = metadata
									.children_commands
									.depth
									.zip(object_inner.and_then(|inner| inner.metadata.depth))
									.map(|(a, b)| a.max(b));
								metadata.children_commands.weight = metadata
									.children_commands
									.weight
									.zip(object_inner.and_then(|inner| inner.metadata.weight))
									.map(|(a, b)| a + b);
							},
							ProcessObjectKind::Output => {
								complete.output = complete.output
									&& object_inner.is_some_and(|inner| inner.complete);
								metadata.output.count = metadata
									.output
									.count
									.zip(object_inner.and_then(|inner| inner.metadata.count))
									.map(|(a, b)| a + b);
								metadata.output.depth = metadata
									.output
									.depth
									.zip(object_inner.and_then(|inner| inner.metadata.depth))
									.map(|(a, b)| a.max(b));
								metadata.output.weight = metadata
									.output
									.weight
									.zip(object_inner.and_then(|inner| inner.metadata.weight))
									.map(|(a, b)| a + b);

								complete.children_outputs = complete.children_outputs
									&& object_inner.is_some_and(|inner| inner.complete);
								metadata.children_outputs.count = metadata
									.children_outputs
									.count
									.zip(object_inner.and_then(|inner| inner.metadata.count))
									.map(|(a, b)| a + b);
								metadata.children_outputs.depth = metadata
									.children_outputs
									.depth
									.zip(object_inner.and_then(|inner| inner.metadata.depth))
									.map(|(a, b)| a.max(b));
								metadata.children_outputs.weight = metadata
									.children_outputs
									.weight
									.zip(object_inner.and_then(|inner| inner.metadata.weight))
									.map(|(a, b)| a + b);
							},
							_ => {},
						}
					}
					let (_, node) = graph.nodes.get_index_mut(index).unwrap();
					let node_inner = node.inner.as_mut().unwrap().unwrap_process_mut();
					node_inner.complete = complete;
					node_inner.metadata = metadata;
				},
				NodeInner::Object(node) => {
					let mut complete = true;
					let mut metadata = tg::object::Metadata {
						count: Some(1),
						depth: Some(1),
						weight: Some(node.size),
					};
					for child_index in &node.children {
						let (_, child_node) = graph.nodes.get_index(*child_index).unwrap();
						let child_inner = if let Some(child_inner) = &child_node.inner {
							let child_inner = child_inner
								.try_unwrap_object_ref()
								.ok()
								.ok_or_else(|| tg::error!("expected an object"))?;
							Some(child_inner)
						} else {
							None
						};
						complete = complete && child_inner.is_some_and(|inner| inner.complete);
						metadata.count = metadata
							.count
							.zip(child_inner.and_then(|inner| inner.metadata.count))
							.map(|(a, b)| a + b);
						metadata.depth = metadata
							.depth
							.zip(child_inner.and_then(|inner| inner.metadata.depth))
							.map(|(a, b)| a.max(1 + b));
						metadata.weight = metadata
							.weight
							.zip(child_inner.and_then(|inner| inner.metadata.weight))
							.map(|(a, b)| a + b);
					}
					let (_, node) = graph.nodes.get_index_mut(index).unwrap();
					let node_inner = node.inner.as_mut().unwrap().unwrap_object_mut();
					node_inner.complete = complete;
					node_inner.metadata = metadata;
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
			.filter_map(|(index, (_, node))| node.parents.is_empty().then_some((index, 0)))
			.collect::<Vec<_>>();
		while let Some((index, level)) = stack.pop() {
			let (id, node) = graph.nodes.get_index(index).unwrap();
			let Some(node_inner) = &node.inner else {
				continue;
			};
			if !node.stored {
				continue;
			}
			match node_inner {
				NodeInner::Process(node) => {
					let id = id.unwrap_process_ref().clone();
					let children = node
						.children
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
					let complete = node.complete.clone();
					let metadata = node.metadata.clone();
					let objects = node
						.objects
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
							complete,
							id,
							metadata,
							objects,
							touched_at,
						});
					messages.entry(level).or_insert(Vec::new()).push(message);
					stack.extend(node.children.iter().map(|index| (*index, level + 1)));
					stack.extend(node.objects.iter().map(|(index, _)| (*index, level + 1)));
				},
				NodeInner::Object(node) => {
					let id = id.unwrap_object_ref().clone();
					let children = node
						.children
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
							complete: node.complete,
							id,
							metadata: node.metadata.clone(),
							size: node.size,
							touched_at,
						});
					messages.entry(level).or_insert(Vec::new()).push(message);
					stack.extend(node.children.iter().map(|index| (*index, level + 1)));
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
					if batch.len() + message.len() <= INDEX_MESSAGE_MAX_BYTES {
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
