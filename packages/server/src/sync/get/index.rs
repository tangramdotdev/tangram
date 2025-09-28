use {
	super::{Graph, NodeInner},
	crate::{Server, index::message::ProcessObjectKind},
	bytes::Bytes,
	futures::prelude::*,
	std::{
		collections::BTreeMap,
		pin::pin,
		sync::{Arc, Mutex},
	},
	tangram_client as tg,
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

const PROCESS_COMPLETE_BATCH_SIZE: usize = 8;
const PROCESS_COMPLETE_CONCURRENCY: usize = 8;
const OBJECT_COMPLETE_BATCH_SIZE: usize = 64;
const OBJECT_COMPLETE_CONCURRENCY: usize = 8;
const INDEX_MESSAGE_MAX_BYTES: usize = 1_000_000;

impl Server {
	pub(super) async fn sync_get_index(
		&self,
		graph: Arc<Mutex<Graph>>,
		process_complete_receiver: tokio::sync::mpsc::Receiver<tg::sync::ProcessPutMessage>,
		object_complete_receiver: tokio::sync::mpsc::Receiver<tg::sync::ObjectPutMessage>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		// Create the process future.
		let process_stream = ReceiverStream::new(process_complete_receiver);
		let process_stream = pin!(process_stream);
		let process_future = process_stream
			.ready_chunks(PROCESS_COMPLETE_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(PROCESS_COMPLETE_CONCURRENCY, {
				let server = self.clone();
				let graph = graph.clone();
				let sender = sender.clone();
				move |messages| {
					let server = server.clone();
					let graph = graph.clone();
					let sender = sender.clone();
					async move {
						server
							.sync_get_index_process_batch(messages, graph, sender)
							.await
					}
				}
			});

		// Create the object future.
		let object_stream = ReceiverStream::new(object_complete_receiver);
		let object_stream = pin!(object_stream);
		let object_future = object_stream
			.ready_chunks(OBJECT_COMPLETE_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(OBJECT_COMPLETE_CONCURRENCY, {
				let server = self.clone();
				let graph = graph.clone();
				let sender = sender.clone();
				move |ids| {
					let server = server.clone();
					let graph = graph.clone();
					let sender = sender.clone();
					async move { server.sync_get_index_object_batch(ids, graph, sender).await }
				}
			});

		// Join the futures.
		future::try_join(process_future, object_future).await?;

		// Create the messages.
		let messages = Self::sync_get_index_create_messages(&mut graph.lock().unwrap())?;

		// Publish the messages.
		self.sync_get_index_publish_messages(messages).await?;

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
						command: true,
						commands: true,
						output: true,
						outputs: true,
					};
					let mut metadata = tg::process::Metadata {
						children: tg::process::metadata::Children { count: Some(1) },
						command: tg::object::Metadata {
							count: None,
							depth: None,
							weight: None,
						},
						commands: tg::object::Metadata {
							count: Some(0),
							depth: Some(0),
							weight: Some(0),
						},
						output: tg::object::Metadata {
							count: Some(0),
							depth: Some(0),
							weight: Some(0),
						},
						outputs: tg::object::Metadata {
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
						complete.commands = complete.commands
							&& child_inner.is_some_and(|inner| inner.complete.commands);
						metadata.commands.count = metadata
							.commands
							.count
							.zip(child_inner.and_then(|inner| inner.metadata.commands.count))
							.map(|(a, b)| a + b);
						metadata.commands.depth = metadata
							.commands
							.depth
							.zip(child_inner.and_then(|inner| inner.metadata.commands.depth))
							.map(|(a, b)| a.max(b));
						metadata.commands.weight = metadata
							.commands
							.weight
							.zip(child_inner.and_then(|inner| inner.metadata.commands.weight))
							.map(|(a, b)| a + b);
						metadata.outputs.count = metadata
							.outputs
							.count
							.zip(child_inner.and_then(|inner| inner.metadata.outputs.count))
							.map(|(a, b)| a + b);
						metadata.outputs.depth = metadata
							.outputs
							.depth
							.zip(child_inner.and_then(|inner| inner.metadata.outputs.depth))
							.map(|(a, b)| a.max(b));
						metadata.outputs.weight = metadata
							.outputs
							.weight
							.zip(child_inner.and_then(|inner| inner.metadata.outputs.weight))
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

								complete.commands = complete.commands
									&& object_inner.is_some_and(|inner| inner.complete);
								metadata.commands.count = metadata
									.commands
									.count
									.zip(object_inner.and_then(|inner| inner.metadata.count))
									.map(|(a, b)| a + b);
								metadata.commands.depth = metadata
									.commands
									.depth
									.zip(object_inner.and_then(|inner| inner.metadata.depth))
									.map(|(a, b)| a.max(b));
								metadata.commands.weight = metadata
									.commands
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

								complete.outputs = complete.outputs
									&& object_inner.is_some_and(|inner| inner.complete);
								metadata.outputs.count = metadata
									.outputs
									.count
									.zip(object_inner.and_then(|inner| inner.metadata.count))
									.map(|(a, b)| a + b);
								metadata.outputs.depth = metadata
									.outputs
									.depth
									.zip(object_inner.and_then(|inner| inner.metadata.depth))
									.map(|(a, b)| a.max(b));
								metadata.outputs.weight = metadata
									.outputs
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

	async fn sync_get_index_publish_messages(&self, messages: Vec<Vec<Bytes>>) -> tg::Result<()> {
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

	async fn sync_get_index_process_batch(
		&self,
		messages: Vec<tg::sync::ProcessPutMessage>,
		graph: Arc<Mutex<Graph>>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		// Get the ids.
		let ids = messages
			.iter()
			.map(|message| message.id.clone())
			.collect::<Vec<_>>();

		// Touch the processes and get completes and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let outputs = self
			.try_touch_process_and_get_complete_and_metadata_batch(&ids, touched_at)
			.await?;

		// Handle each message.
		for (message, output) in std::iter::zip(messages, outputs) {
			let (complete, metadata) = output.unwrap_or((
				crate::process::complete::Output::default(),
				tg::process::Metadata::default(),
			));

			// Update the graph.
			let data = serde_json::from_slice(&message.bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the process data"))?;
			graph
				.lock()
				.unwrap()
				.update_process(&message.id, &data, complete.clone(), metadata);

			// If the process is complete, then send a complete message.
			if complete.children || complete.commands || complete.outputs {
				let complete =
					tg::sync::CompleteMessage::Process(tg::sync::ProcessCompleteMessage {
						command_complete: complete.command,
						commands_complete: complete.commands,
						children_complete: complete.children,
						id: message.id,
						output_complete: complete.output,
						outputs_complete: complete.outputs,
					});
				let message = tg::sync::Message::Complete(complete);
				sender.send(Ok(message)).await.ok();
			}
		}

		Ok(())
	}

	async fn sync_get_index_object_batch(
		&self,
		messages: Vec<tg::sync::ObjectPutMessage>,
		graph: Arc<Mutex<Graph>>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		// Get the ids.
		let ids = messages
			.iter()
			.map(|message| message.id.clone())
			.collect::<Vec<_>>();

		// Touch the objects and get completes and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let outputs = self
			.try_touch_object_and_get_complete_and_metadata_batch(&ids, touched_at)
			.await?;

		// Handle each message.
		for (message, output) in std::iter::zip(messages, outputs) {
			let (complete, metadata) = output.unwrap_or((false, tg::object::Metadata::default()));

			// Update the graph.
			let data = tg::object::Data::deserialize(message.id.kind(), message.bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;
			graph
				.lock()
				.unwrap()
				.update_object(&message.id, &data, complete, metadata);

			// If the object is complete, then send a complete message.
			if complete {
				let complete = tg::sync::CompleteMessage::Object(tg::sync::ObjectCompleteMessage {
					id: message.id,
				});
				let message = tg::sync::Message::Complete(complete);
				sender.send(Ok(message)).await.ok();
			}
		}

		Ok(())
	}
}
