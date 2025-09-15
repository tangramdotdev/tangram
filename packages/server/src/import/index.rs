use super::{
	Graph, INDEX_MESSAGE_MAX_BYTES, NodeInner, OBJECT_COMPLETE_BATCH_SIZE,
	OBJECT_COMPLETE_CONCURRENCY, PROCESS_COMPLETE_BATCH_SIZE, PROCESS_COMPLETE_CONCURRENCY,
};
use crate::{Server, index::message::ProcessObjectKind};
use futures::StreamExt as _;
use std::{
	collections::BTreeMap,
	pin::pin,
	sync::{Arc, Mutex},
};
use tangram_client as tg;
use tangram_messenger::prelude::*;
use tokio_stream::wrappers::ReceiverStream;

impl Server {
	pub(super) async fn import_index_task(
		&self,
		graph: Arc<Mutex<Graph>>,
		process_complete_receiver: tokio::sync::mpsc::Receiver<tg::export::ProcessItem>,
		object_complete_receiver: tokio::sync::mpsc::Receiver<tg::export::ObjectItem>,
		event_sender: tokio::sync::mpsc::Sender<tg::Result<tg::import::Event>>,
	) -> tg::Result<()> {
		// Create the process future.
		let process_stream = ReceiverStream::new(process_complete_receiver);
		let process_stream = pin!(process_stream);
		let process_future = process_stream
			.ready_chunks(PROCESS_COMPLETE_BATCH_SIZE)
			.for_each_concurrent(PROCESS_COMPLETE_CONCURRENCY, {
				let server = self.clone();
				let graph = graph.clone();
				let event_sender = event_sender.clone();
				move |items| {
					let server = server.clone();
					let graph = graph.clone();
					let event_sender = event_sender.clone();
					async move {
						server
							.import_index_task_process_batch(items, graph, event_sender)
							.await;
					}
				}
			});

		// Create the object future.
		let object_stream = ReceiverStream::new(object_complete_receiver);
		let object_stream = pin!(object_stream);
		let object_future = object_stream
			.ready_chunks(OBJECT_COMPLETE_BATCH_SIZE)
			.for_each_concurrent(OBJECT_COMPLETE_CONCURRENCY, {
				let server = self.clone();
				let graph = graph.clone();
				let event_sender = event_sender.clone();
				move |ids| {
					let server = server.clone();
					let graph = graph.clone();
					let event_sender = event_sender.clone();
					async move {
						server
							.import_index_task_object_batch(ids, graph, event_sender)
							.await;
					}
				}
			});

		// Join the futures.
		futures::join!(process_future, object_future);

		// Get the graph.
		let graph = Arc::into_inner(graph).unwrap().into_inner().unwrap();

		// Publish the index messages.
		self.import_index_publish(graph).await?;

		Ok(())
	}

	async fn import_index_publish(&self, mut graph: Graph) -> tg::Result<()> {
		// Get a topological ordering.
		let toposort = petgraph::algo::toposort(&graph, None)
			.map_err(|_| tg::error!("failed to toposort the graph"))?;

		// Set the items' complete and metadata.
		for index in toposort.into_iter().rev() {
			let (_, node) = graph.nodes.get_index(index).unwrap();
			let Some(node_inner) = &node.inner else {
				continue;
			};
			match node_inner {
				NodeInner::Process(node) => {
					let mut complete = crate::process::complete::Output {
						children: true,
						commands: true,
						outputs: true,
					};
					let mut metadata = tg::process::Metadata {
						children: tg::process::metadata::Children { count: Some(1) },
						commands: tg::object::Metadata {
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
		let messages = batched;

		// Publish the messages.
		for messages in messages.into_values().rev() {
			self.messenger
				.stream_batch_publish("index".to_owned(), messages)
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the messages"))?
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the messages"))?;
		}

		Ok(())
	}

	async fn import_index_task_process_batch(
		&self,
		items: Vec<tg::export::ProcessItem>,
		graph: Arc<Mutex<Graph>>,
		event_sender: tokio::sync::mpsc::Sender<tg::Result<tg::import::Event>>,
	) {
		// Get the ids.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();

		// Touch the processes and get completes and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let result = self
			.try_touch_process_and_get_complete_and_metadata_batch(&ids, touched_at)
			.await;
		let outputs = match result {
			Ok(outputs) => outputs,
			Err(error) => {
				tracing::error!(?error, "failed to get the process complete batch");
				return;
			},
		};

		// Handle each item.
		for (item, output) in std::iter::zip(items, outputs) {
			let (complete, metadata) = output.unwrap_or((
				crate::process::complete::Output::default(),
				tg::process::Metadata::default(),
			));

			// Update the graph.
			graph
				.lock()
				.unwrap()
				.update_process(&item.id, &item.data, complete.clone(), metadata);

			// If the process is complete, then send the complete event.
			if complete.children || complete.commands || complete.outputs {
				let complete = tg::import::Complete::Process(tg::import::ProcessComplete {
					commands_complete: complete.commands,
					complete: complete.children,
					id: item.id,
					outputs_complete: complete.outputs,
				});
				let event = tg::import::Event::Complete(complete);
				event_sender.send(Ok(event)).await.ok();
			}
		}
	}

	async fn import_index_task_object_batch(
		&self,
		items: Vec<tg::export::ObjectItem>,
		graph: Arc<Mutex<Graph>>,
		event_sender: tokio::sync::mpsc::Sender<tg::Result<tg::import::Event>>,
	) {
		// Get the ids.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();

		// Touch the objects and get completes and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let result = self
			.try_touch_object_and_get_complete_and_metadata_batch(&ids, touched_at)
			.await;
		let outputs = match result {
			Ok(outputs) => outputs,
			Err(error) => {
				tracing::error!(
					?error,
					"failed to touch the objects and get completes and metadata"
				);
				return;
			},
		};

		// Handle each item.
		for (item, output) in std::iter::zip(items, outputs) {
			let (complete, metadata) = output.unwrap_or((false, tg::object::Metadata::default()));

			// Update the graph.
			graph
				.lock()
				.unwrap()
				.update_object(&item.id, &item.data, complete, metadata);

			// If the object is complete, then send the complete event.
			if complete {
				let complete =
					tg::import::Complete::Object(tg::import::ObjectComplete { id: item.id });
				let event = tg::import::Event::Complete(complete);
				event_sender.send(Ok(event)).await.ok();
			}
		}
	}
}
