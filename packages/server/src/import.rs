use self::{graph::Graph, progress::Progress};
use crate::{Server, index::message::ProcessObjectKind, store::Store};
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future, stream,
};
use graph::NodeInner;
use num::ToPrimitive as _;
use std::{
	collections::BTreeMap,
	pin::{Pin, pin},
	sync::{Arc, Mutex},
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::prelude::*;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

mod graph;
mod progress;

const PROCESS_COMPLETE_BATCH_SIZE: usize = 8;
const PROCESS_COMPLETE_CONCURRENCY: usize = 8;
const OBJECT_COMPLETE_BATCH_SIZE: usize = 64;
const OBJECT_COMPLETE_CONCURRENCY: usize = 8;
const INDEX_MESSAGE_MAX_BYTES: usize = 1_000_000;

impl Server {
	pub async fn import(
		&self,
		mut arg: tg::import::Arg,
		mut stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::import::Event>> + Send + 'static> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let client = self.get_remote_client(remote.clone()).await?;
			let stream = client.import(arg, stream).await?;
			return Ok(stream.boxed());
		}

		// Check if the items are already complete.
		let complete = self.import_items_complete(&arg).await.unwrap_or_default();
		if complete {
			let event = tg::import::Event::End;
			let stream = stream::once(future::ok(event)).boxed();
			return Ok(stream);
		}

		// Create the progress.
		let processes = arg.items.iter().flatten().any(Either::is_left);
		let progress = Arc::new(Progress::new(processes));

		// Create the channels.
		let (event_sender, event_receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::import::Event>>(256);
		let (process_index_sender, process_index_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ProcessItem>(256);
		let (object_index_sender, object_index_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ObjectItem>(256);
		let (process_sender, process_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ProcessItem>(256);
		let (object_sender, object_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ObjectItem>(256);

		// Spawn the import index task.
		self.import_index_tasks
			.lock()
			.unwrap()
			.as_mut()
			.unwrap()
			.spawn({
				let server = self.clone();
				let event_sender = event_sender.clone();
				async move {
					if let Err(error) = server
						.import_index_task(
							process_index_receiver,
							object_index_receiver,
							event_sender,
						)
						.await
					{
						tracing::error!(?error, "the import index task failed");
					}
				}
			});

		// Spawn the processes task.
		let processes_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				server
					.import_processes_task(process_receiver, &progress)
					.await
			}
		}));

		// Spawn the objects task.
		let objects_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move { server.import_objects_task(object_receiver, &progress).await }
		}));

		// Spawn a task that sends items from the stream to the other tasks.
		let task = AbortOnDropHandle::new(tokio::spawn({
			let event_sender = event_sender.clone();
			async move {
				// Read the items from the stream and send them to the tasks.
				loop {
					let event = match stream.try_next().await {
						Ok(Some(event)) => event,
						Ok(None) => break,
						Err(error) => {
							event_sender.send(Err(error)).await.ok();
							return;
						},
					};
					let tg::export::Event::Item(item) = event else {
						continue;
					};
					let index_sender_future = match &item {
						tg::export::Item::Process(item) => process_index_sender
							.send(item.clone())
							.map_err(|_| ())
							.left_future(),
						tg::export::Item::Object(item) => object_index_sender
							.send(item.clone())
							.map_err(|_| ())
							.right_future(),
					};
					let (process_send_future, object_send_future) = match item {
						tg::export::Item::Process(item) => (
							process_sender.send(item).map_err(|_| ()).left_future(),
							future::ok(()).left_future(),
						),
						tg::export::Item::Object(item) => (
							future::ok(()).right_future(),
							object_sender.send(item).map_err(|_| ()).right_future(),
						),
					};
					let result = futures::try_join!(
						index_sender_future,
						process_send_future,
						object_send_future
					);
					if result.is_err() {
						event_sender
							.send(Err(tg::error!(?result, "failed to send the item")))
							.await
							.ok();
						return;
					}
				}

				// Close the channels
				drop(process_index_sender);
				drop(object_index_sender);
				drop(process_sender);
				drop(object_sender);

				// Join the processes and objects tasks.
				let result = futures::try_join!(processes_task, objects_task);

				match result {
					Ok((Ok(()), Ok(()))) => {
						let event = tg::import::Event::End;
						event_sender.send(Ok(event)).await.ok();
					},
					Ok((_, Err(error)) | (Err(error), _)) => {
						event_sender.send(Err(error)).await.ok();
					},
					Err(error) => {
						let error = tg::error!(!error, "the task panicked");
						event_sender.send(Err(error)).await.ok();
					},
				}
			}
		}));

		// Create the stream.
		let event_stream = ReceiverStream::new(event_receiver);
		let progress_stream = progress.stream().map_ok(tg::import::Event::Progress);
		let stream = stream::select(event_stream, progress_stream)
			.take_while_inclusive(|event| {
				future::ready(!matches!(event, Err(_) | Ok(tg::import::Event::End)))
			})
			.attach(task);

		Ok(stream.boxed())
	}

	async fn import_items_complete(&self, arg: &tg::import::Arg) -> tg::Result<bool> {
		let Some(items) = &arg.items else {
			return Ok(false);
		};
		let processes = items
			.iter()
			.filter_map(|item| item.clone().left())
			.collect::<Vec<_>>();
		let objects = items
			.iter()
			.filter_map(|item| item.clone().right())
			.collect::<Vec<_>>();
		let (process_completes, object_completes) = futures::try_join!(
			self.try_get_process_complete_batch(&processes),
			self.try_get_object_complete_batch(&objects),
		)?;
		let processes_complete = process_completes.iter().all(|option| {
			option.as_ref().is_some_and(|process_complete| {
				process_complete.children && process_complete.commands && process_complete.outputs
			})
		});
		let objects_complete = object_completes
			.iter()
			.all(|option| option.is_some_and(|object_complete| object_complete));
		let complete = processes_complete && objects_complete;
		Ok(complete)
	}

	async fn import_index_task(
		&self,
		process_complete_receiver: tokio::sync::mpsc::Receiver<tg::export::ProcessItem>,
		object_complete_receiver: tokio::sync::mpsc::Receiver<tg::export::ObjectItem>,
		event_sender: tokio::sync::mpsc::Sender<tg::Result<tg::import::Event>>,
	) -> tg::Result<()> {
		// Create the graph.
		let graph = Arc::new(Mutex::new(Graph::new()));

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
		let mut graph = Arc::into_inner(graph).unwrap().into_inner().unwrap();

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
						commands: tg::object::Metadata {
							count: Some(0),
							depth: Some(0),
							weight: Some(0),
						},
						count: Some(1),
						outputs: tg::object::Metadata {
							count: Some(0),
							depth: Some(0),
							weight: Some(0),
						},
					};
					for child_index in &node.children {
						let (_, child_node) = graph.nodes.get_index(*child_index).unwrap();
						let Some(child_inner) = &child_node.inner else {
							continue;
						};
						let child_inner =
							child_inner.try_unwrap_process_ref().ok().ok_or_else(|| {
								tg::error!("all children of processes must be processes")
							})?;
						complete.children = complete.children && child_inner.complete.children;
						metadata.count = metadata
							.count
							.zip(child_inner.metadata.count)
							.map(|(a, b)| a + b);
					}
					for (object_index, object_kind) in &node.objects {
						let (_, object_node) = graph.nodes.get_index(*object_index).unwrap();
						let Some(object_inner) = &object_node.inner else {
							continue;
						};
						let object_inner = object_inner
							.try_unwrap_object_ref()
							.ok()
							.ok_or_else(|| tg::error!("expected an object"))?;
						match object_kind {
							ProcessObjectKind::Command => {
								complete.commands = complete.commands && object_inner.complete;
								metadata.commands.count = metadata
									.commands
									.count
									.zip(object_inner.metadata.count)
									.map(|(a, b)| a + b);
								metadata.commands.depth = metadata
									.commands
									.depth
									.zip(object_inner.metadata.depth)
									.map(|(a, b)| a.max(b));
								metadata.commands.weight = metadata
									.commands
									.weight
									.zip(object_inner.metadata.weight)
									.map(|(a, b)| a + b);
							},
							ProcessObjectKind::Output => {
								complete.outputs = complete.outputs && object_inner.complete;
								metadata.outputs.count = metadata
									.outputs
									.count
									.zip(object_inner.metadata.count)
									.map(|(a, b)| a + b);
								metadata.outputs.depth = metadata
									.outputs
									.depth
									.zip(object_inner.metadata.depth)
									.map(|(a, b)| a.max(b));
								metadata.outputs.weight = metadata
									.outputs
									.weight
									.zip(object_inner.metadata.weight)
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
						let Some(child_inner) = &child_node.inner else {
							continue;
						};
						let child_inner = child_inner
							.try_unwrap_object_ref()
							.ok()
							.ok_or_else(|| tg::error!("all children of objects must be objects"))?;
						complete = complete && child_inner.complete;
						metadata.count = metadata
							.count
							.zip(child_inner.metadata.count)
							.map(|(a, b)| a + b);
						metadata.depth = metadata
							.depth
							.zip(child_inner.metadata.depth)
							.map(|(a, b)| a.max(1 + b));
						metadata.weight = metadata
							.weight
							.zip(child_inner.metadata.weight)
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
							id,
							touched_at,
							objects,
						});
					messages.entry(level).or_insert(Vec::new()).push(message);
					stack.extend(node.children.iter().map(|index| (*index, level + 1)));
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
							cache_reference: None,
							children,
							complete: node.complete,
							count: node.metadata.count,
							depth: node.metadata.depth,
							id,
							size: node.size,
							touched_at,
							weight: node.metadata.weight,
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
		// Get the completes and metadata.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();
		let result = self.try_get_process_complete_and_metadata_batch(&ids).await;
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
		// Get the completes and metadata.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();
		let result = self.try_get_object_complete_and_metadata_batch(&ids).await;
		let outputs = match result {
			Ok(outputs) => outputs,
			Err(error) => {
				tracing::error!(?error, "failed to get the object complete batch");
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

	async fn import_processes_task(
		&self,
		process_receiver: tokio::sync::mpsc::Receiver<tg::export::ProcessItem>,
		progress: &Progress,
	) -> tg::Result<()> {
		let stream = ReceiverStream::new(process_receiver);
		let mut stream = pin!(stream);
		while let Some(item) = stream.next().await {
			let arg = tg::process::put::Arg { data: item.data };
			self.put_process(&item.id, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to put the process"))?;
			progress.increment_processes();
		}
		Ok(())
	}

	async fn import_objects_task(
		&self,
		object_receiver: tokio::sync::mpsc::Receiver<tg::export::ObjectItem>,
		progress: &Arc<Progress>,
	) -> tg::Result<()> {
		// Choose the batch parameters.
		let (concurrency, max_objects_per_batch, max_bytes_per_batch) = match &self.store {
			#[cfg(feature = "foundationdb")]
			Store::Fdb(_) => (64, 1_000, 1_000_000),
			Store::Lmdb(_) => (1, 1_000, 1_000_000),
			Store::Memory(_) => (1, 1, u64::MAX),
			Store::S3(_) => (256, 1, u64::MAX),
		};

		// Create a stream of batches.
		struct State {
			item: Option<tg::export::ObjectItem>,
			object_receiver: tokio::sync::mpsc::Receiver<tg::export::ObjectItem>,
		}
		let state = State {
			item: None,
			object_receiver,
		};
		let stream = stream::unfold(state, |mut state| async {
			let mut batch_bytes = state
				.item
				.as_ref()
				.map(|item| item.size)
				.unwrap_or_default();
			let mut batch = state.item.take().map(|item| vec![item]).unwrap_or_default();
			while let Some(item) = state.object_receiver.recv().await {
				let size = item.size;
				if !batch.is_empty()
					&& (batch.len() + 1 >= max_objects_per_batch
						|| batch_bytes + size >= max_bytes_per_batch)
				{
					state.item.replace(item);
					return Some((batch, state));
				}
				batch_bytes += 100 + size;
				batch.push(item);
			}
			if batch.is_empty() {
				return None;
			}
			Some((batch, state))
		});

		// Write the batches.
		stream
			.map(Ok)
			.try_for_each_concurrent(concurrency, |batch| {
				self.import_objects_task_inner(batch, progress)
			})
			.await?;

		Ok(())
	}

	async fn import_objects_task_inner(
		&self,
		items: Vec<tg::export::ObjectItem>,
		progress: &Arc<Progress>,
	) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Store the items.
		let args = items
			.clone()
			.into_iter()
			.map(|item| {
				let bytes = item
					.data
					.serialize()
					.map_err(|source| tg::error!(!source, "failed to serialize object data"))?;
				Ok(crate::store::PutArg {
					id: item.id,
					bytes: Some(bytes),
					cache_reference: None,
					touched_at,
				})
			})
			.collect::<tg::Result<_>>()?;
		self.store.put_batch(args).await?;

		// Update the progress.
		let objects = items.len().to_u64().unwrap();
		let bytes = items.iter().map(|item| item.size).sum();
		progress.increment_objects(objects);
		progress.increment_bytes(bytes);

		Ok(())
	}

	pub(crate) async fn handle_import_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the arg.
		let arg = request
			.query_params()
			.transpose()?
			.ok_or_else(|| tg::error!("query parameters required"))?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Validate the request content type.
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if content_type != Some(tg::import::CONTENT_TYPE.parse().unwrap()) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}

		// Get the stop signal.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		// Create the request stream.
		let body = request.reader();
		let stream = stream::try_unfold(body, |mut reader| async move {
			let Some(event) = tg::export::Event::from_reader(&mut reader).await? else {
				return Ok(None);
			};
			if let tg::export::Event::Item(tg::export::Item::Object(object)) = &event {
				let bytes = object.data.serialize()?;
				let actual = tg::object::Id::new(object.id.kind(), &bytes);
				if object.id != actual {
					return Err(tg::error!(%expected = object.id, %actual, "invalid object id"));
				}
			}
			Ok(Some((event, reader)))
		})
		.boxed();

		// Create the outgoing stream.
		let stream = handle.import(arg, stream).await?;

		// Stop the output stream when the server stops.
		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);

		// Create the response body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
