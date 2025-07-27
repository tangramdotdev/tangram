use self::graph::Graph;
use crate::Server;
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream::FuturesUnordered,
};
use rusqlite as sqlite;
use std::{
	collections::VecDeque,
	panic::AssertUnwindSafe,
	path::PathBuf,
	pin::{Pin, pin},
	sync::{Arc, Mutex, atomic::AtomicUsize},
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

mod graph;

const PROCESS_BATCH_SIZE: usize = 16;
const PROCESS_CONCURRENCY: usize = 8;
const OBJECT_BATCH_SIZE: usize = 16;
const OBJECT_CONCURRENCY: usize = 8;

struct State {
	arg: tg::export::Arg,
	event_sender: tokio::sync::mpsc::Sender<tg::Result<tg::export::Event>>,
	graph: Mutex<Graph>,
	queue_counter: AtomicUsize,
	process_queue_sender: async_channel::Sender<ProcessQueueItem>,
	object_queue_sender: async_channel::Sender<ObjectQueueItem>,
}

struct StateSync {
	arg: tg::export::Arg,
	database: sqlite::Connection,
	event_sender: tokio::sync::mpsc::Sender<tg::Result<tg::export::Event>>,
	file: Option<(tg::artifact::Id, Option<PathBuf>, std::fs::File)>,
	graph: Graph,
	import_complete_receiver: tokio::sync::mpsc::Receiver<tg::import::Complete>,
	index: sqlite::Connection,
	queue: VecDeque<QueueItem>,
}

enum QueueItem {
	Process(ProcessQueueItem),
	Object(ObjectQueueItem),
}

struct ProcessQueueItem {
	parent: Option<tg::process::Id>,
	process: tg::process::Id,
}

struct ObjectQueueItem {
	parent: Option<Either<tg::process::Id, tg::object::Id>>,
	object: tg::object::Id,
}

impl Server {
	pub async fn export(
		&self,
		mut arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Complete>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let client = self.get_remote_client(remote.clone()).await?;
			let stream = client.export(arg, stream).await?;
			return Ok(stream.left_stream());
		}

		// Create the task.
		let (event_sender, event_receiver) = tokio::sync::mpsc::channel(4096);
		let task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			async move {
				let result =
					AssertUnwindSafe(server.export_inner(arg, stream, event_sender.clone()))
						.catch_unwind()
						.await;
				match result {
					Ok(Ok(())) => {
						event_sender
							.send(Ok(tg::export::Event::End))
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to send the export end event");
							})
							.ok();
					},
					Ok(Err(error)) => {
						event_sender
							.send(Err(error))
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to send the export error");
							})
							.ok();
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						event_sender
							.send(Err(tg::error!(?message, "the task panicked")))
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to send the export panic");
							})
							.ok();
					},
				}
			}
		}));

		// Create the stream.
		let stream = ReceiverStream::new(event_receiver).attach(task);

		Ok(stream.right_stream())
	}

	async fn export_inner(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Complete>> + Send + 'static>>,
		event_sender: tokio::sync::mpsc::Sender<tg::Result<tg::export::Event>>,
	) -> tg::Result<()> {
		// If the database, index, and store are synchronous, and all items are complete, then export synchronously.
		if self.database.is_sqlite()
			&& self.index.is_sqlite()
			&& matches!(self.store, crate::Store::Lmdb(_) | crate::Store::Memory(_))
		{
			let complete = self.export_items_complete(&arg).await?;
			if complete {
				let (import_complete_sender, import_complete_receiver) =
					tokio::sync::mpsc::channel(4096);
				let import_task = tokio::spawn(async move {
					let mut stream = std::pin::pin!(stream);
					while let Some(event) = stream.try_next().await? {
						import_complete_sender.send(event).await.ok();
					}
					Ok::<_, tg::Error>(())
				});
				scopeguard::defer! {
					import_task.abort();
				}
				tokio::task::spawn_blocking({
					let server = self.clone();
					let arg = arg.clone();
					let event_sender = event_sender.clone();
					move || server.export_sync_task(arg, import_complete_receiver, event_sender)
				})
				.await
				.unwrap()?;
				return Ok(());
			}
		}

		// Create the state.
		let queue_counter = AtomicUsize::new(0);
		let (process_queue_sender, process_queue_receiver) =
			async_channel::unbounded::<ProcessQueueItem>();
		let (object_queue_sender, object_queue_receiver) =
			async_channel::unbounded::<ObjectQueueItem>();
		let state = Arc::new(State {
			arg,
			graph: Mutex::new(Graph::new()),
			event_sender,
			queue_counter,
			process_queue_sender,
			object_queue_sender,
		});

		// Spawn a task to receive import completion events and update the graph.
		let import_complete_task = tokio::spawn({
			let state = state.clone();
			async move {
				let mut stream = pin!(stream);
				while let Some(complete) = stream.try_next().await? {
					match complete {
						tg::import::Complete::Process(complete) => {
							let id = Either::Left(complete.id.clone());
							let complete = complete.complete
								&& (!state.arg.commands || complete.commands_complete)
								&& (!state.arg.outputs || complete.outputs_complete);
							state.graph.lock().unwrap().update(None, id, complete);
						},
						tg::import::Complete::Object(complete) => {
							let id = Either::Right(complete.id.clone());
							let complete = true;
							state.graph.lock().unwrap().update(None, id, complete);
						},
					}
				}
				Ok::<_, tg::Error>(())
			}
		});
		scopeguard::defer! {
			import_complete_task.abort();
		}
		// Enqueue the items.
		state
			.queue_counter
			.fetch_add(state.arg.items.len(), std::sync::atomic::Ordering::SeqCst);
		for item in state.arg.items.iter().cloned() {
			match item {
				Either::Left(process) => {
					let item = ProcessQueueItem {
						parent: None,
						process,
					};
					state.process_queue_sender.force_send(item).unwrap();
				},
				Either::Right(object) => {
					let item = ObjectQueueItem {
						parent: None,
						object,
					};
					state.object_queue_sender.force_send(item).unwrap();
				},
			}
		}

		// Handle the items.
		let processes_future = process_queue_receiver
			.ready_chunks(PROCESS_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(PROCESS_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move {
					server
						.export_inner_processes(&state, items)
						.await
						.map_err(|source| tg::error!(!source, "failed to export the process"))?;
					Ok::<_, tg::Error>(())
				}
			});
		let objects_future = object_queue_receiver
			.ready_chunks(OBJECT_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(OBJECT_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move {
					server
						.export_inner_objects(&state, items)
						.await
						.map_err(|source| tg::error!(!source, "failed to export the object"))?;
					Ok::<_, tg::Error>(())
				}
			});
		futures::try_join!(processes_future, objects_future)?;

		Ok(())
	}

	async fn export_items_complete(&self, arg: &tg::export::Arg) -> tg::Result<bool> {
		Ok(arg
			.items
			.iter()
			.map(async |item| self.export_item_complete(arg, item.as_ref()).await)
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.all(|complete| complete))
	}

	async fn export_item_complete(
		&self,
		arg: &tg::export::Arg,
		item: Either<&tg::process::Id, &tg::object::Id>,
	) -> Result<bool, tg::Error> {
		match item {
			Either::Left(process) => {
				if arg.recursive {
					let Some(output) =
						self.try_get_process_complete(process)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to get the process complete")
							})?
					else {
						return Ok(false);
					};
					Ok(output.complete
						&& (!arg.commands || output.commands_complete)
						&& (!arg.outputs || output.outputs_complete))
				} else {
					let Some(process) = self
						.try_get_process_local(process)
						.await
						.map_err(|source| tg::error!(!source, "failed to get the process"))?
					else {
						return Ok(false);
					};
					if arg.commands {
						let command_complete = self
							.try_get_object_complete(&process.data.command.clone().into())
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to get the object complete")
							})?
							.is_some_and(|complete| complete);
						if !command_complete {
							return Ok(false);
						}
					}
					if arg.outputs {
						if let Some(output) = process.data.output {
							let output_complete = output
								.children()
								.map(|child| async move {
									Ok::<_, tg::Error>(
										self.try_get_object_complete(&child)
											.await
											.map_err(|source| {
												tg::error!(
													!source,
													"failed to get the object complete"
												)
											})?
											.is_some_and(|complete| complete),
									)
								})
								.collect::<FuturesUnordered<_>>()
								.try_collect::<Vec<_>>()
								.await?
								.into_iter()
								.all(|complete| complete);
							if !output_complete {
								return Ok(false);
							}
						}
					}
					Ok(true)
				}
			},
			Either::Right(object) => Ok::<_, tg::Error>(
				self.try_get_object_complete(object)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the object complete"))?
					.is_some_and(|complete| complete),
			),
		}
	}

	fn export_sync_task(
		&self,
		arg: tg::export::Arg,
		import_complete_receiver: tokio::sync::mpsc::Receiver<tg::import::Complete>,
		event_sender: tokio::sync::mpsc::Sender<tg::Result<tg::export::Event>>,
	) -> tg::Result<()> {
		// Create a database connection.
		let database = self
			.database
			.try_unwrap_sqlite_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected the database to be sqlite"))?
			.create_connection(true)
			.map_err(|source| tg::error!(!source, "failed to create a connection"))?;

		// Create an index connection.
		let index = self
			.index
			.try_unwrap_sqlite_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected the index to be sqlite"))?
			.create_connection(true)
			.map_err(|source| tg::error!(!source, "failed to create a connection"))?;

		// Create the state.
		let mut state = StateSync {
			arg,
			database,
			event_sender,
			file: None,
			graph: Graph::new(),
			import_complete_receiver,
			index,
			queue: VecDeque::new(),
		};

		// Enqueue the items.
		for item in state.arg.items.iter().cloned() {
			let item = match item {
				Either::Left(process) => QueueItem::Process(ProcessQueueItem {
					parent: None,
					process,
				}),
				Either::Right(object) => QueueItem::Object(ObjectQueueItem {
					parent: None,
					object,
				}),
			};
			state.queue.push_back(item);
		}

		// Export each item.
		while let Some(item) = state.queue.pop_front() {
			// Update the graph.
			while let Ok(complete) = state.import_complete_receiver.try_recv() {
				match complete {
					tg::import::Complete::Process(complete) => {
						let id = Either::Left(complete.id.clone());
						let complete = complete.complete
							&& (!state.arg.commands || complete.commands_complete)
							&& (!state.arg.outputs || complete.outputs_complete);
						state.graph.update(None, id, complete);
					},
					tg::import::Complete::Object(complete) => {
						let id = Either::Right(complete.id.clone());
						let complete = true;
						state.graph.update(None, id, complete);
					},
				}
			}

			// Export the item.
			match item {
				QueueItem::Process(item) => {
					Self::export_sync_inner_process(&mut state, item)?;
				},
				QueueItem::Object(item) => {
					self.export_sync_inner_object(&mut state, item)?;
				},
			}
		}

		Ok(())
	}

	async fn export_inner_processes(
		&self,
		state: &State,
		mut items: Vec<ProcessQueueItem>,
	) -> tg::Result<()> {
		// Update the graph and handle inserted and complete processes.
		let n = items.len();
		let mut i = 0;
		while i < items.len() {
			let item = &items[i];
			let (inserted, complete) = state.graph.lock().unwrap().update(
				item.parent.clone().map(Either::Left),
				Either::Left(item.process.clone()),
				false,
			);
			if complete {
				let process_complete = self
					.export_get_process_complete(&item.process)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the process complete"))?;
				let event = tg::export::Event::Complete(tg::export::Complete::Process(
					process_complete.clone(),
				));
				state.event_sender.send(Ok(event)).await.map_err(|source| {
					tg::error!(!source, "failed to send the process complete event")
				})?;
			}
			if !inserted || complete {
				items.remove(i);
			} else {
				i += 1;
			}
		}
		if items.is_empty() {
			if state
				.queue_counter
				.fetch_sub(n, std::sync::atomic::Ordering::SeqCst)
				== n
			{
				state.process_queue_sender.close();
				state.object_queue_sender.close();
			}
			return Ok(());
		}

		// Get the processes.
		let ids = items
			.iter()
			.map(|item| item.process.clone())
			.collect::<Vec<_>>();
		let outputs = self
			.try_get_process_batch(&ids)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the processes"))?;

		// Handle the processes.
		for (item, output) in std::iter::zip(items, outputs) {
			let ProcessQueueItem { process, .. } = item;
			let Some(output) = output else {
				return Err(tg::error!(%id = process, "failed to find the process"));
			};
			let data = output.data;

			// Send the process.
			let event =
				tg::export::Event::Item(tg::export::Item::Process(tg::export::ProcessItem {
					id: process.clone(),
					data: data.clone(),
				}));
			state
				.event_sender
				.send(Ok(event))
				.await
				.map_err(|source| tg::error!(!source, "failed to send the process"))?;

			// Enqueue the children.
			let children = data
				.children
				.as_ref()
				.ok_or_else(|| tg::error!("expected the children to be set"))?;
			if state.arg.recursive {
				state
					.queue_counter
					.fetch_add(children.len(), std::sync::atomic::Ordering::SeqCst);
				for child in children {
					let item = ProcessQueueItem {
						parent: Some(process.clone()),
						process: child.item.clone(),
					};
					state.process_queue_sender.force_send(item).unwrap();
				}
			}

			// Enqueue the objects.
			let mut objects: Vec<tg::object::Id> = Vec::new();
			if state.arg.commands {
				objects.push(data.command.clone().into());
			}
			if state.arg.outputs {
				if let Some(output) = &data.output {
					objects.extend(output.children());
				}
			}
			state
				.queue_counter
				.fetch_add(objects.len(), std::sync::atomic::Ordering::SeqCst);
			for child in objects {
				let item = ObjectQueueItem {
					parent: Some(Either::Left(process.clone())),
					object: child,
				};
				state.object_queue_sender.force_send(item).unwrap();
			}
		}

		// Decrement the queue counter and close the queue if the counter hits zero.
		if state
			.queue_counter
			.fetch_sub(n, std::sync::atomic::Ordering::SeqCst)
			== n
		{
			state.process_queue_sender.close();
			state.object_queue_sender.close();
		}

		Ok(())
	}

	fn export_sync_inner_process(state: &mut StateSync, item: ProcessQueueItem) -> tg::Result<()> {
		let ProcessQueueItem { parent, process } = item;

		// If the process has already been sent or is complete, then update the progress and return.
		let (inserted, complete) = state.graph.update(
			parent.map(Either::Left),
			Either::Left(process.clone()),
			false,
		);
		if complete {
			let process_complete = Self::export_sync_get_process_complete(state, &process)
				.map_err(|source| tg::error!(!source, "failed to get the process complete"))?;
			let event = tg::export::Event::Complete(tg::export::Complete::Process(
				process_complete.clone(),
			));
			state
				.event_sender
				.blocking_send(Ok(event))
				.map_err(|source| {
					tg::error!(!source, "failed to send the process complete event")
				})?;
		}
		if !inserted || complete {
			return Ok(());
		}

		// Get the process
		let data = Self::try_get_process_local_sync(&state.database, &process)?
			.ok_or_else(|| tg::error!("failed to find the process"))?;

		// Enqueue the children.
		if state.arg.recursive {
			for child in data.children.into_iter().flatten() {
				let item = QueueItem::Process(ProcessQueueItem {
					parent: Some(process.clone()),
					process: child.item,
				});
				state.queue.push_back(item);
			}
		}

		// Enqueue the objects.
		let mut objects: Vec<tg::object::Id> = Vec::new();
		if state.arg.commands {
			objects.push(data.command.clone().into());
		}
		if state.arg.outputs {
			if let Some(output) = &data.output {
				objects.extend(output.children());
			}
		}
		for child in objects {
			let item = QueueItem::Object(ObjectQueueItem {
				parent: Some(Either::Left(process.clone())),
				object: child,
			});
			state.queue.push_back(item);
		}

		Ok(())
	}

	async fn export_inner_objects(
		&self,
		state: &State,
		mut items: Vec<ObjectQueueItem>,
	) -> tg::Result<()> {
		// Update the graph and handle inserted and complete processes.
		let n = items.len();
		let mut i = 0;
		while i < items.len() {
			let item = &items[i];
			let (inserted, complete) = state.graph.lock().unwrap().update(
				item.parent.clone(),
				Either::Right(item.object.clone()),
				false,
			);
			if complete {
				let object_complete = self
					.export_get_object_complete(&item.object)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the object complete"))?;
				let event = tg::export::Event::Complete(tg::export::Complete::Object(
					object_complete.clone(),
				));
				state.event_sender.send(Ok(event)).await.map_err(|source| {
					tg::error!(!source, "failed to send export object complete event")
				})?;
			}
			if !inserted || complete {
				items.remove(i);
			} else {
				i += 1;
			}
		}
		if items.is_empty() {
			if state
				.queue_counter
				.fetch_sub(n, std::sync::atomic::Ordering::SeqCst)
				== n
			{
				state.process_queue_sender.close();
				state.object_queue_sender.close();
			}
			return Ok(());
		}

		// Get the objects.
		let ids = items
			.iter()
			.map(|item| item.object.clone())
			.collect::<Vec<_>>();
		let outputs = self
			.try_get_object_batch(&ids)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects"))?;

		// Handle the objects.
		for (item, output) in std::iter::zip(items, outputs) {
			let ObjectQueueItem { object, .. } = item;
			let Some(output) = output else {
				return Err(tg::error!(%id = object, "failed to find the object"));
			};
			let bytes = output.bytes;
			let data = tg::object::Data::deserialize(object.kind(), bytes.clone())?;

			// Send the object.
			let item = tg::export::Item::Object(tg::export::ObjectItem {
				id: object.clone(),
				bytes: bytes.clone(),
			});
			let event = tg::export::Event::Item(item);
			state
				.event_sender
				.send(Ok(event))
				.await
				.map_err(|source| tg::error!(!source, "failed to send"))?;

			// Enqueue the children.
			let children = data.children().collect::<Vec<_>>();
			state
				.queue_counter
				.fetch_add(children.len(), std::sync::atomic::Ordering::SeqCst);
			for child in children {
				let item = ObjectQueueItem {
					parent: Some(Either::Right(object.clone())),
					object: child,
				};
				state.object_queue_sender.force_send(item).unwrap();
			}
		}

		// Decrement the queue counter and close the queue if the counter hits zero.
		if state
			.queue_counter
			.fetch_sub(n, std::sync::atomic::Ordering::SeqCst)
			== n
		{
			state.process_queue_sender.close();
			state.object_queue_sender.close();
		}

		Ok(())
	}

	fn export_sync_inner_object(
		&self,
		state: &mut StateSync,
		item: ObjectQueueItem,
	) -> tg::Result<()> {
		let ObjectQueueItem { parent, object } = item;

		// If the object has already been sent or is complete, then update the progress and return.
		let (inserted, complete) = state
			.graph
			.update(parent, Either::Right(object.clone()), false);
		if complete {
			let object_complete = Self::export_sync_get_object_complete(state, &object)?;
			let event =
				tg::export::Event::Complete(tg::export::Complete::Object(object_complete.clone()));
			state
				.event_sender
				.blocking_send(Ok(event))
				.map_err(|source| tg::error!(!source, "failed to send"))?;
		}
		if !inserted || complete {
			return Ok(());
		}

		// Get the object.
		let bytes = self
			.try_get_object_sync(&object, &mut state.file)?
			.ok_or_else(|| tg::error!("failed to find the object"))?
			.bytes;
		let data = tg::object::Data::deserialize(object.kind(), bytes.clone())?;

		// Send the object.
		let item = tg::export::Item::Object(tg::export::ObjectItem {
			id: object.clone(),
			bytes: bytes.clone(),
		});
		let event = tg::export::Event::Item(item);
		state
			.event_sender
			.blocking_send(Ok(event))
			.map_err(|source| tg::error!(!source, "failed to send"))?;

		// Enqueue the children.
		for child in data.children() {
			let item = QueueItem::Object(ObjectQueueItem {
				parent: Some(Either::Right(object.clone())),
				object: child,
			});
			state.queue.push_back(item);
		}

		Ok(())
	}

	async fn export_get_process_complete(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<tg::export::ProcessComplete> {
		let metadata = self
			.try_get_process_metadata_local(id)
			.await?
			.ok_or_else(|| tg::error!("failed to find the process"))?;
		let complete = tg::export::ProcessComplete {
			commands_count: metadata.commands_count,
			commands_weight: metadata.commands_weight,
			count: metadata.count,
			id: id.clone(),
			outputs_count: metadata.outputs_count,
			outputs_weight: metadata.outputs_weight,
		};
		Ok(complete)
	}

	fn export_sync_get_process_complete(
		state: &mut StateSync,
		id: &tg::process::Id,
	) -> tg::Result<tg::export::ProcessComplete> {
		let metadata = Self::try_get_process_metadata_local_sync(&state.index, id)?
			.ok_or_else(|| tg::error!("failed to find the process"))?;
		let complete = tg::export::ProcessComplete {
			commands_count: metadata.commands_count,
			commands_weight: metadata.commands_weight,
			count: metadata.count,
			id: id.clone(),
			outputs_count: metadata.outputs_count,
			outputs_weight: metadata.outputs_weight,
		};
		Ok(complete)
	}

	async fn export_get_object_complete(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<tg::export::ObjectComplete> {
		let metadata = self.try_get_object_metadata(id).await?;
		let output = tg::export::ObjectComplete {
			id: id.clone(),
			count: metadata.as_ref().and_then(|metadata| metadata.count),
			weight: metadata.as_ref().and_then(|metadata| metadata.weight),
		};
		Ok(output)
	}

	fn export_sync_get_object_complete(
		state: &mut StateSync,
		id: &tg::object::Id,
	) -> tg::Result<tg::export::ObjectComplete> {
		let metadata = Self::try_get_object_metadata_local_sync(&state.index, id)?;
		let complete = tg::export::ObjectComplete {
			count: metadata.as_ref().and_then(|metadata| metadata.count),
			id: id.clone(),
			weight: metadata.as_ref().and_then(|metadata| metadata.weight),
		};
		Ok(complete)
	}

	pub(crate) async fn handle_export_request<H>(
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

		// Get the stop signal.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		// Create the request stream.
		let stream = request
			.sse()
			.map_err(|source| tg::error!(!source, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			})
			.boxed();

		// Create the response stream.
		let stream = handle.export(arg, stream).await?;

		// Stop the outgoing stream when the server stops.
		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);

		// Create the response body.
		let (content_type, body) = if accept == Some(tg::export::CONTENT_TYPE.parse().unwrap()) {
			let content_type = Some(tg::export::CONTENT_TYPE);
			let stream = stream.then(|result| async {
				let frame = match result {
					Ok(item) => {
						let bytes = item.to_bytes().await;
						hyper::body::Frame::data(bytes)
					},
					Err(error) => {
						let mut trailers = http::HeaderMap::new();
						trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
						let json = serde_json::to_string(&error.to_data()).unwrap();
						trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
						hyper::body::Frame::trailers(trailers)
					},
				};
				Ok::<_, tg::Error>(frame)
			});
			let body = Body::with_stream(stream);
			(content_type, body)
		} else {
			return Err(tg::error!(?accept, "invalid accept header"));
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
