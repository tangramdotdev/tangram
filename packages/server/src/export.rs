use self::graph::Graph;
use crate::Server;
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream::FuturesUnordered,
};
use indoc::formatdoc;
use itertools::Itertools as _;
use rusqlite as sqlite;
use std::{
	collections::VecDeque,
	panic::AssertUnwindSafe,
	path::PathBuf,
	pin::{Pin, pin},
	sync::{Arc, Mutex, atomic::AtomicUsize},
};
use tangram_client::{self as tg, prelude::*};
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

mod graph;

const BATCH_SIZE: usize = 16;
const CONCURRENCY: usize = 8;

struct State {
	arg: tg::export::Arg,
	event_sender: tokio::sync::mpsc::Sender<tg::Result<tg::export::Event>>,
	graph: Mutex<Graph>,
	queue_counter: AtomicUsize,
	queue_sender: async_channel::Sender<QueueItem>,
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
		let task = tokio::spawn({
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
		});

		// Create the stream.
		let stream = ReceiverStream::new(event_receiver).attach(AbortOnDropHandle::new(task));

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
		let (queue_sender, queue_receiver) = async_channel::unbounded::<QueueItem>();
		let state = Arc::new(State {
			arg,
			graph: Mutex::new(Graph::new()),
			event_sender,
			queue_counter,
			queue_sender,
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
			state.queue_sender.force_send(item).unwrap();
		}

		// Export the items.
		if cfg!(feature = "foundationdb") {
			queue_receiver
				.ready_chunks(BATCH_SIZE)
				.map(Ok)
				.try_for_each_concurrent(CONCURRENCY, |batch| {
					let mut processes = vec![];
					let mut objects = vec![];
					for item in batch {
						match item {
							QueueItem::Process(ProcessQueueItem { parent, process }) => {
								processes.push((parent, process));
							},
							QueueItem::Object(ObjectQueueItem { parent, object }) => {
								objects.push((parent, object));
							},
						}
					}
					let state = state.clone();
					async move {
						self.export_inner_process_batch(&state, processes).await?;
						self.export_inner_object_batch(&state, objects).await?;
						Ok::<_, tg::Error>(())
					}
				})
				.await?;
		} else {
			queue_receiver
				.map(Ok)
				.try_for_each_concurrent(CONCURRENCY, |item| {
					let state = state.clone();
					async move {
						match item {
							QueueItem::Process(ProcessQueueItem { parent, process }) => {
								self.export_inner_process(&state, parent.as_ref(), &process)
									.await?;
							},
							QueueItem::Object(ObjectQueueItem { parent, object }) => {
								self.export_inner_object(
									&state,
									parent.as_ref().map(|either| either.as_ref()),
									&object,
								)
								.await?;
							},
						}
						Ok::<_, tg::Error>(())
					}
				})
				.await?;
		}

		Ok(())
	}

	async fn export_items_complete(&self, arg: &tg::export::Arg) -> tg::Result<bool> {
		Ok(arg
			.items
			.iter()
			.map(async |item| self.export_item_complete(arg, item).await)
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.all(|complete| complete))
	}

	async fn export_item_complete(
		&self,
		arg: &tg::export::Arg,
		item: &Either<tg::process::Id, tg::object::Id>,
	) -> Result<bool, tg::Error> {
		match item {
			Either::Left(process) => {
				if arg.recursive {
					let Some(output) =
						self.try_get_process_complete_local(process)
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
							.try_get_object_complete_local(&process.data.command.clone().into())
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
										self.try_get_object_complete_local(&child)
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
				self.try_get_object_complete_local(object)
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
			let len = state.import_complete_receiver.len();
			let mut buffer = Vec::with_capacity(len);
			state
				.import_complete_receiver
				.blocking_recv_many(&mut buffer, len);
			for complete in buffer {
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
				QueueItem::Process(ProcessQueueItem { parent, process }) => {
					Self::export_sync_inner_process(&mut state, parent, &process)?;
				},
				QueueItem::Object(ObjectQueueItem { parent, object }) => {
					self.export_sync_inner_object(&mut state, parent, &object)?;
				},
			}
		}

		Ok(())
	}

	async fn export_inner_process(
		&self,
		state: &State,
		parent: Option<&tg::process::Id>,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		// If the process has already been sent or is complete, then update the progress and return.
		let (inserted, complete) = state.graph.lock().unwrap().update(
			parent.cloned().map(Either::Left),
			Either::Left(process.clone()),
			false,
		);
		if complete {
			let process_complete = self
				.export_get_process_complete(process)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the process complete"))?;
			state
				.event_sender
				.send(Ok(tg::export::Event::Complete(
					tg::export::Complete::Process(process_complete.clone()),
				)))
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to send the process complete event")
				})?;
		}
		if !inserted || complete {
			if state
				.queue_counter
				.fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
				== 1
			{
				state.queue_sender.close();
			}
			return Ok(());
		}

		// Get the process
		let tg::process::get::Output { data, .. } = self
			.get_process(process)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?;

		// Send the process.
		let item = tg::export::Item::Process(tg::export::ProcessItem {
			id: process.clone(),
			data: data.clone(),
		});
		state
			.event_sender
			.send(Ok(tg::export::Event::Item(item)))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the process"))?;

		// Get the children.
		let children_arg = tg::process::children::get::Arg::default();
		let children = self
			.get_process_children(process, children_arg)
			.await?
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.flat_map(|chunk| chunk.data)
			.collect_vec();

		// Enqueue the children.
		if state.arg.recursive {
			state
				.queue_counter
				.fetch_add(children.len(), std::sync::atomic::Ordering::SeqCst);
			for child in children {
				let item = QueueItem::Process(ProcessQueueItem {
					parent: Some(process.clone()),
					process: child.item,
				});
				state.queue_sender.force_send(item).unwrap();
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
			let item = QueueItem::Object(ObjectQueueItem {
				parent: Some(Either::Left(process.clone())),
				object: child,
			});
			state.queue_sender.force_send(item).unwrap();
		}

		// Decrement the queue counter and close the queue if the counter hits zero.
		if state
			.queue_counter
			.fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
			== 1
		{
			state.queue_sender.close();
		}

		Ok(())
	}

	fn export_sync_inner_process(
		state: &mut StateSync,
		parent: Option<tg::process::Id>,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		// If the process has already been sent or is complete, then update the progress and return.
		let (inserted, complete) = state.graph.update(
			parent.map(Either::Left),
			Either::Left(process.clone()),
			false,
		);
		if complete {
			let process_complete = Self::export_sync_get_process_complete(state, process)
				.map_err(|source| tg::error!(!source, "failed to get the process complete"))?;
			state
				.event_sender
				.blocking_send(Ok(tg::export::Event::Complete(
					tg::export::Complete::Process(process_complete.clone()),
				)))
				.map_err(|source| {
					tg::error!(!source, "failed to send the process complete event")
				})?;
		}
		if !inserted || complete {
			return Ok(());
		}

		// Get the process
		let data = Self::try_get_process_local_sync(&state.database, process)?
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

	async fn export_inner_process_batch(
		&self,
		state: &State,
		processes: Vec<(Option<tg::process::Id>, tg::process::Id)>,
	) -> tg::Result<()> {
		let mut processes_to_send = Vec::new();
		// If the process has already been sent or is complete, then update the progress and return.
		for (parent, process) in processes.into_iter() {
			let (inserted, complete) = state.graph.lock().unwrap().update(
				parent.map(Either::Left),
				Either::Left(process.clone()),
				false,
			);
			if complete {
				let process_complete = self
					.export_get_process_complete(&process)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the process complete"))?;
				state
					.event_sender
					.send(Ok(tg::export::Event::Complete(
						tg::export::Complete::Process(process_complete.clone()),
					)))
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to send the process complete event")
					})?;
			}
			if !inserted || complete {
				continue;
			}
			processes_to_send.push(process);
		}

		// Get the processes.
		let tg::process::get::Output { data, .. } = self
			.get_process(process)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?;

		// Send the process.
		let item = tg::export::Item::Process(tg::export::ProcessItem {
			id: process.clone(),
			data: data.clone(),
		});
		state
			.event_sender
			.send(Ok(tg::export::Event::Item(item)))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the process"))?;

		// Get the children.
		let children_arg = tg::process::children::get::Arg::default();
		let children = self
			.get_process_children(process, children_arg)
			.await?
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.flat_map(|chunk| chunk.data)
			.collect_vec();

		// Enqueue the children.
		if state.arg.recursive {
			state
				.queue_counter
				.fetch_add(children.len(), std::sync::atomic::Ordering::SeqCst);
			for child in children {
				let item = QueueItem::Process(ProcessQueueItem {
					parent: Some(process.clone()),
					process: child.item,
				});
				state.queue_sender.force_send(item).unwrap();
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
			let item = QueueItem::Object(ObjectQueueItem {
				parent: Some(Either::Left(process.clone())),
				object: child,
			});
			state.queue_sender.force_send(item).unwrap();
		}

		// Decrement the queue counter and close the queue if the counter hits zero.
		if state
			.queue_counter
			.fetch_sub(processes.len(), std::sync::atomic::Ordering::SeqCst)
			== processes.len()
		{
			state.queue_sender.close();
		}

		Ok(())
	}

	async fn export_inner_object(
		&self,
		state: &State,
		parent: Option<Either<&tg::process::Id, &tg::object::Id>>,
		object: &tg::object::Id,
	) -> tg::Result<()> {
		// If the object has already been sent or is complete, then update the progress and return.
		let (inserted, complete) = state.graph.lock().unwrap().update(
			parent.as_ref().map(Either::cloned),
			Either::Right(object.clone()),
			false,
		);
		if complete {
			let object_complete =
				self.export_get_object_complete(object)
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							?object,
							"failed to locate the object to check count and weight"
						)
					})?;
			state
				.event_sender
				.send(Ok(tg::export::Event::Complete(
					tg::export::Complete::Object(object_complete.clone()),
				)))
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to send export object complete event")
				})?;
		}
		if !inserted || complete {
			// Decrement the queue counter and close the queue if the counter hits zero.
			if state
				.queue_counter
				.fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
				== 1
			{
				state.queue_sender.close();
			}
			return Ok(());
		}

		// Get the object.
		let tg::object::get::Output { bytes, .. } = self
			.get_object(object)
			.await
			.map_err(|source| tg::error!(!source, %object, "failed to get the object"))?;
		let data = tg::object::Data::deserialize(object.kind(), bytes.clone())?;

		// Send the object.
		let item = tg::export::Item::Object(tg::export::ObjectItem {
			id: object.clone(),
			bytes: bytes.clone(),
		});
		state
			.event_sender
			.send(Ok(tg::export::Event::Item(item)))
			.await
			.map_err(|source| tg::error!(!source, "failed to send"))?;

		// Enqueue the children.
		let children = data.children().collect::<Vec<_>>();
		state
			.queue_counter
			.fetch_add(children.len(), std::sync::atomic::Ordering::SeqCst);
		for child in children {
			let item = QueueItem::Object(ObjectQueueItem {
				parent: Some(Either::Right(object.clone())),
				object: child,
			});
			state.queue_sender.force_send(item).unwrap();
		}

		// Decrement the queue counter and close the queue if the counter hits zero.
		if state
			.queue_counter
			.fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
			== 1
		{
			state.queue_sender.close();
		}

		Ok(())
	}

	async fn export_inner_object_batch(
		&self,
		state: &State,
		objects: Vec<(
			std::option::Option<
				tangram_either::Either<tangram_client::process::Id, tangram_client::object::Id>,
			>,
			tangram_client::object::Id,
		)>,
	) -> tg::Result<()> {
		let mut objects_to_send = Vec::new();
		for (parent, object) in objects.iter() {
			// If the object has already been sent or is complete, then update the progress and return.
			let (inserted, complete) = state.graph.lock().unwrap().update(
				parent.clone(),
				Either::Right(object.clone()),
				false,
			);
			if complete {
				let object_complete =
					self.export_get_object_complete(&object)
						.await
						.map_err(|source| {
							tg::error!(
								!source,
								?object,
								"failed to locate the object to check count and weight"
							)
						})?;
				state
					.event_sender
					.send(Ok(tg::export::Event::Complete(
						tg::export::Complete::Object(object_complete.clone()),
					)))
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to send export object complete event")
					})?;
			}
			if !inserted || complete {
				// Skip this object.
				continue;
			}
			objects_to_send.push((parent, object));
		}

		let object_ids = objects_to_send
			.into_iter()
			.map(|(_, object)| object)
			.collect::<Vec<_>>();
		// Get the objects.
		let bytes = self.store.try_get_batch(object_ids.as_slice()).await?;
		for (object, bytes) in object_ids.into_iter().zip(bytes) {
			let bytes = bytes.expect("failed to get the object");
			let data = tg::object::Data::deserialize(object.kind(), bytes.clone())?;

			// Send the object.
			let item = tg::export::Item::Object(tg::export::ObjectItem {
				id: object.clone(),
				bytes: bytes.clone(),
			});
			state
				.event_sender
				.send(Ok(tg::export::Event::Item(item)))
				.await
				.map_err(|source| tg::error!(!source, "failed to send"))?;

			// Enqueue the children.
			let children = data.children().collect::<Vec<_>>();
			state
				.queue_counter
				.fetch_add(children.len(), std::sync::atomic::Ordering::SeqCst);
			for child in children {
				let item = QueueItem::Object(ObjectQueueItem {
					parent: Some(Either::Right(object.clone())),
					object: child,
				});
				state.queue_sender.force_send(item).unwrap();
			}
		}

		// Decrement the queue counter and close the queue if the counter hits zero.
		if state
			.queue_counter
			.fetch_sub(objects.len(), std::sync::atomic::Ordering::SeqCst)
			== objects.len()
		{
			state.queue_sender.close();
		}

		Ok(())
	}

	fn export_sync_inner_object(
		&self,
		state: &mut StateSync,
		parent: Option<Either<tg::process::Id, tg::object::Id>>,
		object: &tg::object::Id,
	) -> tg::Result<()> {
		// If the object has already been sent or is complete, then update the progress and return.
		let (inserted, complete) = state
			.graph
			.update(parent, Either::Right(object.clone()), false);
		if complete {
			let object_complete = Self::export_sync_get_object_complete(state, object)?;
			state
				.event_sender
				.blocking_send(Ok(tg::export::Event::Complete(
					tg::export::Complete::Object(object_complete.clone()),
				)))
				.map_err(|source| tg::error!(!source, "failed to send"))?;
		}
		if !inserted || complete {
			return Ok(());
		}

		// Get the object.
		let bytes = self
			.try_get_object_sync(object, &mut state.file)?
			.ok_or_else(|| tg::error!("failed to find the object"))?
			.bytes;
		let data = tg::object::Data::deserialize(object.kind(), bytes.clone())?;

		// Send the object.
		let item = tg::export::Item::Object(tg::export::ObjectItem {
			id: object.clone(),
			bytes: bytes.clone(),
		});
		state
			.event_sender
			.blocking_send(Ok(tg::export::Event::Item(item)))
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
		// Get an index connection.
		let connection = self
			.index
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the object metadata.
		#[derive(serde::Deserialize)]
		struct Row {
			commands_count: Option<u64>,
			commands_weight: Option<u64>,
			count: Option<u64>,
			outputs_count: Option<u64>,
			outputs_weight: Option<u64>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					commands_count,
					commands_weight,
					count,
					outputs_count,
					outputs_weight
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let output = tg::export::ProcessComplete {
			commands_count: row.as_ref().and_then(|row| row.commands_count),
			commands_weight: row.as_ref().and_then(|row| row.commands_weight),
			count: row.as_ref().and_then(|row| row.count),
			id: id.clone(),
			outputs_count: row.as_ref().and_then(|row| row.outputs_count),
			outputs_weight: row.as_ref().and_then(|row| row.outputs_weight),
		};

		Ok(output)
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
			.query_params::<tg::export::QueryArg>()
			.transpose()?
			.ok_or_else(|| tg::error!("query parameters required"))?
			.into();

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
