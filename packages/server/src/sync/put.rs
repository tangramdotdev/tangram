use {
	self::graph::Graph,
	crate::Server,
	futures::prelude::*,
	rusqlite as sqlite,
	std::{
		collections::{BTreeSet, VecDeque},
		path::PathBuf,
		pin::{Pin, pin},
		sync::{
			Arc, Mutex,
			atomic::{AtomicBool, AtomicUsize},
		},
	},
	tangram_client as tg,
	tangram_either::Either,
};

mod graph;

const PROCESS_BATCH_SIZE: usize = 16;
const PROCESS_CONCURRENCY: usize = 8;
const OBJECT_BATCH_SIZE: usize = 16;
const OBJECT_CONCURRENCY: usize = 8;

struct State {
	arg: tg::sync::Arg,
	graph: Mutex<Graph>,
	last_get_message_received: AtomicBool,
	object_queue_sender: async_channel::Sender<ObjectQueueItem>,
	process_queue_sender: async_channel::Sender<ProcessQueueItem>,
	queue_counter: AtomicUsize,
	sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
}

struct StateSync {
	arg: tg::sync::Arg,
	complete_receiver: tokio::sync::mpsc::Receiver<tg::sync::CompleteMessage>,
	database: sqlite::Connection,
	file: Option<(tg::artifact::Id, Option<PathBuf>, std::fs::File)>,
	get_receiver: tokio::sync::mpsc::Receiver<Option<tg::sync::GetMessage>>,
	graph: Graph,
	index: sqlite::Connection,
	object_queue: VecDeque<ObjectQueueItem>,
	process_queue: VecDeque<ProcessQueueItem>,
	sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
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
	pub(super) async fn sync_put(
		&self,
		arg: tg::sync::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::sync::Message> + Send + 'static>>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		// If the database, index, and store are synchronous, and all items are complete, then put the objects synchronously.
		if let Ok(database) = self.database.try_unwrap_sqlite_ref()
			&& let Ok(index) = self.index.try_unwrap_sqlite_ref()
			&& matches!(self.store, crate::Store::Lmdb(_) | crate::Store::Memory(_))
		{
			let database = database
				.create_connection(true)
				.map_err(|source| tg::error!(!source, "failed to create a connection"))?;
			let index = index
				.create_connection(true)
				.map_err(|source| tg::error!(!source, "failed to create a connection"))?;
			let complete = Self::sync_put_items_complete_sync(&database, &index, &arg)?;
			if complete {
				let (complete_sender, complete_receiver) = tokio::sync::mpsc::channel(1024);
				let (get_sender, get_receiver) = tokio::sync::mpsc::channel(1024);
				let task = tokio::spawn(async move {
					let mut stream = std::pin::pin!(stream);
					while let Some(message) = stream.next().await {
						match message {
							tg::sync::Message::Get(message) => {
								get_sender.send(message).await.ok();
							},
							tg::sync::Message::Complete(message) => {
								complete_sender.send(message).await.ok();
							},
							_ => unreachable!(),
						}
					}
					Ok::<_, tg::Error>(())
				});
				scopeguard::defer! {
					task.abort();
				}
				tokio::task::spawn_blocking({
					let server = self.clone();
					let arg = arg.clone();
					let sender = sender.clone();
					move || {
						server.sync_put_sync(
							database,
							index,
							arg,
							complete_receiver,
							get_receiver,
							sender,
						)
					}
				})
				.await
				.unwrap()?;
				return Ok(());
			}
		}

		// Create the state.
		let graph = Mutex::new(Graph::new());
		let last_get_message_received = AtomicBool::new(false);
		let (process_queue_sender, process_queue_receiver) =
			async_channel::unbounded::<ProcessQueueItem>();
		let (object_queue_sender, object_queue_receiver) =
			async_channel::unbounded::<ObjectQueueItem>();
		let queue_counter = AtomicUsize::new(0);
		let state = Arc::new(State {
			arg,
			graph,
			last_get_message_received,
			object_queue_sender,
			process_queue_sender,
			queue_counter,
			sender,
		});

		// Enqueue the items.
		state.queue_counter.fetch_add(
			state.arg.put.as_ref().map_or(0, Vec::len),
			std::sync::atomic::Ordering::SeqCst,
		);
		for item in state.arg.put.iter().flatten().cloned() {
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

		// Spawn a task to receive complete messages and update the graph and receive get messages and enqueue the items.
		let stream_task = tokio::spawn({
			let state = state.clone();
			async move {
				let mut stream = pin!(stream);
				while let Some(message) = stream.next().await {
					match message {
						tg::sync::Message::Get(Some(tg::sync::GetMessage::Process(message))) => {
							let item = ProcessQueueItem {
								parent: None,
								process: message.id,
							};
							state
								.queue_counter
								.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
							state.process_queue_sender.force_send(item).unwrap();
						},
						tg::sync::Message::Get(Some(tg::sync::GetMessage::Object(message))) => {
							let item = ObjectQueueItem {
								parent: None,
								object: message.id,
							};
							state
								.queue_counter
								.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
							state.object_queue_sender.force_send(item).unwrap();
						},
						tg::sync::Message::Get(None) => {
							state
								.last_get_message_received
								.store(true, std::sync::atomic::Ordering::SeqCst);
							if state
								.queue_counter
								.load(std::sync::atomic::Ordering::SeqCst)
								== 0
							{
								state.process_queue_sender.close();
								state.object_queue_sender.close();
							}
						},
						tg::sync::Message::Complete(tg::sync::CompleteMessage::Process(
							complete,
						)) => {
							let id = Either::Left(complete.id.clone());
							let complete = if state.arg.recursive {
								complete.children_complete
									&& (!state.arg.commands || complete.children_commands_complete)
									&& (!state.arg.outputs || complete.children_outputs_complete)
							} else {
								(!state.arg.commands || complete.command_complete)
									&& (!state.arg.outputs || complete.output_complete)
							};
							state.graph.lock().unwrap().update(None, id, complete);
						},
						tg::sync::Message::Complete(tg::sync::CompleteMessage::Object(
							complete,
						)) => {
							let id = Either::Right(complete.id.clone());
							let complete = true;
							state.graph.lock().unwrap().update(None, id, complete);
						},
						_ => unreachable!(),
					}
				}
				Ok::<_, tg::Error>(())
			}
		});
		scopeguard::defer! {
			stream_task.abort();
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
						.sync_put_inner_process_batch(&state, items)
						.await
						.map_err(|source| tg::error!(!source, "failed to put the process"))?;
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
						.sync_put_inner_object_batch(&state, items)
						.await
						.map_err(|source| tg::error!(!source, "failed to put the object"))?;
					Ok::<_, tg::Error>(())
				}
			});
		futures::try_join!(processes_future, objects_future)?;

		// Send the last put message.
		state
			.sender
			.send(Ok(tg::sync::Message::Put(None)))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the last put message"))?;

		Ok(())
	}

	fn sync_put_items_complete_sync(
		database: &sqlite::Connection,
		index: &sqlite::Connection,
		arg: &tg::sync::Arg,
	) -> tg::Result<bool> {
		for item in arg.put.iter().flatten() {
			let complete = match item.as_ref() {
				Either::Left(process) => {
					if arg.recursive {
						let Some(output) =
							Self::try_get_process_complete_sqlite_sync(index, process)?
						else {
							return Ok(false);
						};
						output.children
							&& (!arg.commands || output.children_commands)
							&& (!arg.outputs || output.children_outputs)
					} else {
						let Some(process_data) =
							Self::try_get_process_sqlite_sync(database, process)?
						else {
							return Ok(false);
						};
						if arg.commands {
							let command_complete = Self::try_get_object_complete_sqlite_sync(
								index,
								&process_data.data.command.clone().into(),
							)?
							.unwrap_or(false);
							if !command_complete {
								return Ok(false);
							}
						}
						if arg.outputs {
							if let Some(output) = process_data.data.output {
								let mut children = std::collections::BTreeSet::new();
								output.children(&mut children);
								for child in children {
									let child_complete =
										Self::try_get_object_complete_sqlite_sync(index, &child)?
											.unwrap_or(false);
									if !child_complete {
										return Ok(false);
									}
								}
							}
						}
						true
					}
				},
				Either::Right(object) => {
					Self::try_get_object_complete_sqlite_sync(index, object)?.unwrap_or(false)
				},
			};
			if !complete {
				return Ok(false);
			}
		}
		Ok(true)
	}

	fn sync_put_sync(
		&self,
		database: sqlite::Connection,
		index: sqlite::Connection,
		arg: tg::sync::Arg,
		complete_receiver: tokio::sync::mpsc::Receiver<tg::sync::CompleteMessage>,
		get_receiver: tokio::sync::mpsc::Receiver<Option<tg::sync::GetMessage>>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		// Create the state.
		let mut state = StateSync {
			arg,
			complete_receiver,
			database,
			file: None,
			get_receiver,
			graph: Graph::new(),
			index,
			object_queue: VecDeque::new(),
			process_queue: VecDeque::new(),
			sender,
		};

		// Enqueue the items.
		for item in state.arg.put.iter().flatten().cloned() {
			match item {
				Either::Left(process) => state.process_queue.push_back(ProcessQueueItem {
					parent: None,
					process,
				}),
				Either::Right(object) => state.object_queue.push_back(ObjectQueueItem {
					parent: None,
					object,
				}),
			}
		}

		// Handle each item from the queues.
		let mut done = false;
		loop {
			while !state.process_queue.is_empty() || !state.object_queue.is_empty() {
				// Handle get messages.
				if !done {
					while let Ok(message) = state.get_receiver.try_recv() {
						match message {
							Some(tg::sync::GetMessage::Process(message)) => {
								let item = ProcessQueueItem {
									parent: None,
									process: message.id,
								};
								state.process_queue.push_back(item);
							},
							Some(tg::sync::GetMessage::Object(message)) => {
								let item = ObjectQueueItem {
									parent: None,
									object: message.id,
								};
								state.object_queue.push_back(item);
							},
							None => {
								done = true;
							},
						}
					}
				}

				// Handle complete messages.
				while let Ok(message) = state.complete_receiver.try_recv() {
					match message {
						tg::sync::CompleteMessage::Process(message) => {
							let id = Either::Left(message.id.clone());
							let complete = if state.arg.recursive {
								message.children_complete
									&& (!state.arg.commands || message.children_commands_complete)
									&& (!state.arg.outputs || message.children_outputs_complete)
							} else {
								(!state.arg.commands || message.command_complete)
									&& (!state.arg.outputs || message.output_complete)
							};
							state.graph.update(None, id, complete);
						},
						tg::sync::CompleteMessage::Object(message) => {
							let id = Either::Right(message.id.clone());
							let complete = true;
							state.graph.update(None, id, complete);
						},
					}
				}
				if let Some(item) = state.object_queue.pop_front() {
					self.sync_put_sync_inner_object(&mut state, item)?;
				} else if let Some(item) = state.process_queue.pop_front() {
					Self::sync_put_sync_inner_process(&mut state, item)?;
				}
			}

			// If the last get message has been received, then break.
			if done {
				break;
			}

			// Otherwise, wait to receive the next get message.
			let message = state
				.get_receiver
				.blocking_recv()
				.ok_or_else(|| tg::error!("expected to receive the last get message"))?;
			match message {
				Some(tg::sync::GetMessage::Process(message)) => {
					let item = ProcessQueueItem {
						parent: None,
						process: message.id,
					};
					state.process_queue.push_back(item);
				},
				Some(tg::sync::GetMessage::Object(message)) => {
					let item = ObjectQueueItem {
						parent: None,
						object: message.id,
					};
					state.object_queue.push_back(item);
				},
				None => {
					done = true;
				},
			}
		}

		// Send the last put message.
		state
			.sender
			.blocking_send(Ok(tg::sync::Message::Put(None)))
			.map_err(|source| tg::error!(!source, "failed to send the last put message"))?;

		Ok(())
	}

	async fn sync_put_inner_process_batch(
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
			if !inserted || complete {
				let metadata = self
					.try_get_process_metadata_local(&item.process)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				let mut message = tg::sync::ProgressMessage::default();
				if state.arg.recursive {
					if let Some(children_count) = metadata.children.count {
						message.processes += children_count;
					}
					if state.arg.commands {
						if let Some(commands_count) = metadata.children_commands.count {
							message.objects += commands_count;
						}
						if let Some(commands_weight) = metadata.children_commands.weight {
							message.bytes += commands_weight;
						}
					}
					if state.arg.outputs {
						if let Some(outputs_count) = metadata.children_outputs.count {
							message.objects += outputs_count;
						}
						if let Some(outputs_weight) = metadata.children_outputs.weight {
							message.bytes += outputs_weight;
						}
					}
				} else {
					if state.arg.commands {
						if let Some(command_count) = metadata.command.count {
							message.objects += command_count;
						}
						if let Some(command_weight) = metadata.command.weight {
							message.bytes += command_weight;
						}
					}
					if state.arg.outputs {
						if let Some(output_count) = metadata.output.count {
							message.objects += output_count;
						}
						if let Some(output_weight) = metadata.output.weight {
							message.bytes += output_weight;
						}
					}
				}
				if message != tg::sync::ProgressMessage::default() {
					let message = tg::sync::Message::Progress(message);
					state.sender.send(Ok(message)).await.map_err(|source| {
						tg::error!(!source, "failed to send the progress message")
					})?;
				}
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
				== n && state
				.last_get_message_received
				.load(std::sync::atomic::Ordering::SeqCst)
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
			let bytes = serde_json::to_string(&data)
				.map_err(|source| tg::error!(!source, "failed to serialize the process"))?;
			let message = tg::sync::Message::Put(Some(tg::sync::PutMessage::Process(
				tg::sync::ProcessPutMessage {
					id: process.clone(),
					bytes: bytes.into(),
				},
			)));
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|source| tg::error!(!source, "failed to send the put message"))?;

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
					let mut children = BTreeSet::new();
					output.children(&mut children);
					objects.extend(children);
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
			== n && state
			.last_get_message_received
			.load(std::sync::atomic::Ordering::SeqCst)
		{
			state.process_queue_sender.close();
			state.object_queue_sender.close();
		}

		Ok(())
	}

	fn sync_put_sync_inner_process(
		state: &mut StateSync,
		item: ProcessQueueItem,
	) -> tg::Result<()> {
		let ProcessQueueItem { parent, process } = item;

		// If the process has already been sent or is complete, then update the progress and return.
		let (inserted, complete) = state.graph.update(
			parent.map(Either::Left),
			Either::Left(process.clone()),
			false,
		);
		if !inserted || complete {
			let metadata = Self::try_get_process_metadata_sqlite_sync(&state.index, &process)?
				.ok_or_else(|| tg::error!("failed to find the process"))?;
			let mut message = tg::sync::ProgressMessage::default();
			if state.arg.recursive {
				if let Some(children_count) = metadata.children.count {
					message.processes += children_count;
				}
				if state.arg.commands {
					if let Some(commands_count) = metadata.children_commands.count {
						message.objects += commands_count;
					}
					if let Some(commands_weight) = metadata.children_commands.weight {
						message.bytes += commands_weight;
					}
				}
				if state.arg.outputs {
					if let Some(outputs_count) = metadata.children_outputs.count {
						message.objects += outputs_count;
					}
					if let Some(outputs_weight) = metadata.children_outputs.weight {
						message.bytes += outputs_weight;
					}
				}
			} else {
				if state.arg.commands {
					if let Some(command_count) = metadata.command.count {
						message.objects += command_count;
					}
					if let Some(command_weight) = metadata.command.weight {
						message.bytes += command_weight;
					}
				}
				if state.arg.outputs {
					if let Some(output_count) = metadata.output.count {
						message.objects += output_count;
					}
					if let Some(output_weight) = metadata.output.weight {
						message.bytes += output_weight;
					}
				}
			}
			if message != tg::sync::ProgressMessage::default() {
				let message = tg::sync::Message::Progress(message);
				state
					.sender
					.blocking_send(Ok(message))
					.map_err(|source| tg::error!(!source, "failed to send the progress message"))?;
			}
		}
		if !inserted || complete {
			return Ok(());
		}

		// Get the process
		let tg::process::get::Output { data } =
			Self::try_get_process_sqlite_sync(&state.database, &process)?
				.ok_or_else(|| tg::error!("failed to find the process"))?;

		// Enqueue the children.
		if state.arg.recursive {
			for child in data.children.iter().flatten() {
				let item = ProcessQueueItem {
					parent: Some(process.clone()),
					process: child.item.clone(),
				};
				state.process_queue.push_back(item);
			}
		}

		// Enqueue the objects.
		let mut objects: Vec<tg::object::Id> = Vec::new();
		if state.arg.commands {
			objects.push(data.command.clone().into());
		}
		if state.arg.outputs {
			if let Some(output) = &data.output {
				let mut children = BTreeSet::new();
				output.children(&mut children);
				objects.extend(children);
			}
		}
		for child in objects {
			let item = ObjectQueueItem {
				parent: Some(Either::Left(process.clone())),
				object: child,
			};
			state.object_queue.push_back(item);
		}

		// Send the process.
		let bytes = serde_json::to_string(&data)
			.map_err(|source| tg::error!(!source, "failed to serialize the process"))?;
		let message = tg::sync::Message::Put(Some(tg::sync::PutMessage::Process(
			tg::sync::ProcessPutMessage {
				id: process.clone(),
				bytes: bytes.into(),
			},
		)));
		state
			.sender
			.blocking_send(Ok(message))
			.map_err(|source| tg::error!(!source, "failed to send the put message"))?;

		Ok(())
	}

	async fn sync_put_inner_object_batch(
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
			if !inserted || complete {
				let metadata = self.try_get_object_metadata_local(&item.object).await?;
				let message = tg::sync::ProgressMessage {
					processes: 0,
					objects: metadata
						.as_ref()
						.and_then(|metadata| metadata.count)
						.unwrap_or_default(),
					bytes: metadata
						.as_ref()
						.and_then(|metadata| metadata.weight)
						.unwrap_or_default(),
				};
				if message != tg::sync::ProgressMessage::default() {
					let message = tg::sync::Message::Progress(message);
					state.sender.send(Ok(message)).await.map_err(|source| {
						tg::error!(!source, "failed to send the progress message")
					})?;
				}
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
				== n && state
				.last_get_message_received
				.load(std::sync::atomic::Ordering::SeqCst)
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
			let mut children = BTreeSet::new();
			data.children(&mut children);

			// Send the object.
			let message = tg::sync::Message::Put(Some(tg::sync::PutMessage::Object(
				tg::sync::ObjectPutMessage {
					id: object.clone(),
					bytes,
				},
			)));
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|source| tg::error!(!source, "failed to send the put message"))?;

			// Enqueue the children.
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
			== n && state
			.last_get_message_received
			.load(std::sync::atomic::Ordering::SeqCst)
		{
			state.process_queue_sender.close();
			state.object_queue_sender.close();
		}

		Ok(())
	}

	fn sync_put_sync_inner_object(
		&self,
		state: &mut StateSync,
		item: ObjectQueueItem,
	) -> tg::Result<()> {
		let ObjectQueueItem { parent, object } = item;

		// If the object has already been sent or is complete, then update the progress and return.
		let (inserted, complete) = state
			.graph
			.update(parent, Either::Right(object.clone()), false);
		if !inserted || complete {
			let metadata = Self::try_get_object_metadata_sqlite_sync(&state.index, &object)?;
			let message = tg::sync::ProgressMessage {
				processes: 0,
				objects: metadata
					.as_ref()
					.and_then(|metadata| metadata.count)
					.unwrap_or_default(),
				bytes: metadata
					.as_ref()
					.and_then(|metadata| metadata.weight)
					.unwrap_or_default(),
			};
			if message != tg::sync::ProgressMessage::default() {
				let message = tg::sync::Message::Progress(message);
				state
					.sender
					.blocking_send(Ok(message))
					.map_err(|source| tg::error!(!source, "failed to send the progress message"))?;
			}
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

		// Enqueue the children.
		let mut children = BTreeSet::new();
		data.children(&mut children);
		for child in children {
			let item = ObjectQueueItem {
				parent: Some(Either::Right(object.clone())),
				object: child,
			};
			if object.is_blob() {
				state.object_queue.push_front(item);
			} else {
				state.object_queue.push_back(item);
			}
		}

		// Send the object.
		let message = tg::sync::Message::Put(Some(tg::sync::PutMessage::Object(
			tg::sync::ObjectPutMessage {
				id: object.clone(),
				bytes,
			},
		)));
		state
			.sender
			.blocking_send(Ok(message))
			.map_err(|source| tg::error!(!source, "failed to send the put message"))?;

		Ok(())
	}
}
