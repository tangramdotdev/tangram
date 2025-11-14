use {
	self::graph::Graph,
	super::{
		progress::Progress,
		queue::{ObjectItem, ProcessItem, Queue},
	},
	crate::Server,
	futures::{prelude::*, stream::BoxStream},
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
	tangram_either::Either,
	tangram_futures::task::Task,
};

mod graph;
mod queue;
mod sync;

struct State {
	arg: tg::sync::Arg,
	graph: Mutex<Graph>,
	progress: Progress,
	queue: Queue,
	sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
}

impl Server {
	pub(super) async fn sync_put(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::sync::Message>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		// If the database, index, and store are synchronous, and all items are complete, then put the items synchronously.
		if let Ok(database) = self.database.try_unwrap_sqlite_ref()
			&& let Ok(index) = self.index.try_unwrap_sqlite_ref()
			&& matches!(self.store, crate::Store::Lmdb(_) | crate::Store::Memory(_))
		{
			// Create the database and index connections.
			let database = database
				.create_connection(true)
				.map_err(|source| tg::error!(!source, "failed to create a connection"))?;
			let index = index
				.create_connection(true)
				.map_err(|source| tg::error!(!source, "failed to create a connection"))?;

			// Determine if the items are complete.
			let complete = Self::sync_put_items_complete_sync(&database, &index, &arg)?;

			// If the items are complete, then put synchronously.
			if complete {
				// Create the channels.
				let (get_sender, get_receiver) = tokio::sync::mpsc::channel(1024);
				let (complete_sender, complete_receiver) = tokio::sync::mpsc::channel(1024);

				// Spawn the stream task.
				let stream_task = Task::spawn({
					|_| async move {
						Self::sync_put_stream_task_sync(stream, get_sender, complete_sender).await
					}
				});

				// Spawn and await the queue task.
				tokio::task::spawn_blocking({
					let server = self.clone();
					let arg = arg.clone();
					let sender = sender.clone();
					move || {
						server.sync_put_queue_task_sync(
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

				// Abort the stream task.
				stream_task.abort();

				return Ok(());
			}
		}

		// Create the state.
		let graph = Mutex::new(Graph::new());
		let (process_queue_sender, process_queue_receiver) =
			async_channel::unbounded::<ProcessItem>();
		let (object_queue_sender, object_queue_receiver) = async_channel::unbounded::<ObjectItem>();
		let progress = Progress::new();
		let queue = Queue::new(process_queue_sender, object_queue_sender);
		let state = Arc::new(State {
			arg,
			graph,
			progress,
			queue,
			sender,
		});

		// Enqueue the items.
		for item in state.arg.put.iter().flatten().cloned() {
			match item {
				Either::Left(process) => {
					let item = ProcessItem {
						parent: None,
						process,
						eager: state.arg.eager,
					};
					state.queue.enqueue_process(item);
				},
				Either::Right(object) => {
					let item = ObjectItem {
						parent: None,
						object,
						eager: state.arg.eager,
					};
					state.queue.enqueue_object(item);
				},
			}
		}

		// Spawn the stream task.
		let stream_task = Task::spawn({
			let server = self.clone();
			let state = state.clone();
			|_| async move { server.sync_put_stream_task(&state, stream).await }
		});

		// Spawn the queue task.
		let queue_task = Task::spawn({
			let server = self.clone();
			let state = state.clone();
			|_| async move {
				server
					.sync_put_queue_task(state, process_queue_receiver, object_queue_receiver)
					.await
			}
		});

		// Spawn the progress task.
		let progress_task = Task::spawn({
			let server = self.clone();
			let state = state.clone();
			|stop| async move {
				server
					.sync_progress_task(&state.progress, stop, &state.sender)
					.await;
			}
		});

		// Await the queue task.
		queue_task.wait().await.unwrap()?;

		// Abort the stream task.
		stream_task.abort();

		// Stop and await the progress task.
		progress_task.stop();
		progress_task.wait().await.unwrap();

		Ok(())
	}

	async fn sync_put_stream_task(
		&self,
		state: &State,
		mut stream: BoxStream<'static, tg::sync::Message>,
	) -> tg::Result<()> {
		while let Some(message) = stream.next().await {
			match message {
				// Handle a get message for a process.
				tg::sync::Message::Get(Some(tg::sync::GetMessage::Process(message))) => {
					let item = ProcessItem {
						parent: None,
						process: message.id,
						eager: message.eager,
					};
					state.queue.enqueue_process(item);
				},

				// Handle a get message for an object.
				tg::sync::Message::Get(Some(tg::sync::GetMessage::Object(message))) => {
					let item = ObjectItem {
						parent: None,
						object: message.id,
						eager: message.eager,
					};
					state.queue.enqueue_object(item);
				},

				// Handle the get end message.
				tg::sync::Message::Get(None) => {
					state.queue.end();
				},

				// Handle the complete message for a process.
				tg::sync::Message::Complete(tg::sync::CompleteMessage::Process(complete)) => {
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

				// Handle the complete message for an object.
				tg::sync::Message::Complete(tg::sync::CompleteMessage::Object(complete)) => {
					let id = Either::Right(complete.id.clone());
					let complete = true;
					state.graph.lock().unwrap().update(None, id, complete);
				},

				_ => unreachable!(),
			}
		}
		Ok(())
	}
}
