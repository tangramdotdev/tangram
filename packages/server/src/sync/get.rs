use {
	self::graph::{Graph, NodeInner},
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
mod index;
mod queue;
mod store;

struct State {
	arg: tg::sync::Arg,
	graph: Mutex<Graph>,
	progress: Progress,
	queue: Queue,
	sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
}

impl Server {
	pub(super) async fn sync_get(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::sync::Message>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		// Create the state.
		let graph = Mutex::new(Graph::new());
		let progress = Progress::new();
		let (queue_process_sender, queue_process_receiver) =
			async_channel::unbounded::<ProcessItem>();
		let (queue_object_sender, queue_object_receiver) = async_channel::unbounded::<ObjectItem>();
		let queue = Queue::new(queue_process_sender, queue_object_sender);
		let (store_process_sender, store_process_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::ProcessPutMessage>(256);
		let (store_object_sender, store_object_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::ObjectPutMessage>(256);
		let state = Arc::new(State {
			arg,
			graph,
			progress,
			queue,
			sender,
		});

		// Enqueue the items.
		for item in state.arg.get.iter().flatten().cloned() {
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
			|_| async move {
				server
					.sync_get_stream_task(&state, stream, store_process_sender, store_object_sender)
					.await
			}
		});

		// Spawn the queue task.
		let queue_task = Task::spawn({
			let server = self.clone();
			let state = state.clone();
			|_| async move {
				server
					.sync_get_queue_task(state, queue_process_receiver, queue_object_receiver)
					.await
			}
		});

		// Spawn the store task.
		let store_task = Task::spawn({
			let server = self.clone();
			let state = state.clone();
			|_| async move {
				server
					.sync_get_store_task(&state, store_process_receiver, store_object_receiver)
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

		// Await the stream, queue, and store tasks.
		let (stream_result, queue_result, store_result) =
			futures::try_join!(stream_task.wait(), queue_task.wait(), store_task.wait()).unwrap();
		stream_result.and(queue_result).and(store_result)?;

		// Stop and await the progress task.
		progress_task.stop();
		progress_task.wait().await.unwrap();

		// Spawn the index task.
		self.tasks.spawn({
			let server = self.clone();
			|_| async move {
				let result = server.sync_get_index(state).await;
				if let Err(error) = result {
					tracing::error!(?error);
				}
			}
		});

		Ok(())
	}

	async fn sync_get_stream_task(
		&self,
		state: &State,
		mut stream: BoxStream<'static, tg::sync::Message>,
		store_process_sender: tokio::sync::mpsc::Sender<tg::sync::ProcessPutMessage>,
		store_object_sender: tokio::sync::mpsc::Sender<tg::sync::ObjectPutMessage>,
	) -> tg::Result<()> {
		while let Some(message) = stream.next().await {
			match message {
				tg::sync::Message::Put(Some(tg::sync::PutMessage::Process(message))) => {
					// add to graph with no complete or metadata
					// if not eager:
					//   enqueue objects and children subject to the arg
					store_process_sender
						.send(message)
						.await
						.map_err(|_| tg::error!("failed to send the process to the store task"))?;
				},

				tg::sync::Message::Put(Some(tg::sync::PutMessage::Object(message))) => {
					// add to graph with no complete or metadata
					// if not eager:
					//   enqueue children
					store_object_sender
						.send(message)
						.await
						.map_err(|_| tg::error!("failed to send the object to the store task"))?;
				},

				tg::sync::Message::Put(None) => {
					break;
				},

				tg::sync::Message::Missing(message) => {
					// touch in store.
					// if not stored, then error.
					// if not eager:
					//   get complete and metadata.
					//   add to graph with complete and metadata
					//   if complete, send complete message
					//   if not complete, then enqueue children subject to the arg.
					// else:
					// add to the graph no complete or metadata
					todo!()
				},

				_ => {
					unreachable!()
				},
			}
		}
		Ok(())
	}
}
