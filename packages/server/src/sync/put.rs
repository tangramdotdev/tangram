use {
	self::graph::Graph,
	super::{progress::Progress, queue::Queue},
	crate::Server,
	futures::{future, stream::BoxStream},
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
	tangram_either::Either,
	tangram_futures::task::Task,
	tracing::Instrument as _,
};

mod graph;
mod index;
mod input;
mod queue;
mod store;

struct State {
	arg: tg::sync::Arg,
	graph: Mutex<Graph>,
	progress: Progress,
	queue: Queue,
	sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::PutMessage>>,
}

impl Server {
	pub(super) async fn sync_put(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::sync::GetMessage>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::PutMessage>>,
	) -> tg::Result<()> {
		// Create the graph.
		let graph = Mutex::new(Graph::new());

		// Create the progress.
		let progress = Progress::new();

		// Create the queue.
		let (queue_object_sender, queue_object_receiver) =
			async_channel::unbounded::<super::queue::ObjectItem>();
		let (queue_process_sender, queue_process_receiver) =
			async_channel::unbounded::<super::queue::ProcessItem>();
		let queue = Queue::new(queue_object_sender, queue_process_sender);

		// Create the state.
		let state = Arc::new(State {
			arg,
			graph,
			progress,
			queue,
			sender,
		});

		// Initialize the counter to 1.
		state.queue.increment(1);

		// Enqueue the items.
		for item in &state.arg.put {
			match item {
				Either::Left(object) => {
					let item = super::queue::ObjectItem {
						parent: None,
						id: object.clone(),
						kind: None,
						eager: state.arg.eager,
					};
					state.queue.enqueue_object(item);
				},
				Either::Right(process) => {
					let item = super::queue::ProcessItem {
						parent: None,
						id: process.clone(),
						eager: state.arg.eager,
					};
					state.queue.enqueue_process(item);
				},
			}
		}

		// Create the channels.
		let (index_object_sender, index_object_receiver) =
			tokio::sync::mpsc::channel::<self::index::ObjectItem>(256);
		let (index_process_sender, index_process_receiver) =
			tokio::sync::mpsc::channel::<self::index::ProcessItem>(256);
		let (store_object_sender, store_object_receiver) =
			tokio::sync::mpsc::channel::<self::store::ObjectItem>(256);
		let (store_process_sender, store_process_receiver) =
			tokio::sync::mpsc::channel::<self::store::ProcessItem>(256);

		// Spawn the input task.
		let input_task = Task::spawn({
			let server = self.clone();
			let state = state.clone();
			|_| {
				async move { server.sync_put_input_task(&state, stream).await }
					.instrument(tracing::Span::current())
			}
		});

		// Create the queue future.
		let queue_future = self
			.sync_put_queue(
				state.clone(),
				queue_object_receiver,
				queue_process_receiver,
				index_object_sender,
				index_process_sender,
				store_object_sender,
				store_process_sender,
			)
			.instrument(tracing::Span::current());

		// Create the index future.
		let index_future = self
			.sync_put_index(state.clone(), index_object_receiver, index_process_receiver)
			.instrument(tracing::Span::current());

		// Create the store future.
		let store_future = self
			.sync_put_store(state.clone(), store_object_receiver, store_process_receiver)
			.instrument(tracing::Span::current());

		// Spawn the progress task.
		let progress_task = Task::spawn({
			let server = self.clone();
			let state = state.clone();
			|stop| {
				async move {
					server
						.sync_put_progress_task(&state.progress, stop, &state.sender)
						.await;
				}
				.instrument(tracing::Span::current())
			}
		});

		drop(state);

		// Await the futures.
		future::try_join3(queue_future, index_future, store_future).await?;

		// Abort the input task.
		input_task.abort();

		// Stop and await the progress task.
		progress_task.stop();
		progress_task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "the progress task panicked"))?;

		Ok(())
	}
}
