use {
	super::{graph::Graph, progress::Progress, queue::Queue},
	crate::Server,
	futures::{future, stream::BoxStream},
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tracing::Instrument as _,
};

mod index;
mod input;
mod queue;
mod store;

struct State {
	arg: tg::sync::Arg,
	graph: Arc<Mutex<Graph>>,
	progress: Progress,
	queue: Queue,
	sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::GetMessage>>,
}

impl Server {
	pub(super) async fn sync_get(
		&self,
		arg: tg::sync::Arg,
		graph: Arc<Mutex<Graph>>,
		stream: BoxStream<'static, tg::sync::PutMessage>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::GetMessage>>,
	) -> tg::Result<()> {
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

		// Enqueue the items.
		for item in &state.arg.get {
			match item {
				tg::Either::Left(object) => {
					let item = super::queue::ObjectItem {
						parent: None,
						id: object.clone(),
						kind: None,
						eager: state.arg.eager,
					};
					state.queue.enqueue_object(item);
				},
				tg::Either::Right(process) => {
					let item = super::queue::ProcessItem {
						parent: None,
						id: process.clone(),
						eager: state.arg.eager,
					};
					state.queue.enqueue_process(item);
				},
			}
		}

		// Close the queue if there are no items.
		if state.arg.get.is_empty() {
			state.queue.close();
		}

		// Create the channels.
		let (store_object_sender, store_object_receiver) =
			tokio::sync::mpsc::channel::<self::store::ObjectItem>(256);
		let (store_process_sender, store_process_receiver) =
			tokio::sync::mpsc::channel::<self::store::ProcessItem>(256);
		let (index_object_sender, index_object_receiver) =
			tokio::sync::mpsc::channel::<self::index::ObjectItem>(256);
		let (index_process_sender, index_process_receiver) =
			tokio::sync::mpsc::channel::<self::index::ProcessItem>(256);

		// Create the input future.
		let input_future = {
			let server = self.clone();
			let state = state.clone();
			async move {
				server
					.sync_get_input(
						&state,
						stream,
						index_object_sender,
						index_process_sender,
						store_object_sender,
						store_process_sender,
					)
					.await
			}
			.instrument(tracing::Span::current())
		};

		// Create the queue future.
		let queue_future = self
			.sync_get_queue(state.clone(), queue_object_receiver, queue_process_receiver)
			.instrument(tracing::Span::current());

		// Create the index future.
		let index_future = self
			.sync_get_index(state.clone(), index_object_receiver, index_process_receiver)
			.instrument(tracing::Span::current());

		// Create the store future.
		let store_future = {
			let server = self.clone();
			let state = state.clone();
			async move {
				server
					.sync_get_store(&state, store_object_receiver, store_process_receiver)
					.await
			}
			.instrument(tracing::Span::current())
		};

		// Spawn the progress task.
		let progress_task = Task::spawn({
			let server = self.clone();
			let state = state.clone();
			|stop| {
				async move {
					server
						.sync_get_progress_task(&state.progress, stop, &state.sender)
						.await;
				}
				.instrument(tracing::Span::current())
			}
		});

		// Spawn the index publish task after the get finishes, even if it is interrupted.
		let index_publish_guard = scopeguard::guard((), {
			let state = state.clone();
			|()| {
				self.tasks
					.spawn({
						let server = self.clone();
						|_| {
							async move {
								let result = server.sync_get_index_publish(state).await;
								if let Err(error) = result {
									tracing::error!(?error);
								}
							}
							.instrument(tracing::Span::current())
						}
					})
					.detach();
			}
		});

		// Await the futures.
		future::try_join4(input_future, queue_future, index_future, store_future).await?;

		// Stop and await the progress task.
		progress_task.stop();
		progress_task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "the progress task panicked"))?;

		// Defuse the index publish guard and publish directly.
		scopeguard::ScopeGuard::into_inner(index_publish_guard);
		self.sync_get_index_publish(state).await?;

		Ok(())
	}
}
