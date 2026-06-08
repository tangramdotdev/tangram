use {
	super::{graph::Graph, progress::Progress, queue::Queue},
	crate::Session,
	futures::{future, stream::BoxStream},
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_object_store::prelude::*,
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

impl Session {
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
			let session = self.clone();
			let state = state.clone();
			async move {
				session
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
			let session = self.clone();
			let state = state.clone();
			async move {
				session
					.sync_get_store(&state, store_object_receiver, store_process_receiver)
					.await
			}
			.instrument(tracing::Span::current())
		};

		// Spawn the progress task.
		let progress_task = Task::spawn({
			let session = self.clone();
			let state = state.clone();
			|stop| {
				async move {
					session
						.sync_get_progress_task(&state.progress, stop, &state.sender)
						.await;
				}
				.instrument(tracing::Span::current())
			}
		});

		// Spawn the index task after the get finishes, even if it is interrupted.
		let _index_guard = scopeguard::guard((), {
			let graph = state.graph.clone();
			|()| {
				self.server
					.index_tasks
					.spawn({
						let session = self.clone();
						|_| {
							async move {
								let result = session.sync_get_index_put(graph).await;
								if let Err(error) = result {
									tracing::error!(error = %error.trace());
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

		// Grant.
		self.sync_get_grant(&state).await?;

		// Stop and await the progress task.
		progress_task.stop();
		progress_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the progress task panicked"))?;

		Ok(())
	}

	async fn sync_get_grant(&self, state: &State) -> tg::Result<()> {
		let Some(principal) = self.context.principal.clone() else {
			return Ok(());
		};
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let args = state
			.arg
			.get
			.iter()
			.filter_map(|item| {
				let tg::Either::Left(id) = item else {
					return None;
				};
				let arg = crate::object::store::GrantArg {
					created_at,
					id: id.clone(),
					principal: principal.clone(),
					subtree: true,
				};
				Some(arg)
			})
			.collect::<Vec<_>>();
		self.server
			.object_store
			.grant_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to grant access to the object roots"))?;
		Ok(())
	}
}
