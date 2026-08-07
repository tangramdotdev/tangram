use {
	super::{graph::Graph, progress::Progress, queue::Queue},
	crate::Session,
	futures::stream::BoxStream,
	std::{
		collections::BTreeMap,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tracing::Instrument as _,
};

mod database;
mod index;
mod input;
mod queue;
mod sandbox;
mod store;

struct State {
	arg: tg::sync::Arg,
	graph: Arc<Mutex<Graph>>,
	progress: Progress,
	queue: Queue,
	sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::PutMessage>>,
	tokens: BTreeMap<tg::object::Id, tg::grant::Token>,
}

impl Session {
	pub(super) async fn sync_put(
		&self,
		arg: tg::sync::Arg,
		graph: Arc<Mutex<Graph>>,
		stream: BoxStream<'static, tg::sync::GetMessage>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::PutMessage>>,
	) -> tg::Result<()> {
		// Create the progress.
		let progress = Progress::new();

		// Create the queue.
		let (queue_database_sender, queue_database_receiver) =
			async_channel::unbounded::<super::queue::DatabaseItem>();
		let (queue_object_sender, queue_object_receiver) =
			async_channel::unbounded::<super::queue::ObjectItem>();
		let (queue_process_sender, queue_process_receiver) =
			async_channel::unbounded::<super::queue::ProcessItem>();
		let (queue_sandbox_sender, queue_sandbox_receiver) =
			async_channel::unbounded::<super::queue::SandboxItem>();
		let queue = Queue::new(
			queue_database_sender,
			queue_object_sender,
			queue_process_sender,
			queue_sandbox_sender,
		);

		// Get the sandbox's tokens so an object that is missing locally can be reported with proof of access.
		let sandbox = self
			.try_get_principal_sandbox()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the principal's sandbox"))?;
		let tokens = match sandbox {
			Some(sandbox) => self
				.server
				.runner
				.state()
				.sandboxes()
				.get_by_id(&sandbox)
				.map(|state| state.tokens.clone())
				.unwrap_or_default(),
			None => BTreeMap::new(),
		};

		// Create the state.
		let state = Arc::new(State {
			arg,
			graph,
			progress,
			queue,
			sender,
			tokens,
		});

		// Enqueue the items.
		for item in &state.arg.put {
			let token = item.options.token.clone();
			state
				.queue
				.enqueue(state.arg.eager, item.item.clone(), token)?;
		}

		// Create the channels.
		let (database_sender, database_receiver) =
			tokio::sync::mpsc::channel::<self::database::Item>(256);
		let (index_object_sender, index_object_receiver) =
			tokio::sync::mpsc::channel::<self::index::ObjectItem>(256);
		let (index_process_sender, index_process_receiver) =
			tokio::sync::mpsc::channel::<self::index::ProcessItem>(256);
		let (store_object_sender, store_object_receiver) =
			tokio::sync::mpsc::channel::<self::store::ObjectItem>(256);
		let (store_process_sender, store_process_receiver) =
			tokio::sync::mpsc::channel::<self::store::ProcessItem>(256);
		let (sandbox_sender, sandbox_receiver) =
			tokio::sync::mpsc::channel::<self::sandbox::Item>(256);

		// Spawn the input task.
		let input_task = Task::spawn({
			let session = self.clone();
			let state = state.clone();
			|_| {
				async move { session.sync_put_input_task(&state, stream).await }
					.instrument(tracing::Span::current())
			}
		});

		// Create the queue future.
		let queue_arg = self::queue::SyncPutQueueArg {
			database_sender,
			state: state.clone(),
			queue_database_receiver,
			queue_object_receiver,
			queue_process_receiver,
			queue_sandbox_receiver,
			index_object_sender,
			index_process_sender,
			sandbox_sender,
			store_object_sender,
			store_process_sender,
		};
		let queue_future = self
			.sync_put_queue(queue_arg)
			.instrument(tracing::Span::current());

		// Create the database future.
		let database_future = self
			.sync_put_database(state.clone(), database_receiver)
			.instrument(tracing::Span::current());

		// Create the index future.
		let index_future = self
			.sync_put_index(state.clone(), index_object_receiver, index_process_receiver)
			.instrument(tracing::Span::current());

		// Create the store future.
		let store_future = self
			.sync_put_store(state.clone(), store_object_receiver, store_process_receiver)
			.instrument(tracing::Span::current());

		// Create the sandbox future.
		let sandbox_future = self
			.sync_put_sandbox(state.clone(), sandbox_receiver)
			.instrument(tracing::Span::current());

		// Spawn the progress task.
		let progress_task = Task::spawn({
			let session = self.clone();
			let state = state.clone();
			|stop| {
				async move {
					session
						.sync_put_progress_task(&state.progress, stop, &state.sender)
						.await;
				}
				.instrument(tracing::Span::current())
			}
		});

		// Await the futures.
		futures::try_join!(
			database_future,
			index_future,
			queue_future,
			sandbox_future,
			store_future
		)?;

		// Send the put end message after all futures complete.
		state
			.sender
			.send(Ok(tg::sync::PutMessage::End))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the put end message"))?;

		// Abort the input task.
		input_task.abort();

		// Stop and await the progress task.
		progress_task.stop();
		progress_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the progress task panicked"))?;

		Ok(())
	}
}
