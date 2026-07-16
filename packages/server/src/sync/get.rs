use {
	super::{graph::Graph, progress::Progress, queue::Queue},
	crate::Session,
	futures::{future, stream::BoxStream},
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_index::prelude::*,
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
			let (item, token) = match item {
				tg::Either::Left(item) => (item, None),
				tg::Either::Right(item) => (&item.id, Some(item.token.clone())),
			};
			match item {
				tg::Either::Left(object) => {
					let item = super::queue::ObjectItem {
						eager: state.arg.eager,
						id: object.clone(),
						kind: None,
						parent: None,
						token,
					};
					state.queue.enqueue_object(item);
				},
				tg::Either::Right(process) => {
					let item = super::queue::ProcessItem {
						eager: state.arg.eager,
						id: process.clone(),
						parent: None,
						token,
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

		// Stop and await the progress task.
		progress_task.stop();
		progress_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the progress task panicked"))?;

		Ok(())
	}

	async fn sync_get_authorize_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::grant::permission::Set>>> {
		let permissions = Self::sync_get_object_permissions();
		self.sync_get_authorize(ids.iter().cloned().map(tg::Id::from), permissions)
			.await
	}

	async fn sync_get_touch_authorized_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<(
		Vec<Option<tangram_index::object::Object>>,
		Vec<Option<tg::grant::permission::Set>>,
	)> {
		let mut permissions = self.sync_get_authorize_objects(ids).await?;
		let mut touch_indices = Vec::new();
		let mut touch_ids = Vec::new();
		for (index, (id, permissions)) in std::iter::zip(ids, &permissions).enumerate() {
			if permissions.is_some() {
				touch_indices.push(index);
				touch_ids.push(id.clone());
			}
		}
		let touched = self
			.server
			.index
			.touch_objects(&touch_ids, touched_at, time_to_touch)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch the objects"))?;
		let mut outputs = vec![None; ids.len()];
		for (index, output) in std::iter::zip(touch_indices, touched) {
			if output.is_none() {
				permissions[index] = None;
			}
			outputs[index] = output;
		}
		Ok((outputs, permissions))
	}

	fn sync_get_object_permissions() -> tg::grant::permission::Set {
		tg::grant::permission::Set::from_permission(tg::grant::Permission::Object(
			tg::grant::permission::object::Permission::Subtree,
		))
	}

	async fn sync_get_authorize_processes(
		&self,
		ids: &[tg::process::Id],
		arg: &tg::sync::Arg,
	) -> tg::Result<Vec<Option<tg::grant::permission::Set>>> {
		let Some(permissions) = Self::sync_get_process_permissions(arg) else {
			return Ok(vec![None; ids.len()]);
		};
		self.sync_get_authorize(ids.iter().cloned().map(tg::Id::from), permissions)
			.await
	}

	async fn sync_get_touch_authorized_processes(
		&self,
		ids: &[tg::process::Id],
		arg: &tg::sync::Arg,
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<(
		Vec<Option<tangram_index::process::Process>>,
		Vec<Option<tg::grant::permission::Set>>,
	)> {
		let mut permissions = self.sync_get_authorize_processes(ids, arg).await?;
		let mut touch_indices = Vec::new();
		let mut touch_ids = Vec::new();
		for (index, (id, permissions)) in std::iter::zip(ids, &permissions).enumerate() {
			if permissions.is_some() {
				touch_indices.push(index);
				touch_ids.push(id.clone());
			}
		}
		let touched = self
			.server
			.index
			.touch_processes(&touch_ids, touched_at, time_to_touch)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch the processes"))?;
		let mut outputs = vec![None; ids.len()];
		for (index, output) in std::iter::zip(touch_indices, touched) {
			if output.is_none() {
				permissions[index] = None;
			}
			outputs[index] = output;
		}
		Ok((outputs, permissions))
	}

	async fn sync_get_authorize(
		&self,
		ids: impl IntoIterator<Item = tg::Id>,
		required: tg::grant::permission::Set,
	) -> tg::Result<Vec<Option<tg::grant::permission::Set>>> {
		let ids = ids.into_iter().collect::<Vec<_>>();
		let args = ids
			.iter()
			.map(|id| tangram_index::authorize::Arg {
				resource: tg::grant::Resource::Id(id.clone()),
				permissions: required,
				token: None,
			})
			.collect::<Vec<_>>();

		let mut permissions = vec![None; ids.len()];
		if args.is_empty() {
			return Ok(permissions);
		}
		let outputs = self
			.server
			.index
			.authorize_batch(&args, &self.context.principal)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the sync items"))?;
		for (permissions, output) in std::iter::zip(&mut permissions, outputs) {
			*permissions = output
				.map(|output| output.permissions)
				.filter(|permissions| permissions.contains(required));
		}
		Ok(permissions)
	}

	fn sync_get_process_permissions(arg: &tg::sync::Arg) -> Option<tg::grant::permission::Set> {
		let mut permissions =
			tg::grant::permission::Set::Process(tg::grant::permission::process::Set::empty());
		let mut insert = |permission| {
			permissions.insert(tg::grant::permission::Set::from_permission(
				tg::grant::Permission::Process(permission),
			));
		};
		if arg.recursive {
			insert(tg::grant::permission::process::Permission::Subtree);
			if arg.commands {
				insert(tg::grant::permission::process::Permission::SubtreeCommand);
			}
			if arg.errors {
				insert(tg::grant::permission::process::Permission::SubtreeError);
			}
			if arg.logs {
				insert(tg::grant::permission::process::Permission::SubtreeLog);
			}
			if arg.outputs {
				insert(tg::grant::permission::process::Permission::SubtreeOutput);
			}
		} else {
			if arg.commands {
				insert(tg::grant::permission::process::Permission::NodeCommand);
			}
			if arg.errors {
				insert(tg::grant::permission::process::Permission::NodeError);
			}
			if arg.logs {
				insert(tg::grant::permission::process::Permission::NodeLog);
			}
			if arg.outputs {
				insert(tg::grant::permission::process::Permission::NodeOutput);
			}
		}
		(!permissions.is_empty()).then_some(permissions)
	}
}
