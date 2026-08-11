use {
	super::{graph::Graph, progress::Progress, queue::Queue},
	crate::Session,
	futures::stream::BoxStream,
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_index::prelude::*,
	tracing::Instrument as _,
};

mod database;
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
		let (queue_database_sender, queue_database_receiver) =
			async_channel::unbounded::<super::queue::DatabaseNode>();
		let (queue_object_sender, queue_object_receiver) =
			async_channel::unbounded::<super::queue::ObjectNode>();
		let (queue_process_sender, queue_process_receiver) =
			async_channel::unbounded::<super::queue::ProcessNode>();
		let (queue_sandbox_sender, queue_sandbox_receiver) =
			async_channel::unbounded::<super::queue::SandboxNode>();
		let queue = Queue::new(
			queue_database_sender,
			queue_object_sender,
			queue_process_sender,
			queue_sandbox_sender,
		);

		// Create the state.
		let state = Arc::new(State {
			arg,
			graph,
			progress,
			queue,
			sender,
		});

		// Resolve all initial specifiers through one index batch.
		if state.arg.ancestors == tg::node::AncestorsPull::Missing {
			let specifiers = state
				.arg
				.get
				.iter()
				.filter_map(|node| match &node.node {
					tg::Selector::Id(_) => None,
					tg::Selector::Specifier(specifier) => Some(specifier.clone()),
				})
				.collect::<Vec<_>>();
			let ids = self
				.try_get_ids_for_specifiers_from_index(&specifiers)
				.await?;
			state
				.graph
				.lock()
				.unwrap()
				.set_local_selector_ids(std::iter::zip(specifiers, ids));
		}

		// Enqueue the nodes.
		for node in &state.arg.get {
			let token = node.options.token.clone();
			match &node.node {
				tg::Selector::Id(id) => {
					state.queue.enqueue(state.arg.eager, id.clone(), token)?;
				},
				tg::Selector::Specifier(specifier) => {
					let message = tg::sync::GetMessage::Node(tg::sync::GetNodeMessage {
						descendants: true,
						eager: state.arg.eager,
						selector: tg::Selector::Specifier(specifier.clone()),
						token,
					});
					state
						.sender
						.send(Ok(message))
						.await
						.map_err(|error| tg::error!(!error, "failed to send the message"))?;
				},
			}
		}

		// Close the queue if there are no nodes.
		if state.arg.get.is_empty() {
			state.queue.close();
		}

		// Create the channels.
		let (store_object_sender, store_object_receiver) =
			tokio::sync::mpsc::channel::<self::store::ObjectNode>(256);
		let (store_process_sender, store_process_receiver) =
			tokio::sync::mpsc::channel::<self::store::ProcessNode>(256);
		let (index_object_sender, index_object_receiver) =
			tokio::sync::mpsc::channel::<self::index::ObjectNode>(256);
		let (index_process_sender, index_process_receiver) =
			tokio::sync::mpsc::channel::<self::index::ProcessNode>(256);
		// Create the input future.
		let input_future = {
			let session = self.clone();
			let arg = self::input::SyncGetInputArg {
				index_object_sender,
				index_process_sender,
				state: state.clone(),
				store_object_sender,
				store_process_sender,
				stream,
			};
			async move { session.sync_get_input(arg).await }.instrument(tracing::Span::current())
		};

		// Create the queue future.
		let queue_future = self
			.sync_get_queue(
				state.clone(),
				queue_database_receiver,
				queue_object_receiver,
				queue_process_receiver,
				queue_sandbox_receiver,
			)
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

		// Index the partial graph on a best-effort basis if the sync is interrupted.
		let index_guard = scopeguard::guard(state.graph.clone(), {
			let session = self.clone();
			let span = tracing::Span::current();
			move |graph| {
				tokio::spawn(
					async move {
						if let Err(error) = session.sync_get_index_put_partial(graph).await {
							tracing::error!(error = %error.trace(), "failed to index the partial sync");
						}
					}
					.instrument(span),
				);
			}
		});

		// Await the futures.
		futures::try_join!(index_future, input_future, queue_future, store_future)?;

		// Index the objects, processes, and sandboxes and finalize the graph permissions.
		let graph = scopeguard::ScopeGuard::into_inner(index_guard);
		self.sync_get_index_put(graph.clone()).await?;

		// Stop and await the progress task.
		progress_task.stop();
		progress_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the progress task panicked"))?;

		// Commit the database nodes.
		self.sync_get_database(&graph).await?;

		Ok(())
	}

	fn sync_get_create_temporary_grant(
		&self,
		id: &tg::Id,
	) -> tg::Result<Option<tangram_index::grant::put::Arg>> {
		if matches!(self.context.principal, tg::Principal::Root) {
			return Ok(None);
		}
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let time_to_live = i64::try_from(self.server.config.sync.grant_time_to_live.as_secs())
			.map_err(|error| tg::error!(!error, "failed to convert the grant time to live"))?;
		let expires_at = created_at
			.checked_add(time_to_live)
			.ok_or_else(|| tg::error!("the grant expiration overflowed"))?;
		let permission = Self::admin_permission_for_resource(id)?;
		let permissions = tg::grant::permission::Set::from_permission(permission);
		let principal = self.context.principal.try_to_grant_principal()?;
		let arg = tangram_index::grant::put::Arg {
			created_at,
			creator: Some(self.context.principal.clone()),
			expires_at: Some(expires_at),
			permissions,
			principal,
			resource: id.clone(),
			time_to_touch: Some(self.server.config.sync.grant_time_to_touch),
		};

		Ok(Some(arg))
	}

	async fn sync_get_touch_authorized_objects(
		&self,
		graph: &Arc<Mutex<Graph>>,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<(
		Vec<Option<tangram_index::object::Object>>,
		Vec<Option<tg::grant::permission::Set>>,
	)> {
		let mut permissions = self.sync_get_authorize_objects(graph, ids).await?;
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

	async fn sync_get_authorize_objects(
		&self,
		graph: &Arc<Mutex<Graph>>,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::grant::permission::Set>>> {
		let required = Self::sync_get_object_permissions();
		self.sync_get_authorize(graph, ids.iter().cloned().map(tg::Id::from), required)
			.await
	}

	fn sync_get_object_permissions() -> tg::grant::permission::Set {
		tg::grant::permission::Set::from_permission(tg::grant::Permission::Object(
			tg::grant::permission::object::Permission::Subtree,
		))
	}

	async fn sync_get_touch_authorized_processes(
		&self,
		graph: &Arc<Mutex<Graph>>,
		ids: &[tg::process::Id],
		arg: &tg::sync::Arg,
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<(
		Vec<Option<tangram_index::process::Process>>,
		Vec<Option<tg::grant::permission::Set>>,
	)> {
		let mut permissions = self.sync_get_authorize_processes(graph, ids, arg).await?;
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

	async fn sync_get_authorize_processes(
		&self,
		graph: &Arc<Mutex<Graph>>,
		ids: &[tg::process::Id],
		arg: &tg::sync::Arg,
	) -> tg::Result<Vec<Option<tg::grant::permission::Set>>> {
		let Some(required) = Self::sync_get_process_permissions(arg) else {
			return Ok(vec![None; ids.len()]);
		};

		self.sync_get_authorize(graph, ids.iter().cloned().map(tg::Id::from), required)
			.await
	}

	fn sync_get_process_permissions(arg: &tg::sync::Arg) -> Option<tg::grant::permission::Set> {
		let mut permissions =
			tg::grant::permission::Set::Process(tg::grant::permission::process::Set::empty());
		let mut insert = |permission| {
			permissions.insert(tg::grant::permission::Set::from_permission(
				tg::grant::Permission::Process(permission),
			));
		};
		if arg.process_children {
			insert(tg::grant::permission::process::Permission::Subtree);
			if arg.process_commands {
				insert(tg::grant::permission::process::Permission::SubtreeCommand);
			}
			if arg.process_errors {
				insert(tg::grant::permission::process::Permission::SubtreeError);
			}
			if arg.process_logs {
				insert(tg::grant::permission::process::Permission::SubtreeLog);
			}
			if arg.process_outputs {
				insert(tg::grant::permission::process::Permission::SubtreeOutput);
			}
		} else {
			if arg.process_commands {
				insert(tg::grant::permission::process::Permission::NodeCommand);
			}
			if arg.process_errors {
				insert(tg::grant::permission::process::Permission::NodeError);
			}
			if arg.process_logs {
				insert(tg::grant::permission::process::Permission::NodeLog);
			}
			if arg.process_outputs {
				insert(tg::grant::permission::process::Permission::NodeOutput);
			}
		}
		(!permissions.is_empty()).then_some(permissions)
	}

	async fn sync_get_authorize(
		&self,
		graph: &Arc<Mutex<Graph>>,
		ids: impl IntoIterator<Item = tg::Id>,
		required: tg::grant::permission::Set,
	) -> tg::Result<Vec<Option<tg::grant::permission::Set>>> {
		let ids = ids.into_iter().collect::<Vec<_>>();

		// Collect the nodes whose permissions cannot be proven by the graph.
		let mut args = Vec::<(tg::Referent<tg::Id>, tg::grant::permission::Set)>::new();
		let mut positions = Vec::new();
		let mut outputs = vec![None; ids.len()];
		{
			let mut graph = graph.lock().unwrap();
			for (position, id) in ids.iter().enumerate() {
				let authorization = match id.kind() {
					tg::id::Kind::Process => {
						graph.get_process_local_authorization(&id.clone().try_into()?, required)
					},
					_ => graph.get_object_local_authorization(&id.clone().try_into()?, required),
				};
				if authorization.permissions.contains(required) {
					outputs[position] = Some(authorization.permissions);
					continue;
				}
				let resource = tg::Referent::with_node_and_token(id.clone(), authorization.token);
				args.push((resource, required));
				positions.push(position);
			}
		}

		if args.is_empty() {
			return Ok(outputs);
		}

		// Authorize the remaining nodes.
		let authorization_outputs = self
			.authorize_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the sync nodes"))?;
		let mut graph = graph.lock().unwrap();
		for (position, output) in std::iter::zip(positions, authorization_outputs) {
			if let Some(permissions) = output {
				match ids[position].kind() {
					tg::id::Kind::Process => graph.update_process_local_permissions(
						&ids[position].clone().try_into()?,
						permissions,
					),
					_ => graph.update_object_local_permissions(
						&ids[position].clone().try_into()?,
						permissions,
					),
				}
			}
		}
		for (position, id) in ids.iter().enumerate() {
			let authorization = match id.kind() {
				tg::id::Kind::Process => {
					graph.get_process_local_authorization(&id.clone().try_into()?, required)
				},
				_ => graph.get_object_local_authorization(&id.clone().try_into()?, required),
			};
			if authorization.permissions.contains(required) {
				outputs[position] = Some(authorization.permissions);
			}
		}

		Ok(outputs)
	}
}
