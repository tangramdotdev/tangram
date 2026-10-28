use {
	super::{graph::Graph, progress::Progress, queue::Queue, service},
	crate::Component,
	futures::{FutureExt as _, stream::BoxStream},
	std::{
		collections::BTreeSet,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_server_core::Session,
	tracing::Instrument as _,
};

mod checkout;
mod database;
mod index;
mod input;
mod nodes;
mod output;
mod pending;
mod queue;
mod store;

pub(super) struct SyncGetArg {
	pub(super) get: Option<tokio::sync::mpsc::Receiver<tg::Referent<tg::Selector<tg::Id>>>>,
	pub(super) graph: Arc<Mutex<Graph>>,
	pub(super) sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::GetMessage>>,
	pub(super) stream: BoxStream<'static, tg::sync::PutMessage>,
	pub(super) sync_arg: tg::sync::Arg,
	pub(super) trust: bool,
}

pub(super) struct State {
	pub(super) arg: tg::sync::Arg,
	pub(super) graph: Arc<Mutex<Graph>>,
	pub(super) id: tg::sync::Id,
	pending: Mutex<BTreeSet<tg::Id>>,
	progress: Progress,
	queue: Queue,
	sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::GetMessage>>,
	pub(super) service: service::Handle,
}

struct Completion {
	enqueued: bool,
	result: tg::Result<()>,
}

struct SyncGetTouchAuthorizedProcessesArg<'a> {
	graph: &'a Arc<Mutex<Graph>>,
	ids: &'a [tg::process::Id],
	sync: &'a tg::sync::Id,
	sync_arg: &'a tg::sync::Arg,
	time_to_touch: std::time::Duration,
	touched_at: i64,
}

struct SyncGetTouchAuthorizedProcessesWithDataArg<'a> {
	graph: &'a Arc<Mutex<Graph>>,
	ids: &'a [tg::process::Id],
	sync: &'a tg::sync::Id,
	sync_arg: &'a tg::sync::Arg,
	time_to_touch: std::time::Duration,
	touched_at: i64,
}

impl Component {
	pub(super) fn sync_get_checkout_pointers_enabled(session: &Session) -> bool {
		session.server.config.sync.get.checkout_pointers
			&& session.server.checkouts_enabled()
			&& !session.server.config.advanced.multi_server
	}

	pub(super) async fn sync_get(&self, session: &Session, arg: SyncGetArg) -> tg::Result<()> {
		let SyncGetArg {
			get,
			graph,
			sender,
			stream,
			sync_arg,
			trust,
		} = arg;

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
		let id = sync_arg
			.sync
			.as_ref()
			.and_then(tg::sync::Join::sync)
			.map_or_else(tg::sync::Id::new, |sync| sync.node.clone());
		let service = self.spawn_sync_service_task(session, graph.clone(), &id);
		let state = Arc::new(State {
			arg: sync_arg,
			graph,
			id,
			pending: Mutex::default(),
			progress,
			queue,
			sender,
			service,
		});

		// Keep the service alive until the permissions are queued, including when the transfer is cancelled.
		let completion = Completion {
			enqueued: false,
			result: Err(tg::error!(
				code = tg::error::Code::Cancellation,
				"the sync was cancelled"
			)),
		};
		let mut completion = scopeguard::guard(completion, {
			let component = self.clone();
			let session = session.clone();
			let state = state.clone();
			move |completion| {
				if completion.enqueued {
					state.service.finish(completion.result);
				} else {
					component.spawn_sync_get_index_partial_task(&session, state, completion.result);
				}
			}
		});
		let result = async {
			// Receive the initial and additional nodes.
			let nodes_future =
				Self::sync_get_nodes(&state.arg, &state.graph, &state.queue, &state.sender, get);

			// Create the channels.
			let config = &session.server.config.sync.get;
			let (store_object_sender, store_object_receiver) =
				tokio::sync::mpsc::channel::<self::store::ObjectNode>(config.channel_capacity);
			let (store_process_sender, store_process_receiver) =
				tokio::sync::mpsc::channel::<self::store::ProcessNode>(config.channel_capacity);
			let (checkout_sender, checkout_receiver) = tokio::sync::mpsc::channel::<
				self::checkout::ObjectNode,
			>(config.checkout.channel_capacity);
			let (index_object_sender, index_object_receiver) =
				tokio::sync::mpsc::channel::<self::index::ObjectNode>(config.channel_capacity);
			let (index_process_sender, index_process_receiver) =
				tokio::sync::mpsc::channel::<self::index::ProcessNode>(config.channel_capacity);

			// Create the input future.
			let input_future = {
				let component = self.clone();
				let session = session.clone();
				let arg = self::input::SyncGetInputArg {
					checkout_sender: checkout_sender.clone(),
					index_object_sender,
					index_process_sender,
					state: state.clone(),
					store_object_sender: store_object_sender.clone(),
					store_process_sender,
					stream,
					trust,
				};
				async move { component.sync_get_input(&session, arg).await }
					.instrument(tracing::Span::current())
			};

			// Create the queue future.
			let arg = self::queue::SyncGetQueueArg {
				checkout_sender: checkout_sender.clone(),
				queue_database_receiver,
				queue_object_receiver,
				queue_process_receiver,
				queue_sandbox_receiver,
				state: state.clone(),
			};
			let queue_future = self
				.sync_get_queue(session, arg)
				.instrument(tracing::Span::current());

			// Create the checkout future.
			let checkout_future = self
				.sync_get_checkout(
					session,
					state.clone(),
					checkout_receiver,
					store_object_sender.clone(),
				)
				.instrument(tracing::Span::current());

			// Create the index future.
			let index_future = self
				.sync_get_index(
					session,
					state.clone(),
					checkout_sender,
					index_object_receiver,
					index_process_receiver,
				)
				.instrument(tracing::Span::current());

			// Create the store future.
			let store_future = {
				let component = self.clone();
				let session = session.clone();
				let state = state.clone();
				async move {
					component
						.sync_get_store(
							&session,
							&state,
							store_object_receiver,
							store_process_receiver,
						)
						.await
				}
				.instrument(tracing::Span::current())
			};
			drop(store_object_sender);

			// Spawn the progress task.
			let progress_task = Task::spawn({
				let component = self.clone();
				let session = session.clone();
				let state = state.clone();
				|stop| {
					async move {
						component
							.sync_get_progress_task(&session, &state.progress, stop, &state.sender)
							.await;
					}
					.instrument(tracing::Span::current())
				}
			});

			// Await the futures.
			let (get, (), (), (), (), ()) = futures::try_join!(
				nodes_future,
				checkout_future,
				index_future,
				input_future,
				queue_future,
				store_future
			)?;

			// Send the get output before indexing.
			self.sync_get_output(session, &state, &get).await?;

			// Index the objects, processes, and sandboxes and update the graph permissions.
			self.sync_get_index_put(session, state.graph.clone(), &state.id)
				.await?;
			completion.enqueued = true;

			// Stop and await the progress task.
			progress_task.stop();
			progress_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the progress task panicked"))?;

			// Commit the database nodes.
			self.sync_get_database(session, &state.graph, state.arg.force, &state.id)
				.await?;

			Ok(())
		}
		.boxed()
		.await;
		completion.result = result.clone();
		result
	}

	fn spawn_sync_get_index_partial_task(
		&self,
		session: &Session,
		state: Arc<State>,
		result: tg::Result<()>,
	) {
		self.index_tasks
			.spawn({
				let component = self.clone();
				let session = session.clone();
				move |_| {
					async move {
						component
							.sync_get_index_partial_task(&session, state, result)
							.await
					}
					.instrument(tracing::Span::current())
				}
			})
			.detach();
	}

	async fn sync_get_index_partial_task(
		&self,
		session: &Session,
		state: Arc<State>,
		result: tg::Result<()>,
	) -> tg::Result<()> {
		let indexed = self
			.sync_get_index_put_partial(session, state.graph.clone(), &state.id)
			.await;
		if let Err(error) = &indexed {
			tracing::error!(error = %error.trace(), "failed to index the partial sync");
		}
		state.service.finish(indexed.clone().and(result));
		indexed?;
		Ok(())
	}

	fn sync_get_create_permission(
		session: &Session,
		id: &tg::Id,
		sync: Option<&tg::sync::Id>,
	) -> tg::Result<Option<tangram_index::permission::put::Arg>> {
		if sync.is_none()
			&& id.kind() != tg::id::Kind::Sync
			&& matches!(session.context.principal, tg::Principal::Root)
		{
			return Ok(None);
		}
		let created_at = session.server.clock.unix_timestamp();
		let time_to_live = i64::try_from(
			session.server.config.sync.permission_time_to_live.as_secs(),
		)
		.map_err(|error| tg::error!(!error, "failed to convert the permission time to live"))?;
		let expires_at = created_at
			.checked_add(time_to_live)
			.ok_or_else(|| tg::error!("the permission expiration overflowed"))?;
		let permission = match id.kind() {
			tg::id::Kind::Process => tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Node,
			),
			tg::id::Kind::Sandbox => tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Node,
			),
			tg::id::Kind::Sync => tg::authorization::Permission::Sync(
				tg::authorization::permission::sync::Permission::Read,
			),
			_ => tangram_server_grant_common::admin_permission_for_resource(id)?,
		};
		let permissions = tg::authorization::permission::Set::from_permission(permission);
		let subject = match sync {
			Some(id) => tg::authorization::Subject::Sync(id.clone()),
			None => match &session.context.principal {
				tg::Principal::Anonymous => tg::authorization::Subject::Public,
				principal => principal.try_to_subject()?,
			},
		};
		let arg = tangram_index::permission::put::Arg {
			created_at,
			creator: Some(session.context.principal.clone()),
			permissions,
			resource: id.clone(),
			source: tangram_index::permission::Source::Direct {
				expires_at: Some(expires_at),
			},
			subject,
			time_to_touch: Some(session.server.config.sync.permission_time_to_touch),
			version: None,
		};

		Ok(Some(arg))
	}

	async fn sync_get_touch_authorized_objects(
		&self,
		session: &Session,
		graph: &Arc<Mutex<Graph>>,
		sync: &tg::sync::Id,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<(
		Vec<Option<tangram_index::object::Object>>,
		Vec<Option<tg::authorization::permission::Set>>,
	)> {
		let permissions = self
			.sync_get_authorize_objects(session, graph, sync, ids)
			.await?;
		let mut touch_indices = Vec::new();
		let mut touch_ids = Vec::new();
		for (index, (id, permissions)) in std::iter::zip(ids, &permissions).enumerate() {
			if permissions.is_some() {
				touch_indices.push(index);
				touch_ids.push(id.clone());
			}
		}
		let account = session
			.server
			.components
			.usage
			.usage_account(session, &session.context.principal)
			.await?;
		let touched = session
			.server
			.index
			.touch_objects_with_account(&touch_ids, account.as_ref(), touched_at, time_to_touch)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch the objects"))?;
		let mut outputs = vec![None; ids.len()];
		for (index, output) in std::iter::zip(touch_indices, touched) {
			outputs[index] = output;
		}
		Ok((outputs, permissions))
	}

	async fn sync_get_authorize_objects(
		&self,
		session: &Session,
		graph: &Arc<Mutex<Graph>>,
		sync: &tg::sync::Id,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::authorization::permission::Set>>> {
		let mut requested = tg::authorization::permission::object::Set::empty();
		requested.insert(tg::authorization::permission::object::Set::NODE);
		requested.insert(tg::authorization::permission::object::Set::SUBTREE);
		let requested = tg::authorization::permission::Set::Object(requested);
		let required = tg::authorization::permission::Set::from_permission(
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			),
		);
		self.sync_get_authorize(
			session,
			graph,
			sync,
			ids.iter().cloned().map(tg::Id::from),
			requested,
			required,
		)
		.await
	}

	async fn sync_get_touch_authorized_processes(
		&self,
		session: &Session,
		arg: SyncGetTouchAuthorizedProcessesArg<'_>,
	) -> tg::Result<(
		Vec<Option<tangram_index::process::Process>>,
		Vec<Option<tg::authorization::permission::Set>>,
	)> {
		let SyncGetTouchAuthorizedProcessesArg {
			graph,
			ids,
			sync,
			sync_arg,
			time_to_touch,
			touched_at,
		} = arg;
		let permissions = self
			.sync_get_authorize_processes(session, graph, sync, ids, sync_arg)
			.await?;
		let mut touch_indices = Vec::new();
		let mut touch_ids = Vec::new();
		for (index, (id, permissions)) in std::iter::zip(ids, &permissions).enumerate() {
			if permissions.is_some() {
				touch_indices.push(index);
				touch_ids.push(id.clone());
			}
		}
		let account = session
			.server
			.components
			.usage
			.usage_account(session, &session.context.principal)
			.await?;
		let touched = session
			.server
			.index
			.touch_processes_with_account(&touch_ids, account.as_ref(), touched_at, time_to_touch)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch the processes"))?;
		let mut outputs = vec![None; ids.len()];
		for (index, output) in std::iter::zip(touch_indices, touched) {
			// Treat a process without data as absent.
			outputs[index] = output.filter(|process| process.status.is_some());
		}
		Ok((outputs, permissions))
	}

	async fn sync_get_touch_authorized_processes_with_data(
		&self,
		session: &Session,
		arg: SyncGetTouchAuthorizedProcessesWithDataArg<'_>,
	) -> tg::Result<(
		Vec<Option<(tangram_index::process::Process, tg::process::Data)>>,
		Vec<Option<tg::authorization::permission::Set>>,
	)> {
		let SyncGetTouchAuthorizedProcessesWithDataArg {
			graph,
			ids,
			sync,
			sync_arg,
			time_to_touch,
			touched_at,
		} = arg;
		let permissions = self
			.sync_get_authorize_processes(session, graph, sync, ids, sync_arg)
			.await?;
		let mut touch_indices = Vec::new();
		let mut touch_ids = Vec::new();
		for (index, (id, permissions)) in std::iter::zip(ids, &permissions).enumerate() {
			if permissions.is_some() {
				touch_indices.push(index);
				touch_ids.push(id.clone());
			}
		}
		let account = session
			.server
			.components
			.usage
			.usage_account(session, &session.context.principal)
			.await?;
		let touched = session
			.server
			.index
			.touch_processes_with_data_with_account(
				&touch_ids,
				account.as_ref(),
				touched_at,
				time_to_touch,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch the processes"))?;
		let mut outputs = vec![None; ids.len()];
		for (index, output) in std::iter::zip(touch_indices, touched) {
			// Treat a process without data as absent.
			outputs[index] = output.and_then(|(process, data)| Some((process, data?)));
		}
		Ok((outputs, permissions))
	}

	async fn sync_get_authorize_processes(
		&self,
		session: &Session,
		graph: &Arc<Mutex<Graph>>,
		sync: &tg::sync::Id,
		ids: &[tg::process::Id],
		arg: &tg::sync::Arg,
	) -> tg::Result<Vec<Option<tg::authorization::permission::Set>>> {
		let required = Self::sync_get_process_permissions(arg);

		self.sync_get_authorize(
			session,
			graph,
			sync,
			ids.iter().cloned().map(tg::Id::from),
			required,
			required,
		)
		.await
	}

	fn sync_get_process_permissions(arg: &tg::sync::Arg) -> tg::authorization::permission::Set {
		let mut permissions = tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::empty(),
		);
		let mut insert = |permission| {
			permissions.insert(tg::authorization::permission::Set::from_permission(
				tg::authorization::Permission::Process(permission),
			));
		};
		insert(tg::authorization::permission::process::Permission::Node);
		if arg.process_children {
			insert(tg::authorization::permission::process::Permission::Subtree);
			if arg.process_command_objects {
				insert(tg::authorization::permission::process::Permission::SubtreeCommandObjects);
			}
			if arg.process_error_objects {
				insert(tg::authorization::permission::process::Permission::SubtreeErrorObjects);
			}
			if arg.process_log_objects {
				insert(tg::authorization::permission::process::Permission::SubtreeLogObjects);
			}
			if arg.process_output_objects {
				insert(tg::authorization::permission::process::Permission::SubtreeOutputObjects);
			}
		} else {
			if arg.process_command_objects {
				insert(tg::authorization::permission::process::Permission::NodeCommandObjects);
			}
			if arg.process_error_objects {
				insert(tg::authorization::permission::process::Permission::NodeErrorObjects);
			}
			if arg.process_log_objects {
				insert(tg::authorization::permission::process::Permission::NodeLogObjects);
			}
			if arg.process_output_objects {
				insert(tg::authorization::permission::process::Permission::NodeOutputObjects);
			}
		}
		permissions
	}

	async fn sync_get_authorize(
		&self,
		session: &Session,
		graph: &Arc<Mutex<Graph>>,
		sync: &tg::sync::Id,
		ids: impl IntoIterator<Item = tg::Id>,
		requested: tg::authorization::permission::Set,
		required: tg::authorization::permission::Set,
	) -> tg::Result<Vec<Option<tg::authorization::permission::Set>>> {
		let ids = ids.into_iter().collect::<Vec<_>>();

		// Collect the nodes whose permissions cannot be proven by the graph.
		let mut args = Vec::<(tg::Referent<tg::Id>, tg::authorization::permission::Set)>::new();
		let mut positions = Vec::new();
		let mut outputs = vec![None; ids.len()];
		{
			let graph = graph.lock().unwrap();
			for (position, id) in ids.iter().enumerate() {
				let authorization = match id.kind() {
					tg::id::Kind::Process => {
						graph.get_process_local_authorization(&id.clone().try_into()?, requested)
					},
					_ => graph.get_object_local_authorization(&id.clone().try_into()?, requested),
				};
				if authorization.permissions.contains(requested) {
					outputs[position] = Some(authorization.permissions);
					continue;
				}
				let tokens = tg::authorization::Tokens::with_local_entry(authorization.tokens);
				let resource = tg::Referent::with_node_and_tokens(id.clone(), tokens);
				args.push((resource, requested));
				positions.push(position);
			}
		}

		if args.is_empty() {
			return Ok(outputs);
		}

		// Authorize the remaining nodes.
		let authorization_outputs = session
			.authorize_batch_with_required(args, required, Some(sync))
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the sync nodes"))?;
		let mut graph = graph.lock().unwrap();
		for (position, output) in std::iter::zip(positions, authorization_outputs) {
			{
				let permissions = output.permissions;
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
					graph.get_process_local_authorization(&id.clone().try_into()?, requested)
				},
				_ => graph.get_object_local_authorization(&id.clone().try_into()?, requested),
			};
			if authorization.permissions.contains(required) {
				outputs[position] = Some(authorization.permissions);
			}
		}

		Ok(outputs)
	}
}
