use {
	crate::sync::graph::{Graph, Node, UpdateObjectLocalArg, UpdateProcessLocalArg},
	crate::{Session, sync::get::State},
	futures::{StreamExt as _, TryStreamExt as _},
	num::ToPrimitive as _,
	std::{
		collections::BTreeSet,
		ops::ControlFlow,
		sync::{Arc, Mutex},
	},
	tangram_cache::prelude::*,
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ObjectNode {
	pub id: tg::object::Id,
	pub missing: bool,
}

pub struct ProcessNode {
	pub id: tg::process::Id,
	pub missing: bool,
}

impl Session {
	pub(super) async fn sync_get_index(
		&self,
		state: Arc<State>,
		checkout_sender: tokio::sync::mpsc::Sender<super::checkout::ObjectNode>,
		index_object_receiver: tokio::sync::mpsc::Receiver<ObjectNode>,
		index_process_receiver: tokio::sync::mpsc::Receiver<ProcessNode>,
	) -> tg::Result<()> {
		// Create the retry queue.
		let (retry_sender, mut retry_receiver) =
			tokio::sync::mpsc::channel::<tg::Either<ObjectNode, ProcessNode>>(256);

		// Create the objects future.
		let object_batch_size = self.server.config.sync.get.index.object_batch_size;
		let object_batch_timeout = self.server.config.sync.get.index.object_batch_timeout;
		let object_concurrency = self.server.config.sync.get.index.object_concurrency;
		let object_retry_sender = retry_sender.clone();
		let object_checkout_sender = checkout_sender.clone();
		let object_session = self.clone();
		let object_state = state.clone();
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(index_object_receiver),
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, move |nodes| {
			let checkout_sender = object_checkout_sender.clone();
			let retry_sender = object_retry_sender.clone();
			let session = object_session.clone();
			let state = object_state.clone();
			async move {
				session
					.sync_get_index_object_batch(
						&state,
						&checkout_sender,
						nodes,
						Some(&retry_sender),
					)
					.await
			}
		});

		// Create the processes future.
		let process_batch_size = self.server.config.sync.get.index.process_batch_size;
		let process_batch_timeout = self.server.config.sync.get.index.process_batch_timeout;
		let process_concurrency = self.server.config.sync.get.index.process_concurrency;
		let process_retry_sender = retry_sender.clone();
		let process_session = self.clone();
		let process_state = state.clone();
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(index_process_receiver),
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, move |nodes| {
			let retry_sender = process_retry_sender.clone();
			let session = process_session.clone();
			let state = process_state.clone();
			async move {
				session
					.sync_get_index_process_batch(&state, nodes, Some(&retry_sender))
					.await
			}
		});

		// Create the retries future.
		drop(retry_sender);
		let retries_future = async {
			while let Some(node) = retry_receiver.recv().await {
				// Drain the retry queue.
				let mut objects = Vec::new();
				let mut processes = Vec::new();
				match node {
					tg::Either::Left(node) => objects.push(node),
					tg::Either::Right(node) => processes.push(node),
				}
				while let Ok(node) = retry_receiver.try_recv() {
					match node {
						tg::Either::Left(node) => objects.push(node),
						tg::Either::Right(node) => processes.push(node),
					}
				}

				// Index before retrying the nodes.
				for node in &objects {
					crate::checkpoint!(self.server, "sync.get.index.object.retry", id = %node.id)
						.await;
				}
				for node in &processes {
					crate::checkpoint!(self.server, "sync.get.index.process.retry", id = %node.id)
						.await;
				}
				self.index()
					.await
					.map_err(|error| tg::error!(!error, "failed to index"))?
					.try_last()
					.await
					.map_err(|error| tg::error!(!error, "failed to index"))?;

				// Retry the nodes.
				let objects_future = async {
					if objects.is_empty() {
						return Ok(());
					}
					self.sync_get_index_object_batch(&state, &checkout_sender, objects, None)
						.await
				};
				let processes_future = async {
					if processes.is_empty() {
						return Ok(());
					}
					self.sync_get_index_process_batch(&state, processes, None)
						.await
				};
				futures::try_join!(objects_future, processes_future)?;
			}

			Ok(())
		};

		// Join the objects, processes, and retries futures.
		futures::try_join!(objects_future, processes_future, retries_future)?;

		Ok(())
	}

	pub(super) async fn sync_get_index_object_batch(
		&self,
		state: &State,
		checkout_sender: &tokio::sync::mpsc::Sender<super::checkout::ObjectNode>,
		nodes: Vec<ObjectNode>,
		retry_sender: Option<&tokio::sync::mpsc::Sender<tg::Either<ObjectNode, ProcessNode>>>,
	) -> tg::Result<()> {
		for node in &nodes {
			crate::checkpoint!(self.server, "sync.get.index.object.filter", id = %node.id).await;
		}

		// Separate the available nodes. Missing nodes still need the local index as a fallback.
		let (available_nodes, nodes): (Vec<_>, Vec<_>) = {
			let graph = state.graph.lock().unwrap();
			nodes.into_iter().partition(|node| {
				!node.missing && graph.get_object_local_availability(&node.id).subtree
			})
		};
		for node in available_nodes {
			if self.sync_get_checkout_pointers_enabled() {
				self.sync_get_queue_checkout_object(state, checkout_sender, node.id.clone())
					.await?;
			}
			Self::sync_get_index_send_object_available(state, &node.id).await?;
		}
		if nodes.is_empty() {
			Self::sync_get_index_close_queue_if_end(state);

			return Ok(());
		}

		for node in &nodes {
			crate::checkpoint!(self.server, "sync.get.index.object", id = %node.id).await;
		}

		// Get the ids.
		let ids = nodes.iter().map(|node| node.id.clone()).collect::<Vec<_>>();

		// Authorize and touch the objects, then get their storage and metadata.
		let touched_at = self.server.clock.unix_timestamp()?;
		let (outputs, permissions) = self
			.sync_get_touch_authorized_objects(
				&state.graph,
				&ids,
				touched_at,
				self.server.config.object.time_to_touch,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch and get object metadata"))?;

		for ((node, output), permissions) in
			std::iter::zip(std::iter::zip(nodes, outputs), permissions)
		{
			// Send a missing object to the retry queue.
			if node.missing
				&& output.is_none()
				&& let Some(retry_sender) = retry_sender
			{
				retry_sender
					.send(tg::Either::Left(node))
					.await
					.map_err(|_| tg::error!("failed to send the object to the retry queue"))?;
				continue;
			}

			// Update the graph.
			let arg = UpdateObjectLocalArg {
				data: None,
				id: &node.id,
				marked: None,
				metadata: output.as_ref().map(|object| object.metadata.clone()),
				permissions,
				put: output.as_ref().map(|object| object.put),
				requested: None,
				storage: output.as_ref().map(|object| object.storage.clone()),
			};
			state.graph.lock().unwrap().update_object_local(arg);
			let mut availability = state
				.graph
				.lock()
				.unwrap()
				.get_object_local_availability(&node.id);

			// Send the object's availability.
			if availability.subtree {
				if self.sync_get_checkout_pointers_enabled() {
					self.sync_get_queue_checkout_object(state, checkout_sender, node.id.clone())
						.await?;
				}
				Self::sync_get_index_send_object_available(state, &node.id).await?;
			}

			if node.missing {
				// Retry a missing object while also listening for incoming sync notifications.
				let entry = state
					.graph
					.lock()
					.unwrap()
					.get_node_local_tokens(&tg::Id::from(node.id.clone()));
				let stored = if output.is_none() {
					crate::checkpoint!(self.server, "sync.get.index.object.wait", id = %node.id)
						.await;
					let tokens = tg::Tokens::with_local_entry(entry.clone());
					let request = tg::sync::control::ClientRequestArg::object(
						node.id.clone(),
						tg::authorization::permission::object::Set::NODE,
						Some(tg::object::Storage::default()),
					);
					self.try_get_with_sync_wait(&tokens, request.clone(), |output| {
						let id = node.id.clone();
						let tg::sync::control::ClientRequestArg::Get(request) = &request else {
							unreachable!();
						};
						async move {
							if let Some(output) = output {
								let mut graph = state.graph.lock().unwrap();
								graph.update_node_local_control_output(
									&id.clone().into(),
									&output,
								)?;
								let required = tg::authorization::permission::Set::Object(
									tg::authorization::permission::object::Set::NODE,
								);
								if !graph.object_local_permissions(&id).contains(required) {
									return Ok(None);
								}
							} else {
								let ids = std::slice::from_ref(&id);
								let touched_at = self.server.clock.unix_timestamp()?;
								let (mut objects, mut permissions) = self
									.sync_get_touch_authorized_objects(
										&state.graph,
										ids,
										touched_at,
										self.server.config.object.time_to_touch,
									)
									.await?;
								let permissions = permissions.pop().flatten();
								if permissions.is_none() {
									return Ok(None);
								}
								if let Some(object) = objects.pop().flatten() {
									let arg = UpdateObjectLocalArg {
										data: None,
										id: &id,
										marked: None,
										metadata: Some(object.metadata),
										permissions,
										put: Some(object.put),
										requested: None,
										storage: Some(object.storage),
									};
									state.graph.lock().unwrap().update_object_local(arg);
								}
							}
							if state
								.graph
								.lock()
								.unwrap()
								.try_get_node_local_control_output(request)?
								.is_none()
							{
								return Ok(None);
							}
							let output = self.server.try_get_object_local(&id, false).await?;
							Ok(output.map(|_| ()))
						}
					})
					.await?
					.is_some()
				} else {
					false
				};

				// If the object is not stored, then error.
				if output.is_none() && !stored {
					return Err(tg::error!(id = %node.id, "failed to find the object"));
				}
				availability = state
					.graph
					.lock()
					.unwrap()
					.get_object_local_availability(&node.id);

				if availability.subtree {
					Self::sync_get_index_send_object_available(state, &node.id).await?;
				}

				// If the object's subtree is unavailable, then enqueue the children.
				if !availability.subtree {
					// Get the object.
					let bytes = self
						.server
						.try_get_object_local(&node.id, false)
						.await
						.map_err(
							|error| tg::error!(!error, id = %node.id, "failed to get the object locally"),
						)?
						.ok_or_else(|| tg::error!(id = %node.id, "expected the object to exist"))?
						.bytes;
					let data = tg::object::Data::deserialize(node.id.kind(), bytes).map_err(
						|error| tg::error!(!error, id = %node.id, "failed to deserialize the object"),
					)?;

					// Update the graph.
					let arg = UpdateObjectLocalArg {
						data: Some(&data),
						id: &node.id,
						marked: None,
						metadata: None,
						permissions: None,
						put: None,
						requested: None,
						storage: None,
					};
					{
						let mut graph = state.graph.lock().unwrap();
						graph.update_object_local(arg);
						graph.update_checkout_object(&node.id, &data);
					}
					if self.sync_get_checkout_pointers_enabled()
						&& matches!(&data, tg::object::Data::Blob(_))
					{
						let node = super::checkout::ObjectNode {
							bytes: None,
							id: node.id.unwrap_blob_ref().clone(),
							metadata: output.as_ref().map(|object| object.metadata.clone()),
							put: uuid::Uuid::now_v7().into_bytes(),
						};
						checkout_sender.send(node).await.map_err(|_| {
							tg::error!("failed to send the blob to the checkout task")
						})?;
					}

					// Enqueue the children.
					let remote_tokens = state
						.graph
						.lock()
						.unwrap()
						.get_node_remote_tokens(&tg::Id::from(node.id.clone()));
					Self::sync_get_enqueue_object_children(
						state,
						&node.id,
						&data,
						None,
						&entry,
						&remote_tokens,
					);
					if state
						.graph
						.lock()
						.unwrap()
						.get_object_local_availability(&node.id)
						.subtree
					{
						Self::sync_get_index_send_object_available(state, &node.id).await?;
					}
				}
			}
		}

		Self::sync_get_index_close_queue_if_end(state);

		Ok(())
	}

	pub(super) async fn sync_get_index_send_object_available(
		state: &State,
		id: &tg::object::Id,
	) -> tg::Result<()> {
		let message = tg::sync::GetMessage::Available(tg::sync::GetAvailableMessage::Object(
			tg::sync::GetAvailableObjectMessage { id: id.clone() },
		));
		state
			.sender
			.send(Ok(message))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the available message"))?;

		Ok(())
	}

	pub(super) async fn sync_get_index_process_batch(
		&self,
		state: &State,
		nodes: Vec<ProcessNode>,
		retry_sender: Option<&tokio::sync::mpsc::Sender<tg::Either<ObjectNode, ProcessNode>>>,
	) -> tg::Result<()> {
		for node in &nodes {
			crate::checkpoint!(self.server, "sync.get.index.process.filter", id = %node.id).await;
		}

		// Separate the available nodes. Missing nodes still need the local index as a fallback.
		let (available_nodes, nodes): (Vec<_>, Vec<_>) = {
			let graph = state.graph.lock().unwrap();
			nodes.into_iter().partition(|node| {
				if node.missing {
					return false;
				}
				let availability = graph.get_process_local_availability(&node.id);

				graph.process_available(&availability)
			})
		};
		for node in available_nodes {
			let availability = state
				.graph
				.lock()
				.unwrap()
				.get_process_local_availability(&node.id);
			Self::sync_get_index_send_process_available(state, &node.id, &availability).await?;
		}
		if nodes.is_empty() {
			Self::sync_get_index_close_queue_if_end(state);

			return Ok(());
		}

		for node in &nodes {
			crate::checkpoint!(self.server, "sync.get.index.process", id = %node.id).await;
		}

		// Get the ids.
		let ids = nodes.iter().map(|node| node.id.clone()).collect::<Vec<_>>();

		// Authorize and touch the processes, then get their storage and metadata.
		let touched_at = self.server.clock.unix_timestamp()?;
		let (mut outputs, permissions) = self
			.sync_get_touch_authorized_processes(
				&state.graph,
				&ids,
				&state.arg,
				touched_at,
				self.server.config.process.time_to_touch,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch and get process metadata"))?;
		if state.arg.process_children {
			for (id, process) in std::iter::zip(&ids, &mut outputs) {
				let Some(process) = process else {
					continue;
				};
				let children_set = process.set.children;
				if let Some(data) = &mut process.data {
					self.set_process_children_from_index(id, children_set, data)
						.await?;
				}
			}
		}

		for ((node, output), permissions) in
			std::iter::zip(std::iter::zip(nodes, outputs), permissions)
		{
			// Send a missing process to the retry queue.
			if node.missing
				&& output.is_none()
				&& let Some(retry_sender) = retry_sender
			{
				retry_sender
					.send(tg::Either::Right(node))
					.await
					.map_err(|_| tg::error!("failed to send the process to the retry queue"))?;
				continue;
			}

			// Update the graph.
			let arg = UpdateProcessLocalArg {
				data: output.as_ref().and_then(|process| process.data.as_ref()),
				id: &node.id,
				marked: None,
				metadata: output.as_ref().map(|p| p.metadata.clone()),
				permissions,
				requested: None,
				storage: output.as_ref().map(|p| p.storage.clone()),
			};
			state.graph.lock().unwrap().update_process_local(arg);
			let availability = state
				.graph
				.lock()
				.unwrap()
				.get_process_local_availability(&node.id);

			// Send the process's availability.
			Self::sync_get_index_send_process_available(state, &node.id, &availability).await?;

			if node.missing {
				// If the process is not stored, then error.
				if output.is_none() {
					return Err(tg::error!(id = %node.id, "failed to find the process"));
				}

				let data = output
					.as_ref()
					.and_then(|process| process.data.clone())
					.ok_or_else(
						|| tg::error!(id = %node.id, "expected the process data to be set"),
					)?;

				// Update the graph.
				let arg = UpdateProcessLocalArg {
					data: Some(&data),
					id: &node.id,
					marked: None,
					metadata: None,
					permissions: None,
					requested: None,
					storage: None,
				};
				state.graph.lock().unwrap().update_process_local(arg);

				// Enqueue the children.
				let id = tg::Id::from(node.id.clone());
				let (local_tokens, remote_tokens) = {
					let graph = state.graph.lock().unwrap();
					(
						graph.get_node_local_tokens(&id),
						graph.get_node_remote_tokens(&id),
					)
				};
				Self::sync_get_enqueue_process_children(
					state,
					&node.id,
					&data,
					Some(&availability),
					&local_tokens,
					&remote_tokens,
				);
			}
		}

		Self::sync_get_index_close_queue_if_end(state);

		Ok(())
	}

	pub(super) async fn sync_get_index_send_process_available(
		state: &State,
		id: &tg::process::Id,
		availability: &tg::process::Availability,
	) -> tg::Result<()> {
		if !Graph::process_any_available(availability) {
			return Ok(());
		}
		let message = tg::sync::GetMessage::Available(tg::sync::GetAvailableMessage::Process(
			tg::sync::GetAvailableProcessMessage {
				id: id.clone(),
				node_command_available: availability.node_command,
				node_error_available: availability.node_error,
				node_log_available: availability.node_log,
				node_output_available: availability.node_output,
				subtree_available: availability.subtree,
				subtree_command_available: availability.subtree_command,
				subtree_error_available: availability.subtree_error,
				subtree_log_available: availability.subtree_log,
				subtree_output_available: availability.subtree_output,
			},
		));
		state
			.sender
			.send(Ok(message))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the available message"))?;

		Ok(())
	}

	fn sync_get_index_close_queue_if_end(state: &State) {
		if state.graph.lock().unwrap().end_local() {
			state.queue.close();
		}
	}

	pub(super) async fn sync_get_index_put(
		&self,
		graph: Arc<Mutex<Graph>>,
		sync: &tg::sync::Id,
	) -> tg::Result<()> {
		let (put_sandbox_args, put_sandbox_grant_args) =
			self.sync_get_index_sandbox_args(&graph, sync).await?;
		self.sync_get_index_put_inner(graph, put_sandbox_args, put_sandbox_grant_args, sync)
			.await?;

		Ok(())
	}

	pub(super) async fn sync_get_index_put_partial(
		&self,
		graph: Arc<Mutex<Graph>>,
		sync: &tg::sync::Id,
	) -> tg::Result<()> {
		self.sync_get_index_put_inner(graph, Vec::new(), Vec::new(), sync)
			.await?;

		Ok(())
	}

	async fn sync_get_index_put_inner(
		&self,
		graph: Arc<Mutex<Graph>>,
		put_sandbox_args: Vec<tangram_index::sandbox::put::Arg>,
		put_sandbox_grant_args: Vec<tangram_index::grant::put::Arg>,
		sync: &tg::sync::Id,
	) -> tg::Result<()> {
		let options = self.server.config.sync.retry.clone().into();
		tangram_futures::retry(&options, || {
			self.sync_get_index_put_attempt(
				graph.clone(),
				put_sandbox_args.clone(),
				put_sandbox_grant_args.clone(),
				sync,
			)
		})
		.await?;

		Ok(())
	}

	async fn sync_get_index_put_attempt(
		&self,
		graph: Arc<Mutex<Graph>>,
		put_sandbox_args: Vec<tangram_index::sandbox::put::Arg>,
		put_sandbox_grant_args: Vec<tangram_index::grant::put::Arg>,
		sync: &tg::sync::Id,
	) -> tg::Result<ControlFlow<(), tg::Error>> {
		// Flush the cache.
		self.server
			.cache
			.flush()
			.await
			.map_err(|error| tg::error!(!error, "failed to flush the cache"))?;

		// Authorize process objects whose subtrees were already stored locally.
		let process_objects = {
			let graph = graph.lock().unwrap();
			let mut process_objects = BTreeSet::new();
			for (_, node) in graph.nodes() {
				let Node::Process(process) = node else {
					continue;
				};
				if !process.marked() {
					continue;
				}
				for (object_index, _) in process.objects().map(Vec::as_slice).unwrap_or_default() {
					let (id, object) = graph.nodes().get_index(*object_index).unwrap();
					let stored = object
						.unwrap_object_ref()
						.local_storage()
						.is_some_and(|storage| storage.subtree);
					if stored {
						process_objects.insert(tg::object::Id::try_from(id.clone())?);
					}
				}
			}
			process_objects.into_iter().collect::<Vec<_>>()
		};
		self.sync_get_authorize_objects(&graph, &process_objects)
			.await?;

		// Create the index args and update the graph with the permissions being granted.
		let account = self.usage_account(&self.context.principal).await?;
		let touched_at = self.server.clock.unix_timestamp()?;
		let (
			put_checkout_args,
			mut put_grant_args,
			put_object_args,
			put_process_args,
			storage_roots,
		) = {
			let mut graph = graph.lock().unwrap();
			let args = self
				.sync_get_index_create_args(&graph, sync)
				.map_err(|error| tg::error!(!error, "failed to create the index args"))?;
			for arg in &args.0 {
				match arg.resource.kind() {
					tg::id::Kind::Process => graph.update_process_local_permissions(
						&arg.resource.clone().try_into()?,
						arg.permissions,
					),
					_ => graph.update_object_local_permissions(
						&arg.resource.clone().try_into()?,
						arg.permissions,
					),
				}
			}
			let put_checkout_args = graph
				.checkouts()
				.iter()
				.map(|(id, dependencies)| tangram_index::checkout::put::Arg {
					dependencies: dependencies.clone(),
					id: id.clone().into(),
					touched_at,
				})
				.collect::<Vec<_>>();
			let storage_roots = graph.remote_roots().iter().cloned().collect::<Vec<_>>();
			(put_checkout_args, args.0, args.1, args.2, storage_roots)
		};
		if let Some(arg) = self.sync_get_create_implicit_grant(&sync.clone().into(), None)? {
			put_grant_args.push(arg);
		}

		// Index the objects, processes, and sandboxes.
		let arg = tangram_index::batch::Arg {
			items: put_checkout_args
				.into_iter()
				.map(tangram_index::batch::Item::PutCheckout)
				.chain(
					put_object_args
						.into_iter()
						.map(tangram_index::batch::Item::PutObject),
				)
				.chain(
					put_process_args
						.into_iter()
						.map(tangram_index::batch::Item::PutProcess),
				)
				.chain(
					put_sandbox_args
						.into_iter()
						.map(tangram_index::batch::Item::PutSandbox),
				)
				.chain(
					put_grant_args
						.into_iter()
						.chain(put_sandbox_grant_args)
						.map(tangram_index::batch::Item::PutGrant),
				)
				.chain(account.into_iter().flat_map(|account| {
					storage_roots.iter().filter_map(move |id| match id.kind() {
						tg::id::Kind::Process => {
							Some(tangram_index::batch::Item::PutAccountProcess(
								tangram_index::usage::storage::put::ProcessArg {
									account: account.clone(),
									process: id.clone().try_into().unwrap(),
									touched_at,
								},
							))
						},
						_ => tg::object::Id::try_from(id.clone()).ok().map(|object| {
							tangram_index::batch::Item::PutAccountObject(
								tangram_index::usage::storage::put::ObjectArg {
									account: account.clone(),
									object,
									touched_at,
								},
							)
						}),
					})
				}))
				.collect(),
		};
		crate::checkpoint!(self.server, "sync.get.index.enqueue", sync = %sync).await;
		match self.server.index_batch(arg).await {
			Ok(()) => Ok(ControlFlow::Break(())),
			Err(error) => {
				let error = tg::error!(!error, "failed to index the sync");
				Ok(ControlFlow::Continue(error))
			},
		}
	}

	async fn sync_get_index_sandbox_args(
		&self,
		graph: &Arc<Mutex<Graph>>,
		sync: &tg::sync::Id,
	) -> tg::Result<(
		Vec<tangram_index::sandbox::put::Arg>,
		Vec<tangram_index::grant::put::Arg>,
	)> {
		// Get the sandbox messages.
		let messages = graph
			.lock()
			.unwrap()
			.local_messages()
			.into_iter()
			.filter_map(|message| match message {
				tg::sync::PutNodeMessage::Sandbox(message) => Some(message),
				_ => None,
			})
			.collect::<Vec<_>>();
		if messages.is_empty() {
			return Ok((Vec::new(), Vec::new()));
		}
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}

		// Create the sandbox and grant args.
		let touched_at = self.server.clock.unix_timestamp()?;
		let mut put_grant_args = Vec::new();
		let mut put_sandbox_args = Vec::with_capacity(messages.len());
		for message in messages {
			let account = match message.data.data.owner.as_ref() {
				Some(owner) => self.usage_account(owner).await?,
				None => None,
			};
			let existing = self
				.try_get_sandbox_from_index(&message.id)
				.await?
				.is_some();
			if existing {
				let permission = tg::authorization::Permission::Sandbox(
					tg::authorization::permission::sandbox::Permission::Write,
				);
				let authorized = self.authorize(message.id.clone(), permission).await?;
				if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
					return Err(tg::error!("unauthorized"));
				}
			}
			if let Some(arg) =
				self.sync_get_create_implicit_grant(&message.id.clone().into(), Some(sync))?
			{
				put_grant_args.push(arg);
			}
			// Preserve the caller's write permission when the sandbox is synced again.
			if let Some(arg) =
				self.sync_get_create_implicit_grant(&message.id.clone().into(), None)?
			{
				put_grant_args.push(arg);
			}
			put_sandbox_args.push(tangram_index::sandbox::put::Arg {
				account,
				created_at: message.created_at,
				data: Some(message.data),
				id: message.id,
				location: None,
				runner: None,
				touched_at,
			});
		}

		Ok((put_sandbox_args, put_grant_args))
	}

	fn sync_get_index_create_args(
		&self,
		graph: &Graph,
		sync: &tg::sync::Id,
	) -> tg::Result<(
		Vec<tangram_index::grant::put::Arg>,
		Vec<tangram_index::object::put::Arg>,
		Vec<tangram_index::process::put::Arg>,
	)> {
		// Get a reverse topological ordering using Tarjan's algorithm.
		let sccs = petgraph::algo::tarjan_scc(graph);
		for scc in &sccs {
			if scc.len() > 1 {
				return Err(tg::error!("the graph had a cycle"));
			}
		}
		let indices = sccs.into_iter().flatten().collect::<Vec<_>>();

		let touched_at = self.server.clock.unix_timestamp()?;

		// Create the grant args.
		let mut put_grant_args = Vec::new();
		let grant_subject = tg::authorization::Subject::Sync(sync.clone());
		let expires_at = touched_at
			+ self
				.server
				.config
				.sync
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		let mut object_covered = vec![false; graph.nodes().len()];
		let mut process_covered =
			vec![tg::authorization::permission::process::Set::empty(); graph.nodes().len()];
		for index in indices.iter().rev().copied() {
			let (id, node) = graph.nodes().get_index(index).unwrap();
			match node {
				Node::Group(_)
				| Node::Organization(_)
				| Node::Sandbox(_)
				| Node::Tag(_)
				| Node::User(_) => {},
				Node::Object(node) => {
					let id = tg::object::Id::try_from(id.clone())?;
					let proven = graph.object_local_permissions(&id);
					let node_permission = tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Node,
					);
					let availability = node
						.local_availability()
						.is_some_and(|availability| availability.subtree);
					let mut subtree = false;
					if (node.marked() || proven.contains(node_permission)) && !object_covered[index]
					{
						let permissions = Graph::object_grant_permissions(availability);
						subtree = availability;
						put_grant_args.push(tangram_index::grant::put::Arg {
							created_at: touched_at,
							creator: Some(self.context.principal.clone()),
							implicit: Some(Some(expires_at)),
							permissions: tg::authorization::permission::Set::Object(permissions),
							subject: grant_subject.clone(),
							resource: id.into(),
							time_to_touch: Some(self.server.config.object.grant_time_to_touch),
						});
					}
					let covered = object_covered[index] || subtree;
					if covered && let Some(children) = node.children() {
						for child in children {
							object_covered[*child] = true;
						}
					}
				},
				Node::Process(node) => {
					let availability = node.local_availability().cloned().unwrap_or_default();
					let mut permissions = if node.marked() {
						Graph::process_grant_permissions(&availability)
					} else {
						tg::authorization::permission::process::Set::empty()
					};
					let tg::authorization::permission::Set::Process(proven) =
						graph.process_local_permissions(&id.clone().try_into()?)
					else {
						return Err(tg::error!("expected process permissions"));
					};
					permissions.insert(proven);
					Self::sync_get_index_remove_process_permissions_covered_by_ancestors(
						&mut permissions,
						process_covered[index],
					);
					if !permissions.is_empty() {
						put_grant_args.push(tangram_index::grant::put::Arg {
							created_at: touched_at,
							creator: Some(self.context.principal.clone()),
							implicit: Some(Some(expires_at)),
							permissions: tg::authorization::permission::Set::Process(permissions),
							subject: grant_subject.clone(),
							resource: tg::process::Id::try_from(id.clone())?.into(),
							time_to_touch: Some(self.server.config.process.grant_time_to_touch),
						});
					}
					let subtree_permissions =
						Self::sync_get_index_process_subtree_permissions(permissions);
					let mut covered = process_covered[index];
					covered.insert(subtree_permissions);
					if let Some(children) = node.children() {
						for child in children {
							process_covered[*child].insert(covered);
						}
					}
				},
			}
		}

		// Create non-expiring implicit grants for the process objects proven locally.
		for index in indices.iter().copied() {
			let (id, node) = graph.nodes().get_index(index).unwrap();
			let Node::Process(node) = node else {
				continue;
			};
			if !node.marked() {
				continue;
			}
			let process = tg::process::Id::try_from(id.clone())?;
			let creator = tg::Principal::Process(process.clone());
			let subject = tg::authorization::Subject::Process(process);
			for (object_index, _) in node.objects().map(Vec::as_slice).unwrap_or_default() {
				let (object, node) = graph.nodes().get_index(*object_index).unwrap();
				let availability = node
					.unwrap_object_ref()
					.local_availability()
					.is_some_and(|availability| availability.subtree);
				if !availability {
					continue;
				}
				put_grant_args.push(tangram_index::grant::put::Arg {
					created_at: touched_at,
					creator: Some(creator.clone()),
					implicit: Some(None),
					permissions: tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					)
					.into(),
					resource: tg::object::Id::try_from(object.clone())?.into(),
					subject: subject.clone(),
					time_to_touch: None,
				});
			}
		}

		// Create the args.
		let mut put_object_args = Vec::new();
		let mut put_process_args = Vec::new();
		let mut visited = std::collections::HashSet::new();
		let mut stack = graph
			.nodes()
			.iter()
			.enumerate()
			.filter_map(|(index, (_, node))| node.parents().is_empty().then_some(index))
			.collect::<Vec<_>>();
		while let Some(index) = stack.pop() {
			if !visited.insert(index) {
				continue;
			}
			let (id, node) = graph.nodes().get_index(index).unwrap();
			match node {
				Node::Group(node)
				| Node::Organization(node)
				| Node::Sandbox(node)
				| Node::Tag(node)
				| Node::User(node) => {
					if let Some(children) = node.children() {
						stack.extend(children.iter().copied());
					}
				},
				Node::Object(node) => {
					let id = tg::object::Id::try_from(id.clone())?;
					if node.marked() {
						let put = node.put().ok_or_else(
							|| tg::error!(%id, "the stored object was missing its put"),
						)?;
						let children = node
							.children()
							.unwrap()
							.iter()
							.map(|index| {
								graph
									.nodes()
									.get_index(*index)
									.unwrap()
									.0
									.clone()
									.try_into()
							})
							.collect::<tg::Result<std::collections::BTreeSet<_>>>()?;
						let metadata = node.metadata().cloned().unwrap();
						let storage = node.local_storage().cloned().unwrap();
						let checkout = graph.checkout_objects().get(&id).cloned();
						let arg = tangram_index::object::put::Arg {
							checkout,
							children,
							id,
							metadata,
							put,
							storage,
							time_to_touch: self.server.config.object.time_to_touch,
							touched_at,
						};
						put_object_args.push(arg);
					}
					if let Some(children) = node.children() {
						stack.extend(children.iter().copied());
					}
				},
				Node::Process(node) => {
					let id = tg::process::Id::try_from(id.clone())?;
					if node.marked() {
						let data = node
							.data()
							.ok_or_else(|| tg::error!("expected the process data to be set"))?;
						let children = data
							.children
							.clone()
							.ok_or_else(|| tg::error!("expected the process children to be set"))?;
						let command_id = data.command.command_id()?.into();
						let storage = node.local_storage().cloned().unwrap();
						let metadata = node.metadata().cloned().unwrap();
						let objects = node
							.objects()
							.unwrap()
							.iter()
							.copied()
							.map(|(index, kind)| {
								let id: tg::object::Id = graph
									.nodes()
									.get_index(index)
									.unwrap()
									.0
									.clone()
									.try_into()?;
								Ok((id, kind))
							})
							.collect::<tg::Result<Vec<_>>>()?;
						let mut command = Vec::new();
						let mut error = Vec::new();
						let mut log = None;
						let mut output = Vec::new();
						for (object, kind) in objects {
							match kind {
								tangram_index::process::object::Kind::Command => {
									command.push(object);
								},
								tangram_index::process::object::Kind::Error => {
									error.push(object);
								},
								tangram_index::process::object::Kind::Log => {
									log = Some(object);
								},
								tangram_index::process::object::Kind::Output => {
									output.push(object);
								},
							}
						}
						let arg = tangram_index::process::put::Arg {
							cached: false,
							children: Some(children),
							command: Some(command),
							command_id,
							data: Some(data.clone().without_location_and_tokens()),
							error: Some((!error.is_empty()).then_some(error)),
							id,
							location: None,
							log: Some(log),
							metadata,
							options: tg::referent::Options::default(),
							output: Some((!output.is_empty()).then_some(output)),
							parent: None,
							sandbox: None,
							storage,
							time_to_touch: self.server.config.process.time_to_touch,
							touched_at,
						};
						put_process_args.push(arg);
					}
					if let Some(children) = node.children() {
						stack.extend(children.iter().copied());
					}
					if let Some(objects) = node.objects() {
						stack.extend(objects.iter().map(|(index, _)| *index));
					}
				},
			}
		}

		Ok((put_grant_args, put_object_args, put_process_args))
	}

	fn sync_get_index_remove_process_permissions_covered_by_ancestors(
		permissions: &mut tg::authorization::permission::process::Set,
		covered: tg::authorization::permission::process::Set,
	) {
		if covered.contains(tg::authorization::permission::process::Set::SUBTREE) {
			permissions.remove(tg::authorization::permission::process::Set::NODE);
			permissions.remove(tg::authorization::permission::process::Set::SUBTREE);
		}
		if covered.contains(tg::authorization::permission::process::Set::SUBTREE_COMMAND) {
			permissions.remove(tg::authorization::permission::process::Set::NODE_COMMAND);
			permissions.remove(tg::authorization::permission::process::Set::SUBTREE_COMMAND);
		}
		if covered.contains(tg::authorization::permission::process::Set::SUBTREE_ERROR) {
			permissions.remove(tg::authorization::permission::process::Set::NODE_ERROR);
			permissions.remove(tg::authorization::permission::process::Set::SUBTREE_ERROR);
		}
		if covered.contains(tg::authorization::permission::process::Set::SUBTREE_LOG) {
			permissions.remove(tg::authorization::permission::process::Set::NODE_LOG);
			permissions.remove(tg::authorization::permission::process::Set::SUBTREE_LOG);
		}
		if covered.contains(tg::authorization::permission::process::Set::SUBTREE_OUTPUT) {
			permissions.remove(tg::authorization::permission::process::Set::NODE_OUTPUT);
			permissions.remove(tg::authorization::permission::process::Set::SUBTREE_OUTPUT);
		}
	}

	fn sync_get_index_process_subtree_permissions(
		permissions: tg::authorization::permission::process::Set,
	) -> tg::authorization::permission::process::Set {
		let mut subtree_permissions = tg::authorization::permission::process::Set::empty();
		if permissions.contains(tg::authorization::permission::process::Set::SUBTREE) {
			subtree_permissions.insert(tg::authorization::permission::process::Set::SUBTREE);
		}
		if permissions.contains(tg::authorization::permission::process::Set::SUBTREE_COMMAND) {
			subtree_permissions
				.insert(tg::authorization::permission::process::Set::SUBTREE_COMMAND);
		}
		if permissions.contains(tg::authorization::permission::process::Set::SUBTREE_ERROR) {
			subtree_permissions.insert(tg::authorization::permission::process::Set::SUBTREE_ERROR);
		}
		if permissions.contains(tg::authorization::permission::process::Set::SUBTREE_LOG) {
			subtree_permissions.insert(tg::authorization::permission::process::Set::SUBTREE_LOG);
		}
		if permissions.contains(tg::authorization::permission::process::Set::SUBTREE_OUTPUT) {
			subtree_permissions.insert(tg::authorization::permission::process::Set::SUBTREE_OUTPUT);
		}
		subtree_permissions
	}
}
