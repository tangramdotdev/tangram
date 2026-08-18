use {
	crate::sync::graph::{Graph, Node, UpdateObjectLocalArg, UpdateProcessLocalArg},
	crate::{Session, sync::get::State},
	futures::{StreamExt as _, TryStreamExt as _},
	num::ToPrimitive as _,
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_object_store::prelude::*,
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
		let object_session = self.clone();
		let object_state = state.clone();
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(index_object_receiver),
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, move |nodes| {
			let retry_sender = object_retry_sender.clone();
			let session = object_session.clone();
			let state = object_state.clone();
			async move {
				session
					.sync_get_index_object_batch(&state, nodes, Some(&retry_sender))
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
					self.sync_get_index_object_batch(&state, objects, None)
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

	async fn sync_get_index_object_batch(
		&self,
		state: &State,
		nodes: Vec<ObjectNode>,
		retry_sender: Option<&tokio::sync::mpsc::Sender<tg::Either<ObjectNode, ProcessNode>>>,
	) -> tg::Result<()> {
		for node in &nodes {
			crate::checkpoint!(self.server, "sync.get.index.object.filter", id = %node.id).await;
		}

		// Separate the visible nodes. Missing nodes still need the local index as a fallback.
		let (visible_nodes, nodes): (Vec<_>, Vec<_>) = {
			let graph = state.graph.lock().unwrap();
			nodes
				.into_iter()
				.partition(|node| !node.missing && graph.get_object_local_visible(&node.id).subtree)
		};
		for node in visible_nodes {
			Self::sync_get_index_send_object_stored(state, &node.id).await?;
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

		// Authorize and touch the objects, then get stored and metadata.
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
				requested: None,
				stored: output.as_ref().map(|object| object.stored.clone()),
			};
			state.graph.lock().unwrap().update_object_local(arg);
			let visible = state
				.graph
				.lock()
				.unwrap()
				.get_object_local_visible(&node.id);

			// If the object is visible, then send a stored message.
			if visible.subtree {
				Self::sync_get_index_send_object_stored(state, &node.id).await?;
			}

			if node.missing {
				// If the object is not stored, then error.
				if output.is_none() {
					return Err(tg::error!(id = %node.id, "failed to find the object"));
				}

				// If the object's subtree is not visible, then enqueue the children.
				if !visible.subtree {
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
						requested: None,
						stored: None,
					};
					state.graph.lock().unwrap().update_object_local(arg);

					// Enqueue the children.
					Self::sync_get_enqueue_object_children(state, &node.id, &data, None, None);
				}
			}
		}

		Self::sync_get_index_close_queue_if_end(state);

		Ok(())
	}

	async fn sync_get_index_send_object_stored(
		state: &State,
		id: &tg::object::Id,
	) -> tg::Result<()> {
		let message = tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Object(
			tg::sync::GetStoredObjectMessage { id: id.clone() },
		));
		state
			.sender
			.send(Ok(message))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the stored message"))?;

		Ok(())
	}

	async fn sync_get_index_process_batch(
		&self,
		state: &State,
		nodes: Vec<ProcessNode>,
		retry_sender: Option<&tokio::sync::mpsc::Sender<tg::Either<ObjectNode, ProcessNode>>>,
	) -> tg::Result<()> {
		// Separate the visible nodes. Missing nodes still need the local index as a fallback.
		let (visible_nodes, nodes): (Vec<_>, Vec<_>) = {
			let graph = state.graph.lock().unwrap();
			nodes.into_iter().partition(|node| {
				if node.missing {
					return false;
				}
				let visible = graph.get_process_local_visible(&node.id);

				graph.process_visible(&visible)
			})
		};
		for node in visible_nodes {
			let visible = state
				.graph
				.lock()
				.unwrap()
				.get_process_local_visible(&node.id);
			Self::sync_get_index_send_process_stored(state, &node.id, &visible).await?;
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

		// Authorize and touch the processes, then get stored and metadata.
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
				stored: output.as_ref().map(|p| p.stored.clone()),
			};
			state.graph.lock().unwrap().update_process_local(arg);
			let visible = state
				.graph
				.lock()
				.unwrap()
				.get_process_local_visible(&node.id);

			// If the process is visible, then send a stored message.
			Self::sync_get_index_send_process_stored(state, &node.id, &visible).await?;

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
					stored: None,
				};
				state.graph.lock().unwrap().update_process_local(arg);

				// Enqueue the children.
				Self::sync_get_enqueue_process_children(
					state,
					&node.id,
					&data,
					Some(&visible),
					None,
				);
			}
		}

		Self::sync_get_index_close_queue_if_end(state);

		Ok(())
	}

	async fn sync_get_index_send_process_stored(
		state: &State,
		id: &tg::process::Id,
		visible: &tangram_index::process::Stored,
	) -> tg::Result<()> {
		if !Graph::process_visible_any(visible) {
			return Ok(());
		}
		let message = tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Process(
			tg::sync::GetStoredProcessMessage {
				id: id.clone(),
				node_command_stored: visible.node_command,
				node_error_stored: visible.node_error,
				node_log_stored: visible.node_log,
				node_output_stored: visible.node_output,
				subtree_command_stored: visible.subtree_command,
				subtree_error_stored: visible.subtree_error,
				subtree_log_stored: visible.subtree_log,
				subtree_output_stored: visible.subtree_output,
				subtree_stored: visible.subtree,
			},
		));
		state
			.sender
			.send(Ok(message))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the stored message"))?;

		Ok(())
	}

	fn sync_get_index_close_queue_if_end(state: &State) {
		if state.graph.lock().unwrap().end_local() {
			state.queue.close();
		}
	}

	pub(super) async fn sync_get_index_put(&self, graph: Arc<Mutex<Graph>>) -> tg::Result<()> {
		let (put_sandbox_args, put_sandbox_grant_args) =
			self.sync_get_index_sandbox_args(&graph).await?;
		self.sync_get_index_put_inner(graph, put_sandbox_args, put_sandbox_grant_args)
			.await?;

		Ok(())
	}

	pub(super) async fn sync_get_index_put_partial(
		&self,
		graph: Arc<Mutex<Graph>>,
	) -> tg::Result<()> {
		self.sync_get_index_put_inner(graph, Vec::new(), Vec::new())
			.await?;

		Ok(())
	}

	async fn sync_get_index_put_inner(
		&self,
		graph: Arc<Mutex<Graph>>,
		put_sandbox_args: Vec<tangram_index::sandbox::put::Arg>,
		put_sandbox_grant_args: Vec<tangram_index::grant::put::Arg>,
	) -> tg::Result<()> {
		// Flush the store.
		self.server
			.object_store
			.flush()
			.await
			.map_err(|error| tg::error!(!error, "failed to flush the store"))?;

		// Create the index args and update the graph with the permissions being granted.
		let account = self.usage_account(&self.context.principal).await?;
		let (put_grant_args, put_object_args, put_process_args, storage_roots) = {
			let mut graph = graph.lock().unwrap();
			let args = self
				.sync_get_index_create_args(&mut graph)
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
			let storage_roots = graph.remote_roots.iter().cloned().collect::<Vec<_>>();
			(args.0, args.1, args.2, storage_roots)
		};
		let touched_at = self.server.clock.unix_timestamp()?;

		// Index the objects, processes, and sandboxes.
		let arg = tangram_index::batch::Arg {
			items: put_object_args
				.into_iter()
				.map(tangram_index::batch::Item::PutObject)
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
		self.server
			.index_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the sync"))?;

		Ok(())
	}

	async fn sync_get_index_sandbox_args(
		&self,
		graph: &Arc<Mutex<Graph>>,
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
			let account = match message.data.owner.as_ref() {
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
			} else if let Some(arg) =
				self.sync_get_create_temporary_grant(&message.id.clone().into())?
			{
				put_grant_args.push(arg);
			}
			put_sandbox_args.push(tangram_index::sandbox::put::Arg {
				account,
				created_at: message.created_at,
				data: Some(message.data),
				id: message.id,
				runner: None,
				touched_at,
			});
		}

		Ok((put_sandbox_args, put_grant_args))
	}

	fn sync_get_index_create_args(
		&self,
		graph: &mut Graph,
	) -> tg::Result<(
		Vec<tangram_index::grant::put::Arg>,
		Vec<tangram_index::object::put::Arg>,
		Vec<tangram_index::process::put::Arg>,
	)> {
		// Get a reverse topological ordering using Tarjan's algorithm.
		let sccs = petgraph::algo::tarjan_scc(&*graph);
		for scc in &sccs {
			if scc.len() > 1 {
				return Err(tg::error!("the graph had a cycle"));
			}
		}
		let indices = sccs.into_iter().flatten().collect::<Vec<_>>();

		// Set stored and metadata.
		for index in indices.iter().copied() {
			let (_, node) = graph.nodes.get_index(index).unwrap();
			match node {
				Node::Group(_)
				| Node::Organization(_)
				| Node::Sandbox(_)
				| Node::Tag(_)
				| Node::User(_) => {},
				Node::Object(node) => {
					let Some(children) = &node.children else {
						continue;
					};
					let Some(metadata) = &node.metadata else {
						continue;
					};
					let existing_metadata = metadata.clone();

					// Initialize the metadata.
					let mut metadata = tg::object::Metadata {
						node: metadata.node.clone(),
						subtree: tg::object::metadata::Subtree {
							count: Some(1),
							depth: Some(1),
							size: Some(metadata.node.size),
							solvable: Some(metadata.node.solvable),
							solved: Some(metadata.node.solved),
						},
					};

					// Handle each child.
					for child_index in children {
						let (_, child_node) = graph.nodes.get_index(*child_index).unwrap();
						let child_node = child_node
							.try_unwrap_object_ref()
							.ok()
							.ok_or_else(|| tg::error!("expected an object"))?;
						metadata.subtree.count = metadata
							.subtree
							.count
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.count),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.depth = metadata
							.subtree
							.depth
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.depth),
							)
							.map(|(a, b)| a.max(1 + b));
						metadata.subtree.size = metadata
							.subtree
							.size
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.size),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.solvable = metadata
							.subtree
							.solvable
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.solvable),
							)
							.map(|(a, b)| a || b);
						metadata.subtree.solved = metadata
							.subtree
							.solved
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.solved),
							)
							.map(|(a, b)| a && b);
					}

					// Merge the existing metadata.
					metadata.merge(&existing_metadata);

					// Update the node.
					let (_, node) = graph.nodes.get_index_mut(index).unwrap();
					let node = node.unwrap_object_mut();
					node.metadata = Some(metadata);
				},

				Node::Process(node) => {
					let Some(children) = &node.children else {
						continue;
					};
					let Some(objects) = &node.objects else {
						continue;
					};

					// Initialize the metadata.
					let mut metadata = tg::process::Metadata {
						node: tg::process::metadata::Node {
							command: tg::object::metadata::Subtree {
								count: None,
								depth: None,
								size: None,
								solvable: None,
								solved: None,
							},
							error: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
							log: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
							output: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
						},
						subtree: tg::process::metadata::Subtree {
							count: Some(1),
							depth: Some(1),
							command: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
							error: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
							log: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
							output: tg::object::metadata::Subtree {
								count: Some(0),
								depth: Some(0),
								size: Some(0),
								solvable: None,
								solved: None,
							},
						},
					};

					// Handle the children.
					for child_index in children {
						let (_, child_node) = graph.nodes.get_index(*child_index).unwrap();
						let child_node =
							child_node.try_unwrap_process_ref().ok().ok_or_else(|| {
								tg::error!("all children of processes must be processes")
							})?;
						metadata.subtree.count = metadata
							.subtree
							.count
							.zip(child_node.metadata.as_ref().and_then(|m| m.subtree.count))
							.map(|(a, b)| a + b);

						// Aggregate child process's subtree command metadata.
						metadata.subtree.command.count = metadata
							.subtree
							.command
							.count
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.command.count),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.command.depth = metadata
							.subtree
							.command
							.depth
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.command.depth),
							)
							.map(|(a, b)| a.max(b));
						metadata.subtree.command.size = metadata
							.subtree
							.command
							.size
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.command.size),
							)
							.map(|(a, b)| a + b);

						// Aggregate the child process's subtree error metadata.
						metadata.subtree.error.count = metadata
							.subtree
							.error
							.count
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.error.count),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.error.depth = metadata
							.subtree
							.error
							.depth
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.error.depth),
							)
							.map(|(a, b)| a.max(b));
						metadata.subtree.error.size = metadata
							.subtree
							.error
							.size
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.error.size),
							)
							.map(|(a, b)| a + b);

						// Aggregate the child process's subtree log metadata.
						metadata.subtree.log.count = metadata
							.subtree
							.log
							.count
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.log.count),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.log.depth = metadata
							.subtree
							.log
							.depth
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.log.depth),
							)
							.map(|(a, b)| a.max(b));
						metadata.subtree.log.size = metadata
							.subtree
							.log
							.size
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.log.size),
							)
							.map(|(a, b)| a + b);

						// Aggregate the child process's subtree output metadata.
						metadata.subtree.output.count = metadata
							.subtree
							.output
							.count
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.output.count),
							)
							.map(|(a, b)| a + b);
						metadata.subtree.output.depth = metadata
							.subtree
							.output
							.depth
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.output.depth),
							)
							.map(|(a, b)| a.max(b));
						metadata.subtree.output.size = metadata
							.subtree
							.output
							.size
							.zip(
								child_node
									.metadata
									.as_ref()
									.and_then(|m| m.subtree.output.size),
							)
							.map(|(a, b)| a + b);
					}

					// Handle the objects.
					for (object_index, object_kind) in objects {
						let (_, object_node) = graph.nodes.get_index(*object_index).unwrap();
						let object_node = object_node
							.try_unwrap_object_ref()
							.ok()
							.ok_or_else(|| tg::error!("expected an object"))?;
						match object_kind {
							tangram_index::process::object::Kind::Command => {
								metadata.node.command.count = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.count);
								metadata.node.command.depth = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.depth);
								metadata.node.command.size = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.size);

								metadata.subtree.command.count = metadata
									.subtree
									.command
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.subtree.command.depth = metadata
									.subtree
									.command
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.subtree.command.size = metadata
									.subtree
									.command
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);
							},

							tangram_index::process::object::Kind::Error => {
								metadata.node.error.count = metadata
									.node
									.error
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.node.error.depth = metadata
									.node
									.error
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.node.error.size = metadata
									.node
									.error
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);

								metadata.subtree.error.count = metadata
									.subtree
									.error
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.subtree.error.depth = metadata
									.subtree
									.error
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.subtree.error.size = metadata
									.subtree
									.error
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);
							},

							tangram_index::process::object::Kind::Log => {
								metadata.node.log.count = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.count);
								metadata.node.log.depth = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.depth);
								metadata.node.log.size = object_node
									.metadata
									.as_ref()
									.and_then(|metadata| metadata.subtree.size);

								metadata.subtree.log.count = metadata
									.subtree
									.log
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.subtree.log.depth = metadata
									.subtree
									.log
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.subtree.log.size = metadata
									.subtree
									.log
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);
							},

							tangram_index::process::object::Kind::Output => {
								metadata.node.output.count = metadata
									.node
									.output
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.node.output.depth = metadata
									.node
									.output
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.node.output.size = metadata
									.node
									.output
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);

								metadata.subtree.output.count = metadata
									.subtree
									.output
									.count
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.count),
									)
									.map(|(a, b)| a + b);
								metadata.subtree.output.depth = metadata
									.subtree
									.output
									.depth
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.depth),
									)
									.map(|(a, b)| a.max(b));
								metadata.subtree.output.size = metadata
									.subtree
									.output
									.size
									.zip(
										object_node
											.metadata
											.as_ref()
											.and_then(|metadata| metadata.subtree.size),
									)
									.map(|(a, b)| a + b);
							},
						}
					}

					// Merge the existing metadata.
					if let Some(existing) = &node.metadata {
						metadata.merge(existing);
					}

					// Update the node.
					let (_, node) = graph.nodes.get_index_mut(index).unwrap();
					let node_inner = node.unwrap_process_mut();
					node_inner.metadata = Some(metadata);
				},
			}
		}

		let touched_at = self.server.clock.unix_timestamp()?;

		// Create the grant args.
		let mut put_grant_args = Vec::new();
		let grant_subject = match &self.context.principal {
			tg::Principal::Root => None,
			tg::Principal::Anonymous => Some(tg::authorization::Subject::Public),
			principal => Some(principal.try_to_subject()?),
		};
		if let Some(grant_subject) = grant_subject {
			let object_expires_at = touched_at
				+ self
					.server
					.config
					.object
					.grant_time_to_live
					.as_secs()
					.to_i64()
					.unwrap();
			let process_expires_at = touched_at
				+ self
					.server
					.config
					.process
					.grant_time_to_live
					.as_secs()
					.to_i64()
					.unwrap();
			let mut object_covered = vec![false; graph.nodes.len()];
			let mut process_covered =
				vec![tg::authorization::permission::process::Set::empty(); graph.nodes.len()];
			for index in indices.iter().rev().copied() {
				let (id, node) = graph.nodes.get_index(index).unwrap();
				match node {
					Node::Group(_)
					| Node::Organization(_)
					| Node::Sandbox(_)
					| Node::Tag(_)
					| Node::User(_) => {},
					Node::Object(node) => {
						let visible = node
							.local_visible
							.as_ref()
							.is_some_and(|visible| visible.subtree);
						let mut subtree = false;
						if node.marked && !object_covered[index] {
							let permission = if visible {
								tg::authorization::permission::object::Permission::Subtree
							} else {
								tg::authorization::permission::object::Permission::Node
							};
							subtree = visible;
							put_grant_args.push(tangram_index::grant::put::Arg {
								created_at: touched_at,
								creator: Some(self.context.principal.clone()),
								expires_at: Some(object_expires_at),
								permissions: tg::authorization::permission::Set::Object(
									tg::authorization::permission::object::Set::from_permission(
										permission,
									),
								),
								subject: grant_subject.clone(),
								resource: tg::object::Id::try_from(id.clone())?.into(),
								time_to_touch: Some(self.server.config.object.grant_time_to_touch),
							});
						}
						let covered = object_covered[index] || subtree;
						if covered && let Some(children) = node.children.as_ref() {
							for child in children {
								object_covered[*child] = true;
							}
						}
					},
					Node::Process(node) => {
						let visible = node.local_visible.clone().unwrap_or_default();
						let mut permissions = if node.marked {
							Self::sync_get_index_process_grant_permissions(&visible)
						} else {
							tg::authorization::permission::process::Set::empty()
						};
						Self::sync_get_index_remove_process_permissions_covered_by_ancestors(
							&mut permissions,
							process_covered[index],
						);
						if !permissions.is_empty() {
							put_grant_args.push(tangram_index::grant::put::Arg {
								created_at: touched_at,
								creator: Some(self.context.principal.clone()),
								expires_at: Some(process_expires_at),
								permissions: tg::authorization::permission::Set::Process(
									permissions,
								),
								subject: grant_subject.clone(),
								resource: tg::process::Id::try_from(id.clone())?.into(),
								time_to_touch: Some(self.server.config.process.grant_time_to_touch),
							});
						}
						let subtree_permissions =
							Self::sync_get_index_process_subtree_permissions(permissions);
						let mut covered = process_covered[index];
						covered.insert(subtree_permissions);
						if let Some(children) = node.children.as_ref() {
							for child in children {
								process_covered[*child].insert(covered);
							}
						}
					},
				}
			}
		}

		// Create the args.
		let mut put_object_args = Vec::new();
		let mut put_process_args = Vec::new();
		let mut visited = std::collections::HashSet::new();
		let mut stack = graph
			.nodes
			.iter()
			.enumerate()
			.filter_map(|(index, (_, node))| node.parents().is_empty().then_some(index))
			.collect::<Vec<_>>();
		while let Some(index) = stack.pop() {
			if !visited.insert(index) {
				continue;
			}
			let (id, node) = graph.nodes.get_index(index).unwrap();
			match node {
				Node::Group(node)
				| Node::Organization(node)
				| Node::Sandbox(node)
				| Node::Tag(node)
				| Node::User(node) => {
					if let Some(children) = node.children.as_ref() {
						stack.extend(children.iter().copied());
					}
				},
				Node::Object(node) => {
					let id = tg::object::Id::try_from(id.clone())?;
					if node.marked {
						let children = node
							.children
							.as_ref()
							.unwrap()
							.iter()
							.map(|index| {
								graph.nodes.get_index(*index).unwrap().0.clone().try_into()
							})
							.collect::<tg::Result<std::collections::BTreeSet<_>>>()?;
						let metadata = node.metadata.clone().unwrap();
						let stored = node.local_stored.clone().unwrap();
						let arg = tangram_index::object::put::Arg {
							checkout: None,
							children,
							id,
							metadata,
							stored,
							time_to_touch: self.server.config.object.time_to_touch,
							touched_at,
						};
						put_object_args.push(arg);
					}
					if let Some(children) = node.children.as_ref() {
						stack.extend(children.iter().copied());
					}
				},
				Node::Process(node) => {
					let id = tg::process::Id::try_from(id.clone())?;
					if node.marked {
						let children = node
							.data
							.as_ref()
							.and_then(|data| data.children.clone())
							.ok_or_else(|| tg::error!("expected the process children to be set"))?;
						let stored = node.local_stored.clone().unwrap();
						let metadata = node.metadata.clone().unwrap();
						let objects = node
							.objects
							.as_ref()
							.unwrap()
							.iter()
							.copied()
							.map(|(index, kind)| {
								let id =
									graph.nodes.get_index(index).unwrap().0.clone().try_into()?;
								Ok((id, kind))
							})
							.collect::<tg::Result<Vec<_>>>()?;
						let mut command = None;
						let mut error = Vec::new();
						let mut log = None;
						let mut output = Vec::new();
						for (object, kind) in objects {
							match kind {
								tangram_index::process::object::Kind::Command => {
									command = Some(object);
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
						let command =
							command.ok_or_else(|| tg::error!("expected the command to be set"))?;
						let arg = tangram_index::process::put::Arg {
							cached: false,
							children: Some(children),
							command,
							data: node
								.data
								.clone()
								.map(tg::process::Data::without_location_and_tokens),
							error: Some((!error.is_empty()).then_some(error)),
							id,
							log: Some(log),
							metadata,
							options: tg::referent::Options::default(),
							output: Some((!output.is_empty()).then_some(output)),
							parent: None,
							sandbox: None,
							stored,
							time_to_touch: self.server.config.process.time_to_touch,
							touched_at,
						};
						put_process_args.push(arg);
					}
					if let Some(children) = node.children.as_ref() {
						stack.extend(children.iter().copied());
					}
					if let Some(objects) = node.objects.as_ref() {
						stack.extend(objects.iter().map(|(index, _)| *index));
					}
				},
			}
		}

		Ok((put_grant_args, put_object_args, put_process_args))
	}

	fn sync_get_index_process_grant_permissions(
		visible: &tangram_index::process::Stored,
	) -> tg::authorization::permission::process::Set {
		let mut permissions = tg::authorization::permission::process::Set::empty();
		if visible.subtree {
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE);
		} else {
			permissions.insert(tg::authorization::permission::process::Set::NODE);
		}
		if visible.subtree_command {
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE_COMMAND);
		} else if visible.node_command {
			permissions.insert(tg::authorization::permission::process::Set::NODE_COMMAND);
		}
		if visible.subtree_error {
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE_ERROR);
		} else if visible.node_error {
			permissions.insert(tg::authorization::permission::process::Set::NODE_ERROR);
		}
		if visible.subtree_log {
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE_LOG);
		} else if visible.node_log {
			permissions.insert(tg::authorization::permission::process::Set::NODE_LOG);
		}
		if visible.subtree_output {
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE_OUTPUT);
		} else if visible.node_output {
			permissions.insert(tg::authorization::permission::process::Set::NODE_OUTPUT);
		}
		permissions
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
