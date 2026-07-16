use {
	crate::{
		Session,
		sync::{
			get::State,
			graph::{Graph, Requested, UpdateObjectLocalArg, UpdateProcessLocalArg},
			queue::{ObjectItem, ProcessItem},
		},
	},
	futures::{StreamExt as _, TryStreamExt as _},
	std::{collections::BTreeSet, sync::Arc},
	tangram_client::prelude::*,
};

impl Session {
	pub(super) async fn sync_get_queue(
		&self,
		state: Arc<State>,
		queue_object_receiver: async_channel::Receiver<ObjectItem>,
		queue_process_receiver: async_channel::Receiver<ProcessItem>,
	) -> tg::Result<()> {
		// Create the objects future.
		let object_batch_size = self.server.config.sync.get.queue.object_batch_size;
		let object_batch_timeout = self.server.config.sync.get.queue.object_batch_timeout;
		let object_concurrency = self.server.config.sync.get.queue.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			queue_object_receiver,
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |items| {
			let session = self.clone();
			let state = state.clone();
			async move { session.sync_get_queue_object_batch(&state, items).await }
		});

		// Create the processes future.
		let process_batch_size = self.server.config.sync.get.queue.process_batch_size;
		let process_batch_timeout = self.server.config.sync.get.queue.process_batch_timeout;
		let process_concurrency = self.server.config.sync.get.queue.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			queue_process_receiver,
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |items| {
			let session = self.clone();
			let state = state.clone();
			async move { session.sync_get_queue_process_batch(&state, items).await }
		});

		// Join the objects and processes futures.
		futures::try_join!(objects_future, processes_future)?;

		// Send the get end message.
		state
			.sender
			.send(Ok(tg::sync::GetMessage::End))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the get end message"))?;

		Ok(())
	}

	async fn sync_get_queue_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		// Get the ids.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();

		// Authorize and touch the objects, then get stored and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let (outputs, permissions) = if state.arg.force {
			(vec![None; ids.len()], vec![None; ids.len()])
		} else {
			self.sync_get_touch_authorized_objects(
				&ids,
				touched_at,
				self.server.config.object.time_to_touch,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch the objects"))?
		};

		// Handle each item and output.
		for ((item, output), permissions) in
			std::iter::zip(std::iter::zip(items, outputs), permissions)
		{
			match output {
				// If the object is absent, then send a get item message.
				None => {
					// Determine if the object has been requested.
					let requested = {
						let mut graph = state.graph.lock().unwrap();
						let requested = graph.get_object_requested(&item.id);
						if requested.is_some() {
							true
						} else {
							let requested = Requested { eager: item.eager };
							let arg = UpdateObjectLocalArg {
								data: None,
								id: &item.id,
								marked: None,
								metadata: None,
								permissions: None,
								requested: Some(requested),
								stored: None,
							};
							graph.update_object_local(arg);
							false
						}
					};
					if requested {
						continue;
					}
					let message = tg::sync::GetMessage::Item(tg::sync::GetItemMessage::Object(
						tg::sync::GetItemObjectMessage {
							eager: item.eager,
							id: item.id.clone(),
							token: item.token,
						},
					));
					state
						.sender
						.send(Ok(message))
						.await
						.map_err(|error| tg::error!(!error, "failed to send the message"))?;
				},

				Some(object) => {
					let stored = object.stored;
					let metadata = object.metadata;

					// Update the graph with stored and metadata.
					let arg = UpdateObjectLocalArg {
						data: None,
						id: &item.id,
						marked: None,
						metadata: Some(metadata.clone()),
						permissions,
						requested: None,
						stored: Some(stored.clone()),
					};
					state.graph.lock().unwrap().update_object_local(arg);
					let visible = state
						.graph
						.lock()
						.unwrap()
						.get_object_local_visible(&item.id);

					if visible.subtree {
						// If the object is visible, then send a stored message.
						let message = tg::sync::GetMessage::Stored(
							tg::sync::GetStoredMessage::Object(tg::sync::GetStoredObjectMessage {
								id: item.id.clone(),
							}),
						);
						state
							.sender
							.send(Ok(message))
							.await
							.map_err(|error| tg::error!(!error, "failed to send the message"))?;

						// Increment the progress.
						let objects = metadata.subtree.count.unwrap_or(1);
						let bytes = metadata.subtree.size.unwrap_or(metadata.node.size);
						state.progress.increment_skipped(0, objects, bytes);
					} else {
						// If the object is stored but its subtree is not visible, then enqueue the children.
						let bytes = self
							.try_get_object_local(&item.id, false)
							.await
							.map_err(|error| tg::error!(!error, "failed to get the object"))?
							.ok_or_else(|| tg::error!("expected the object to exist"))?
							.bytes;
						let data = tg::object::Data::deserialize(item.id.kind(), bytes).map_err(
							|error| tg::error!(!error, "failed to deserialize the object"),
						)?;

						// Update the graph with data.
						let arg = UpdateObjectLocalArg {
							data: Some(&data),
							id: &item.id,
							marked: None,
							metadata: None,
							permissions: None,
							requested: None,
							stored: None,
						};
						state.graph.lock().unwrap().update_object_local(arg);

						Self::sync_get_enqueue_object_children(
							state,
							&item.id,
							&data,
							item.kind,
							item.token.as_ref(),
						);

						// Increment the progress.
						state.progress.increment_skipped(0, 1, metadata.node.size);
					}
				},
			}
		}

		let end = state.graph.lock().unwrap().end_local(&state.arg);
		if end {
			state.queue.close();
		}

		Ok(())
	}

	async fn sync_get_queue_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
	) -> tg::Result<()> {
		// Get the ids.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();

		// Authorize and touch the processes, then get stored and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let (outputs, permissions) = if state.arg.force {
			(vec![None; ids.len()], vec![None; ids.len()])
		} else {
			self.sync_get_touch_authorized_processes(
				&ids,
				&state.arg,
				touched_at,
				self.server.config.process.time_to_touch,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch the processes"))?
		};

		// Handle each item and output.
		for ((item, output), permissions) in
			std::iter::zip(std::iter::zip(items, outputs), permissions)
		{
			match &output {
				// If the process is absent, then send a get item message.
				None => {
					// Determine if the object has been requested.
					let requested = {
						let mut graph = state.graph.lock().unwrap();
						let requested = graph.get_process_requested(&item.id);
						if requested.is_some() {
							true
						} else {
							let requested = Requested { eager: item.eager };
							let arg = UpdateProcessLocalArg {
								data: None,
								id: &item.id,
								marked: None,
								metadata: None,
								permissions: None,
								requested: Some(requested),
								stored: None,
							};
							graph.update_process_local(arg);
							false
						}
					};
					if requested {
						continue;
					}
					let message = tg::sync::GetMessage::Item(tg::sync::GetItemMessage::Process(
						tg::sync::GetItemProcessMessage {
							eager: item.eager,
							id: item.id.clone(),
							token: item.token,
						},
					));
					state
						.sender
						.send(Ok(message))
						.await
						.map_err(|error| tg::error!(!error, "failed to send the message"))?;
				},

				// If the process is present, then enqueue children and objects as necessary, and send a stored message if necessary.
				Some(process) => {
					let stored = &process.stored;
					let metadata = &process.metadata;
					// Get the process.
					let data = self
						.try_get_process_local(&item.id, false)
						.await
						.map_err(|error| tg::error!(!error, "failed to get the process"))?
						.ok_or_else(|| tg::error!("expected the process to exist"))?
						.data;

					// Update the graph with stored and metadata and data.
					let arg = UpdateProcessLocalArg {
						data: Some(&data),
						id: &item.id,
						marked: None,
						metadata: Some(metadata.clone()),
						permissions,
						requested: None,
						stored: Some(stored.clone()),
					};
					state.graph.lock().unwrap().update_process_local(arg);
					let visible = state
						.graph
						.lock()
						.unwrap()
						.get_process_local_visible(&item.id);

					// Enqueue the children as necessary.
					Self::sync_get_enqueue_process_children(
						state,
						&item.id,
						&data,
						Some(&visible),
						item.token.as_ref(),
					);

					// Send a stored message if the process is visible.
					if Graph::process_visible_any(&visible) {
						let message =
							tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Process(
								tg::sync::GetStoredProcessMessage {
									id: item.id.clone(),
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
						state.sender.send(Ok(message)).await.map_err(|error| {
							tg::error!(!error, "failed to send the stored message")
						})?;
					}
				},
			}
		}

		let end = state.graph.lock().unwrap().end_local(&state.arg);
		if end {
			state.queue.close();
		}

		Ok(())
	}

	pub(super) fn sync_get_enqueue_object_children(
		state: &State,
		id: &tg::object::Id,
		data: &tg::object::Data,
		kind: Option<crate::sync::queue::ObjectKind>,
		token: Option<&tg::grant::Token>,
	) {
		let mut children = BTreeSet::new();
		data.children(&mut children);
		state
			.queue
			.enqueue_objects(children.into_iter().map(|object| ObjectItem {
				eager: state.arg.eager,
				id: object,
				kind,
				parent: Some(tg::Either::Left(id.clone())),
				token: token.cloned(),
			}));
	}

	pub(super) fn sync_get_enqueue_process_children(
		state: &State,
		id: &tg::process::Id,
		data: &tg::process::Data,
		stored: Option<&tangram_index::process::Stored>,
		token: Option<&tg::grant::Token>,
	) {
		// Enqueue the children if necessary.
		if state.arg.recursive
			&& (!stored.is_some_and(|stored| stored.subtree)
				|| (state.arg.commands && !stored.is_some_and(|stored| stored.subtree_command))
				|| (state.arg.errors && !stored.is_some_and(|stored| stored.subtree_error))
				|| (state.arg.logs && !stored.is_some_and(|stored| stored.subtree_log))
				|| (state.arg.outputs && !stored.is_some_and(|stored| stored.subtree_output)))
			&& let Some(children) = &data.children
		{
			for child in children {
				state.queue.enqueue_process(ProcessItem {
					eager: state.arg.eager,
					id: child
						.process
						.clone()
						.map_right(|process| process.id)
						.into_inner(),
					parent: Some(id.clone()),
					token: token.cloned(),
				});
			}
		}

		// Enqueue the command if necessary.
		if state.arg.commands && !stored.is_some_and(|stored| stored.node_command) {
			let item = ObjectItem {
				eager: state.arg.eager,
				id: data.command.clone().into(),
				kind: Some(crate::sync::queue::ObjectKind::Command),
				parent: Some(tg::Either::Right(id.clone())),
				token: token.cloned(),
			};
			state.queue.enqueue_object(item);
		}

		// Enqueue the error if necessary.
		if state.arg.errors
			&& !stored.is_some_and(|stored| stored.node_error)
			&& let Some(error) = &data.error
		{
			match error {
				tg::Either::Left(data) => {
					let mut children = BTreeSet::new();
					data.children(&mut children);
					state
						.queue
						.enqueue_objects(children.into_iter().map(|object| ObjectItem {
							eager: state.arg.eager,
							id: object,
							kind: Some(crate::sync::queue::ObjectKind::Error),
							parent: Some(tg::Either::Right(id.clone())),
							token: token.cloned(),
						}));
				},
				tg::Either::Right(error) => {
					let item = ObjectItem {
						eager: state.arg.eager,
						id: error
							.clone()
							.map_right(|error| error.id)
							.into_inner()
							.into(),
						kind: Some(crate::sync::queue::ObjectKind::Error),
						parent: Some(tg::Either::Right(id.clone())),
						token: token.cloned(),
					};
					state.queue.enqueue_object(item);
				},
			}
		}

		// Enqueue the log if necessary.
		if state.arg.logs
			&& !stored.is_some_and(|stored| stored.node_log)
			&& let Some(log) = data.log.clone()
		{
			let item = ObjectItem {
				eager: state.arg.eager,
				id: log.map_right(|log| log.id).into_inner().into(),
				kind: Some(crate::sync::queue::ObjectKind::Log),
				parent: Some(tg::Either::Right(id.clone())),
				token: token.cloned(),
			};
			state.queue.enqueue_object(item);
		}

		// Enqueue the output if necessary.
		if (state.arg.outputs && !stored.is_some_and(|stored| stored.node_output))
			&& let Some(output) = &data.output
		{
			let mut children = BTreeSet::new();
			output.children(&mut children);
			state
				.queue
				.enqueue_objects(children.into_iter().map(|object| ObjectItem {
					eager: state.arg.eager,
					id: object,
					kind: Some(crate::sync::queue::ObjectKind::Output),
					parent: Some(tg::Either::Right(id.clone())),
					token: token.cloned(),
				}));
		}
	}
}
