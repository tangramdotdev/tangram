use {
	crate::{
		Server,
		sync::{
			get::State,
			graph::Requested,
			queue::{ObjectItem, ProcessItem},
		},
	},
	futures::{StreamExt as _, TryStreamExt as _},
	std::{collections::BTreeSet, sync::Arc},
	tangram_client::prelude::*,
};

impl Server {
	pub(super) async fn sync_get_queue(
		&self,
		state: Arc<State>,
		queue_object_receiver: async_channel::Receiver<ObjectItem>,
		queue_process_receiver: async_channel::Receiver<ProcessItem>,
	) -> tg::Result<()> {
		// Create the objects future.
		let object_batch_size = self.config.sync.get.queue.object_batch_size;
		let object_batch_timeout = self.config.sync.get.queue.object_batch_timeout;
		let object_concurrency = self.config.sync.get.queue.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			queue_object_receiver,
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |items| {
			let server = self.clone();
			let state = state.clone();
			async move { server.sync_get_queue_object_batch(&state, items).await }
		});

		// Create the processes future.
		let process_batch_size = self.config.sync.get.queue.process_batch_size;
		let process_batch_timeout = self.config.sync.get.queue.process_batch_timeout;
		let process_concurrency = self.config.sync.get.queue.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			queue_process_receiver,
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |items| {
			let server = self.clone();
			let state = state.clone();
			async move { server.sync_get_queue_process_batch(&state, items).await }
		});

		// Join the objects and processes futures.
		futures::try_join!(objects_future, processes_future)?;

		// Send the get end message.
		state
			.sender
			.send(Ok(tg::sync::GetMessage::End))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the get end message"))?;

		Ok(())
	}

	async fn sync_get_queue_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		// Get the ids.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();

		// Touch the objects and get stored and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let outputs = if state.arg.force {
			vec![None; ids.len()]
		} else {
			self.try_touch_object_and_get_stored_and_metadata_batch(&ids, touched_at)
				.await
				.map_err(|source| tg::error!(!source, "failed to touch the objects"))?
		};

		// Handle each item and output.
		for (item, output) in std::iter::zip(items, outputs) {
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
							graph.update_object_local(
								&item.id,
								None,
								None,
								None,
								None,
								Some(requested),
							);
							false
						}
					};
					if requested {
						continue;
					}
					let message = tg::sync::GetMessage::Item(tg::sync::GetItemMessage::Object(
						tg::sync::GetItemObjectMessage {
							id: item.id.clone(),
							eager: state.arg.eager,
						},
					));
					state
						.sender
						.send(Ok(message))
						.await
						.map_err(|source| tg::error!(!source, "failed to send the message"))?;
				},

				Some((stored, metadata)) => {
					// Update the graph with stored and metadata.
					state.graph.lock().unwrap().update_object_local(
						&item.id,
						None,
						Some(stored.clone()),
						Some(metadata.clone()),
						None,
						None,
					);

					if stored.subtree {
						// If the object is stored, then send a stored message.
						let message = tg::sync::GetMessage::Stored(
							tg::sync::GetStoredMessage::Object(tg::sync::GetStoredObjectMessage {
								id: item.id.clone(),
							}),
						);
						state
							.sender
							.send(Ok(message))
							.await
							.map_err(|source| tg::error!(!source, "failed to send the message"))?;
					} else {
						// If the object is stored but its subtree is not stored, then enqueue the children.
						let bytes = self
							.try_get_object_local(&item.id)
							.await
							.map_err(|source| tg::error!(!source, "failed to get the object"))?
							.ok_or_else(|| tg::error!("expected the object to exist"))?
							.bytes;
						let data = tg::object::Data::deserialize(item.id.kind(), bytes).map_err(
							|source| tg::error!(!source, "failed to deserialize the object"),
						)?;

						// Update the graph with data.
						state.graph.lock().unwrap().update_object_local(
							&item.id,
							Some(&data),
							None,
							None,
							None,
							None,
						);

						Self::sync_get_enqueue_object_children(state, &item.id, &data, item.kind);
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

		// Touch the processes and get stored and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let outputs = self
			.try_touch_process_and_get_stored_and_metadata_batch(&ids, touched_at)
			.await
			.map_err(|source| tg::error!(!source, "failed to touch the processes"))?;

		// Handle each item and output.
		for (item, output) in std::iter::zip(items, outputs) {
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
							graph.update_process_local(
								&item.id,
								None,
								None,
								None,
								None,
								Some(requested),
							);
							false
						}
					};
					if requested {
						continue;
					}
					let message = tg::sync::GetMessage::Item(tg::sync::GetItemMessage::Process(
						tg::sync::GetItemProcessMessage {
							id: item.id.clone(),
							eager: state.arg.eager,
						},
					));
					state
						.sender
						.send(Ok(message))
						.await
						.map_err(|source| tg::error!(!source, "failed to send the message"))?;
				},

				// If the process is present, then enqueue children and objects as necessary, and send a stored message if necessary.
				Some((stored, metadata)) => {
					// Get the process.
					let data = self
						.try_get_process_local(&item.id)
						.await
						.map_err(|source| tg::error!(!source, "failed to get the process"))?
						.ok_or_else(|| tg::error!("expected the process to exist"))?
						.data;

					// Update the graph with stored and metadata and data.
					state.graph.lock().unwrap().update_process_local(
						&item.id,
						Some(&data),
						Some(stored.clone()),
						Some(metadata.clone()),
						None,
						None,
					);

					// Enqueue the children as necessary.
					Self::sync_get_enqueue_process_children(state, &item.id, &data, Some(stored));

					// Send a stored message if necessary.
					if stored.subtree
						|| stored.subtree_command
						|| stored.subtree_error
						|| stored.subtree_log
						|| stored.subtree_output
					{
						let message =
							tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Process(
								tg::sync::GetStoredProcessMessage {
									id: item.id.clone(),
									node_command_stored: stored.node_command,
									node_error_stored: stored.node_error,
									node_log_stored: stored.node_log,
									node_output_stored: stored.node_output,
									subtree_command_stored: stored.subtree_command,
									subtree_error_stored: stored.subtree_error,
									subtree_log_stored: stored.subtree_log,
									subtree_output_stored: stored.subtree_output,
									subtree_stored: stored.subtree,
								},
							));
						state.sender.send(Ok(message)).await.map_err(|source| {
							tg::error!(!source, "failed to send the stored message")
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
	) {
		let mut children = BTreeSet::new();
		data.children(&mut children);
		state
			.queue
			.enqueue_objects(children.into_iter().map(|object| ObjectItem {
				parent: Some(tg::Either::Left(id.clone())),
				id: object,
				kind,
				eager: state.arg.eager,
			}));
	}

	pub(super) fn sync_get_enqueue_process_children(
		state: &State,
		id: &tg::process::Id,
		data: &tg::process::Data,
		stored: Option<&crate::process::stored::Output>,
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
			for referent in children {
				state.queue.enqueue_process(ProcessItem {
					parent: Some(id.clone()),
					id: referent.item.clone(),
					eager: state.arg.eager,
				});
			}
		}

		// Enqueue the command if necessary.
		if state.arg.commands && !stored.is_some_and(|stored| stored.node_command) {
			let item = ObjectItem {
				parent: Some(tg::Either::Right(id.clone())),
				id: data.command.clone().into(),
				kind: Some(crate::sync::queue::ObjectKind::Command),
				eager: state.arg.eager,
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
							parent: Some(tg::Either::Right(id.clone())),
							id: object,
							kind: Some(crate::sync::queue::ObjectKind::Error),
							eager: state.arg.eager,
						}));
				},
				tg::Either::Right(error) => {
					let item = ObjectItem {
						parent: Some(tg::Either::Right(id.clone())),
						id: error.clone().into(),
						kind: Some(crate::sync::queue::ObjectKind::Error),
						eager: state.arg.eager,
					};
					state.queue.enqueue_object(item);
				},
			}
		}

		// Enqueue the log if necessary.
		if state.arg.logs && !stored.is_some_and(|stored| stored.node_log) {
			if let Some(log) = data.log.clone() {
				let item = ObjectItem {
					parent: Some(tg::Either::Right(id.clone())),
					id: log.into(),
					kind: Some(crate::sync::queue::ObjectKind::Log),
					eager: state.arg.eager,
				};
				state.queue.enqueue_object(item);
			} else {
				tracing::warn!(process = %id, "cannot sync logs: missing log id");
			}
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
					parent: Some(tg::Either::Right(id.clone())),
					id: object,
					kind: Some(crate::sync::queue::ObjectKind::Output),
					eager: state.arg.eager,
				}));
		}
	}
}
