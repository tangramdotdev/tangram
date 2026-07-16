use {
	crate::{Session, sync::put::State},
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _},
	std::{collections::BTreeSet, sync::Arc},
	tangram_client::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ObjectItem {
	pub eager: bool,
	pub id: tg::object::Id,
	pub kind: Option<crate::sync::queue::ObjectKind>,
}

pub struct ProcessItem {
	pub eager: bool,
	pub id: tg::process::Id,
}

impl Session {
	#[tracing::instrument(err, level = "trace", name = "store", ret, skip_all)]
	pub(super) async fn sync_put_store(
		&self,
		state: Arc<State>,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
	) -> tg::Result<()> {
		// Create the objects future.
		let object_batch_size = self.server.config.sync.put.store.object_batch_size;
		let object_batch_timeout = self.server.config.sync.put.store.object_batch_timeout;
		let object_concurrency = self.server.config.sync.put.store.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(object_receiver),
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |items| {
			let session = self.clone();
			let state = state.clone();
			async move { session.sync_put_store_object_batch(&state, items).await }
		});

		// Create the processes future.
		let process_batch_size = self.server.config.sync.put.store.process_batch_size;
		let process_batch_timeout = self.server.config.sync.put.store.process_batch_timeout;
		let process_concurrency = self.server.config.sync.put.store.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(process_receiver),
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |items| {
			let session = self.clone();
			let state = state.clone();
			async move { session.sync_put_store_process_batch(&state, items).await }
		});

		// Join the objects and processes futures.
		futures::try_join!(objects_future, processes_future)?;

		Ok(())
	}

	pub(super) async fn sync_put_store_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		// Get the objects.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();
		let outputs = self
			.server
			.try_get_object_batch_local(&ids, state.arg.metadata)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the objects"))?;

		// Handle the objects.
		for (item, output) in std::iter::zip(items, outputs) {
			// If the object is missing, then send a missing message.
			let Some(mut output) = output else {
				let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage::Object(
					tg::sync::PutMissingObjectMessage {
						id: item.id.clone(),
					},
				));
				state.sender.send(Ok(message)).await.ok();
				continue;
			};

			// Deserialize the object and update the graph.
			let data = tg::object::Data::deserialize(item.id.kind(), output.bytes.clone())
				.map_err(|error| tg::error!(!error, "failed to deserialize the object"))?;
			let update = crate::sync::graph::UpdateObjectLocalArg {
				data: Some(&data),
				id: &item.id,
				marked: None,
				metadata: None,
				permissions: None,
				requested: None,
				stored: None,
			};
			state.graph.lock().unwrap().update_object_local(update);

			// Mask the metadata with the permissions already proven by the graph.
			if let Some(metadata) = output.metadata.take() {
				let required =
					tg::grant::permission::Set::from_permission(tg::grant::Permission::Object(
						tg::grant::permission::object::Permission::Subtree,
					));
				let permissions = state
					.graph
					.lock()
					.unwrap()
					.get_object_local_authorization(&item.id, required)
					.permissions;
				output.metadata =
					Self::mask_object_metadata_with_permissions(metadata, permissions);
			}

			// Send the object.
			let message = tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Object(
				tg::sync::PutItemObjectMessage {
					id: item.id.clone(),
					bytes: output.bytes.clone(),
					metadata: output.metadata,
				},
			));
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|error| tg::error!(!error, "failed to send the put message"))?;
			state.graph.lock().unwrap().update_object_remote(
				&item.id,
				None,
				item.kind,
				Some(&tangram_index::object::Stored { subtree: true }),
			);

			// Enqueue the children.
			if item.eager {
				let mut children = BTreeSet::new();
				data.children(&mut children);
				let items = children
					.into_iter()
					.map(|child| crate::sync::queue::ObjectItem {
						eager: item.eager,
						id: child,
						kind: item.kind,
						parent: Some(tg::Either::Left(item.id.clone())),
						token: None,
					});
				state.queue.enqueue_objects(items);
			}
		}

		if state.graph.lock().unwrap().end_remote(&state.arg) {
			state.queue.close();
		}

		Ok(())
	}

	pub(super) async fn sync_put_store_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
	) -> tg::Result<()> {
		// Get the processes.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();
		let outputs = self
			.server
			.try_get_process_batch_local(&ids, state.arg.metadata)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the processes"))?;

		// Handle the processes.
		for (item, output) in std::iter::zip(items, outputs) {
			let Some(mut output) = output else {
				let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage::Process(
					tg::sync::PutMissingProcessMessage {
						id: item.id.clone(),
					},
				));
				state.sender.send(Ok(message)).await.ok();
				continue;
			};

			// Compact the log if needed before sending the process data.
			if state.arg.logs && output.data.log.is_none() {
				let permission = tg::grant::Permission::Process(
					tg::grant::permission::process::Permission::NodeLog,
				);
				let required = tg::grant::permission::Set::from_permission(permission);
				let permissions = state
					.graph
					.lock()
					.unwrap()
					.get_process_local_authorization(&item.id, required)
					.permissions;
				if !permissions.contains(permission) {
					return Err(tg::error!("unauthorized"));
				}

				// Compact.
				self.compact_process_log(&item.id).boxed().await.map_err(
					|error| tg::error!(!error, process = %item.id, "failed to compact the log"),
				)?;

				// Get the compacted process data from the process store.
				output.data = self
					.server
					.try_get_process_local(&item.id, false)
					.await?
					.ok_or_else(
						|| tg::error!(process = %item.id, "failed to get the process after compaction"),
					)?
					.data;
			}

			// Update the graph.
			let update = crate::sync::graph::UpdateProcessLocalArg {
				data: Some(&output.data),
				id: &item.id,
				marked: None,
				metadata: None,
				permissions: None,
				requested: None,
				stored: None,
			};
			state.graph.lock().unwrap().update_process_local(update);

			// Mask the metadata with the permissions already proven by the graph.
			if let Some(metadata) = output.metadata.take() {
				let required =
					tg::grant::permission::Set::Process(tg::grant::permission::process::Set::all());
				let permissions = state
					.graph
					.lock()
					.unwrap()
					.get_process_local_authorization(&item.id, required)
					.permissions;
				output.metadata =
					Self::mask_process_metadata_with_permissions(&metadata, permissions);
			}

			// Send the process.
			let bytes = serde_json::to_string(&output.data)
				.map_err(|error| tg::error!(!error, "failed to serialize the process"))?;
			let message = tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Process(
				tg::sync::PutItemProcessMessage {
					id: item.id.clone(),
					bytes: bytes.into(),
					metadata: output.metadata,
				},
			));
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|error| tg::error!(!error, "failed to send the put message"))?;
			let stored = tangram_index::process::Stored::default();
			state
				.graph
				.lock()
				.unwrap()
				.update_process_remote(&item.id, None, Some(&stored));

			// Enqueue the children.
			if state.arg.recursive && item.eager {
				let children = output
					.data
					.children
					.as_ref()
					.ok_or_else(|| tg::error!("expected the children to be set"))?;
				let items = children
					.iter()
					.map(|child| crate::sync::queue::ProcessItem {
						eager: item.eager,
						id: child.process.item.clone(),
						parent: Some(item.id.clone()),
						token: None,
					});
				state.queue.enqueue_processes(items);
			}

			// Enqueue the command.
			if item.eager && state.arg.commands {
				let item = crate::sync::queue::ObjectItem {
					eager: item.eager,
					id: output.data.command.clone().into(),
					kind: Some(crate::sync::queue::ObjectKind::Command),
					parent: Some(tg::Either::Right(item.id.clone())),
					token: None,
				};
				state.queue.enqueue_object(item);
			}

			// Enqueue the error.
			if item.eager
				&& state.arg.errors
				&& let Some(error) = &output.data.error
			{
				match error {
					tg::Either::Left(data) => {
						let mut children = BTreeSet::new();
						data.children(&mut children);
						let items =
							children
								.into_iter()
								.map(|child| crate::sync::queue::ObjectItem {
									eager: item.eager,
									id: child,
									kind: Some(crate::sync::queue::ObjectKind::Error),
									parent: Some(tg::Either::Right(item.id.clone())),
									token: None,
								});
						state.queue.enqueue_objects(items);
					},
					tg::Either::Right(id) => {
						let item = crate::sync::queue::ObjectItem {
							eager: item.eager,
							id: id.clone().map_right(|error| error.id).into_inner().into(),
							kind: Some(crate::sync::queue::ObjectKind::Error),
							parent: Some(tg::Either::Right(item.id.clone())),
							token: None,
						};
						state.queue.enqueue_object(item);
					},
				}
			}

			// Enqueue the log.
			if item.eager
				&& state.arg.logs
				&& let Some(log) = output.data.log.clone()
			{
				let item = crate::sync::queue::ObjectItem {
					eager: item.eager,
					id: log.map_right(|log| log.id).into_inner().into(),
					kind: Some(crate::sync::queue::ObjectKind::Log),
					parent: Some(tg::Either::Right(item.id.clone())),
					token: None,
				};
				state.queue.enqueue_object(item);
			}

			// Enqueue the outputs.
			if item.eager
				&& state.arg.outputs
				&& let Some(output) = &output.data.output
			{
				let mut children = BTreeSet::new();
				output.children(&mut children);
				let items = children
					.into_iter()
					.map(|child| crate::sync::queue::ObjectItem {
						eager: item.eager,
						id: child,
						kind: Some(crate::sync::queue::ObjectKind::Output),
						parent: Some(tg::Either::Right(item.id.clone())),
						token: None,
					});
				state.queue.enqueue_objects(items);
			}
		}

		if state.graph.lock().unwrap().end_remote(&state.arg) {
			state.queue.close();
		}

		Ok(())
	}
}
