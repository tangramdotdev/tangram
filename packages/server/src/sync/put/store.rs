use {
	crate::{Server, sync::put::State},
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _},
	std::{collections::BTreeSet, sync::Arc},
	tangram_client::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ObjectItem {
	pub id: tg::object::Id,
	pub kind: Option<crate::sync::queue::ObjectKind>,
	pub eager: bool,
}

pub struct ProcessItem {
	pub id: tg::process::Id,
	pub eager: bool,
}

impl Server {
	#[tracing::instrument(err, level = "debug", name = "store", ret, skip_all)]
	pub(super) async fn sync_put_store(
		&self,
		state: Arc<State>,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
	) -> tg::Result<()> {
		// Create the objects future.
		let object_batch_size = self.config.sync.put.store.object_batch_size;
		let object_batch_timeout = self.config.sync.put.store.object_batch_timeout;
		let object_concurrency = self.config.sync.put.store.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(object_receiver),
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |items| {
			let server = self.clone();
			let state = state.clone();
			async move { server.sync_put_store_object_batch(&state, items).await }
		});

		// Create the processes future.
		let process_batch_size = self.config.sync.put.store.process_batch_size;
		let process_batch_timeout = self.config.sync.put.store.process_batch_timeout;
		let process_concurrency = self.config.sync.put.store.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(process_receiver),
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |items| {
			let server = self.clone();
			let state = state.clone();
			async move { server.sync_put_store_process_batch(&state, items).await }
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
			.try_get_object_batch_local(&ids)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects"))?;

		// Handle the objects.
		for (item, output) in std::iter::zip(items, outputs) {
			// If the object is missing, then send a missing message.
			let Some(output) = output else {
				let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage::Object(
					tg::sync::PutMissingObjectMessage {
						id: item.id.clone(),
					},
				));
				state.sender.send(Ok(message)).await.ok();
				continue;
			};

			// Send the object.
			let message = tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Object(
				tg::sync::PutItemObjectMessage {
					id: item.id.clone(),
					bytes: output.bytes.clone(),
				},
			));
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|source| tg::error!(!source, "failed to send the put message"))?;
			state.graph.lock().unwrap().update_object_remote(
				&item.id,
				None,
				item.kind,
				Some(&crate::object::stored::Output { subtree: true }),
			);

			// Enqueue the children.
			if item.eager {
				let bytes = output.bytes;
				let data = tg::object::Data::deserialize(item.id.kind(), bytes.clone())
					.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;
				let mut children = BTreeSet::new();
				data.children(&mut children);
				let items = children
					.into_iter()
					.map(|child| crate::sync::queue::ObjectItem {
						parent: Some(tg::Either::Left(item.id.clone())),
						id: child,
						kind: item.kind,
						eager: item.eager,
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
			.try_get_process_batch_local(&ids)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the processes"))?;

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
			if item.eager && state.arg.logs && output.data.log.is_none() {
				self.compact_process_log(&item.id).boxed().await.map_err(
					|source| tg::error!(!source, process = %item.id, "failed to compact the log"),
				)?;
				output = self.try_get_process_local(&item.id).await?.ok_or_else(
					|| tg::error!(process = %item.id, "failed to get the process after compaction"),
				)?;
			}

			// Send the process.
			let bytes = serde_json::to_string(&output.data)
				.map_err(|source| tg::error!(!source, "failed to serialize the process"))?;
			let message = tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Process(
				tg::sync::PutItemProcessMessage {
					id: item.id.clone(),
					bytes: bytes.into(),
				},
			));
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|source| tg::error!(!source, "failed to send the put message"))?;
			let stored = crate::process::stored::Output::default();
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
						parent: Some(item.id.clone()),
						id: child.item.clone(),
						eager: item.eager,
					});
				state.queue.enqueue_processes(items);
			}

			// Enqueue the command.
			if item.eager && state.arg.commands {
				let item = crate::sync::queue::ObjectItem {
					parent: Some(tg::Either::Right(item.id.clone())),
					id: output.data.command.clone().into(),
					kind: Some(crate::sync::queue::ObjectKind::Command),
					eager: item.eager,
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
									parent: Some(tg::Either::Right(item.id.clone())),
									id: child,
									kind: Some(crate::sync::queue::ObjectKind::Error),
									eager: item.eager,
								});
						state.queue.enqueue_objects(items);
					},
					tg::Either::Right(id) => {
						let item = crate::sync::queue::ObjectItem {
							parent: Some(tg::Either::Right(item.id.clone())),
							id: id.clone().into(),
							kind: Some(crate::sync::queue::ObjectKind::Error),
							eager: item.eager,
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
					parent: Some(tg::Either::Right(item.id.clone())),
					id: log.into(),
					kind: Some(crate::sync::queue::ObjectKind::Log),
					eager: item.eager,
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
						parent: Some(tg::Either::Right(item.id.clone())),
						id: child,
						kind: Some(crate::sync::queue::ObjectKind::Output),
						eager: item.eager,
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
