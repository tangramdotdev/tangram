use {
	crate::{Server, sync::put::State},
	futures::{StreamExt as _, TryStreamExt as _},
	std::{collections::BTreeSet, sync::Arc},
	tangram_client::prelude::*,
	tangram_either::Either,
	tokio_stream::wrappers::ReceiverStream,
};

const PROCESS_BATCH_SIZE: usize = 16;
const PROCESS_CONCURRENCY: usize = 8;
const OBJECT_BATCH_SIZE: usize = 16;
const OBJECT_CONCURRENCY: usize = 8;

pub struct ProcessItem {
	pub id: tg::process::Id,
	pub eager: bool,
}

pub struct ObjectItem {
	pub id: tg::object::Id,
	pub kind: Option<crate::sync::queue::ObjectKind>,
	pub eager: bool,
}

impl Server {
	#[tracing::instrument(err, level = "debug", name = "store", ret, skip_all)]
	pub(super) async fn sync_put_store(
		&self,
		state: Arc<State>,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
	) -> tg::Result<()> {
		// Create the processes future.
		let processes_future = ReceiverStream::new(process_receiver)
			.ready_chunks(PROCESS_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(PROCESS_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move { server.sync_put_store_process_batch(&state, items).await }
			});

		// Create the objects future.
		let objects_future = ReceiverStream::new(object_receiver)
			.ready_chunks(OBJECT_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(OBJECT_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move { server.sync_put_store_object_batch(&state, items).await }
			});

		// Join the processes and objects futures.
		futures::try_join!(processes_future, objects_future)?;

		Ok(())
	}

	pub(super) async fn sync_put_store_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
	) -> tg::Result<()> {
		// Get the processes.
		let n = items.len();
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();
		let outputs = self
			.try_get_process_batch(&ids)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the processes"))?;

		// Handle the processes.
		for (item, output) in std::iter::zip(items, outputs) {
			let Some(output) = output else {
				let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage::Process(
					tg::sync::PutMissingProcessMessage {
						id: item.id.clone(),
					},
				));
				state.sender.send(Ok(message)).await.ok();
				continue;
			};

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
					parent: Some(Either::Left(item.id.clone())),
					id: output.data.command.clone().into(),
					kind: Some(crate::sync::queue::ObjectKind::Command),
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
						parent: Some(Either::Left(item.id.clone())),
						id: child,
						kind: Some(crate::sync::queue::ObjectKind::Output),
						eager: item.eager,
					});
				state.queue.enqueue_objects(items);
			}
		}

		// Ack the items.
		state.queue.decrement(n);

		Ok(())
	}

	pub(super) async fn sync_put_store_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		// Get the objects.
		let n = items.len();
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();
		let outputs = self
			.try_get_object_batch(&ids)
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

			// Enqueue the children.
			if item.eager {
				let bytes = output.bytes;
				let data = tg::object::Data::deserialize(item.id.kind(), bytes.clone())?;
				let mut children = BTreeSet::new();
				data.children(&mut children);
				let items = children
					.into_iter()
					.map(|child| crate::sync::queue::ObjectItem {
						parent: Some(Either::Right(item.id.clone())),
						id: child,
						kind: item.kind,
						eager: item.eager,
					});
				state.queue.enqueue_objects(items);
			}
		}

		// Ack the items.
		state.queue.decrement(n);

		Ok(())
	}
}
