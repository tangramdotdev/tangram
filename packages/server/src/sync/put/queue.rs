use {
	crate::{
		Server,
		sync::{
			put::State,
			queue::{ObjectItem, ProcessItem},
		},
	},
	futures::{StreamExt as _, TryStreamExt as _},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_either::Either,
};

const PROCESS_BATCH_SIZE: usize = 16;
const PROCESS_CONCURRENCY: usize = 8;
const OBJECT_BATCH_SIZE: usize = 16;
const OBJECT_CONCURRENCY: usize = 8;

impl Server {
	#[expect(clippy::too_many_arguments)]
	#[tracing::instrument(err, level = "debug", name = "queue", ret, skip_all)]
	pub(super) async fn sync_put_queue_task(
		&self,
		state: Arc<State>,
		queue_object_receiver: async_channel::Receiver<ObjectItem>,
		queue_process_receiver: async_channel::Receiver<ProcessItem>,
		index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectItem>,
		index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessItem>,
		store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectItem>,
		store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessItem>,
	) -> tg::Result<()> {
		// Create the processes future.
		let processes_future = queue_process_receiver
			.ready_chunks(PROCESS_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(PROCESS_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				let index_process_sender = index_process_sender.clone();
				let store_process_sender = store_process_sender.clone();
				async move {
					server
						.sync_put_queue_process_batch(
							&state,
							items,
							index_process_sender,
							store_process_sender,
						)
						.await
				}
			});

		// Create the objects future.
		let objects_future = queue_object_receiver
			.ready_chunks(OBJECT_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(OBJECT_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				let index_object_sender = index_object_sender.clone();
				let store_object_sender = store_object_sender.clone();
				async move {
					server
						.sync_put_queue_object_batch(
							&state,
							items,
							index_object_sender,
							store_object_sender,
						)
						.await
				}
			});

		// Join the processes and objects futures.
		futures::try_join!(processes_future, objects_future)?;

		// Send the put end message.
		state
			.sender
			.send(Ok(tg::sync::PutMessage::End))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the put end message"))?;

		Ok(())
	}

	async fn sync_put_queue_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
		index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessItem>,
		store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessItem>,
	) -> tg::Result<()> {
		for item in items {
			let parent = item.parent.clone().map(super::graph::Id::Process);
			let (inserted, complete) = state
				.graph
				.lock()
				.unwrap()
				.update_process(&item.id, parent, None);
			let complete = complete.is_some_and(|complete| {
				if state.arg.recursive {
					complete.children
						&& (!state.arg.commands || complete.children_commands)
						&& (!state.arg.outputs || complete.children_outputs)
				} else {
					(!state.arg.commands || complete.command)
						&& (!state.arg.outputs || complete.output)
				}
			});
			if !inserted || complete {
				let item = super::index::ProcessItem { id: item.id };
				index_process_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the process to the index task"))?;
			} else {
				let item = super::store::ProcessItem {
					id: item.id,
					eager: item.eager,
				};
				store_process_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the process to the store task"))?;
			}
		}
		Ok(())
	}

	async fn sync_put_queue_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
		index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectItem>,
		store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectItem>,
	) -> tg::Result<()> {
		for item in items {
			let parent = item.parent.clone().map(|either| match either {
				Either::Left(id) => super::graph::Id::Process(id),
				Either::Right(id) => super::graph::Id::Object(id),
			});
			let (inserted, complete) = state
				.graph
				.lock()
				.unwrap()
				.update_object(&item.id, parent, item.kind, None);
			let complete = complete.is_some_and(|complete| complete);
			if !inserted || complete {
				let item = super::index::ObjectItem { id: item.id };
				index_object_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the object to the index task"))?;
			} else {
				let item = super::store::ObjectItem {
					id: item.id,
					kind: item.kind,
					eager: item.eager,
				};
				store_object_sender
					.send(item)
					.await
					.map_err(|_| tg::error!("failed to send the object to the store task"))?;
			}
		}
		Ok(())
	}
}
