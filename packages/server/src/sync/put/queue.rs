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
};

impl Server {
	#[expect(clippy::too_many_arguments)]
	#[tracing::instrument(err, level = "debug", name = "queue", ret, skip_all)]
	pub(super) async fn sync_put_queue(
		&self,
		state: Arc<State>,
		queue_object_receiver: async_channel::Receiver<ObjectItem>,
		queue_process_receiver: async_channel::Receiver<ProcessItem>,
		index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectItem>,
		index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessItem>,
		store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectItem>,
		store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessItem>,
	) -> tg::Result<()> {
		// Create the objects future.
		let object_batch_size = self.config.sync.put.queue.object_batch_size;
		let object_batch_timeout = self.config.sync.put.queue.object_batch_timeout;
		let object_concurrency = self.config.sync.put.queue.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			queue_object_receiver,
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |items| {
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

		// Create the processes future.
		let process_batch_size = self.config.sync.put.queue.process_batch_size;
		let process_batch_timeout = self.config.sync.put.queue.process_batch_timeout;
		let process_concurrency = self.config.sync.put.queue.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			queue_process_receiver,
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |items| {
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

		// Join the objects and processes futures.
		futures::try_join!(objects_future, processes_future)?;

		// Send the put end message.
		state
			.sender
			.send(Ok(tg::sync::PutMessage::End))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the put end message"))?;

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
				tg::Either::Left(id) => super::graph::Id::Object(id),
				tg::Either::Right(id) => super::graph::Id::Process(id),
			});
			let (inserted, stored) = state
				.graph
				.lock()
				.unwrap()
				.update_object(&item.id, parent, item.kind, None);
			let stored = stored.is_some_and(|stored| stored.subtree);
			if !inserted || stored {
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

	async fn sync_put_queue_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
		index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessItem>,
		store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessItem>,
	) -> tg::Result<()> {
		for item in items {
			let parent = item.parent.clone().map(super::graph::Id::Process);
			let (inserted, stored) = state
				.graph
				.lock()
				.unwrap()
				.update_process(&item.id, parent, None);
			let stored = stored.is_some_and(|stored| {
				if state.arg.recursive {
					stored.subtree
						&& (!state.arg.commands || stored.subtree_command)
						&& (!state.arg.errors || stored.subtree_error)
						&& (!state.arg.logs || stored.subtree_log)
						&& (!state.arg.outputs || stored.subtree_output)
				} else {
					(!state.arg.commands || stored.node_command)
						&& (!state.arg.errors || stored.node_error)
						&& (!state.arg.logs || stored.node_log)
						&& (!state.arg.outputs || stored.node_output)
				}
			});
			if !inserted || stored {
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
}
