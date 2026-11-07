use {
	crate::{Component, progress::Progress, put::State},
	futures::{StreamExt as _, TryStreamExt as _},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_server_core::Session,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ObjectNode {
	pub id: tg::object::Id,
}

pub struct ProcessNode {
	pub id: tg::process::Id,
}

impl Component {
	#[tracing::instrument(err, level = "trace", name = "index", ret, skip_all)]
	pub(super) async fn sync_put_index(
		&self,
		session: &Session,
		state: Arc<State>,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectNode>,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessNode>,
	) -> tg::Result<()> {
		// Create the objects future.
		let object_batch_size = session.server.config.sync.put.index.object_batch_size;
		let object_batch_timeout = session.server.config.sync.put.index.object_batch_timeout;
		let object_concurrency = session.server.config.sync.put.index.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(object_receiver),
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |nodes| {
			let component = self.clone();
			let session = session.clone();
			let state = state.clone();
			async move {
				component
					.sync_put_index_object_batch(&session, &state, nodes)
					.await
			}
		});

		// Create the processes future.
		let process_batch_size = session.server.config.sync.put.index.process_batch_size;
		let process_batch_timeout = session.server.config.sync.put.index.process_batch_timeout;
		let process_concurrency = session.server.config.sync.put.index.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(process_receiver),
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |nodes| {
			let component = self.clone();
			let session = session.clone();
			let state = state.clone();
			async move {
				component
					.sync_put_index_process_batch(&session, &state, nodes)
					.await
			}
		});

		// Join the objects and processes futures.
		futures::try_join!(objects_future, processes_future)?;

		Ok(())
	}

	pub(super) async fn sync_put_index_object_batch(
		&self,
		session: &Session,
		state: &State,
		nodes: Vec<ObjectNode>,
	) -> tg::Result<()> {
		let ids = nodes.into_iter().map(|node| node.id).collect::<Vec<_>>();
		let outputs = session
			.server
			.index
			.try_get_objects(&ids)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the object storage and metadata"))?;
		for output in outputs {
			let Some(object) = output else {
				continue;
			};
			let metadata = object.metadata;
			let processes = 0;
			let objects = metadata.subtree.count.unwrap_or_default();
			let bytes = metadata.subtree.size.unwrap_or_default();
			state.progress.increment_skipped(processes, objects, bytes);
		}

		state.queue.close_if_end();

		Ok(())
	}

	pub(super) async fn sync_put_index_process_batch(
		&self,
		session: &Session,
		state: &State,
		nodes: Vec<ProcessNode>,
	) -> tg::Result<()> {
		let ids = nodes.into_iter().map(|node| node.id).collect::<Vec<_>>();
		let outputs = session
			.server
			.index
			.try_get_processes(&ids)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to get the process storage and metadata")
			})?;
		for output in outputs {
			let Some(process) = output else {
				continue;
			};
			let (processes, objects, bytes) =
				Progress::process_amounts(&state.arg, &process.metadata);
			state.progress.increment_skipped(processes, objects, bytes);
		}

		state.queue.close_if_end();

		Ok(())
	}
}
