use super::{Graph, Progress};
use crate::{Server, database::Database, store::Store};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use num::ToPrimitive as _;
use std::{
	pin::pin,
	sync::{Arc, Mutex},
};
use tangram_client as tg;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

impl Server {
	pub(super) async fn import_store_task(
		&self,
		graph: Arc<Mutex<Graph>>,
		process_receiver: tokio::sync::mpsc::Receiver<tg::export::ProcessItem>,
		object_receiver: tokio::sync::mpsc::Receiver<tg::export::ObjectItem>,
		progress: Arc<Progress>,
	) -> tg::Result<()> {
		let process_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let graph = graph.clone();
			let progress = progress.clone();
			async move {
				server
					.import_process_store_task(graph, process_receiver, &progress)
					.await
			}
		}));
		let object_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let graph = graph.clone();
			let progress = progress.clone();
			async move {
				server
					.import_object_store_task(graph, object_receiver, &progress)
					.await
			}
		}));
		let result = futures::try_join!(process_task, object_task);
		match result {
			Ok((Ok(()), Ok(()))) => Ok(()),
			Ok((_, Err(error)) | (Err(error), _)) => Err(error),
			Err(error) => Err(tg::error!(!error, "the task panicked")),
		}
	}

	async fn import_process_store_task(
		&self,
		graph: Arc<Mutex<Graph>>,
		process_receiver: tokio::sync::mpsc::Receiver<tg::export::ProcessItem>,
		progress: &Progress,
	) -> tg::Result<()> {
		let stream = ReceiverStream::new(process_receiver);
		let mut stream = pin!(stream);
		while let Some(item) = stream.next().await {
			let id = &item.id;
			let arg = tg::process::put::Arg { data: item.data };
			let now = time::OffsetDateTime::now_utc().unix_timestamp();
			match &self.database {
				#[cfg(feature = "postgres")]
				Database::Postgres(database) => {
					Self::put_process_postgres(id, &arg, database, now)
						.await
						.map_err(|source| tg::error!(!source, "failed to put the process"))?;
				},
				Database::Sqlite(database) => {
					Self::put_process_sqlite(id, &arg, database, now)
						.await
						.map_err(|source| tg::error!(!source, "failed to put the process"))?;
				},
			}
			graph.lock().unwrap().set_process_stored(&item.id);
			progress.increment_processes();
		}
		Ok(())
	}

	async fn import_object_store_task(
		&self,
		graph: Arc<Mutex<Graph>>,
		object_receiver: tokio::sync::mpsc::Receiver<tg::export::ObjectItem>,
		progress: &Arc<Progress>,
	) -> tg::Result<()> {
		// Choose the batch parameters.
		let (concurrency, max_objects_per_batch, max_bytes_per_batch) = match &self.store {
			#[cfg(feature = "foundationdb")]
			Store::Fdb(_) => (64, 1_000, 1_000_000),
			Store::Lmdb(_) => (1, 1_000, 1_000_000),
			Store::Memory(_) => (1, 1, u64::MAX),
			Store::S3(_) => (256, 1, u64::MAX),
			#[cfg(feature = "scylla")]
			Store::Scylla(_) => (64, 1_000, 65_536),
		};

		// Create a stream of batches.
		struct State {
			item: Option<tg::export::ObjectItem>,
			object_receiver: tokio::sync::mpsc::Receiver<tg::export::ObjectItem>,
		}
		let state = State {
			item: None,
			object_receiver,
		};
		let stream = stream::unfold(state, |mut state| async {
			let mut batch_bytes = state
				.item
				.as_ref()
				.map(|item| item.size)
				.unwrap_or_default();
			let mut batch = state.item.take().map(|item| vec![item]).unwrap_or_default();
			while let Some(item) = state.object_receiver.recv().await {
				let size = item.size;
				if !batch.is_empty()
					&& (batch.len() + 1 >= max_objects_per_batch
						|| batch_bytes + size >= max_bytes_per_batch)
				{
					state.item.replace(item);
					return Some((batch, state));
				}
				batch_bytes += 100 + size;
				batch.push(item);
			}
			if batch.is_empty() {
				return None;
			}
			Some((batch, state))
		});

		// Write the batches.
		stream
			.map(Ok)
			.try_for_each_concurrent(concurrency, move |batch| {
				let graph = graph.clone();
				async move { self.import_objects_task_inner(graph, batch, progress).await }
			})
			.await?;

		Ok(())
	}

	async fn import_objects_task_inner(
		&self,
		graph: Arc<Mutex<Graph>>,
		items: Vec<tg::export::ObjectItem>,
		progress: &Arc<Progress>,
	) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Store the items.
		let args = items
			.clone()
			.into_iter()
			.map(|item| {
				let bytes = item
					.data
					.serialize()
					.map_err(|source| tg::error!(!source, "failed to serialize object data"))?;
				Ok(crate::store::PutArg {
					id: item.id,
					bytes: Some(bytes),
					cache_reference: None,
					touched_at,
				})
			})
			.collect::<tg::Result<_>>()?;
		self.store.put_batch(args).await?;

		// Mark the nodes as stored.
		let mut graph = graph.lock().unwrap();
		for item in &items {
			graph.set_object_stored(&item.id);
		}
		drop(graph);

		// Update the progress.
		let objects = items.len().to_u64().unwrap();
		let bytes = items.iter().map(|item| item.size).sum();
		progress.increment_objects(objects);
		progress.increment_bytes(bytes);

		Ok(())
	}
}
