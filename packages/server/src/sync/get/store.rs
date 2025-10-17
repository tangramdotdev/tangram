use {
	super::{Graph, Progress},
	crate::{Server, database::Database, store::Store},
	futures::{StreamExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::{
		pin::pin,
		sync::{Arc, Mutex},
	},
	tangram_client as tg,
	tangram_store::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::task::AbortOnDropHandle,
};

impl Server {
	pub(super) async fn sync_get_store(
		&self,
		graph: Arc<Mutex<Graph>>,
		process_receiver: tokio::sync::mpsc::Receiver<tg::sync::ProcessPutMessage>,
		object_receiver: tokio::sync::mpsc::Receiver<tg::sync::ObjectPutMessage>,
		progress: Arc<Progress>,
	) -> tg::Result<()> {
		let process_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let graph = graph.clone();
			let progress = progress.clone();
			async move {
				server
					.sync_get_process_store(graph, process_receiver, &progress)
					.await
			}
		}));
		let object_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let graph = graph.clone();
			let progress = progress.clone();
			async move {
				server
					.sync_get_object_store(graph, object_receiver, &progress)
					.await
			}
		}));
		let (process_result, object_result) =
			future::try_join(process_task, object_task).await.unwrap();
		process_result.and(object_result)?;
		Ok(())
	}

	async fn sync_get_process_store(
		&self,
		graph: Arc<Mutex<Graph>>,
		process_receiver: tokio::sync::mpsc::Receiver<tg::sync::ProcessPutMessage>,
		progress: &Progress,
	) -> tg::Result<()> {
		let stream = ReceiverStream::new(process_receiver);
		let mut stream = pin!(stream);
		while let Some(message) = stream.next().await {
			let id = &message.id;
			let data = serde_json::from_slice(&message.bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the process data"))?;
			let arg = tg::process::put::Arg { data };
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
			graph.lock().unwrap().set_process_stored(&message.id);
			progress.increment_processes();
		}
		Ok(())
	}

	async fn sync_get_object_store(
		&self,
		graph: Arc<Mutex<Graph>>,
		object_receiver: tokio::sync::mpsc::Receiver<tg::sync::ObjectPutMessage>,
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
			message: Option<tg::sync::ObjectPutMessage>,
			object_receiver: tokio::sync::mpsc::Receiver<tg::sync::ObjectPutMessage>,
		}
		let state = State {
			message: None,
			object_receiver,
		};
		let stream = stream::unfold(state, |mut state| async {
			let mut batch_bytes = state
				.message
				.as_ref()
				.map(|message| message.bytes.len().to_u64().unwrap())
				.unwrap_or_default();
			let mut batch = state
				.message
				.take()
				.map(|message| vec![message])
				.unwrap_or_default();
			while let Some(message) = state.object_receiver.recv().await {
				let size = message.bytes.len().to_u64().unwrap();
				if !batch.is_empty()
					&& (batch.len() + 1 >= max_objects_per_batch
						|| batch_bytes + size >= max_bytes_per_batch)
				{
					state.message.replace(message);
					return Some((batch, state));
				}
				batch_bytes += 100 + size;
				batch.push(message);
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
				async move {
					self.sync_get_object_store_inner(graph, batch, progress)
						.await
				}
			})
			.await?;

		Ok(())
	}

	async fn sync_get_object_store_inner(
		&self,
		graph: Arc<Mutex<Graph>>,
		messages: Vec<tg::sync::ObjectPutMessage>,
		progress: &Arc<Progress>,
	) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Store the objects.
		let args = messages
			.iter()
			.map(|message| {
				Ok(crate::store::PutArg {
					id: message.id.clone(),
					bytes: Some(message.bytes.clone()),
					cache_reference: None,
					touched_at,
				})
			})
			.collect::<tg::Result<_>>()?;
		self.store
			.put_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to put objects"))?;

		// Mark the nodes as stored.
		let mut graph = graph.lock().unwrap();
		for message in &messages {
			graph.set_object_stored(&message.id);
		}
		drop(graph);

		// Update the progress.
		let objects = messages.len().to_u64().unwrap();
		let bytes = messages
			.iter()
			.map(|message| message.bytes.len().to_u64().unwrap())
			.sum();
		progress.increment_objects(objects);
		progress.increment_bytes(bytes);

		Ok(())
	}
}
