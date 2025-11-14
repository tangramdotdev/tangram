use {
	crate::{Server, database::Database, store::Store, sync::get::State},
	futures::{StreamExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_store::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

impl Server {
	pub(super) async fn sync_get_store_task(
		&self,
		state: &State,
		process_receiver: tokio::sync::mpsc::Receiver<tg::sync::ProcessPutMessage>,
		object_receiver: tokio::sync::mpsc::Receiver<tg::sync::ObjectPutMessage>,
	) -> tg::Result<()> {
		let processes_future =
			async { self.sync_get_store_processes(state, process_receiver).await };
		let objects_future = async { self.sync_get_store_objects(state, object_receiver).await };
		future::try_join(processes_future, objects_future).await?;
		Ok(())
	}

	async fn sync_get_store_processes(
		&self,
		state: &State,
		process_receiver: tokio::sync::mpsc::Receiver<tg::sync::ProcessPutMessage>,
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
			state.graph.lock().unwrap().set_process_stored(&message.id);
			state.progress.increment_processes();
		}
		Ok(())
	}

	async fn sync_get_store_objects(
		&self,
		state: &State,
		object_receiver: tokio::sync::mpsc::Receiver<tg::sync::ObjectPutMessage>,
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
		struct State_ {
			message: Option<tg::sync::ObjectPutMessage>,
			object_receiver: tokio::sync::mpsc::Receiver<tg::sync::ObjectPutMessage>,
		}
		let state_ = State_ {
			message: None,
			object_receiver,
		};
		let stream = stream::unfold(state_, |mut state| async {
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
			.try_for_each_concurrent(concurrency, move |batch| async {
				self.sync_get_store_objects_inner(state, batch).await
			})
			.await?;

		Ok(())
	}

	async fn sync_get_store_objects_inner(
		&self,
		state: &State,
		messages: Vec<tg::sync::ObjectPutMessage>,
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
		let mut graph = state.graph.lock().unwrap();
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
		state.progress.increment_objects(objects);
		state.progress.increment_bytes(bytes);

		Ok(())
	}
}
