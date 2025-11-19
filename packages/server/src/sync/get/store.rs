use {
	crate::{Server, database::Database, store::Store, sync::get::State},
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_store::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ProcessItem {
	pub id: tg::process::Id,
	pub bytes: Bytes,
}

pub struct ObjectItem {
	pub id: tg::object::Id,
	pub bytes: Bytes,
}

impl Server {
	pub(super) async fn sync_get_store_task(
		&self,
		state: &State,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
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
		process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
	) -> tg::Result<()> {
		let stream = ReceiverStream::new(process_receiver);
		let mut stream = pin!(stream);
		while let Some(item) = stream.next().await {
			let id = &item.id;
			let data = serde_json::from_slice(&item.bytes)
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
			let data = serde_json::from_slice(&item.bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the process"))?;
			state.graph.lock().unwrap().update_process(
				&item.id,
				Some(&data),
				None,
				None,
				Some(true),
			);
			state.progress.increment_processes();
		}
		Ok(())
	}

	async fn sync_get_store_objects(
		&self,
		state: &State,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
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
			item: Option<ObjectItem>,
			object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
		}
		let state_ = State_ {
			item: None,
			object_receiver,
		};
		let stream = stream::unfold(state_, |mut state| async {
			let mut batch_bytes = state
				.item
				.as_ref()
				.map(|item| item.bytes.len().to_u64().unwrap())
				.unwrap_or_default();
			let mut batch = state.item.take().map(|item| vec![item]).unwrap_or_default();
			while let Some(item) = state.object_receiver.recv().await {
				let size = item.bytes.len().to_u64().unwrap();
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

		// Store the batches.
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
		items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Store the objects.
		let args = items
			.iter()
			.map(|item| {
				Ok(crate::store::PutArg {
					id: item.id.clone(),
					bytes: Some(item.bytes.clone()),
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
		for item in &items {
			let data = tg::object::Data::deserialize(item.id.kind(), item.bytes.as_ref())?;
			let size = item.bytes.len().to_u64().unwrap();
			graph.update_object(&item.id, Some(&data), None, None, Some(size), Some(true));
		}
		drop(graph);

		// Update the progress.
		let objects = items.len().to_u64().unwrap();
		let bytes = items
			.iter()
			.map(|item| item.bytes.len().to_u64().unwrap())
			.sum();
		state.progress.increment_objects(objects);
		state.progress.increment_bytes(bytes);

		Ok(())
	}
}
