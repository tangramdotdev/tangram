use {
	crate::{Server, database::Database, store::Store, sync::get::State},
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_store::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ObjectItem {
	pub id: tg::object::Id,
	pub bytes: Bytes,
}

pub struct ProcessItem {
	pub id: tg::process::Id,
	pub bytes: Bytes,
}

impl Server {
	pub(super) async fn sync_get_store(
		&self,
		state: &State,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
	) -> tg::Result<()> {
		let objects_future = async { self.sync_get_store_objects(state, object_receiver).await };
		let processes_future =
			async { self.sync_get_store_processes(state, process_receiver).await };
		future::try_join(objects_future, processes_future).await?;
		Ok(())
	}

	async fn sync_get_store_objects(
		&self,
		state: &State,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
	) -> tg::Result<()> {
		// Choose the batch parameters.
		let store_config = match &self.store {
			Store::Lmdb(_) => &self.config.sync.get.store.lmdb,
			Store::Memory(_) => &self.config.sync.get.store.memory,
			#[cfg(feature = "scylla")]
			Store::Scylla(_) => &self.config.sync.get.store.scylla,
		};
		let concurrency = store_config.object_concurrency;
		let max_objects_per_batch = store_config.object_max_batch;
		let max_bytes_per_batch = store_config.object_max_bytes;

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

			let (node_solvable, node_solved) = match &data {
				tg::object::Data::File(file) => match file {
					tg::file::Data::Reference(_) => (false, true),
					tg::file::Data::Node(node) => (node.solvable(), node.solved()),
				},
				tg::object::Data::Graph(graph) => {
					graph
						.nodes
						.iter()
						.fold((false, true), |(solvable, solved), node| {
							if let tg::graph::data::Node::File(file) = node {
								(solvable || file.solvable(), solved && file.solved())
							} else {
								(solvable, solved)
							}
						})
				},
				_ => (false, true),
			};

			let metadata = tg::object::Metadata {
				node: tg::object::metadata::Node {
					size,
					solvable: node_solvable,
					solved: node_solved,
				},
				..Default::default()
			};
			graph.update_object(
				&item.id,
				Some(&data),
				None,
				Some(metadata),
				Some(true),
				None,
			);
		}
		drop(graph);

		// Update the progress.
		let objects = items.len().to_u64().unwrap();
		let bytes = items
			.iter()
			.map(|item| item.bytes.len().to_u64().unwrap())
			.sum();
		state.progress.increment(0, objects, bytes);

		let end = state.graph.lock().unwrap().end(&state.arg);
		if end {
			state.queue.close();
		}

		Ok(())
	}

	async fn sync_get_store_processes(
		&self,
		state: &State,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
	) -> tg::Result<()> {
		let process_batch_size = self.config.sync.get.store.process_batch_size;
		let process_batch_timeout = self.config.sync.get.store.process_batch_timeout;
		let process_concurrency = self.config.sync.get.store.process_concurrency;
		tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(process_receiver),
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |items| async move {
			self.sync_get_store_processes_inner(state, items).await
		})
		.await
	}

	async fn sync_get_store_processes_inner(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
	) -> tg::Result<()> {
		// Deserialize all processes once.
		let batch: Vec<(tg::process::Id, tg::process::Data)> = items
			.iter()
			.map(|item| {
				let data = serde_json::from_slice(&item.bytes).map_err(|source| {
					tg::error!(!source, "failed to deserialize the process data")
				})?;
				Ok((item.id.clone(), data))
			})
			.collect::<tg::Result<_>>()?;

		// Put the processes to the database.
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let batch_refs: Vec<_> = batch.iter().map(|(id, data)| (id, data)).collect();
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				Self::put_process_batch_postgres(&batch_refs, database, now)
					.await
					.map_err(|source| tg::error!(!source, "failed to put the processes"))?;
			},
			Database::Sqlite(database) => {
				Self::put_process_batch_sqlite(&batch_refs, database, now)
					.await
					.map_err(|source| tg::error!(!source, "failed to put the processes"))?;
			},
		}

		// Update the graph for all processes.
		let mut graph = state.graph.lock().unwrap();
		for (id, data) in &batch {
			graph.update_process(id, Some(data), None, None, Some(true), None);
		}
		drop(graph);

		// Update the progress.
		let processes = items.len().to_u64().unwrap();
		state.progress.increment(processes, 0, 0);

		let end = state.graph.lock().unwrap().end(&state.arg);
		if end {
			state.queue.close();
		}

		Ok(())
	}
}
