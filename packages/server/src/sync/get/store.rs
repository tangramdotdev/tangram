use {
	crate::{
		Session,
		sync::{
			get::State,
			graph::{UpdateObjectLocalArg, UpdateProcessLocalArg},
		},
	},
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ObjectNode {
	pub bytes: Option<Bytes>,
	pub checkout_pointer: Option<tangram_store::object::checkout::Pointer>,
	pub id: tg::object::Id,
	pub length: Option<u64>,
	pub metadata: Option<tg::object::Metadata>,
	pub put: [u8; 16],
	pub storage: Option<tangram_index::object::Storage>,
	pub transferred_bytes: u64,
}

pub struct ProcessNode {
	pub id: tg::process::Id,
	pub bytes: Bytes,
	pub metadata: Option<tg::process::Metadata>,
}

impl Session {
	pub(super) async fn sync_get_store(
		&self,
		state: &State,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectNode>,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessNode>,
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
		object_receiver: tokio::sync::mpsc::Receiver<ObjectNode>,
	) -> tg::Result<()> {
		// Choose the batch parameters.
		let store_config = match &self.server.store {
			#[cfg(feature = "lmdb")]
			crate::store::Store::Lmdb(_) => &self.server.config.sync.get.store.lmdb,
			crate::store::Store::Memory(_) => &self.server.config.sync.get.store.memory,
			#[cfg(feature = "scylla")]
			crate::store::Store::Scylla(_) => &self.server.config.sync.get.store.scylla,
		};
		let concurrency = store_config.object_concurrency;
		let max_objects_per_batch = store_config.object_max_batch;
		let max_bytes_per_batch = store_config.object_max_bytes;

		// Create a stream of batches.
		struct State_ {
			node: Option<ObjectNode>,
			object_receiver: tokio::sync::mpsc::Receiver<ObjectNode>,
		}
		let state_ = State_ {
			node: None,
			object_receiver,
		};
		let stream = stream::unfold(state_, |mut state| async {
			let mut batch_bytes = state
				.node
				.as_ref()
				.and_then(|node| node.bytes.as_ref())
				.map(|bytes| bytes.len().to_u64().unwrap())
				.unwrap_or_default();
			let mut batch = state.node.take().map(|node| vec![node]).unwrap_or_default();
			while let Some(node) = state.object_receiver.recv().await {
				let size = node
					.bytes
					.as_ref()
					.map(|bytes| bytes.len().to_u64().unwrap())
					.unwrap_or_default();
				if !batch.is_empty()
					&& (batch.len() + 1 >= max_objects_per_batch
						|| batch_bytes + size >= max_bytes_per_batch)
				{
					state.node.replace(node);
					return Some((batch, state));
				}
				batch_bytes += 100 + size;
				batch.push(node);
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
		nodes: Vec<ObjectNode>,
	) -> tg::Result<()> {
		// Deserialize the objects and create the store args.
		let mut datas = Vec::with_capacity(nodes.len());
		let mut args = Vec::with_capacity(nodes.len());
		for node in &nodes {
			let data = node
				.bytes
				.as_ref()
				.map(|bytes| tg::object::Data::deserialize(node.id.kind(), bytes.as_ref()))
				.transpose()
				.map_err(|error| tg::error!(!error, "failed to deserialize the object"))?;
			let length = node.length.or_else(|| match &data {
				Some(tg::object::Data::Blob(blob)) => Some(blob.length()),
				_ => None,
			});
			args.push(crate::store::object::put::Arg {
				bytes: node.bytes.clone(),
				checkout_pointer: node.checkout_pointer.clone(),
				id: node.id.clone(),
				length,
				put: node.put,
			});
			datas.push(data);
		}

		// Store the objects.
		self.server
			.put_object_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to put objects"))?;

		// Update the graph.
		let mut graph = state.graph.lock().unwrap();
		for (node, data) in nodes.iter().zip(&datas) {
			// Get the metadata.
			let metadata = node.metadata.clone().unwrap_or_else(|| {
				let size = node.transferred_bytes;
				let (node_solvable, node_solved) = match data {
					Some(tg::object::Data::File(file)) => match file {
						tg::file::Data::Pointer(_) => (false, true),
						tg::file::Data::Node(node) => (node.solvable(), node.solved()),
					},
					Some(tg::object::Data::Graph(graph)) => {
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
				tg::object::Metadata {
					node: tg::object::metadata::Node {
						size,
						solvable: node_solvable,
						solved: node_solved,
					},
					..Default::default()
				}
			});

			// Update the graph.
			let arg = UpdateObjectLocalArg {
				data: data.as_ref(),
				id: &node.id,
				marked: Some(true),
				metadata: Some(metadata),
				permissions: None,
				put: Some(node.put),
				requested: None,
				storage: node.storage.clone(),
			};
			graph.update_object_local(arg);
		}
		drop(graph);

		// Update the progress.
		let objects = nodes
			.iter()
			.filter(|node| node.transferred_bytes > 0)
			.count()
			.to_u64()
			.unwrap();
		let bytes = nodes.iter().map(|node| node.transferred_bytes).sum();
		state.progress.increment_transferred(0, objects, bytes);

		let end = state.graph.lock().unwrap().end_local();
		if end {
			state.queue.close();
		}

		Ok(())
	}

	async fn sync_get_store_processes(
		&self,
		state: &State,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessNode>,
	) -> tg::Result<()> {
		let process_batch_size = self.server.config.sync.get.store.process_batch_size;
		let process_batch_timeout = self.server.config.sync.get.store.process_batch_timeout;
		let process_concurrency = self.server.config.sync.get.store.process_concurrency;
		tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(process_receiver),
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |nodes| async move {
			self.sync_get_store_processes_inner(state, nodes).await
		})
		.await
	}

	async fn sync_get_store_processes_inner(
		&self,
		state: &State,
		nodes: Vec<ProcessNode>,
	) -> tg::Result<()> {
		// Deserialize all processes.
		let count = nodes.len();
		let mut batch: Vec<(
			tg::process::Id,
			tg::process::Data,
			Option<tg::process::Metadata>,
		)> = nodes
			.into_iter()
			.map(|node| {
				let data = serde_json::from_slice(&node.bytes).map_err(|error| {
					tg::error!(!error, "failed to deserialize the process data")
				})?;
				Self::validate_process_data(&data)?;
				Ok((node.id, data, node.metadata))
			})
			.collect::<tg::Result<_>>()?;

		// Do not replace an existing compacted log and its metadata with an uncompacted copy.
		let ids = batch
			.iter()
			.map(|(id, _, _)| id.clone())
			.collect::<Vec<_>>();
		let existing = self
			.server
			.index
			.try_get_processes(&ids)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the existing processes"))?;
		for ((_, data, metadata), existing) in std::iter::zip(&mut batch, existing) {
			let Some(existing) = existing else {
				continue;
			};
			if data.log.is_some() {
				continue;
			}
			let Some(log) = existing.data.and_then(|data| data.log) else {
				continue;
			};
			data.log = Some(log);
			match metadata {
				Some(metadata) => metadata.merge(&existing.metadata),
				None => *metadata = Some(existing.metadata),
			}
		}

		// Write the processes to the index.
		let now = self.server.clock.unix_timestamp()?;
		let put_processes: Vec<_> = batch
			.iter()
			.map(|(id, data, metadata)| tangram_index::process::put::Arg {
				cached: false,
				children: data.children.clone(),
				command: data.command.node.clone().into(),
				data: Some(data.clone()),
				error: None,
				id: id.clone(),
				log: None,
				metadata: metadata.clone().unwrap_or_default(),
				options: tg::referent::Options::default(),
				output: None,
				parent: None,
				sandbox: Some(data.sandbox.clone()),
				storage: tangram_index::process::Storage::default(),
				time_to_touch: self.server.config.process.time_to_touch,
				touched_at: now,
			})
			.collect();
		self.server
			.index
			.batch(tangram_index::batch::Arg {
				items: put_processes
					.into_iter()
					.map(tangram_index::batch::Item::PutProcess)
					.collect(),
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to put the processes in the index"))?;

		// Update the graph.
		{
			let mut graph = state.graph.lock().unwrap();
			for (id, data, metadata) in &batch {
				let metadata = metadata.clone();
				let arg = UpdateProcessLocalArg {
					data: Some(data),
					id,
					marked: Some(true),
					metadata,
					permissions: None,
					requested: None,
					storage: None,
				};
				graph.update_process_local(arg);
			}
		}

		// Update the progress.
		let processes = count.to_u64().unwrap();
		state.progress.increment_transferred(processes, 0, 0);
		for (id, _, _) in &batch {
			crate::checkpoint!(self.server, "sync.get.store.process", id = %id).await;
		}

		let end = state.graph.lock().unwrap().end_local();
		for (id, _, _) in &batch {
			crate::checkpoint!(self.server, "sync.get.store.process.end", end, id = %id).await;
		}
		if end {
			state.queue.close();
		}

		Ok(())
	}
}
