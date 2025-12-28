use {
	crate::{Server, sync::put::State},
	futures::{StreamExt as _, TryStreamExt as _},
	std::sync::Arc,
	tangram_client::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ObjectItem {
	pub id: tg::object::Id,
}

pub struct ProcessItem {
	pub id: tg::process::Id,
}

impl Server {
	#[tracing::instrument(err, level = "debug", name = "index", ret, skip_all)]
	pub(super) async fn sync_put_index(
		&self,
		state: Arc<State>,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
	) -> tg::Result<()> {
		// Create the objects future.
		let object_batch_size = self.config.sync.put.index.object_batch_size;
		let object_batch_timeout = self.config.sync.put.index.object_batch_timeout;
		let object_concurrency = self.config.sync.put.index.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(object_receiver),
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |items| {
			let server = self.clone();
			let state = state.clone();
			async move { server.sync_put_index_object_batch(&state, items).await }
		});

		// Create the processes future.
		let process_batch_size = self.config.sync.put.index.process_batch_size;
		let process_batch_timeout = self.config.sync.put.index.process_batch_timeout;
		let process_concurrency = self.config.sync.put.index.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(process_receiver),
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |items| {
			let server = self.clone();
			let state = state.clone();
			async move { server.sync_put_index_process_batch(&state, items).await }
		});

		// Join the objects and processes futures.
		futures::try_join!(objects_future, processes_future)?;

		Ok(())
	}

	pub(super) async fn sync_put_index_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		let ids = items.into_iter().map(|item| item.id).collect::<Vec<_>>();
		let outputs = self
			.try_get_object_stored_and_metadata_batch(&ids)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to get the object stored and metadata")
			})?;
		for output in outputs {
			let Some((_, metadata)) = output else {
				continue;
			};
			let processes = 0;
			let objects = metadata.subtree.count.unwrap_or_default();
			let bytes = metadata.subtree.size.unwrap_or_default();
			state.progress.increment(processes, objects, bytes);
		}

		if state.graph.lock().unwrap().end_put(&state.arg) {
			state.queue.close();
		}

		Ok(())
	}

	pub(super) async fn sync_put_index_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
	) -> tg::Result<()> {
		let ids = items.into_iter().map(|item| item.id).collect::<Vec<_>>();
		let outputs = self
			.try_get_process_stored_and_metadata_batch(&ids)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to get the process stored and metadata")
			})?;
		for output in outputs {
			let Some((_stored, metadata)) = output else {
				continue;
			};
			let mut message = tg::sync::ProgressMessage::default();
			if state.arg.recursive {
				if let Some(count) = metadata.subtree.count {
					message.processes += count;
				}
				if state.arg.commands {
					if let Some(commands_count) = metadata.subtree.command.count {
						message.objects += commands_count;
					}
					if let Some(commands_size) = metadata.subtree.command.size {
						message.bytes += commands_size;
					}
				}
				if state.arg.errors {
					if let Some(errors_count) = metadata.subtree.error.count {
						message.objects += errors_count;
					}
					if let Some(errors_size) = metadata.subtree.error.size {
						message.bytes += errors_size;
					}
				}
				if state.arg.logs {
					if let Some(logs_count) = metadata.subtree.log.count {
						message.objects += logs_count;
					}
					if let Some(logs_size) = metadata.subtree.log.size {
						message.bytes += logs_size;
					}
				}
				if state.arg.outputs {
					if let Some(outputs_count) = metadata.subtree.output.count {
						message.objects += outputs_count;
					}
					if let Some(outputs_size) = metadata.subtree.output.size {
						message.bytes += outputs_size;
					}
				}
			} else {
				if state.arg.commands {
					if let Some(command_count) = metadata.node.command.count {
						message.objects += command_count;
					}
					if let Some(command_size) = metadata.node.command.size {
						message.bytes += command_size;
					}
				}
				if state.arg.logs {
					if let Some(logs_count) = metadata.subtree.log.count {
						message.objects += logs_count;
					}
					if let Some(logs_size) = metadata.subtree.log.size {
						message.bytes += logs_size;
					}
				}
				if state.arg.outputs {
					if let Some(output_count) = metadata.node.output.count {
						message.objects += output_count;
					}
					if let Some(output_size) = metadata.node.output.size {
						message.bytes += output_size;
					}
				}
			}
			state
				.progress
				.increment(message.processes, message.objects, message.bytes);
		}

		if state.graph.lock().unwrap().end_put(&state.arg) {
			state.queue.close();
		}

		Ok(())
	}
}
