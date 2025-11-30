use {
	crate::{Server, sync::put::State},
	futures::{StreamExt as _, TryStreamExt as _},
	std::sync::Arc,
	tangram_client::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

const PROCESS_BATCH_SIZE: usize = 16;
const PROCESS_CONCURRENCY: usize = 8;
const OBJECT_BATCH_SIZE: usize = 16;
const OBJECT_CONCURRENCY: usize = 8;

pub struct ProcessItem {
	pub id: tg::process::Id,
}

pub struct ObjectItem {
	pub id: tg::object::Id,
}

impl Server {
	#[tracing::instrument(err, level = "debug", name = "index", ret, skip_all)]
	pub(super) async fn sync_put_index(
		&self,
		state: Arc<State>,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessItem>,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectItem>,
	) -> tg::Result<()> {
		// Create the processes future.
		let processes_future = ReceiverStream::new(process_receiver)
			.ready_chunks(PROCESS_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(PROCESS_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move { server.sync_put_index_process_batch(&state, items).await }
			});

		// Create the objects future.
		let objects_future = ReceiverStream::new(object_receiver)
			.ready_chunks(OBJECT_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(OBJECT_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move { server.sync_put_index_object_batch(&state, items).await }
			});

		// Join the processes and objects futures.
		futures::try_join!(processes_future, objects_future)?;

		Ok(())
	}

	pub(super) async fn sync_put_index_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
	) -> tg::Result<()> {
		state.queue.decrement(items.len());
		let ids = items.into_iter().map(|item| item.id).collect::<Vec<_>>();
		let outputs = self
			.try_get_process_complete_and_metadata_batch(&ids)
			.await?;
		for output in outputs {
			let Some((_complete, metadata)) = output else {
				continue;
			};
			let mut message = tg::sync::ProgressMessage::default();
			if state.arg.recursive {
				if let Some(children_count) = metadata.children.count {
					message.processes += children_count;
				}
				if state.arg.commands {
					if let Some(commands_count) = metadata.children_commands.count {
						message.objects += commands_count;
					}
					if let Some(commands_weight) = metadata.children_commands.weight {
						message.bytes += commands_weight;
					}
				}
				if state.arg.outputs {
					if let Some(outputs_count) = metadata.children_outputs.count {
						message.objects += outputs_count;
					}
					if let Some(outputs_weight) = metadata.children_outputs.weight {
						message.bytes += outputs_weight;
					}
				}
			} else {
				if state.arg.commands {
					if let Some(command_count) = metadata.command.count {
						message.objects += command_count;
					}
					if let Some(command_weight) = metadata.command.weight {
						message.bytes += command_weight;
					}
				}
				if state.arg.outputs {
					if let Some(output_count) = metadata.output.count {
						message.objects += output_count;
					}
					if let Some(output_weight) = metadata.output.weight {
						message.bytes += output_weight;
					}
				}
			}
			state
				.progress
				.increment(message.processes, message.objects, message.bytes);
		}
		Ok(())
	}

	pub(super) async fn sync_put_index_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		state.queue.decrement(items.len());
		let ids = items.into_iter().map(|item| item.id).collect::<Vec<_>>();
		let outputs = self
			.try_get_object_complete_and_metadata_batch(&ids)
			.await?;
		for output in outputs {
			let Some((_, metadata)) = output else {
				continue;
			};
			let processes = 0;
			let objects = metadata.count.unwrap_or_default();
			let bytes = metadata.weight.unwrap_or_default();
			state.progress.increment(processes, objects, bytes);
		}
		Ok(())
	}
}
