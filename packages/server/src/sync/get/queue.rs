use {
	crate::{
		Server,
		sync::{
			get::State,
			queue::{ObjectItem, ProcessItem},
		},
	},
	futures::{StreamExt as _, TryStreamExt as _},
	std::{collections::BTreeSet, sync::Arc},
	tangram_client::prelude::*,
	tangram_either::Either,
};

const PROCESS_BATCH_SIZE: usize = 16;
const PROCESS_CONCURRENCY: usize = 8;
const OBJECT_BATCH_SIZE: usize = 16;
const OBJECT_CONCURRENCY: usize = 8;

impl Server {
	pub(super) async fn sync_get_queue_task(
		&self,
		state: Arc<State>,
		queue_process_receiver: async_channel::Receiver<ProcessItem>,
		queue_object_receiver: async_channel::Receiver<ObjectItem>,
	) -> tg::Result<()> {
		// Create the processes future.
		let processes_future = queue_process_receiver
			.ready_chunks(PROCESS_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(PROCESS_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move { server.sync_get_queue_process_batch(&state, items).await }
			});

		// Create the objects future.
		let objects_future = queue_object_receiver
			.ready_chunks(OBJECT_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(OBJECT_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move { server.sync_get_queue_object_batch(&state, items).await }
			});

		// Join the processes and objects futures.
		futures::try_join!(processes_future, objects_future)?;

		// Send the get end message.
		state
			.sender
			.send(Ok(tg::sync::GetMessage::End))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the get end message"))?;

		Ok(())
	}

	async fn sync_get_queue_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
	) -> tg::Result<()> {
		let n = items.len();

		// Get the ids.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();

		// Touch the processes and get complete and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let outputs = self
			.try_touch_process_and_get_complete_and_metadata_batch(&ids, touched_at)
			.await?;

		// Handle each item and output.
		for (item, output) in std::iter::zip(items, outputs) {
			// Update the graph.
			state.graph.lock().unwrap().update_process(
				&item.id,
				None,
				output.as_ref().map(|(complete, _)| complete.clone()),
				output.as_ref().map(|(_, metadata)| metadata.clone()),
			);

			match output {
				// If the process is absent, then send a get item message.
				None => {
					if !item.eager {
						state.queue.increment(1);
					}
					let message = tg::sync::GetMessage::Item(tg::sync::GetItemMessage::Process(
						tg::sync::GetItemProcessMessage {
							id: item.id.clone(),
							eager: state.arg.eager,
						},
					));
					state.gets.insert(Either::Left(item.id.clone()), item.eager);
					state
						.sender
						.send(Ok(message))
						.await
						.map_err(|source| tg::error!(!source, "failed to send the message"))?;
				},

				// If the process is present, then enqueue children and objects as necessary, and send a complete if necessary.
				Some((complete, _)) => {
					// Get the process.
					let data = self
						.try_get_process_local(&item.id)
						.await?
						.ok_or_else(|| tg::error!("expected the process to exist"))?
						.data;

					// Enqueue the children as necessary.
					Self::sync_get_enqueue_process_children(state, &item.id, &data, &complete);

					// Send a complete message if necessary.
					if complete.children || complete.children_commands || complete.children_outputs
					{
						let message =
							tg::sync::GetMessage::Complete(tg::sync::GetCompleteMessage::Process(
								tg::sync::GetCompleteProcessMessage {
									command_complete: complete.command,
									children_commands_complete: complete.children_commands,
									children_complete: complete.children,
									id: item.id,
									output_complete: complete.output,
									children_outputs_complete: complete.children_outputs,
								},
							));
						state.sender.send(Ok(message)).await.map_err(|source| {
							tg::error!(!source, "failed to send the complete message")
						})?;
					}
				},
			}
		}

		// Decrement the counter.
		state.queue.decrement(n);

		Ok(())
	}

	async fn sync_get_queue_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		let n = items.len();

		// Get the ids.
		let ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();

		// Touch the objects and get complete and metadata.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let outputs = self
			.try_touch_object_and_get_complete_and_metadata_batch(&ids, touched_at)
			.await?;

		// Handle each item and output.
		for (item, output) in std::iter::zip(items, outputs) {
			// Update the graph.
			state.graph.lock().unwrap().update_object(
				&item.id,
				None,
				output.as_ref().map(|(complete, _)| *complete),
				output.as_ref().map(|(_, metadata)| metadata.clone()),
				None,
			);

			match output {
				// If the object is absent, then send a get item message.
				None => {
					if !item.eager {
						state.queue.increment(1);
					}
					let message = tg::sync::GetMessage::Item(tg::sync::GetItemMessage::Object(
						tg::sync::GetItemObjectMessage {
							id: item.id.clone(),
							eager: state.arg.eager,
						},
					));
					state
						.gets
						.insert(Either::Right(item.id.clone()), item.eager);
					state
						.sender
						.send(Ok(message))
						.await
						.map_err(|source| tg::error!(!source, "failed to send the message"))?;
				},

				// If the object is present but not complete, then enqueue the children.
				Some((false, _)) => {
					let bytes = self
						.try_get_object_local(&item.id)
						.await?
						.ok_or_else(|| tg::error!("expected the object to exist"))?
						.bytes;
					let data = tg::object::Data::deserialize(item.id.kind(), bytes)?;
					Self::sync_get_enqueue_object_children(state, &item.id, &data);
				},

				// If the object is complete, then send a complete message.
				Some((true, _)) => {
					let message =
						tg::sync::GetMessage::Complete(tg::sync::GetCompleteMessage::Object(
							tg::sync::GetCompleteObjectMessage { id: item.id },
						));
					state
						.sender
						.send(Ok(message))
						.await
						.map_err(|source| tg::error!(!source, "failed to send the message"))?;
				},
			}
		}

		// Decrement the counter.
		state.queue.decrement(n);

		Ok(())
	}

	pub(super) fn sync_get_enqueue_process_children(
		state: &State,
		id: &tg::process::Id,
		data: &tg::process::Data,
		complete: &crate::process::complete::Output,
	) {
		// Enqueue the children if necessary.
		if ((state.arg.recursive && !complete.children)
			|| (state.arg.commands && !complete.children_commands)
			|| (state.arg.outputs && !complete.children_outputs))
			&& let Some(children) = &data.children
		{
			for referent in children {
				state.queue.enqueue_process(ProcessItem {
					parent: Some(id.clone()),
					id: referent.item.clone(),
					eager: state.arg.eager,
				});
			}
		}

		// Enqueue the command if necessary.
		if state.arg.commands && !complete.command {
			let item = ObjectItem {
				parent: Some(Either::Left(id.clone())),
				id: data.command.clone().into(),
				eager: state.arg.eager,
			};
			state.queue.enqueue_object(item);
		}

		// Enqueue the output if necessary.
		if (state.arg.outputs && !complete.output)
			&& let Some(output) = &data.output
		{
			let mut children = BTreeSet::new();
			output.children(&mut children);
			state
				.queue
				.enqueue_objects(children.into_iter().map(|object| ObjectItem {
					parent: Some(Either::Left(id.clone())),
					id: object,
					eager: state.arg.eager,
				}));
		}
	}

	pub(super) fn sync_get_enqueue_object_children(
		state: &State,
		id: &tg::object::Id,
		data: &tg::object::Data,
	) {
		let mut children = BTreeSet::new();
		data.children(&mut children);
		state
			.queue
			.enqueue_objects(children.into_iter().map(|object| ObjectItem {
				parent: Some(Either::Right(id.clone())),
				id: object,
				eager: state.arg.eager,
			}));
	}
}
