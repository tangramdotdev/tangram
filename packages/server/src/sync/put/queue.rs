use {
	crate::{
		Server,
		sync::{
			put::State,
			queue::{ObjectItem, ProcessItem},
		},
	},
	futures::{StreamExt as _, TryStreamExt as _},
	std::{collections::BTreeSet, sync::Arc},
	tangram_client as tg,
	tangram_either::Either,
};

const PROCESS_BATCH_SIZE: usize = 16;
const PROCESS_CONCURRENCY: usize = 8;
const OBJECT_BATCH_SIZE: usize = 16;
const OBJECT_CONCURRENCY: usize = 8;

impl Server {
	pub(super) async fn sync_put_queue_task(
		&self,
		state: Arc<State>,
		process_queue_receiver: async_channel::Receiver<ProcessItem>,
		object_queue_receiver: async_channel::Receiver<ObjectItem>,
	) -> tg::Result<()> {
		// Create the processes future.
		let processes_future = process_queue_receiver
			.ready_chunks(PROCESS_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(PROCESS_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move { server.sync_put_queue_process_batch(&state, items).await }
			});

		// Create the objects future.
		let objects_future = object_queue_receiver
			.ready_chunks(OBJECT_BATCH_SIZE)
			.map(Ok)
			.try_for_each_concurrent(OBJECT_CONCURRENCY, |items| {
				let server = self.clone();
				let state = state.clone();
				async move { server.sync_put_queue_object_batch(&state, items).await }
			});

		// Join the processes and objects futures.
		futures::try_join!(processes_future, objects_future)?;

		// Send the put end message.
		state
			.sender
			.send(Ok(tg::sync::Message::Put(None)))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the put end message"))?;

		Ok(())
	}

	async fn sync_put_queue_process_batch(
		&self,
		state: &State,
		mut items: Vec<ProcessItem>,
	) -> tg::Result<()> {
		// Update the graph and handle inserted and complete processes.
		let n = items.len();
		let mut i = 0;
		while i < items.len() {
			let item = &items[i];
			let (inserted, complete) = state.graph.lock().unwrap().update(
				item.parent.clone().map(Either::Left),
				Either::Left(item.process.clone()),
				false,
			);
			if !inserted || complete {
				let metadata = self
					.try_get_process_metadata_local(&item.process)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
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
			if !inserted || complete {
				items.remove(i);
			} else {
				i += 1;
			}
		}
		if items.is_empty() {
			state.queue.ack(n);
			return Ok(());
		}

		// Get the processes.
		let ids = items
			.iter()
			.map(|item| item.process.clone())
			.collect::<Vec<_>>();
		let outputs = self
			.try_get_process_batch(&ids)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the processes"))?;

		// Handle the processes.
		for (item, output) in std::iter::zip(items, outputs) {
			let ProcessItem { process, eager, .. } = item;
			let Some(output) = output else {
				let message = tg::sync::Message::Missing(tg::sync::MissingMessage::Process(
					tg::sync::ProcessMissingMessage {
						id: process.clone(),
					},
				));
				state.sender.send(Ok(message)).await.ok();
				continue;
			};
			let data = output.data;

			// Send the process.
			let bytes = serde_json::to_string(&data)
				.map_err(|source| tg::error!(!source, "failed to serialize the process"))?;
			let message = tg::sync::Message::Put(Some(tg::sync::PutMessage::Process(
				tg::sync::ProcessPutMessage {
					id: process.clone(),
					bytes: bytes.into(),
				},
			)));
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|source| tg::error!(!source, "failed to send the put message"))?;

			// Enqueue the children.
			if state.arg.recursive && eager {
				let children = data
					.children
					.as_ref()
					.ok_or_else(|| tg::error!("expected the children to be set"))?;
				let items = children.iter().map(|child| ProcessItem {
					parent: Some(process.clone()),
					process: child.item.clone(),
					eager,
				});
				state.queue.enqueue_processes(items);
			}

			// Enqueue the objects.
			if eager {
				let mut objects: Vec<tg::object::Id> = Vec::new();
				if state.arg.commands {
					objects.push(data.command.clone().into());
				}
				if state.arg.outputs
					&& let Some(output) = &data.output
				{
					let mut children = BTreeSet::new();
					output.children(&mut children);
					objects.extend(children);
				}
				let items = objects.into_iter().map(|child| ObjectItem {
					parent: Some(Either::Left(process.clone())),
					object: child,
					eager,
				});
				state.queue.enqueue_objects(items);
			}
		}

		// Ack the items.
		state.queue.ack(n);

		Ok(())
	}

	async fn sync_put_queue_object_batch(
		&self,
		state: &State,
		mut items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		// Update the graph and handle inserted and complete processes.
		let n = items.len();
		let mut i = 0;
		while i < items.len() {
			let item = &items[i];
			let (inserted, complete) = state.graph.lock().unwrap().update(
				item.parent.clone(),
				Either::Right(item.object.clone()),
				false,
			);
			if !inserted || complete {
				let metadata = self.try_get_object_metadata_local(&item.object).await?;
				let message = tg::sync::ProgressMessage {
					processes: 0,
					objects: metadata
						.as_ref()
						.and_then(|metadata| metadata.count)
						.unwrap_or_default(),
					bytes: metadata
						.as_ref()
						.and_then(|metadata| metadata.weight)
						.unwrap_or_default(),
				};
				state
					.progress
					.increment(message.processes, message.objects, message.bytes);
			}
			if !inserted || complete {
				items.remove(i);
			} else {
				i += 1;
			}
		}
		if items.is_empty() {
			state.queue.ack(n);
			return Ok(());
		}

		// Get the objects.
		let ids = items
			.iter()
			.map(|item| item.object.clone())
			.collect::<Vec<_>>();
		let outputs = self
			.try_get_object_batch(&ids)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects"))?;

		// Handle the objects.
		for (item, output) in std::iter::zip(items, outputs) {
			let ObjectItem { object, eager, .. } = item;
			let Some(output) = output else {
				let message = tg::sync::Message::Missing(tg::sync::MissingMessage::Object(
					tg::sync::ObjectMissingMessage { id: object.clone() },
				));
				state.sender.send(Ok(message)).await.ok();
				continue;
			};
			let bytes = output.bytes;
			let data = tg::object::Data::deserialize(object.kind(), bytes.clone())?;
			let mut children = BTreeSet::new();
			data.children(&mut children);

			// Send the object.
			let message = tg::sync::Message::Put(Some(tg::sync::PutMessage::Object(
				tg::sync::ObjectPutMessage {
					id: object.clone(),
					bytes,
				},
			)));
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|source| tg::error!(!source, "failed to send the put message"))?;

			// Enqueue the children.
			if eager {
				let items = children.into_iter().map(|child| ObjectItem {
					parent: Some(Either::Right(object.clone())),
					object: child,
					eager,
				});
				state.queue.enqueue_objects(items);
			}
		}

		// Ack the items.
		state.queue.ack(n);

		Ok(())
	}
}
