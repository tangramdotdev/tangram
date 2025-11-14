use {
	crate::{
		Server,
		sync::{
			get::State,
			queue::{ObjectItem, ProcessItem},
		},
	},
	futures::{StreamExt as _, TryStreamExt as _},
	std::sync::Arc,
	tangram_client as tg,
};

const PROCESS_BATCH_SIZE: usize = 16;
const PROCESS_CONCURRENCY: usize = 8;
const OBJECT_BATCH_SIZE: usize = 16;
const OBJECT_CONCURRENCY: usize = 8;

impl Server {
	pub(super) async fn sync_get_queue_task(
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
				async move { server.sync_get_queue_process_batch(&state, items).await }
			});

		// Create the objects future.
		let objects_future = object_queue_receiver
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
			.send(Ok(tg::sync::Message::Get(None)))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the get end message"))?;

		Ok(())
	}

	async fn sync_get_queue_process_batch(
		&self,
		state: &State,
		items: Vec<ProcessItem>,
	) -> tg::Result<()> {
		todo!()

		// // Get the ids.
		// let ids = messages
		// 	.iter()
		// 	.map(|message| message.id.clone())
		// 	.collect::<Vec<_>>();

		// // Touch the processes and get completes and metadata.
		// let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		// let outputs = self
		// 	.try_touch_process_and_get_complete_and_metadata_batch(&ids, touched_at)
		// 	.await?;

		// // Handle each message.
		// for (message, output) in std::iter::zip(messages, outputs) {
		// 	let (complete, metadata) = output.unwrap_or((
		// 		crate::process::complete::Output::default(),
		// 		tg::process::Metadata::default(),
		// 	));

		// 	// If the process is complete, then send a complete message.
		// 	if complete.children || complete.children_commands || complete.children_outputs {
		// 		let complete =
		// 			tg::sync::CompleteMessage::Process(tg::sync::ProcessCompleteMessage {
		// 				command_complete: complete.command,
		// 				children_commands_complete: complete.children_commands,
		// 				children_complete: complete.children,
		// 				id: message.id,
		// 				output_complete: complete.output,
		// 				children_outputs_complete: complete.children_outputs,
		// 			});
		// 		let message = tg::sync::Message::Complete(complete);
		// 		state.sender.send(Ok(message)).await.ok();
		// 	}
		//
		// If not present, send a get.
		// }

		// Ok(())
	}

	async fn sync_get_queue_object_batch(
		&self,
		state: &State,
		items: Vec<ObjectItem>,
	) -> tg::Result<()> {
		// get present, complete and metadata
		// update the graph
		// if complete true, send complete and return
		// if not present, send get
		// if complete false, enqueue children
		todo!()

		// // Get the ids.
		// let ids = messages
		// 	.iter()
		// 	.map(|message| message.id.clone())
		// 	.collect::<Vec<_>>();

		// // Touch the objects and get completes and metadata.
		// let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		// let outputs = self
		// 	.try_touch_object_and_get_complete_and_metadata_batch(&ids, touched_at)
		// 	.await?;

		// // Handle each message.
		// for (message, output) in std::iter::zip(messages, outputs) {
		// 	let (complete, metadata) = output.unwrap_or((false, tg::object::Metadata::default()));

		// 	// If the object is complete, then send a complete message.
		// 	if complete {
		// 		let complete = tg::sync::CompleteMessage::Object(tg::sync::ObjectCompleteMessage {
		// 			id: message.id,
		// 		});
		// 		let message = tg::sync::Message::Complete(complete);
		// 		state.sender.send(Ok(message)).await.ok();
		// 	}
		// }

		// Ok(())
	}
}

// 	// Update the graph.
// 	let data = serde_json::from_slice(&message.bytes)
// 		.map_err(|source| tg::error!(!source, "failed to deserialize the process data"))?;
// 	state.graph.lock().unwrap().update_process(
// 		&message.id,
// 		&data,
// 		complete.clone(),
// 		metadata,
// 	);

// 	// Update the graph.
// 	let data = tg::object::Data::deserialize(message.id.kind(), message.bytes)
// 		.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;
// 	state
// 		.graph
// 		.lock()
// 		.unwrap()
// 		.update_object(&message.id, &data, complete, metadata);

// let mut queue = VecDeque::from(arg.get.unwrap_or_default());
// while let Some(item) = queue.pop_front() {
// 	let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
// 	match item {
// 		Either::Left(id) => {
// 			let output = self
// 				.try_touch_process_and_get_complete_and_metadata(&id, touched_at)
// 				.await?;
// 			if let Some((complete, _)) = output {
// 				let data = self
// 					.try_get_process_local(&id)
// 					.await?
// 					.ok_or_else(|| tg::error!("expected the process to exist"))?
// 					.data;
// 				if arg.commands && !complete.command {
// 					queue.push_back(Either::Right(data.command.clone().into()));
// 				}
// 				if (arg.outputs && !complete.output)
// 					&& let Some(output) = data.output
// 				{
// 					let mut children = BTreeSet::new();
// 					output.children(&mut children);
// 					queue.extend(children.into_iter().map(Either::Right));
// 				}
// 				if ((arg.recursive && !complete.children)
// 					|| (arg.commands && !complete.children_commands)
// 					|| (arg.outputs && !complete.children_outputs))
// 					&& let Some(children) = data.children
// 				{
// 					for referent in children {
// 						queue.push_back(Either::Left(referent.item));
// 					}
// 				}
// 			} else {
// 				let message = tg::sync::Message::Get(Some(tg::sync::GetMessage::Process(
// 					tg::sync::ProcessGetMessage {
// 						id: id.clone(),
// 						eager: arg.eager,
// 					},
// 				)));
// 				sender.send(Ok(message)).await.map_err(|source| {
// 					tg::error!(!source, "failed to send the get message")
// 				})?;
// 			}
// 		},
// 		Either::Right(id) => {
// 			let output = self
// 				.try_touch_object_and_get_complete_and_metadata(&id, touched_at)
// 				.await?;
// 			if let Some((complete, _)) = output {
// 				if !complete {
// 					let bytes = self
// 						.try_get_object_local(&id)
// 						.await?
// 						.ok_or_else(|| tg::error!("expected the object to exist"))?
// 						.bytes;
// 					let object = tg::object::Data::deserialize(id.kind(), bytes)?;
// 					let mut children = BTreeSet::new();
// 					object.children(&mut children);
// 					queue.extend(children.into_iter().map(Either::Right));
// 				}
// 			} else {
// 				let message = tg::sync::Message::Get(Some(tg::sync::GetMessage::Object(
// 					tg::sync::ObjectGetMessage {
// 						id: id.clone(),
// 						eager: arg.eager,
// 					},
// 				)));
// 				sender.send(Ok(message)).await.map_err(|source| {
// 					tg::error!(!source, "failed to send the get message")
// 				})?;
// 			}
// 		},
// 	}
// }
