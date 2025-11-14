use {
	crate::{
		Server,
		sync::{
			put::graph::Graph,
			queue::{ObjectItem, ProcessItem},
		},
	},
	futures::{StreamExt as _, stream::BoxStream},
	rusqlite as sqlite,
	std::collections::{BTreeSet, VecDeque},
	tangram_client as tg,
	tangram_either::Either,
};

struct State {
	arg: tg::sync::Arg,
	complete_receiver: tokio::sync::mpsc::Receiver<tg::sync::CompleteMessage>,
	database: sqlite::Connection,
	file: Option<crate::object::get::File>,
	get_receiver: tokio::sync::mpsc::Receiver<Option<tg::sync::GetMessage>>,
	graph: Graph,
	index: sqlite::Connection,
	object_queue: VecDeque<ObjectItem>,
	process_queue: VecDeque<ProcessItem>,
	sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
}

impl Server {
	pub(super) fn sync_put_items_complete_sync(
		database: &sqlite::Connection,
		index: &sqlite::Connection,
		arg: &tg::sync::Arg,
	) -> tg::Result<bool> {
		for item in arg.put.iter().flatten() {
			let complete = match item.as_ref() {
				Either::Left(process) => {
					if arg.recursive {
						let Some(output) =
							Self::try_get_process_complete_sqlite_sync(index, process)?
						else {
							return Ok(false);
						};
						output.children
							&& (!arg.commands || output.children_commands)
							&& (!arg.outputs || output.children_outputs)
					} else {
						let Some(process_data) =
							Self::try_get_process_sqlite_sync(database, process)?
						else {
							return Ok(false);
						};
						if arg.commands {
							let command_complete = Self::try_get_object_complete_sqlite_sync(
								index,
								&process_data.data.command.clone().into(),
							)?
							.unwrap_or(false);
							if !command_complete {
								return Ok(false);
							}
						}
						if arg.outputs
							&& let Some(output) = process_data.data.output
						{
							let mut children = std::collections::BTreeSet::new();
							output.children(&mut children);
							for child in children {
								let child_complete =
									Self::try_get_object_complete_sqlite_sync(index, &child)?
										.unwrap_or(false);
								if !child_complete {
									return Ok(false);
								}
							}
						}
						true
					}
				},
				Either::Right(object) => {
					Self::try_get_object_complete_sqlite_sync(index, object)?.unwrap_or(false)
				},
			};
			if !complete {
				return Ok(false);
			}
		}
		Ok(true)
	}

	pub(super) async fn sync_put_stream_task_sync(
		mut stream: BoxStream<'static, tg::sync::Message>,
		get_sender: tokio::sync::mpsc::Sender<Option<tg::sync::GetMessage>>,
		complete_sender: tokio::sync::mpsc::Sender<tg::sync::CompleteMessage>,
	) -> tg::Result<()> {
		while let Some(message) = stream.next().await {
			match message {
				tg::sync::Message::Get(message) => {
					get_sender.send(message).await.ok();
				},
				tg::sync::Message::Complete(message) => {
					complete_sender.send(message).await.ok();
				},
				_ => unreachable!(),
			}
		}
		Ok::<_, tg::Error>(())
	}

	pub(super) fn sync_put_queue_task_sync(
		&self,
		database: sqlite::Connection,
		index: sqlite::Connection,
		arg: tg::sync::Arg,
		complete_receiver: tokio::sync::mpsc::Receiver<tg::sync::CompleteMessage>,
		get_receiver: tokio::sync::mpsc::Receiver<Option<tg::sync::GetMessage>>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		// Create the state.
		let mut state = State {
			arg,
			complete_receiver,
			database,
			file: None,
			get_receiver,
			graph: Graph::new(),
			index,
			object_queue: VecDeque::new(),
			process_queue: VecDeque::new(),
			sender,
		};

		// Enqueue the items.
		for item in state.arg.put.iter().flatten().cloned() {
			match item {
				Either::Left(process) => state.process_queue.push_back(ProcessItem {
					parent: None,
					process,
					eager: state.arg.eager,
				}),
				Either::Right(object) => state.object_queue.push_back(ObjectItem {
					parent: None,
					object,
					eager: state.arg.eager,
				}),
			}
		}

		// Handle each item from the queues.
		let mut done = false;
		loop {
			while !state.process_queue.is_empty() || !state.object_queue.is_empty() {
				// Handle get messages.
				if !done {
					while let Ok(message) = state.get_receiver.try_recv() {
						match message {
							Some(tg::sync::GetMessage::Process(message)) => {
								let item = ProcessItem {
									parent: None,
									process: message.id,
									eager: message.eager,
								};
								state.process_queue.push_back(item);
							},
							Some(tg::sync::GetMessage::Object(message)) => {
								let item = ObjectItem {
									parent: None,
									object: message.id,
									eager: message.eager,
								};
								state.object_queue.push_back(item);
							},
							None => {
								done = true;
							},
						}
					}
				}

				// Handle complete messages.
				while let Ok(message) = state.complete_receiver.try_recv() {
					match message {
						tg::sync::CompleteMessage::Process(message) => {
							let id = Either::Left(message.id.clone());
							let complete = if state.arg.recursive {
								message.children_complete
									&& (!state.arg.commands || message.children_commands_complete)
									&& (!state.arg.outputs || message.children_outputs_complete)
							} else {
								(!state.arg.commands || message.command_complete)
									&& (!state.arg.outputs || message.output_complete)
							};
							state.graph.update(None, id, complete);
						},
						tg::sync::CompleteMessage::Object(message) => {
							let id = Either::Right(message.id.clone());
							let complete = true;
							state.graph.update(None, id, complete);
						},
					}
				}

				if let Some(item) = state.object_queue.pop_front() {
					self.sync_put_sync_object(&mut state, item)?;
				} else if let Some(item) = state.process_queue.pop_front() {
					Self::sync_put_sync_process(&mut state, item)?;
				}
			}

			// If the end get message has been received, then break.
			if done {
				break;
			}

			// Otherwise, wait to receive the next get message.
			let message = state
				.get_receiver
				.blocking_recv()
				.ok_or_else(|| tg::error!("expected to receive the end get message"))?;
			match message {
				Some(tg::sync::GetMessage::Process(message)) => {
					let item = ProcessItem {
						parent: None,
						process: message.id,
						eager: message.eager,
					};
					state.process_queue.push_back(item);
				},
				Some(tg::sync::GetMessage::Object(message)) => {
					let item = ObjectItem {
						parent: None,
						object: message.id,
						eager: message.eager,
					};
					state.object_queue.push_back(item);
				},
				None => {
					done = true;
				},
			}
		}

		// Send the put end message.
		state
			.sender
			.blocking_send(Ok(tg::sync::Message::Put(None)))
			.map_err(|source| tg::error!(!source, "failed to send the put end message"))?;

		Ok(())
	}

	fn sync_put_sync_process(state: &mut State, item: ProcessItem) -> tg::Result<()> {
		let ProcessItem {
			parent,
			process,
			eager,
		} = item;

		// If the process has already been sent or is complete, then update the progress and return.
		let (inserted, complete) = state.graph.update(
			parent.map(Either::Left),
			Either::Left(process.clone()),
			false,
		);
		if !inserted || complete {
			let metadata = Self::try_get_process_metadata_sqlite_sync(&state.index, &process)?
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
			if message != tg::sync::ProgressMessage::default() {
				let message = tg::sync::Message::Progress(message);
				state
					.sender
					.blocking_send(Ok(message))
					.map_err(|source| tg::error!(!source, "failed to send the progress message"))?;
			}
		}
		if !inserted || complete {
			return Ok(());
		}

		// Get the process.
		let Some(tg::process::get::Output { data, .. }) =
			Self::try_get_process_sqlite_sync(&state.database, &process)?
		else {
			let message = tg::sync::Message::Missing(tg::sync::MissingMessage::Process(
				tg::sync::ProcessMissingMessage {
					id: process.clone(),
				},
			));
			state
				.sender
				.blocking_send(Ok(message))
				.map_err(|source| tg::error!(!source, "failed to send the missing message"))?;
			return Ok(());
		};

		// Enqueue the children.
		if state.arg.recursive && eager {
			for child in data.children.iter().flatten() {
				let item = ProcessItem {
					parent: Some(process.clone()),
					process: child.item.clone(),
					eager,
				};
				state.process_queue.push_back(item);
			}
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
			for child in objects {
				let item = ObjectItem {
					parent: Some(Either::Left(process.clone())),
					object: child,
					eager,
				};
				state.object_queue.push_back(item);
			}
		}

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
			.blocking_send(Ok(message))
			.map_err(|source| tg::error!(!source, "failed to send the put message"))?;

		Ok(())
	}

	fn sync_put_sync_object(&self, state: &mut State, item: ObjectItem) -> tg::Result<()> {
		let ObjectItem {
			parent,
			object,
			eager,
		} = item;

		// If the object has already been sent or is complete, then update the progress and return.
		let (inserted, complete) = state
			.graph
			.update(parent, Either::Right(object.clone()), false);
		if !inserted || complete {
			let metadata = Self::try_get_object_metadata_sqlite_sync(&state.index, &object)?;
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
			if message != tg::sync::ProgressMessage::default() {
				let message = tg::sync::Message::Progress(message);
				state
					.sender
					.blocking_send(Ok(message))
					.map_err(|source| tg::error!(!source, "failed to send the progress message"))?;
			}
		}
		if !inserted || complete {
			return Ok(());
		}

		// Get the object.
		let Some(output) = self.try_get_object_sync(&object, &mut state.file)? else {
			let message = tg::sync::Message::Missing(tg::sync::MissingMessage::Object(
				tg::sync::ObjectMissingMessage { id: object.clone() },
			));
			state
				.sender
				.blocking_send(Ok(message))
				.map_err(|source| tg::error!(!source, "failed to send the missing message"))?;
			return Ok(());
		};
		let bytes = output.bytes;
		let data = tg::object::Data::deserialize(object.kind(), bytes.clone())?;

		// Enqueue the children.
		if eager {
			let mut children = BTreeSet::new();
			data.children(&mut children);
			for child in children {
				let item = ObjectItem {
					parent: Some(Either::Right(object.clone())),
					object: child,
					eager,
				};
				if object.is_blob() {
					state.object_queue.push_front(item);
				} else {
					state.object_queue.push_back(item);
				}
			}
		}

		// Send the object.
		let message = tg::sync::Message::Put(Some(tg::sync::PutMessage::Object(
			tg::sync::ObjectPutMessage {
				id: object.clone(),
				bytes,
			},
		)));
		state
			.sender
			.blocking_send(Ok(message))
			.map_err(|source| tg::error!(!source, "failed to send the put message"))?;

		Ok(())
	}
}
