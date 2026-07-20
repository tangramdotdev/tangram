use {
	crate::{
		Session,
		sync::{get::State, graph::UpdateObjectLocalArg, graph::UpdateProcessLocalArg},
	},
	futures::{StreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
};

impl Session {
	#[tracing::instrument(level = "trace", name = "input", skip_all)]
	pub(super) async fn sync_get_input(
		&self,
		state: &State,
		mut stream: BoxStream<'static, tg::sync::PutMessage>,
		index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectItem>,
		index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessItem>,
		store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectItem>,
		store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessItem>,
	) -> tg::Result<()> {
		while let Some(message) = stream.next().await {
			match message {
				tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Object(message)) => {
					// Deserialize the object.
					let data =
						tg::object::Data::deserialize(message.id.kind(), message.bytes.as_ref())?;

					// Validate the ID.
					let actual = tg::object::Id::new(message.id.kind(), &message.bytes);
					if message.id != actual {
						return Err(tg::error!(
							expected = %message.id,
							actual = %actual,
							"invalid object id"
						));
					}

					// Update the graph with data and metadata.
					let metadata = message.metadata.clone();
					let arg = UpdateObjectLocalArg {
						data: Some(&data),
						id: &message.id,
						marked: None,
						metadata,
						permissions: None,
						requested: None,
						stored: None,
					};
					state.graph.lock().unwrap().update_object_local(arg);

					// Close the queue if necessary.
					if state.graph.lock().unwrap().end_local(&state.arg) {
						state.queue.close();
					}

					let eager = state
						.graph
						.lock()
						.unwrap()
						.get_object_requested(&message.id)
						.is_none_or(|requested| requested.eager);

					if eager {
						// Send to the index task.
						let item = super::index::ObjectItem {
							id: message.id.clone(),
							missing: false,
						};
						index_object_sender.send(item).await.map_err(|_| {
							tg::error!("failed to send the object to the index task")
						})?;
					} else {
						// Enqueue the children.
						Self::sync_get_enqueue_object_children(
							state,
							&message.id,
							&data,
							None,
							None,
						);
					}

					// Send to the store task.
					let item = super::store::ObjectItem {
						id: message.id,
						bytes: message.bytes,
						metadata: message.metadata,
					};
					store_object_sender
						.send(item)
						.await
						.map_err(|_| tg::error!("failed to send the object to the store task"))?;
				},

				tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Process(message)) => {
					let eager = state
						.graph
						.lock()
						.unwrap()
						.get_process_requested(&message.id)
						.is_none_or(|requested| requested.eager);
					let data: tg::process::Data = serde_json::from_slice(&message.bytes)
						.map_err(|error| tg::error!(!error, "failed to deserialize the process"))?;
					let data = data.without_tokens();
					let bytes = serde_json::to_vec(&data)
						.map_err(|error| tg::error!(!error, "failed to serialize the process"))?;

					// Update the graph with data and metadata.
					let metadata = message.metadata.clone();
					let arg = UpdateProcessLocalArg {
						data: Some(&data),
						id: &message.id,
						marked: None,
						metadata,
						permissions: None,
						requested: None,
						stored: None,
					};
					state.graph.lock().unwrap().update_process_local(arg);

					// Check if all roots are stored and close the queue if so.
					if state.graph.lock().unwrap().end_local(&state.arg) {
						state.queue.close();
					}

					if eager {
						// Send to the index task.
						let item = super::index::ProcessItem {
							id: message.id.clone(),
							missing: false,
						};
						index_process_sender.send(item).await.map_err(|_| {
							tg::error!("failed to send the process to the index task")
						})?;
					} else {
						// Enqueue the children as necessary.
						let visible = state
							.graph
							.lock()
							.unwrap()
							.get_process_local_visible(&message.id);
						Self::sync_get_enqueue_process_children(
							state,
							&message.id,
							&data,
							Some(&visible),
							None,
						);
					}

					// Send to the store task.
					let item = super::store::ProcessItem {
						id: message.id,
						bytes: bytes.into(),
						metadata: message.metadata,
					};
					store_process_sender
						.send(item)
						.await
						.map_err(|_| tg::error!("failed to send the process to the store task"))?;
				},

				tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage::Object(message)) => {
					let eager = state
						.graph
						.lock()
						.unwrap()
						.get_object_requested(&message.id)
						.is_none_or(|requested| requested.eager);

					if eager {
						// Send to the index task.
						let item = super::index::ObjectItem {
							id: message.id.clone(),
							missing: true,
						};
						index_object_sender.send(item).await.map_err(|_| {
							tg::error!("failed to send the object to the index task")
						})?;
					} else {
						return Err(tg::error!(id = %message.id, "failed to find the object"));
					}
				},

				tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage::Process(message)) => {
					let eager = state
						.graph
						.lock()
						.unwrap()
						.get_process_requested(&message.id)
						.is_none_or(|requested| requested.eager);

					if eager {
						// Send to the index task.
						let item = super::index::ProcessItem {
							id: message.id.clone(),
							missing: true,
						};
						index_process_sender.send(item).await.map_err(|_| {
							tg::error!("failed to send the process to the index task")
						})?;
					} else {
						return Err(tg::error!(id = %message.id, "failed to find the process"));
					}
				},

				tg::sync::PutMessage::Progress(_) => (),

				tg::sync::PutMessage::End => {
					tracing::trace!("received end");
					return Ok(());
				},
			}
		}
		Err(tg::error!("failed to receive the put end message"))
	}
}
