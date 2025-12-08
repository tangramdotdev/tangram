use {
	crate::{Server, sync::get::State},
	futures::{StreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_either::Either,
};

impl Server {
	#[tracing::instrument(level = "debug", name = "input", skip_all)]
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
					tracing::trace!(id = %message.id, "received object");

					// Remove the get.
					let eager = state
						.gets
						.remove(&Either::Left(message.id.clone()))
						.is_none_or(|(_, eager)| eager);

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
						let data = tg::object::Data::deserialize(
							message.id.kind(),
							message.bytes.as_ref(),
						)?;
						Self::sync_get_enqueue_object_children(state, &message.id, &data, None);

						// Decrement the queue counter.
						state.queue.decrement(1);
					}

					// Send to the store task.
					let item = super::store::ObjectItem {
						id: message.id,
						bytes: message.bytes,
					};
					store_object_sender
						.send(item)
						.await
						.map_err(|_| tg::error!("failed to send the object to the store task"))?;
				},

				tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Process(message)) => {
					tracing::trace!(id = %message.id, "received process");

					// Remove the get.
					let eager = state
						.gets
						.remove(&Either::Right(message.id.clone()))
						.is_none_or(|(_, eager)| eager);

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
						let data = serde_json::from_slice(&message.bytes).map_err(|source| {
							tg::error!(!source, "failed to deserialize the process")
						})?;
						let graph = state.graph.lock().unwrap();
						let stored = graph.get_process_stored(&message.id);
						Self::sync_get_enqueue_process_children(state, &message.id, &data, stored);

						// Decrement the queue counter.
						state.queue.decrement(1);
					}

					// Send to the store task.
					let item = super::store::ProcessItem {
						id: message.id,
						bytes: message.bytes,
					};
					store_process_sender
						.send(item)
						.await
						.map_err(|_| tg::error!("failed to send the process to the store task"))?;
				},

				tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage::Object(message)) => {
					tracing::trace!(id = %message.id, "received missing object");

					// Remove the get.
					let eager = state
						.gets
						.remove(&Either::Left(message.id.clone()))
						.is_none_or(|(_, eager)| eager);

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
					tracing::trace!(id = %message.id, "received missing process");

					// Remove the get.
					let eager = state
						.gets
						.remove(&Either::Right(message.id.clone()))
						.is_none_or(|(_, eager)| eager);

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
