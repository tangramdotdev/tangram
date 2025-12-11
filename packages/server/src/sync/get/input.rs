use {
	crate::{Server, sync::get::State},
	futures::{StreamExt as _, stream::BoxStream},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
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
					let requested = state
						.graph
						.lock()
						.unwrap()
						.get_object_requested(&message.id);
					let is_requested = requested.is_some();
					let eager = requested.is_none_or(|requested| requested.eager);

					if eager {
						// Parse the data.
						let data = tg::object::Data::deserialize(
							message.id.kind(),
							message.bytes.as_ref(),
						)?;

						// Get the children.
						let mut children = BTreeSet::new();
						data.children(&mut children);

						if is_requested {
							// This object was explicitly requested. Update graph and set pending_children.
							let mut graph = state.graph.lock().unwrap();

							// Update the graph with data to populate children and parents.
							graph.update_object(&message.id, Some(&data), None, None, None, None);

							// Count children that are not already fully received (don't have children set).
							let pending_count = children
								.iter()
								.filter(|child_id| {
									graph
										.nodes
										.get(&super::graph::Id::Object((*child_id).clone()))
										.map_or(true, |node| {
											node.try_unwrap_object_ref()
												.map(|n| n.children.is_none())
												.unwrap_or(true)
										})
								})
								.count();

							graph.set_object_pending_children(&message.id, pending_count);

							// If no children pending, decrement queue counter now.
							if pending_count == 0 {
								drop(graph);
								state.queue.decrement(1);
							}
						} else {
							// This is a child arriving. Decrement parent's pending_children.
							let parents =
								state.graph.lock().unwrap().get_object_parents(&message.id);
							let mut decrement_count = 0;
							for parent_index in parents.iter() {
								if state
									.graph
									.lock()
									.unwrap()
									.decrement_pending_children(*parent_index)
								{
									decrement_count += 1;
								}
							}
							if decrement_count > 0 {
								state.queue.decrement(decrement_count);
							}

							// Update the graph with data.
							state.graph.lock().unwrap().update_object(
								&message.id,
								Some(&data),
								None,
								None,
								None,
								None,
							);
						}

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
					let requested = state
						.graph
						.lock()
						.unwrap()
						.get_process_requested(&message.id);
					let is_requested = requested.is_some();
					let eager = requested.is_none_or(|requested| requested.eager);

					if eager {
						// Parse the data.
						let data: tg::process::Data = serde_json::from_slice(&message.bytes)
							.map_err(|source| {
								tg::error!(!source, "failed to deserialize the process")
							})?;

						if is_requested {
							// This process was explicitly requested. Update graph and set pending_children.
							let mut graph = state.graph.lock().unwrap();

							// Update the graph with data to populate children and parents.
							graph.update_process(&message.id, Some(&data), None, None, None, None);

							// Count children that are not already fully received.
							// Only count process children if recursive is set, because otherwise
							// the sender won't send them and we shouldn't wait.
							let pending_count = if state.arg.recursive {
								if let Some(children) = &data.children {
									children
										.iter()
										.filter(|child| {
											graph
												.nodes
												.get(&super::graph::Id::Process(child.item.clone()))
												.map_or(true, |node| {
													node.try_unwrap_process_ref()
														.map(|n| n.children.is_none())
														.unwrap_or(true)
												})
										})
										.count()
								} else {
									0
								}
							} else {
								0
							};

							graph.set_process_pending_children(&message.id, pending_count);

							// If no children pending, decrement queue counter now.
							if pending_count == 0 {
								drop(graph);
								state.queue.decrement(1);
							}
						} else {
							// This is a child arriving. Decrement parent's pending_children.
							let parents =
								state.graph.lock().unwrap().get_process_parents(&message.id);
							let mut decrement_count = 0;
							for parent_index in parents.iter() {
								if state
									.graph
									.lock()
									.unwrap()
									.decrement_pending_children(*parent_index)
								{
									decrement_count += 1;
								}
							}
							if decrement_count > 0 {
								state.queue.decrement(decrement_count);
							}

							// Update the graph with data.
							state.graph.lock().unwrap().update_process(
								&message.id,
								Some(&data),
								None,
								None,
								None,
								None,
							);
						}

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
						let data: tg::process::Data = serde_json::from_slice(&message.bytes)
							.map_err(|source| {
								tg::error!(!source, "failed to deserialize the process")
							})?;
						let stored = state
							.graph
							.lock()
							.unwrap()
							.get_process_stored(&message.id)
							.cloned();
						Self::sync_get_enqueue_process_children(
							state,
							&message.id,
							&data,
							stored.as_ref(),
						);

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
					tracing::trace!(id = %message.id, "received missing process");

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
