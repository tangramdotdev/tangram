use {
	crate::{
		Session,
		sync::{get::State, graph::UpdateObjectLocalArg, graph::UpdateProcessLocalArg},
	},
	futures::{StreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
};

pub(super) struct SyncGetInputArg {
	pub index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectItem>,
	pub index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessItem>,
	pub state: std::sync::Arc<State>,
	pub store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectItem>,
	pub store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessItem>,
	pub stream: BoxStream<'static, tg::sync::PutMessage>,
}

enum ItemAction {
	Ignore,
	Store { replace: bool },
}

impl Session {
	#[tracing::instrument(level = "trace", name = "input", skip_all)]
	pub(super) async fn sync_get_input(&self, arg: SyncGetInputArg) -> tg::Result<()> {
		let SyncGetInputArg {
			index_object_sender,
			index_process_sender,
			state,
			store_object_sender,
			store_process_sender,
			mut stream,
		} = arg;
		let state = &state;
		while let Some(message) = stream.next().await {
			match message {
				tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Group(message)) => {
					let message = tg::sync::PutItemMessage::Group(message);
					self.sync_get_input_item(state, message).await?;
				},

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
					if state.graph.lock().unwrap().end_local() {
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

				tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Organization(message)) => {
					let message = tg::sync::PutItemMessage::Organization(message);
					self.sync_get_input_item(state, message).await?;
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
					if state.graph.lock().unwrap().end_local() {
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

				tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Sandbox(message)) => {
					let mut message = message;
					if message.data.id != message.id {
						return Err(tg::error!(
							expected = %message.id,
							actual = %message.data.id,
							"invalid sandbox id"
						));
					}
					if !message.data.status.is_destroyed() {
						return Err(tg::error!(id = %message.id, "cannot sync a running sandbox"));
					}
					message.data.location =
						Some(tg::Location::Local(tg::location::Local::default()));
					message.data.token = None;
					let message = tg::sync::PutItemMessage::Sandbox(message);
					self.sync_get_input_item(state, message).await?;
				},

				tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Tag(message)) => {
					let message = tg::sync::PutItemMessage::Tag(message);
					self.sync_get_input_item(state, message).await?;
				},

				tg::sync::PutMessage::Item(tg::sync::PutItemMessage::User(message)) => {
					let message = tg::sync::PutItemMessage::User(message);
					self.sync_get_input_item(state, message).await?;
				},

				tg::sync::PutMessage::Missing(message) => match message.selector {
					tg::Selector::Specifier(specifier) => {
						state
							.graph
							.lock()
							.unwrap()
							.resolve_local_selector_missing(&specifier);
						if state.graph.lock().unwrap().end_local() {
							state.queue.close();
						}
					},
					tg::Selector::Id(id) => match id.kind() {
						tg::id::Kind::Process => {
							let id = id.try_into()?;
							if let Some(token) = message.token {
								state.graph.lock().unwrap().update_process_token(&id, token);
							}
							let eager = state
								.graph
								.lock()
								.unwrap()
								.get_process_requested(&id)
								.is_none_or(|requested| requested.eager);
							if !eager {
								return Err(tg::error!(%id, "failed to find the process"));
							}
							let item = super::index::ProcessItem { id, missing: true };
							index_process_sender.send(item).await.map_err(|_| {
								tg::error!("failed to send the process to the index task")
							})?;
						},
						kind if kind.is_object() => {
							let id = id.try_into()?;
							if let Some(token) = message.token {
								state.graph.lock().unwrap().update_object_token(&id, token);
							}
							let eager = state
								.graph
								.lock()
								.unwrap()
								.get_object_requested(&id)
								.is_none_or(|requested| requested.eager);
							if !eager {
								return Err(tg::error!(%id, "failed to find the object"));
							}
							let item = super::index::ObjectItem { id, missing: true };
							index_object_sender.send(item).await.map_err(|_| {
								tg::error!("failed to send the object to the index task")
							})?;
						},
						_ => {
							return Err(tg::error!(%id, "failed to find the item"));
						},
					},
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

	async fn sync_get_input_item(
		&self,
		state: &State,
		message: tg::sync::PutItemMessage,
	) -> tg::Result<()> {
		let (ancestor, id) = match &message {
			tg::sync::PutItemMessage::Group(message) => (
				Some((message.parent.clone(), message.specifier.clone())),
				message.id.clone().into(),
			),
			tg::sync::PutItemMessage::Object(_) | tg::sync::PutItemMessage::Process(_) => {
				return Err(tg::error!("invalid sync item kind"));
			},
			tg::sync::PutItemMessage::Organization(message) => (
				Some((None, message.specifier.clone())),
				message.id.clone().into(),
			),
			tg::sync::PutItemMessage::Sandbox(message) => (None, message.id.clone().into()),
			tg::sync::PutItemMessage::Tag(message) => (
				Some((message.parent.clone(), message.specifier.clone())),
				message.id.clone().into(),
			),
			tg::sync::PutItemMessage::User(message) => (
				Some((None, message.specifier.clone())),
				message.id.clone().into(),
			),
		};
		let action = if let Some((_, specifier)) = &ancestor {
			self.sync_get_input_item_selector(state, &id, specifier)
				.await?
		} else {
			ItemAction::Store { replace: false }
		};
		let replace = match action {
			ItemAction::Ignore => return Ok(()),
			ItemAction::Store { replace } => replace,
		};
		state
			.graph
			.lock()
			.unwrap()
			.update_item_local_message(message, replace)?;
		if let Some((parent, specifier)) = ancestor {
			self.sync_get_input_item_ancestor(state, parent.as_ref(), &specifier)
				.await?;
		}
		state.progress.increment_transferred_item(&id);
		if state.graph.lock().unwrap().end_local() {
			state.queue.close();
		}

		Ok(())
	}

	async fn sync_get_input_item_selector(
		&self,
		state: &State,
		id: &tg::Id,
		specifier: &tg::Specifier,
	) -> tg::Result<ItemAction> {
		if !state.graph.lock().unwrap().has_local_selector(specifier) {
			return Ok(ItemAction::Store { replace: true });
		}
		match state.arg.ancestors {
			tg::node::AncestorsPull::Always | tg::node::AncestorsPull::Never => {
				state
					.graph
					.lock()
					.unwrap()
					.resolve_local_selector(specifier, id.clone(), true);
				Ok(ItemAction::Store { replace: true })
			},
			tg::node::AncestorsPull::Missing => {
				let local = state.graph.lock().unwrap().local_selector_id(specifier)?;
				match local {
					None => {
						state.graph.lock().unwrap().resolve_local_selector(
							specifier,
							id.clone(),
							false,
						);
						Ok(ItemAction::Store { replace: false })
					},
					Some(local) if local == *id => {
						let requested = {
							let mut graph = state.graph.lock().unwrap();
							let requested = graph.has_local_item(id);
							graph.resolve_local_selector_missing(specifier);
							requested
						};
						if requested {
							return Ok(ItemAction::Store { replace: true });
						}
						if state.graph.lock().unwrap().end_local() {
							state.queue.close();
						}
						Ok(ItemAction::Ignore)
					},
					Some(_) => Err(tg::error!(%specifier, "the node has a different ID")),
				}
			},
		}
	}

	async fn sync_get_input_item_ancestor(
		&self,
		state: &State,
		parent: Option<&tg::Id>,
		specifier: &tg::Specifier,
	) -> tg::Result<()> {
		let (parent, parent_specifier) = match (parent, specifier.parent()) {
			(None, None) => return Ok(()),
			(None, Some(_)) => return Err(tg::error!("the parent is missing")),
			(Some(_), None) => return Err(tg::error!("the parent is unexpected")),
			(Some(parent), Some(parent_specifier)) => (parent, parent_specifier),
		};
		if parent.kind() == tg::id::Kind::Tag {
			return Err(tg::error!("a tag cannot be a parent"));
		}
		if state.graph.lock().unwrap().has_local_item(parent)
			|| state
				.graph
				.lock()
				.unwrap()
				.has_local_selector(&parent_specifier)
		{
			return Ok(());
		}
		let ancestor_specifiers = specifier.ancestors().collect::<Vec<_>>();

		// Apply the ancestor policy to the immediate parent.
		match state.arg.ancestors {
			tg::node::AncestorsPull::Always => {},
			tg::node::AncestorsPull::Missing => {
				let ids = self
					.try_get_ids_for_specifiers_from_index(&ancestor_specifiers)
					.await?;
				let local = ids.last().cloned().flatten();
				state
					.graph
					.lock()
					.unwrap()
					.set_local_selector_ids(std::iter::zip(
						ancestor_specifiers.iter().cloned(),
						ids,
					));
				match local {
					None => {},
					Some(local) if local == *parent => return Ok(()),
					Some(_) => return Err(tg::error!("the parent has a different ID")),
				}
			},
			tg::node::AncestorsPull::Never => {
				let mut ids = self
					.try_get_ids_for_specifiers_from_index(std::slice::from_ref(&parent_specifier))
					.await?;
				match ids.pop().unwrap() {
					None => return Err(tg::error!("the parent does not exist")),
					Some(local) if local == *parent => return Ok(()),
					Some(_) => return Err(tg::error!("the parent has a different ID")),
				}
			},
		}

		// Request all ancestors together through the existing sync stream.
		for specifier in ancestor_specifiers {
			let inserted = state
				.graph
				.lock()
				.unwrap()
				.insert_local_selector(specifier.clone());
			if !inserted {
				continue;
			}
			let selector = tg::Selector::Specifier(specifier);
			let message = tg::sync::GetMessage::Item(tg::sync::GetItemMessage {
				descendants: false,
				eager: state.arg.eager,
				selector,
				token: None,
			});
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|error| tg::error!(!error, "failed to send the message"))?;
		}

		Ok(())
	}
}
