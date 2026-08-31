use {
	crate::{
		Session,
		sync::{get::State, graph::UpdateObjectLocalArg, graph::UpdateProcessLocalArg},
	},
	futures::{StreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
};

pub(super) struct SyncGetInputArg {
	pub checkout_sender: tokio::sync::mpsc::Sender<super::checkout::ObjectNode>,
	pub index_object_sender: tokio::sync::mpsc::Sender<super::index::ObjectNode>,
	pub index_process_sender: tokio::sync::mpsc::Sender<super::index::ProcessNode>,
	pub state: std::sync::Arc<State>,
	pub store_object_sender: tokio::sync::mpsc::Sender<super::store::ObjectNode>,
	pub store_process_sender: tokio::sync::mpsc::Sender<super::store::ProcessNode>,
	pub stream: BoxStream<'static, tg::sync::PutMessage>,
	pub verify_object_ids: bool,
}

impl Session {
	#[tracing::instrument(level = "trace", name = "input", skip_all)]
	pub(super) async fn sync_get_input(&self, arg: SyncGetInputArg) -> tg::Result<()> {
		let SyncGetInputArg {
			checkout_sender,
			index_object_sender,
			index_process_sender,
			state,
			store_object_sender,
			store_process_sender,
			mut stream,
			verify_object_ids,
		} = arg;
		let state = &state;
		while let Some(message) = stream.next().await {
			match message {
				tg::sync::PutMessage::Node(tg::sync::PutNodeMessage::Group(message)) => {
					let message = tg::sync::PutNodeMessage::Group(message);
					self.sync_get_input_node(state, message).await?;
				},

				tg::sync::PutMessage::Node(tg::sync::PutNodeMessage::Object(message)) => {
					crate::checkpoint!(self.server, "sync.get.input.object", id = %message.id)
						.await;

					// Deserialize the object.
					let data = if self.sync_get_checkout_pointers_enabled()
						&& message.id.kind() == tg::object::Kind::Blob
						&& message.bytes.first() == Some(&0)
					{
						let leaf = tg::blob::data::Leaf {
							bytes: bytes::Bytes::new(),
						};
						tg::object::Data::Blob(tg::blob::Data::Leaf(leaf))
					} else {
						tg::object::Data::deserialize(message.id.kind(), message.bytes.as_ref())?
					};

					if verify_object_ids {
						// Validate the ID.
						let actual = tg::object::Id::new(message.id.kind(), &message.bytes);
						if message.id != actual {
							return Err(tg::error!(
								expected = %message.id,
								actual = %actual,
								"invalid object id"
							));
						}
					}

					// Wait for the local availability check of a speculative root.
					let id = tg::Id::from(message.id.clone());
					let local_root = state.graph.lock().unwrap().local_roots.contains(&id);
					let present = if local_root {
						Some(state.wait_for_root_presence(&id).await)
					} else {
						None
					};

					// Update the graph with data and metadata.
					let metadata = message.metadata.clone();
					let put = uuid::Uuid::now_v7().into_bytes();
					let arg = UpdateObjectLocalArg {
						data: Some(&data),
						id: &message.id,
						marked: None,
						metadata,
						permissions: None,
						put: Some(put),
						requested: None,
						storage: None,
					};
					{
						let mut graph = state.graph.lock().unwrap();
						graph.update_object_local(arg);
						graph.update_checkout_object(&message.id, &data);
					}

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

					if present != Some(true) && eager {
						// Send to the index task.
						let node = super::index::ObjectNode {
							id: message.id.clone(),
							missing: false,
						};
						index_object_sender.send(node).await.map_err(|_| {
							tg::error!("failed to send the object to the index task")
						})?;
					} else if present != Some(true) {
						// Enqueue the children.
						Self::sync_get_enqueue_object_children(
							state,
							&message.id,
							&data,
							None,
							None,
						);
					}

					if self.sync_get_checkout_pointers_enabled()
						&& matches!(data, tg::object::Data::Blob(_))
					{
						// Send the blob to the checkout task.
						let id = message.id.unwrap_blob_ref().clone();
						let node = super::checkout::ObjectNode {
							bytes: Some(message.bytes),
							id,
							metadata: message.metadata,
							put,
						};
						checkout_sender.send(node).await.map_err(|_| {
							tg::error!("failed to send the blob to the checkout task")
						})?;
					} else {
						// Send the object directly to the store task.
						let transferred_bytes = u64::try_from(message.bytes.len()).unwrap();
						let node = super::store::ObjectNode {
							bytes: Some(message.bytes),
							checkout_pointer: None,
							id: message.id,
							length: None,
							metadata: message.metadata,
							put,
							storage: None,
							transferred_bytes,
						};
						store_object_sender.send(node).await.map_err(|_| {
							tg::error!("failed to send the object to the store task")
						})?;
					}
				},

				tg::sync::PutMessage::Node(tg::sync::PutNodeMessage::Organization(message)) => {
					let message = tg::sync::PutNodeMessage::Organization(message);
					self.sync_get_input_node(state, message).await?;
				},

				tg::sync::PutMessage::Node(tg::sync::PutNodeMessage::Process(message)) => {
					crate::checkpoint!(self.server, "sync.get.input.process", id = %message.id)
						.await;

					// Wait for the local availability check of a speculative root.
					let id = tg::Id::from(message.id.clone());
					let local_root = state.graph.lock().unwrap().local_roots.contains(&id);
					let present = if local_root {
						Some(state.wait_for_root_presence(&id).await)
					} else {
						None
					};
					let (eager, requested) = {
						let graph = state.graph.lock().unwrap();
						let requested = graph.get_process_requested(&message.id);
						let eager = requested.as_ref().map_or(
							!graph.local_roots.contains(&id) || state.arg.eager,
							|requested| requested.eager,
						);

						(eager, requested.is_some())
					};
					if present == Some(true) && !requested {
						continue;
					}
					let data: tg::process::Data = serde_json::from_slice(&message.bytes)
						.map_err(|error| tg::error!(!error, "failed to deserialize the process"))?;
					let data = data.without_location_and_tokens();
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
						storage: None,
					};
					state.graph.lock().unwrap().update_process_local(arg);

					// Check if all roots are available and close the queue if so.
					if state.graph.lock().unwrap().end_local() {
						state.queue.close();
					}

					if eager {
						// Send to the index task.
						let node = super::index::ProcessNode {
							id: message.id.clone(),
							missing: false,
						};
						index_process_sender.send(node).await.map_err(|_| {
							tg::error!("failed to send the process to the index task")
						})?;
					} else {
						// Enqueue the children as necessary.
						let availability = state
							.graph
							.lock()
							.unwrap()
							.get_process_local_availability(&message.id);
						Self::sync_get_enqueue_process_children(
							state,
							&message.id,
							&data,
							Some(&availability),
							None,
						);
					}

					// Send to the store task.
					let node = super::store::ProcessNode {
						id: message.id,
						bytes: bytes.into(),
						metadata: message.metadata,
					};
					store_process_sender
						.send(node)
						.await
						.map_err(|_| tg::error!("failed to send the process to the store task"))?;
				},

				tg::sync::PutMessage::Node(tg::sync::PutNodeMessage::Sandbox(message)) => {
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
					message.data.tokens.clear();
					let message = tg::sync::PutNodeMessage::Sandbox(message);
					self.sync_get_input_node(state, message).await?;
				},

				tg::sync::PutMessage::Node(tg::sync::PutNodeMessage::Tag(message)) => {
					let message = tg::sync::PutNodeMessage::Tag(message);
					self.sync_get_input_node(state, message).await?;
				},

				tg::sync::PutMessage::Node(tg::sync::PutNodeMessage::User(message)) => {
					let message = tg::sync::PutNodeMessage::User(message);
					self.sync_get_input_node(state, message).await?;
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
							let node = super::index::ProcessNode { id, missing: true };
							index_process_sender.send(node).await.map_err(|_| {
								tg::error!("failed to send the process to the index task")
							})?;
						},
						kind if kind.is_object() => {
							let id = id.try_into()?;
							if let Some(token) = message.token {
								state.graph.lock().unwrap().update_object_token(&id, token);
							}
							let node = super::index::ObjectNode { id, missing: true };
							index_object_sender.send(node).await.map_err(|_| {
								tg::error!("failed to send the object to the index task")
							})?;
						},
						_ => {
							return Err(tg::error!(%id, "failed to find the node"));
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

	async fn sync_get_input_node(
		&self,
		state: &State,
		message: tg::sync::PutNodeMessage,
	) -> tg::Result<()> {
		let (ancestor, id): (Option<(Option<tg::Id>, tg::Specifier)>, tg::Id) = match &message {
			tg::sync::PutNodeMessage::Group(message) => (
				Some((message.parent.clone(), message.specifier.clone())),
				message.id.clone().into(),
			),
			tg::sync::PutNodeMessage::Object(_) | tg::sync::PutNodeMessage::Process(_) => {
				return Err(tg::error!("invalid sync node kind"));
			},
			tg::sync::PutNodeMessage::Organization(message) => (
				Some((None, message.specifier.clone())),
				message.id.clone().into(),
			),
			tg::sync::PutNodeMessage::Sandbox(message) => (None, message.id.clone().into()),
			tg::sync::PutNodeMessage::Tag(message) => (
				Some((message.parent.clone(), message.specifier.clone())),
				message.id.clone().into(),
			),
			tg::sync::PutNodeMessage::User(message) => (
				Some((None, message.specifier.clone())),
				message.id.clone().into(),
			),
		};
		if let Some((parent, specifier)) = &ancestor {
			crate::checkpoint!(
				self.server,
				"sync.get.input.node.ancestor",
				id = %id,
				specifier = %specifier,
			)
			.await;
			self.sync_get_input_node_ancestor(state, parent.as_ref(), specifier)
				.await?;
		}
		{
			let mut graph = state.graph.lock().unwrap();
			if let Some((_, specifier)) = &ancestor {
				graph.resolve_local_selector(specifier, id.clone());
			}
			graph.update_node_local_message(message)?;
		}
		state.progress.increment_transferred_node(&id);
		if state.graph.lock().unwrap().end_local() {
			state.queue.close();
		}

		Ok(())
	}

	async fn sync_get_input_node_ancestor(
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
		if state.graph.lock().unwrap().has_local_node(parent)
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
			let message = tg::sync::GetMessage::Node(tg::sync::GetNodeMessage {
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
