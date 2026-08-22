use {
	crate::{Session, sync::put::State},
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _},
	std::{collections::BTreeSet, sync::Arc},
	tangram_client::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ObjectNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::object::Id,
	pub kind: Option<crate::sync::queue::ObjectKind>,
	pub send: bool,
	pub token: Option<tg::authorization::Token>,
}

pub struct ProcessNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::process::Id,
	pub send: bool,
	pub token: Option<tg::authorization::Token>,
}

impl Session {
	#[tracing::instrument(err, level = "trace", name = "store", ret, skip_all)]
	pub(super) async fn sync_put_store(
		&self,
		state: Arc<State>,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectNode>,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessNode>,
	) -> tg::Result<()> {
		// Create the objects future.
		let object_batch_size = self.server.config.sync.put.store.object_batch_size;
		let object_batch_timeout = self.server.config.sync.put.store.object_batch_timeout;
		let object_concurrency = self.server.config.sync.put.store.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(object_receiver),
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |nodes| {
			let session = self.clone();
			let state = state.clone();
			async move { session.sync_put_store_object_batch(&state, nodes).await }
		});

		// Create the processes future.
		let process_batch_size = self.server.config.sync.put.store.process_batch_size;
		let process_batch_timeout = self.server.config.sync.put.store.process_batch_timeout;
		let process_concurrency = self.server.config.sync.put.store.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(process_receiver),
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |nodes| {
			let session = self.clone();
			let state = state.clone();
			async move {
				session
					.sync_put_store_process_batch(&state, nodes)
					.boxed()
					.await
			}
		});

		// Join the objects and processes futures.
		futures::try_join!(objects_future, processes_future)?;

		Ok(())
	}

	pub(super) async fn sync_put_store_object_batch(
		&self,
		state: &State,
		nodes: Vec<ObjectNode>,
	) -> tg::Result<()> {
		// Get the objects.
		let objects = nodes
			.iter()
			.map(|node| tg::Referent::with_node_and_token(node.id.clone(), node.token.clone()))
			.collect::<Vec<_>>();
		let outputs = self
			.try_get_object_batch_local_or_regions(&objects, state.arg.metadata)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the objects"))?;

		// Handle the objects.
		for (node, output) in std::iter::zip(nodes, outputs) {
			// If the object is missing, then send a missing message.
			let Some(mut output) = output else {
				if node.send {
					let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
						selector: tg::Selector::Id(node.id.clone().into()),
						token: None,
					});
					state.sender.send(Ok(message)).await.ok();
					state
						.graph
						.lock()
						.unwrap()
						.update_object_remote_missing(&node.id);
				}
				if node.descendants {
					state
						.graph
						.lock()
						.unwrap()
						.finish_object_remote_descendants(&node.id, node.eager);
				}
				state.queue.finish_node();
				continue;
			};

			// Deserialize the object and update the graph.
			let data = tg::object::Data::deserialize(node.id.kind(), output.bytes.clone())
				.map_err(|error| tg::error!(!error, "failed to deserialize the object"))?;
			if node.descendants {
				let update = crate::sync::graph::UpdateObjectLocalArg {
					data: Some(&data),
					id: &node.id,
					marked: None,
					metadata: None,
					permissions: None,
					requested: None,
					stored: None,
				};
				state.graph.lock().unwrap().update_object_local(update);
			}

			// Mask the metadata with the permissions already proven by the graph.
			if node.send
				&& let Some(metadata) = output.metadata.take()
			{
				let required = tg::authorization::permission::Set::from_permission(
					tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					),
				);
				let permissions = state
					.graph
					.lock()
					.unwrap()
					.get_object_local_authorization(&node.id, required)
					.permissions;
				output.metadata =
					Self::mask_object_metadata_with_permissions(metadata, permissions);
			}

			// Send the object.
			if node.send {
				let message = tg::sync::PutMessage::Node(tg::sync::PutNodeMessage::Object(
					tg::sync::PutNodeObjectMessage {
						id: node.id.clone(),
						bytes: output.bytes.clone(),
						metadata: output.metadata,
					},
				));
				state
					.sender
					.send(Ok(message))
					.await
					.map_err(|error| tg::error!(!error, "failed to send the put message"))?;
				state
					.graph
					.lock()
					.unwrap()
					.update_object_remote_sent(&node.id);
			}

			// Enqueue the children.
			if node.descendants && node.eager {
				let mut children = BTreeSet::new();
				data.children(&mut children);
				let nodes = children
					.into_iter()
					.map(|child| crate::sync::queue::ObjectNode {
						descendants: true,
						eager: node.eager,
						id: child,
						kind: node.kind,
						parent: Some(node.id.clone().into()),
						token: None,
					});
				state.queue.enqueue_objects(nodes)?;
			}
			if node.descendants {
				state
					.graph
					.lock()
					.unwrap()
					.finish_object_remote_descendants(&node.id, node.eager);
			}
			state.queue.finish_node();
		}

		state.queue.close_if_end();

		Ok(())
	}

	pub(super) async fn sync_put_store_process_batch(
		&self,
		state: &State,
		nodes: Vec<ProcessNode>,
	) -> tg::Result<()> {
		// Get the processes.
		let processes = nodes
			.iter()
			.map(|node| tg::Referent::with_node_and_token(node.id.clone(), node.token.clone()))
			.collect::<Vec<_>>();
		let outputs = self
			.try_get_process_batch_local_or_regions(&processes, state.arg.metadata)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the processes"))?;

		// Handle the processes.
		for (node, output) in std::iter::zip(nodes, outputs) {
			let Some(mut output) = output else {
				if node.send {
					let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
						selector: tg::Selector::Id(node.id.clone().into()),
						token: None,
					});
					state.sender.send(Ok(message)).await.ok();
					state
						.graph
						.lock()
						.unwrap()
						.update_process_remote_missing(&node.id);
				}
				if node.descendants {
					state
						.graph
						.lock()
						.unwrap()
						.finish_process_remote_descendants(&node.id, node.eager);
				}
				state.queue.finish_node();
				continue;
			};

			// Compact the log if needed before sending the process data.
			if node.descendants && state.arg.process_logs && output.data.log.is_none() {
				let permission = tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::NodeLog,
				);
				let required = tg::authorization::permission::Set::from_permission(permission);
				let permissions = state
					.graph
					.lock()
					.unwrap()
					.get_process_local_authorization(&node.id, required)
					.permissions;
				if !permissions.contains(permission) {
					return Err(tg::error!("unauthorized"));
				}

				// Compact.
				self.compact_process_log(&node.id).boxed().await.map_err(
					|error| tg::error!(!error, process = %node.id, "failed to compact the log"),
				)?;

				// Get the compacted process data from the index.
				output.data = self
					.server
					.try_get_process_local(&node.id, false)
					.await?
					.ok_or_else(
						|| tg::error!(process = %node.id, "failed to get the process after compaction"),
					)?
					.data;
			}

			// Validate the process before waiting for all of its children.
			Self::validate_process_data(&output.data)?;

			// Load the children.
			let arg = tg::process::children::get::Arg {
				location: output.location.clone().map(Into::into),
				tokens: tg::authorization::Tokens::with_local(node.token.clone()),
				..Default::default()
			};
			let children = self
				.try_get_process_children(&node.id, arg)
				.await?
				.ok_or_else(
					|| tg::error!(process = %node.id, "failed to get the process children"),
				)?
				.map_ok(|chunk| futures::stream::iter(chunk.data).map(Ok::<_, tg::Error>))
				.try_flatten()
				.try_collect()
				.await?;
			output.data.children = Some(children);
			Self::validate_process_data(&output.data)?;

			// Update the graph.
			if node.descendants {
				let update = crate::sync::graph::UpdateProcessLocalArg {
					data: Some(&output.data),
					id: &node.id,
					marked: None,
					metadata: None,
					permissions: None,
					requested: None,
					stored: None,
				};
				state.graph.lock().unwrap().update_process_local(update);
			}

			// Mask the metadata with the permissions already proven by the graph.
			if node.send
				&& let Some(metadata) = output.metadata.take()
			{
				let required = tg::authorization::permission::Set::Process(
					tg::authorization::permission::process::Set::all(),
				);
				let permissions = state
					.graph
					.lock()
					.unwrap()
					.get_process_local_authorization(&node.id, required)
					.permissions;
				output.metadata =
					Self::mask_process_metadata_with_permissions(&metadata, permissions);
			}

			// Send the process.
			if node.send {
				let bytes = serde_json::to_string(&output.data)
					.map_err(|error| tg::error!(!error, "failed to serialize the process"))?;
				let message = tg::sync::PutMessage::Node(tg::sync::PutNodeMessage::Process(
					tg::sync::PutNodeProcessMessage {
						id: node.id.clone(),
						bytes: bytes.into(),
						metadata: output.metadata,
					},
				));
				state
					.sender
					.send(Ok(message))
					.await
					.map_err(|error| tg::error!(!error, "failed to send the put message"))?;
				state
					.graph
					.lock()
					.unwrap()
					.update_process_remote_sent(&node.id);
			}

			// Enqueue the children.
			if node.descendants && state.arg.process_children && node.eager {
				let children = output
					.data
					.children
					.as_ref()
					.ok_or_else(|| tg::error!("expected the children to be set"))?;
				let nodes = children
					.iter()
					.map(|child| crate::sync::queue::ProcessNode {
						descendants: true,
						eager: node.eager,
						id: child.process.node.clone(),
						parent: Some(node.id.clone()),
						token: None,
					});
				state.queue.enqueue_processes(nodes)?;
			}

			// Enqueue the command.
			if node.descendants && node.eager && state.arg.process_commands {
				let node = crate::sync::queue::ObjectNode {
					descendants: true,
					eager: node.eager,
					id: output.data.command.node.clone().into(),
					kind: Some(crate::sync::queue::ObjectKind::Command),
					parent: Some(node.id.clone().into()),
					token: None,
				};
				state.queue.enqueue_object(node)?;
			}

			// Enqueue the error.
			if node.descendants
				&& node.eager
				&& state.arg.process_errors
				&& let Some(error) = &output.data.error
			{
				match error {
					tg::Either::Left(data) => {
						let mut children = BTreeSet::new();
						data.children(&mut children);
						let nodes =
							children
								.into_iter()
								.map(|child| crate::sync::queue::ObjectNode {
									descendants: true,
									eager: node.eager,
									id: child,
									kind: Some(crate::sync::queue::ObjectKind::Error),
									parent: Some(node.id.clone().into()),
									token: None,
								});
						state.queue.enqueue_objects(nodes)?;
					},
					tg::Either::Right(id) => {
						let node = crate::sync::queue::ObjectNode {
							descendants: true,
							eager: node.eager,
							id: id.node.clone().into(),
							kind: Some(crate::sync::queue::ObjectKind::Error),
							parent: Some(node.id.clone().into()),
							token: None,
						};
						state.queue.enqueue_object(node)?;
					},
				}
			}

			// Enqueue the log.
			if node.descendants
				&& node.eager
				&& state.arg.process_logs
				&& let Some(log) = output.data.log.clone()
			{
				let node = crate::sync::queue::ObjectNode {
					descendants: true,
					eager: node.eager,
					id: log.node.into(),
					kind: Some(crate::sync::queue::ObjectKind::Log),
					parent: Some(node.id.clone().into()),
					token: None,
				};
				state.queue.enqueue_object(node)?;
			}

			// Enqueue the outputs.
			if node.descendants
				&& node.eager
				&& state.arg.process_outputs
				&& let Some(output) = &output.data.output
			{
				let mut children = BTreeSet::new();
				output.children(&mut children);
				let nodes = children
					.into_iter()
					.map(|child| crate::sync::queue::ObjectNode {
						descendants: true,
						eager: node.eager,
						id: child,
						kind: Some(crate::sync::queue::ObjectKind::Output),
						parent: Some(node.id.clone().into()),
						token: None,
					});
				state.queue.enqueue_objects(nodes)?;
			}
			if node.descendants {
				state
					.graph
					.lock()
					.unwrap()
					.finish_process_remote_descendants(&node.id, node.eager);
			}
			state.queue.finish_node();
		}

		state.queue.close_if_end();

		Ok(())
	}
}
