use {
	crate::{Component, put::State},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		stream::{FuturesOrdered, FuturesUnordered},
	},
	std::{collections::BTreeSet, sync::Arc},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_server_core::Session,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ObjectNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::object::Id,
	pub kind: Option<crate::queue::ObjectKind>,
	pub permissions: tg::authorization::permission::Set,
	pub send: bool,
	pub tokens: tg::authorization::tokens::Entry,
}

pub struct ProcessNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::process::Id,
	pub permissions: tg::authorization::permission::Set,
	pub send: bool,
	pub tokens: tg::authorization::tokens::Entry,
}

impl Component {
	#[tracing::instrument(err, level = "trace", name = "store", ret, skip_all)]
	pub(super) async fn sync_put_store(
		&self,
		session: &Session,
		state: Arc<State>,
		object_receiver: tokio::sync::mpsc::Receiver<ObjectNode>,
		process_receiver: tokio::sync::mpsc::Receiver<ProcessNode>,
	) -> tg::Result<()> {
		// Create the objects future.
		let object_batch_size = session.server.config.sync.put.store.object_batch_size;
		let object_batch_timeout = session.server.config.sync.put.store.object_batch_timeout;
		let object_concurrency = session.server.config.sync.put.store.object_concurrency;
		let objects_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(object_receiver),
			object_batch_size,
			object_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(object_concurrency, |nodes| {
			let component = self.clone();
			let session = session.clone();
			let state = state.clone();
			async move {
				component
					.sync_put_store_object_batch(&session, &state, nodes)
					.await
			}
		});

		// Create the processes future.
		let process_batch_size = session.server.config.sync.put.store.process_batch_size;
		let process_batch_timeout = session.server.config.sync.put.store.process_batch_timeout;
		let process_concurrency = session.server.config.sync.put.store.process_concurrency;
		let processes_future = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(process_receiver),
			process_batch_size,
			process_batch_timeout,
		)
		.map(Ok)
		.try_for_each_concurrent(process_concurrency, |nodes| {
			let component = self.clone();
			let session = session.clone();
			let state = state.clone();
			async move {
				component
					.sync_put_store_process_batch(&session, &state, nodes)
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
		session: &Session,
		state: &State,
		nodes: Vec<ObjectNode>,
	) -> tg::Result<()> {
		// Get the objects.
		for node in &nodes {
			tangram_server_core::checkpoint!(session.server, "sync.put.store.object", id = %node.id)
				.await;
		}
		let objects = nodes
			.iter()
			.map(|node| {
				let tokens = tg::authorization::Tokens::with_local_entry(node.tokens.clone());
				tg::Referent::with_node_and_tokens(node.id.clone(), tokens)
			})
			.collect::<Vec<_>>();
		let permissions = nodes
			.iter()
			.map(|node| node.permissions)
			.collect::<Vec<_>>();
		let outputs = self
			.sync_put_store_get_object_batch(
				session,
				state,
				&objects,
				&permissions,
				state.arg.metadata,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the objects"))?;

		// Handle the objects.
		for (node, output) in std::iter::zip(nodes, outputs) {
			if state
				.graph
				.lock()
				.unwrap()
				.object_remote_available(&node.id)
			{
				state.queue.finish_node();
				continue;
			}
			// If the object is missing, then send a missing message.
			let Some(mut output) = output else {
				if node.send {
					let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
						selector: tg::Selector::Id(node.id.clone().into()),
						tokens: Vec::new(),
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
				let update = crate::graph::UpdateObjectLocalArg {
					data: Some(&data),
					id: &node.id,
					marked: None,
					metadata: None,
					permissions: None,
					put: None,
					requested: None,
					storage: None,
				};
				state.graph.lock().unwrap().update_object_local(update);
			}

			// Mask the metadata with the permissions already proven by the graph.
			if node.send
				&& let Some(metadata) = output.metadata.take()
			{
				let permissions = state
					.graph
					.lock()
					.unwrap()
					.object_local_permissions(&node.id);
				output.metadata =
					tangram_server_object_common::mask_object_metadata_with_permissions(
						metadata,
						permissions,
					);
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
				let nodes = children.into_iter().map(|child| crate::queue::ObjectNode {
					descendants: true,
					eager: node.eager,
					id: child,
					kind: node.kind,
					local_tokens: node.tokens.clone(),
					parent: Some(node.id.clone().into()),
					remote_tokens: tg::authorization::tokens::Entry::default(),
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

	async fn sync_put_store_get_object_batch(
		&self,
		session: &Session,
		state: &State,
		objects: &[tg::Referent<tg::object::Id>],
		permissions: &[tg::authorization::permission::Set],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::object::get::Output>>> {
		let outputs = session
			.server
			.components
			.object
			.try_get_object_batch_local(session, objects, permissions, metadata)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the objects locally"))?;
		let location: tg::location::Arg =
			tg::Location::Local(tg::location::Local::default()).into();
		let locations = session
			.locations(Some(&location))
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		let regions = locations.local.map_or_else(Vec::new, |local| local.regions);
		let outputs = std::iter::zip(std::iter::zip(objects, permissions), outputs)
			.map(|((object, permissions), output)| {
				let regions = regions.clone();
				async move {
					if let Some(output) = output {
						return Ok(Some(output));
					}

					// Wait for an incoming sync to supply the missing node.
					self.sync_put_pending(state, object.node.clone().into())
						.await?;
					let local_future = async {
						let required = tg::authorization::permission::Set::Object(
							tg::authorization::permission::object::Set::NODE,
						);
						let storage = tg::storage::Set::Object(tg::object::storage::Set::NODE);
						let args = [(object.clone(), required, Some(*permissions), storage)];
						let mut outputs = session
							.verify_batch_inner(
								args,
								None,
								false,
								state
									.arg
									.sync
									.as_ref()
									.and_then(tg::sync::Join::sync)
									.map(|sync| &sync.node),
							)
							.await?;
						let output = outputs.pop().unwrap();
						if !output.permissions.contains(required) {
							output.check_exhaustion()?;
							return Ok(None);
						}

						// The destination can supply the node if local sync discovery exhausts.
						if output.outcome != tangram_server_core::authorization::Outcome::Satisfied
						{
							return Ok(None);
						}
						{
							let mut graph = state.graph.lock().unwrap();
							if graph.object_remote_available(&object.node) {
								return Ok(None);
							}
							graph
								.update_node_local_verified(&object.node.clone().into(), &output)?;
						}
						let mut outputs = session
							.server
							.components
							.object
							.try_get_object_batch_local(
								session,
								std::slice::from_ref(object),
								std::slice::from_ref(permissions),
								metadata,
							)
							.await?;
						Ok(outputs.pop().flatten())
					};
					let region_future = session.server.components.object.try_get_object_regions(
						session,
						&object.node,
						&regions,
						metadata,
						false,
						&object.options.tokens,
					);
					let mut futures = [local_future.boxed(), region_future.boxed()]
						.into_iter()
						.collect::<FuturesUnordered<_>>();
					let available =
						state.wait_remote(|graph| graph.object_remote_available(&object.node));
					tokio::pin!(available);
					let mut error = None;
					while let Some(result) = tokio::select! {
						biased;
						() = &mut available => return Ok(None),
						result = futures.next() => result,
					} {
						match result {
							Ok(Some(output)) => return Ok(Some(output)),
							Ok(None) => {},
							Err(source) => error = Some(source),
						}
					}
					error.map_or(Ok(None), Err)
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		Ok(outputs)
	}

	pub(super) async fn sync_put_store_process_batch(
		&self,
		session: &Session,
		state: &State,
		nodes: Vec<ProcessNode>,
	) -> tg::Result<()> {
		// Get the processes.
		for node in &nodes {
			tangram_server_core::checkpoint!(session.server, "sync.put.store.process", id = %node.id)
				.await;
		}
		let processes = nodes
			.iter()
			.map(|node| {
				let tokens = tg::authorization::Tokens::with_local_entry(node.tokens.clone());
				tg::Referent::with_node_and_tokens(node.id.clone(), tokens)
			})
			.collect::<Vec<_>>();
		let permissions = nodes
			.iter()
			.map(|node| node.permissions)
			.collect::<Vec<_>>();
		let outputs = self
			.sync_put_store_get_process_batch(
				session,
				state,
				&processes,
				&permissions,
				state.arg.metadata,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the processes"))?;

		// Handle the processes.
		for (node, output) in std::iter::zip(nodes, outputs) {
			if state
				.graph
				.lock()
				.unwrap()
				.process_remote_available(&node.id)
			{
				state.queue.finish_node();
				continue;
			}
			let Some(mut output) = output else {
				if node.send {
					let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
						selector: tg::Selector::Id(node.id.clone().into()),
						tokens: Vec::new(),
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

			// Validate the process before waiting for all of its children.
			tangram_server_process_common::validate_process_data(&output.data)?;

			// Read the local children using the node permission already proven by the graph.
			let permission = tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Node,
			);
			if node.permissions.contains(permission)
				&& let Some(process) = session.server.index.try_get_process(&node.id).await?
			{
				session
					.server
					.components
					.process
					.set_process_children_from_index(
						session,
						&node.id,
						process.set.children,
						&mut output.data,
					)
					.await?;
			}

			// Load any children that are not stored locally.
			if output.data.children.is_none() {
				let arg = tg::process::children::get::Arg {
					location: output.location.clone().map(Into::into),
					tokens: tg::authorization::Tokens::with_local_entry(node.tokens.clone()),
					..Default::default()
				};
				let children = session
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
			}
			tangram_server_process_common::validate_process_data(&output.data)?;

			// Update the graph.
			if node.descendants {
				let update = crate::graph::UpdateProcessLocalArg {
					data: Some(&output.data),
					id: &node.id,
					marked: None,
					metadata: None,
					permissions: None,
					requested: None,
					storage: None,
				};
				state.graph.lock().unwrap().update_process_local(update);
			}

			// Mask the metadata with the permissions already proven by the graph.
			if node.send
				&& let Some(metadata) = output.metadata.take()
			{
				let permissions = state
					.graph
					.lock()
					.unwrap()
					.process_local_permissions(&node.id);
				output.metadata =
					tangram_server_process_common::mask_process_metadata_with_permissions(
						&metadata,
						permissions,
					);
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
				let nodes = children.iter().map(|child| crate::queue::ProcessNode {
					descendants: true,
					eager: node.eager,
					id: child.process.node.clone(),
					local_tokens: node.tokens.clone(),
					parent: Some(node.id.clone()),
					remote_tokens: tg::authorization::tokens::Entry::default(),
				});
				state.queue.enqueue_processes(nodes)?;
			}

			// Enqueue the command.
			if node.descendants && node.eager && state.arg.process_command_objects {
				for command in output.data.command.objects() {
					let node = crate::queue::ObjectNode {
						descendants: true,
						eager: node.eager,
						id: command.node,
						kind: Some(crate::queue::ObjectKind::Command),
						local_tokens: node.tokens.clone(),
						parent: Some(node.id.clone().into()),
						remote_tokens: tg::authorization::tokens::Entry::default(),
					};
					state.queue.enqueue_object(node)?;
				}
			}

			// Enqueue the error.
			if node.descendants
				&& node.eager
				&& state.arg.process_error_objects
				&& let Some(error) = &output.data.error
			{
				match &error.node {
					tg::Either::Left(data) => {
						let mut children = BTreeSet::new();
						data.children(&mut children);
						let nodes = children.into_iter().map(|child| crate::queue::ObjectNode {
							descendants: true,
							eager: node.eager,
							id: child,
							kind: Some(crate::queue::ObjectKind::Error),
							local_tokens: node.tokens.clone(),
							parent: Some(node.id.clone().into()),
							remote_tokens: tg::authorization::tokens::Entry::default(),
						});
						state.queue.enqueue_objects(nodes)?;
					},
					tg::Either::Right(id) => {
						let node = crate::queue::ObjectNode {
							descendants: true,
							eager: node.eager,
							id: id.clone().into(),
							kind: Some(crate::queue::ObjectKind::Error),
							local_tokens: node.tokens.clone(),
							parent: Some(node.id.clone().into()),
							remote_tokens: tg::authorization::tokens::Entry::default(),
						};
						state.queue.enqueue_object(node)?;
					},
				}
			}

			// Enqueue the log.
			if node.descendants
				&& node.eager
				&& state.arg.process_log_objects
				&& let Some(log) = output.data.log.clone()
			{
				let node = crate::queue::ObjectNode {
					descendants: true,
					eager: node.eager,
					id: log.node.into(),
					kind: Some(crate::queue::ObjectKind::Log),
					local_tokens: node.tokens.clone(),
					parent: Some(node.id.clone().into()),
					remote_tokens: tg::authorization::tokens::Entry::default(),
				};
				state.queue.enqueue_object(node)?;
			}

			// Enqueue the outputs.
			if node.descendants
				&& node.eager
				&& state.arg.process_output_objects
				&& let Some(output) = &output.data.output
			{
				let mut children = BTreeSet::new();
				output.node.children(&mut children);
				let nodes = children.into_iter().map(|child| crate::queue::ObjectNode {
					descendants: true,
					eager: node.eager,
					id: child,
					kind: Some(crate::queue::ObjectKind::Output),
					local_tokens: node.tokens.clone(),
					parent: Some(node.id.clone().into()),
					remote_tokens: tg::authorization::tokens::Entry::default(),
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

	async fn sync_put_store_get_process_batch(
		&self,
		session: &Session,
		state: &State,
		processes: &[tg::Referent<tg::process::Id>],
		permissions: &[tg::authorization::permission::Set],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let location: tg::location::Arg =
			tg::Location::Local(tg::location::Local::default()).into();
		let locations = session
			.locations(Some(&location))
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		let regions = locations.local.map_or_else(Vec::new, |local| local.regions);
		let outputs = std::iter::zip(processes, permissions)
			.map(|(process, permissions)| {
				let regions = regions.clone();
				async move {
					if let Some(output) = session
						.server
						.components
						.process
						.try_get_process_local_with_permissions(
							session,
							&process.node,
							*permissions,
							metadata,
						)
						.await?
					{
						return Ok(Some(output));
					}
					self.sync_put_pending(state, process.node.clone().into())
						.await?;
					let local_future = async {
						let required = tg::authorization::permission::Set::Process(
							tg::authorization::permission::process::Set::NODE,
						);
						let storage = tg::storage::Set::Process(tg::process::storage::Set::NODE);
						let args = [(process.clone(), required, Some(*permissions), storage)];
						let mut outputs = session
							.verify_batch_inner(
								args,
								None,
								false,
								state
									.arg
									.sync
									.as_ref()
									.and_then(tg::sync::Join::sync)
									.map(|sync| &sync.node),
							)
							.await?;
						let output = outputs.pop().unwrap();
						if !output.permissions.contains(required) {
							output.check_exhaustion()?;
							return Ok(None);
						}

						// The destination can supply the node if local sync discovery exhausts.
						if output.outcome != tangram_server_core::authorization::Outcome::Satisfied
						{
							return Ok(None);
						}
						{
							let mut graph = state.graph.lock().unwrap();
							if graph.process_remote_available(&process.node) {
								return Ok(None);
							}
							graph.update_node_local_verified(
								&process.node.clone().into(),
								&output,
							)?;
						}
						session
							.server
							.components
							.process
							.try_get_process_local_with_permissions(
								session,
								&process.node,
								*permissions,
								metadata,
							)
							.await
					};
					let arg = tangram_server_core::process::TryGetProcessRegionsArg {
						availability: false,
						id: &process.node,
						metadata,
						regions: &regions,
						source: tg::process::Source::Auto,
						tokens: &process.options.tokens,
					};
					let region_future = session
						.server
						.components
						.process
						.try_get_process_regions(session, arg);
					let mut futures = [local_future.boxed(), region_future.boxed()]
						.into_iter()
						.collect::<FuturesUnordered<_>>();
					let available =
						state.wait_remote(|graph| graph.process_remote_available(&process.node));
					tokio::pin!(available);
					let mut error = None;
					while let Some(result) = tokio::select! {
						biased;
						() = &mut available => return Ok(None),
						result = futures.next() => result,
					} {
						match result {
							Ok(Some(output)) => return Ok(Some(output)),
							Ok(None) => {},
							Err(source) => error = Some(source),
						}
					}
					error.map_or(Ok(None), Err)
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		Ok(outputs)
	}
}
