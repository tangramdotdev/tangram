use {
	crate::{Session, sync::put::State},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		stream::{FuturesOrdered, FuturesUnordered},
	},
	std::{collections::BTreeSet, sync::Arc},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

pub struct ObjectNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::object::Id,
	pub kind: Option<crate::sync::queue::ObjectKind>,
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
		for node in &nodes {
			crate::checkpoint!(self.server, "sync.put.store.object", id = %node.id).await;
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
			.sync_put_store_get_object_batch(state, &objects, &permissions, state.arg.metadata)
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
				let update = crate::sync::graph::UpdateObjectLocalArg {
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
		state: &State,
		objects: &[tg::Referent<tg::object::Id>],
		permissions: &[tg::authorization::permission::Set],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::object::get::Output>>> {
		let outputs = self
			.try_get_object_batch_local(objects, permissions, metadata)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the objects locally"))?;
		let location: tg::location::Arg =
			tg::Location::Local(tg::location::Local::default()).into();
		let locations = self
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
					let tokens = &object.options.tokens;
					let local_future = async {
						if let Some(output) = self
							.try_get_with_sync_wait(
								tokens,
								tg::sync::control::ClientRequestArg::object(
									object.node.clone(),
									tg::authorization::permission::object::Set::NODE,
									Some(tg::object::Storage::default()),
								),
								|control| {
									let object = object.clone();
									let mut permissions = *permissions;
									if let Some(control) = &control {
										permissions.insert(control.permissions());
									}
									async move {
										if state
											.graph
											.lock()
											.unwrap()
											.object_remote_available(&object.node)
										{
											return Ok(Some(None));
										}
										if let Some(control) = control {
											state
												.graph
												.lock()
												.unwrap()
												.update_node_local_control_output(
													&object.node.clone().into(),
													&control,
												)?;
										}
										let objects = std::slice::from_ref(&object);
										let permissions = std::slice::from_ref(&permissions);
										let mut outputs = self
											.try_get_object_batch_local(
												objects,
												permissions,
												metadata,
											)
											.await?;
										Ok(outputs.pop().flatten().map(Some))
									}
								},
							)
							.await?
						{
							return Ok(output);
						}

						Ok(None)
					};
					let region_future = self.try_get_object_regions(
						&object.node,
						&regions,
						metadata,
						false,
						&object.options.tokens,
					);
					let mut futures = [local_future.boxed(), region_future.boxed()]
						.into_iter()
						.collect::<FuturesUnordered<_>>();
					let mut error = None;
					while let Some(result) = futures.next().await {
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
		state: &State,
		nodes: Vec<ProcessNode>,
	) -> tg::Result<()> {
		// Get the processes.
		for node in &nodes {
			crate::checkpoint!(self.server, "sync.put.store.process", id = %node.id).await;
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
			.sync_put_store_get_process_batch(state, &processes, &permissions, state.arg.metadata)
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
			Self::validate_process_data(&output.data)?;

			// Wait for a local log to be compacted, leaving an uncompacted remote log unset.
			if node.descendants
				&& state.arg.process_logs
				&& Self::process_log_needs_compaction(&output.data)
				&& output.location.as_ref().is_none_or(tg::Location::is_local)
			{
				let permission = tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::NodeLog,
				);
				let permissions = state
					.graph
					.lock()
					.unwrap()
					.process_local_permissions(&node.id);
				if !permissions.contains(permission) {
					return Err(tg::error!("unauthorized"));
				}

				self.server.index_inner().await?;
				output.data = self
					.server
					.try_get_process_local(&node.id, false)
					.await?
					.ok_or_else(
						|| tg::error!(process = %node.id, "failed to get the process after indexing"),
					)?
					.data;
				if Self::process_log_needs_compaction(&output.data) {
					return Err(
						tg::error!(process = %node.id, "the process log was not compacted"),
					);
				}
			}

			// Read the local children using the node permission already proven by the graph.
			let permission = tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Node,
			);
			if node.permissions.contains(permission)
				&& let Some(process) = self.server.index.try_get_process(&node.id).await?
			{
				self.set_process_children_from_index(
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
			}
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
						local_tokens: node.tokens.clone(),
						parent: Some(node.id.clone()),
						remote_tokens: tg::authorization::tokens::Entry::default(),
					});
				state.queue.enqueue_processes(nodes)?;
			}

			// Enqueue the command.
			if node.descendants && node.eager && state.arg.process_commands {
				for command in output.data.command.objects() {
					let node = crate::sync::queue::ObjectNode {
						descendants: true,
						eager: node.eager,
						id: command.node,
						kind: Some(crate::sync::queue::ObjectKind::Command),
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
									local_tokens: node.tokens.clone(),
									parent: Some(node.id.clone().into()),
									remote_tokens: tg::authorization::tokens::Entry::default(),
								});
						state.queue.enqueue_objects(nodes)?;
					},
					tg::Either::Right(id) => {
						let node = crate::sync::queue::ObjectNode {
							descendants: true,
							eager: node.eager,
							id: id.node.clone().into(),
							kind: Some(crate::sync::queue::ObjectKind::Error),
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
				&& state.arg.process_logs
				&& let Some(log) = output.data.log.clone()
			{
				let node = crate::sync::queue::ObjectNode {
					descendants: true,
					eager: node.eager,
					id: log.node.into(),
					kind: Some(crate::sync::queue::ObjectKind::Log),
					local_tokens: node.tokens.clone(),
					parent: Some(node.id.clone().into()),
					remote_tokens: tg::authorization::tokens::Entry::default(),
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
		state: &State,
		processes: &[tg::Referent<tg::process::Id>],
		permissions: &[tg::authorization::permission::Set],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let location: tg::location::Arg =
			tg::Location::Local(tg::location::Local::default()).into();
		let locations = self
			.locations(Some(&location))
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		let regions = locations.local.map_or_else(Vec::new, |local| local.regions);
		let outputs = std::iter::zip(processes, permissions)
			.map(|(process, permissions)| {
				let regions = regions.clone();
				async move {
					if let Some(output) = self
						.try_get_process_local_with_permissions(
							&process.node,
							*permissions,
							metadata,
						)
						.await?
					{
						return Ok(Some(output));
					}
					let request = tg::sync::control::ClientRequestArg::process(
						process.node.clone(),
						tg::authorization::permission::process::Set::NODE,
						Some(tg::process::Storage::default()),
					);
					let tokens = &process.options.tokens;
					self.sync_put_pending(state, process.node.clone().into())
						.await?;
					let local_future = async {
						if let Some(output) = self
							.try_get_with_sync_wait(tokens, request, |control| {
								let mut permissions = *permissions;
								async move {
									if state
										.graph
										.lock()
										.unwrap()
										.process_remote_available(&process.node)
									{
										return Ok(Some(None));
									}
									if let Some(control) = control {
										state
											.graph
											.lock()
											.unwrap()
											.update_node_local_control_output(
												&process.node.clone().into(),
												&control,
											)?;
										permissions.insert(control.permissions());
									}
									self.try_get_process_local_with_permissions(
										&process.node,
										permissions,
										metadata,
									)
									.await
									.map(|output| output.map(Some))
								}
							})
							.await?
						{
							return Ok(output);
						}

						Ok(None)
					};
					let region_future = self.try_get_process_regions(
						&process.node,
						&regions,
						metadata,
						false,
						&process.options.tokens,
						tg::process::Source::Auto,
					);
					let mut futures = [local_future.boxed(), region_future.boxed()]
						.into_iter()
						.collect::<FuturesUnordered<_>>();
					let mut error = None;
					while let Some(result) = futures.next().await {
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
