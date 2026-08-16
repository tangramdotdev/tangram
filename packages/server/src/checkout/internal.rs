use {
	crate::{Server, Session, temp::Temp},
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream},
	itertools::Itertools as _,
	num::ToPrimitive as _,
	std::{
		collections::HashSet,
		os::unix::fs::PermissionsExt as _,
		panic::AssertUnwindSafe,
		path::{Path, PathBuf},
		pin::pin,
	},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::{Ext as _, TryExt as _},
		task::Task,
	},
	tangram_index::prelude::*,
	tangram_util::read::InspectReader,
};

pub type Tasks = tangram_futures::task::Map<
	tg::artifact::Id,
	tg::Result<()>,
	crate::progress::Handle<()>,
	tg::id::BuildHasher,
>;

pub type GraphTasks = tangram_futures::task::Map<
	tg::graph::Id,
	tg::Result<()>,
	crate::progress::Handle<()>,
	tg::id::BuildHasher,
>;

struct State {
	artifact: tg::artifact::Id,
	path: PathBuf,
	progress: crate::progress::Handle<()>,
	visiting: HashSet<tg::artifact::Id, tg::id::BuildHasher>,
}

#[derive(Clone)]
pub struct Item {
	pub id: tg::artifact::Id,
	pub node: tg::graph::data::Node,
	pub graph: Option<tg::graph::Id>,
}

impl Session {
	pub(crate) async fn checkout_internal(
		&self,
		artifacts: Vec<tg::Referent<tg::artifact::Id>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + use<>> {
		if artifacts.is_empty() {
			return Ok(stream::once(future::ok(tg::progress::Event::Output(()))).left_stream());
		}

		let progress = crate::progress::Handle::new();
		let task = Task::spawn({
			let session = self.clone();
			let progress = progress.clone();
			|_| async move {
				// Ensure the artifacts are stored and authorized.
				let result = session
					.checkout_internal_ensure_stored_and_authorized(&artifacts, &progress)
					.await
					.map_err(|error| {
						tg::error!(
							!error,
							"failed to ensure the artifacts are stored and authorized"
						)
					});
				if let Err(error) = result {
					tracing::warn!(error = %error.trace());
					progress.error(error);
					return;
				}

				// Create the progress indicators.
				progress.spinner("checkout", "checkout");
				progress.start(
					"artifacts".to_owned(),
					"artifacts".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					None,
				);
				progress.start(
					"bytes".to_owned(),
					"bytes".to_owned(),
					tg::progress::IndicatorFormat::Bytes,
					Some(0),
					None,
				);

				let result = future::try_join_all(artifacts.into_iter().map({
					|artifact| {
						let session = session.clone();
						let progress = progress.clone();
						let artifact = artifact.node;
						async move {
							AssertUnwindSafe(session.checkout_internal_task(&artifact, &progress))
								.catch_unwind()
								.await
						}
					}
				}))
				.await
				.map(|results| results.into_iter().try_collect::<_, (), _>());

				progress.finish_all();

				match result {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						progress.error(tg::error!(?message, "the task panicked"));
					},
				}
			}
		});

		let stream = progress.stream().attach(task).right_stream();

		Ok(stream)
	}

	pub(crate) async fn checkout_index_barrier(&self) -> tg::Result<()> {
		if self.server.vfs.lock().unwrap().is_some() {
			return Ok(());
		}
		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

		Ok(())
	}

	pub(super) async fn checkout_named_nodes(
		&self,
		id: &tg::Id,
	) -> tg::Result<Vec<super::NamedNode>> {
		let mut current = Some(id.clone());
		let mut ids = HashSet::with_hasher(tg::id::BuildHasher);
		let mut nodes = Vec::new();
		while let Some(id) = current {
			if !ids.insert(id.clone()) {
				return Err(tg::error!(%id, "the named node hierarchy contains a cycle"));
			}
			let node =
				match id.kind() {
					tg::id::Kind::Group => {
						let id = tg::group::Id::try_from(id)?;
						let group = self.server.index.try_get_group(&id).await?.ok_or_else(
							|| tg::error!(%id, "the group was not found in the index"),
						)?;
						super::NamedNode {
							id: id.into(),
							parent: group.parent,
							specifier: group.specifier,
							target: None,
						}
					},
					tg::id::Kind::Organization => {
						let id = tg::organization::Id::try_from(id)?;
						let organization = self
							.server
							.index
							.try_get_organization(&id)
							.await?
							.ok_or_else(
								|| tg::error!(%id, "the organization was not found in the index"),
							)?;
						super::NamedNode {
							id: id.into(),
							parent: None,
							specifier: organization.specifier,
							target: None,
						}
					},
					tg::id::Kind::Tag => {
						let id = tg::tag::Id::try_from(id)?;
						let tag =
							self.server.index.try_get_tag(&id).await?.ok_or_else(
								|| tg::error!(%id, "the tag was not found in the index"),
							)?;
						super::NamedNode {
							id: id.into(),
							parent: tag.parent,
							specifier: tag.specifier,
							target: Some(tag.target),
						}
					},
					tg::id::Kind::User => {
						let id = tg::user::Id::try_from(id)?;
						let user = self.server.index.try_get_user(&id).await?.ok_or_else(
							|| tg::error!(%id, "the user was not found in the index"),
						)?;
						super::NamedNode {
							id: id.into(),
							parent: None,
							specifier: user.specifier,
							target: None,
						}
					},
					_ => return Err(tg::error!(%id, "the node is not named")),
				};
			current = node.parent.clone();
			nodes.push(node);
		}
		nodes.reverse();

		Ok(nodes)
	}

	pub(super) fn named_checkout_path(
		&self,
		node: &super::NamedNode,
		suffix: Option<&str>,
	) -> PathBuf {
		self.server
			.store_path()
			.join(Server::named_checkout_relative_path(node, suffix))
	}

	pub(super) async fn materialize_named_checkout(
		&self,
		expected: &[super::NamedNode],
		artifact: Option<&tg::artifact::Id>,
		suffix: Option<&str>,
	) -> tg::Result<()> {
		if self.server.vfs.lock().unwrap().is_some() {
			return Ok(());
		}
		for attempt in 0..2 {
			let guard = self.server.checkout_lock.lock().await;
			if self.server.vfs.lock().unwrap().is_some() {
				return Ok(());
			}
			let actual = self
				.checkout_named_nodes(&expected.last().unwrap().id)
				.await?;
			if actual == expected {
				let touched_at = self.server.clock.unix_timestamp()?;
				let mut batch = tangram_index::batch::Arg::default();
				for node in expected {
					if node.id.kind() == tg::id::Kind::Tag {
						// Replace the tag checkout so a changed target does not retain its old dependency.
						batch
							.items
							.push(tangram_index::batch::Item::DeleteCheckout(node.id.clone()));
					}
					let mut dependencies = node.parent.iter().cloned().collect::<Vec<_>>();
					if node.id.kind() == tg::id::Kind::Tag {
						let artifact = artifact.ok_or_else(
							|| tg::error!(id = %node.id, "the tag does not resolve to an artifact"),
						)?;
						dependencies.push(artifact.clone().into());
					}
					batch.items.push(tangram_index::batch::Item::PutCheckout(
						tangram_index::checkout::put::Arg {
							dependencies,
							id: node.id.clone(),
							touched_at,
						},
					));
				}
				self.server.index.batch(batch).await?;
				self.server
					.materialize_named_checkout_entries_with_lock(
						&guard, expected, artifact, suffix,
					)
					.await?;
				drop(guard);

				return Ok(());
			}
			drop(guard);
			if attempt == 0 {
				self.checkout_index_barrier().await?;
			}
		}

		Err(tg::error!(
			id = %expected.last().unwrap().id,
			"the named node changed while it was checked out"
		))
	}

	async fn checkout_internal_ensure_stored_and_authorized(
		&self,
		artifacts: &[tg::Referent<tg::artifact::Id>],
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		let ids = artifacts
			.iter()
			.map(|artifact| artifact.node.clone().into())
			.collect::<Vec<_>>();
		let stored = self
			.server
			.index
			.try_get_objects(&ids)
			.await?
			.iter()
			.all(|object| object.as_ref().is_some_and(|object| object.stored.subtree));
		if stored {
			let permission = tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			);
			let args = artifacts
				.iter()
				.map(|artifact| {
					(
						artifact.clone(),
						tg::authorization::permission::Set::from_permission(permission),
					)
				})
				.collect::<Vec<_>>();
			let authorized = self.authorize_batch(args).await?;
			if authorized
				.into_iter()
				.all(|output| output.is_some_and(|permissions| permissions.contains(permission)))
			{
				return Ok(());
			}
		}

		// Index.
		let stream = self
			.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to start the index"))?;
		let mut stream = pin!(stream);
		while let Some(event) = stream
			.try_next()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the next index event"))?
		{
			progress.forward(Ok(event));
		}

		let ids = artifacts
			.iter()
			.map(|artifact| artifact.node.clone().into())
			.collect::<Vec<_>>();
		let stored = self
			.server
			.index
			.try_get_objects(&ids)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to check if the artifacts are stored and authorized"
				)
			})?
			.iter()
			.all(|object| object.as_ref().is_some_and(|object| object.stored.subtree));
		if stored {
			let permission = tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			);
			let args = artifacts
				.iter()
				.map(|artifact| {
					(
						artifact.clone(),
						tg::authorization::permission::Set::from_permission(permission),
					)
				})
				.collect::<Vec<_>>();
			let authorized = self.authorize_batch(args).await?;
			if authorized
				.into_iter()
				.all(|output| output.is_some_and(|permissions| permissions.contains(permission)))
			{
				return Ok(());
			}
		}

		// Pull.
		let stream = self
			.pull(tg::pull::Arg {
				nodes: artifacts
					.iter()
					.cloned()
					.map(|artifact| artifact.map(tg::Id::from))
					.collect(),
				..Default::default()
			})
			.await
			.ok();
		if let Some(stream) = stream {
			progress.spinner("pull", "pull");
			let mut stream = pin!(stream);
			while let Some(event) = stream.try_next().await.ok().flatten() {
				progress.forward(Ok(event));
			}
		}

		// Index.
		let stream = self
			.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to start the index"))?;
		let mut stream = pin!(stream);
		while let Some(event) = stream
			.try_next()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the next index event"))?
		{
			progress.forward(Ok(event));
		}

		let ids = artifacts
			.iter()
			.map(|artifact| artifact.node.clone().into())
			.collect::<Vec<_>>();
		let stored = self
			.server
			.index
			.try_get_objects(&ids)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to check if the artifacts are stored and authorized"
				)
			})?
			.iter()
			.all(|object| object.as_ref().is_some_and(|object| object.stored.subtree));
		if stored {
			let permission = tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			);
			let args = artifacts
				.iter()
				.map(|artifact| {
					(
						artifact.clone(),
						tg::authorization::permission::Set::from_permission(permission),
					)
				})
				.collect::<Vec<_>>();
			let authorized = self.authorize_batch(args).await?;
			if authorized
				.into_iter()
				.all(|output| output.is_some_and(|permissions| permissions.contains(permission)))
			{
				progress.finish_all();
				return Ok(());
			}
		}

		progress.finish_all();

		Err(tg::error!("failed to find the artifact"))
	}

	async fn checkout_internal_task(
		&self,
		id: &tg::artifact::Id,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// Get the item in a blocking task.
		let edge = tg::graph::data::Edge::Object(id.clone());
		let item = tokio::task::spawn_blocking({
			let session = self.clone();
			move || session.checkout_internal_get_item(edge)
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))??;

		// Check out the entry and all its dependencies.
		self.checkout_internal_artifact(item, progress.clone())
			.await
	}

	fn checkout_internal_artifact(
		&self,
		item: Item,
		progress: crate::progress::Handle<()>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		let session = self.clone();
		async move {
			let task = session.server.checkout_tasks.get_or_spawn_with_context(
				item.id.clone(),
				|| {
					let progress = crate::progress::Handle::new();
					progress.start(
						"artifacts".to_owned(),
						"artifacts".to_owned(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						None,
					);
					progress.start(
						"bytes".to_owned(),
						"bytes".to_owned(),
						tg::progress::IndicatorFormat::Bytes,
						Some(0),
						None,
					);
					progress
				},
				{
					let session = session.clone();
					move |dependency_progress, _| {
						let session = session.clone();
						let item = item.clone();
						let dependency_progress = dependency_progress.clone();
						async move {
							session
								.checkout_internal_artifact_task(item, dependency_progress)
								.await
						}
					}
				},
			);

			// Forward progress events from the dependency to the progress handle.
			let mut dependency_artifacts = 0u64;
			let mut dependency_bytes = 0u64;
			let mut stream = pin!(task.context().stream().fuse());
			let mut task_future = pin!(task.wait().fuse());
			loop {
				futures::select! {
					event = stream.next() => {
						if let Some(Ok(tg::progress::Event::Indicators(indicators))) = event {
							for indicator in indicators {
								if indicator.name == "artifacts" && let Some(current) = indicator.current {
									progress.increment("artifacts", current.saturating_sub(dependency_artifacts));
									dependency_artifacts = current;
								} else if indicator.name == "bytes" && let Some(current) = indicator.current {
									progress.increment("bytes", current.saturating_sub(dependency_bytes));
									dependency_bytes = current;
								}
							}
						}
					}
					result = task_future => {
						return result
							.map_err(|error| tg::error!(!error, "a checkout task panicked"))
							.and_then(|result| result);
					}
				}
			}
		}
	}

	fn checkout_internal_graph(
		&self,
		graph_id: &tg::graph::Id,
		progress: crate::progress::Handle<()>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		let session = self.clone();
		let graph_id = graph_id.clone();
		async move {
			let task = session
				.server
				.checkout_graph_tasks
				.get_or_spawn_with_context(
					graph_id.clone(),
					|| {
						let progress = crate::progress::Handle::new();
						progress.start(
							"artifacts".to_owned(),
							"artifacts".to_owned(),
							tg::progress::IndicatorFormat::Normal,
							Some(0),
							None,
						);
						progress.start(
							"bytes".to_owned(),
							"bytes".to_owned(),
							tg::progress::IndicatorFormat::Bytes,
							Some(0),
							None,
						);
						progress
					},
					{
						let session = session.clone();
						move |dependency_progress, _| {
							let session = session.clone();
							let graph_id = graph_id.clone();
							let dependency_progress = dependency_progress.clone();
							async move {
								session
									.checkout_internal_graph_task(&graph_id, dependency_progress)
									.await
							}
						}
					},
				);

			// Forward progress events from the dependency to the progress handle.
			let mut dependency_artifacts = 0u64;
			let mut dependency_bytes = 0u64;
			let mut stream = pin!(task.context().stream().fuse());
			let mut task_future = pin!(task.wait().fuse());
			loop {
				futures::select! {
					event = stream.next() => {
						if let Some(Ok(tg::progress::Event::Indicators(indicators))) = event {
							for indicator in indicators {
								if indicator.name == "artifacts" && let Some(current) = indicator.current {
									progress.increment("artifacts", current.saturating_sub(dependency_artifacts));
									dependency_artifacts = current;
								} else if indicator.name == "bytes" && let Some(current) = indicator.current {
									progress.increment("bytes", current.saturating_sub(dependency_bytes));
									dependency_bytes = current;
								}
							}
						}
					}
					result = task_future => {
						return result
							.map_err(|error| tg::error!(!error, "a checkout graph task panicked"))
							.and_then(|result| result);
					}
				}
			}
		}
	}

	async fn checkout_internal_artifact_task(
		&self,
		item: Item,
		progress: crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// If this item is in a graph, ensure the graph's cycle-related items are checked out first.
		if let Some(graph_id) = &item.graph {
			self.checkout_internal_graph(graph_id, progress.clone())
				.await
				.map_err(|error| tg::error!(!error, %graph_id, "failed to check out the graph"))?;
		}

		// Create the path.
		let path = self.server.checkout_path().join(item.id.to_string());

		// If the path exists, then return.
		let exists = tokio::task::spawn_blocking({
			let path = path.clone();
			move || path.try_exists()
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
		.map_err(|error| tg::error!(!error, "failed to determine if the path exists"))?;
		if exists {
			return Ok(());
		}

		// Create the temp and write the artifact.
		let (temp, dependencies) = tokio::task::spawn_blocking({
			let session = self.clone();
			let item = item.clone();
			let progress = progress.clone();
			move || {
				let temp = Temp::new(&session.server);
				let dependencies =
					session.checkout_internal_write(temp.path(), &item, &progress)?;
				Ok::<_, tg::Error>((temp, dependencies))
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))??;

		let dependency_ids: Vec<tg::artifact::Id> = dependencies
			.iter()
			.map(|dependency| dependency.id.clone())
			.collect();

		// Await the dependency checkout tasks.
		future::try_join_all(
			dependencies
				.into_iter()
				.map(|dependency| self.checkout_internal_artifact(dependency, progress.clone())),
		)
		.await
		.map_err(|error| tg::error!(!error, "failed to check out the dependencies"))?;

		// Rename the temp to the checkouts directory.
		let put_checkout_arg = tokio::task::spawn_blocking({
			let session = self.clone();
			move || session.checkout_internal_rename(item, &temp, &dependency_ids)
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))??;

		// Index the checkout.
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutCheckout(put_checkout_arg)],
		};
		self.server
			.index_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the checkout"))?;

		Ok(())
	}

	async fn checkout_internal_graph_task(
		&self,
		graph_id: &tg::graph::Id,
		progress: crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// Load the graph in a blocking task.
		let graph_data = tokio::task::spawn_blocking({
			let session = self.clone();
			let graph_id = graph_id.clone();
			move || {
				let (_size, data) = session
					.server
					.object_store
					.try_get_data_sync(&graph_id.into())?
					.ok_or_else(|| tg::error!("failed to load the graph"))?;
				let data: tg::graph::Data = data
					.try_into()
					.map_err(|_| tg::error!("expected graph data"))?;
				Ok::<_, tg::Error>(data)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))??;

		// Get the items that need checkouts.
		let items = Self::checkout_internal_items_for_graph(graph_id, &graph_data)?;
		if items.is_empty() {
			return Ok(());
		}

		// Check if all items already exist in the checkouts directory.
		let all_exist = tokio::task::spawn_blocking({
			let session = self.clone();
			let items = items.clone();
			move || {
				for item in &items {
					let path = session.server.checkout_path().join(item.id.to_string());
					if !path.try_exists().unwrap_or(false) {
						return false;
					}
				}
				true
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?;
		if all_exist {
			return Ok(());
		}

		// Write each item to a temp and collect dependencies.
		let outputs = tokio::task::spawn_blocking({
			let session = self.clone();
			let items = items.clone();
			let graph_id = graph_id.clone();
			let progress = progress.clone();
			move || {
				let mut outputs = Vec::new();
				for item in items {
					// Create a temp.
					let temp = Temp::new(&session.server);

					// Write the item.
					let dependencies =
						session.checkout_internal_write(temp.path(), &item, &progress)?;

					// Filter out same-graph dependencies.
					let dependencies: Vec<Item> = dependencies
						.into_iter()
						.filter(|dependency| dependency.graph.as_ref() != Some(&graph_id))
						.collect();

					// Add the output.
					let output = (item, temp, dependencies);
					outputs.push(output);
				}
				Ok::<_, tg::Error>(outputs)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))??;

		let dependencies: Vec<Item> = outputs
			.iter()
			.flat_map(|(_, _, dependencies)| dependencies.clone())
			.collect();
		future::try_join_all(
			dependencies
				.into_iter()
				.map(|dependency| self.checkout_internal_artifact(dependency, progress.clone())),
		)
		.await
		.map_err(|error| tg::error!(!error, "failed to check out the dependencies"))?;

		// Rename all entries to the checkouts directory.
		let put_checkout_args = tokio::task::spawn_blocking({
			let session = self.clone();
			move || {
				let mut put_checkout_args = Vec::with_capacity(outputs.len());
				for (item, temp, dependencies) in outputs {
					let dependency_ids: Vec<tg::artifact::Id> = dependencies
						.iter()
						.map(|dependency| dependency.id.clone())
						.collect();
					let put_checkout_arg =
						session.checkout_internal_rename(item, &temp, &dependency_ids)?;
					put_checkout_args.push(put_checkout_arg);
				}
				Ok::<_, tg::Error>(put_checkout_args)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))??;

		// Index the checkouts.
		let arg = tangram_index::batch::Arg {
			items: put_checkout_args
				.into_iter()
				.map(tangram_index::batch::Item::PutCheckout)
				.collect(),
		};
		self.server
			.index_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the checkouts"))?;

		Ok(())
	}

	fn checkout_internal_write(
		&self,
		path: &Path,
		item: &Item,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<Vec<Item>> {
		// Create the state.
		let mut state = State {
			artifact: item.id.clone(),
			path: path.to_owned(),
			progress: progress.clone(),
			visiting: HashSet::default(),
		};

		// Check out the artifact and collect dependencies.
		let dependencies = self
			.checkout_internal_write_artifact(&mut state, path, item)
			.map_err(|error| tg::error!(!error, "failed to write the artifact"))?;

		// Set permissions on the temp directory before rename.
		if state.artifact.is_directory() {
			let permissions = std::fs::Permissions::from_mode(0o755);
			std::fs::set_permissions(path, permissions).map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to set permissions"),
			)?;
		}

		Ok(dependencies)
	}

	fn checkout_internal_write_artifact(
		&self,
		state: &mut State,
		path: &Path,
		item: &Item,
	) -> tg::Result<Vec<Item>> {
		// Write the artifact and collect dependencies.
		let dependencies = match &item.node {
			tg::graph::data::Node::Directory(node) => {
				self.checkout_internal_directory(state, path, item, node)?
			},
			tg::graph::data::Node::File(node) => {
				self.checkout_internal_file(state, path, item, node)?
			},
			tg::graph::data::Node::Symlink(node) => {
				self.checkout_internal_symlink(state, path, item, node)?
			},
		};

		// Set the file times to the epoch.
		let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
		filetime::set_symlink_file_times(path, epoch, epoch).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to set the modified time"),
		)?;

		Ok(dependencies)
	}

	fn checkout_internal_directory(
		&self,
		state: &mut State,
		path: &Path,
		item: &Item,
		node: &tg::graph::data::Directory,
	) -> tg::Result<Vec<Item>> {
		let Item { id, graph, .. } = item;

		// Add to the visiting set to detect cycles.
		state.visiting.insert(id.clone());

		// Create the directory.
		std::fs::create_dir_all(path).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the directory"),
		)?;

		// Collect all entries, recursively flattening branches.
		let entries = crate::directory::collect_directory_entries(
			&self.server.object_store,
			node,
			graph.as_ref(),
		)?;

		// Recurse into the entries.
		let mut dependencies = Vec::new();
		let mut visited = HashSet::<tg::artifact::Id, tg::id::BuildHasher>::default();
		for (name, mut edge) in entries {
			if let tg::graph::data::Edge::Pointer(pointer) = &mut edge
				&& pointer.graph.is_none()
			{
				pointer.graph = graph.clone();
			}
			let path = path.join(&name);
			let item = self
				.checkout_internal_get_item(edge)
				.map_err(|error| tg::error!(!error, "failed to get the item"))?;

			// Check for a cycle.
			if state.visiting.contains(&item.id) {
				return Err(tg::error!("detected a directory cycle"));
			}

			let item_id = item.id.clone();
			let entry_dependencies = self
				.checkout_internal_write_artifact(state, &path, &item)
				.map_err(|error| tg::error!(!error, "failed to write the artifact"))?;
			if visited.insert(item_id) {
				for dependency in entry_dependencies {
					dependencies.push(dependency);
				}
			}
		}

		// Remove from the visiting set.
		state.visiting.remove(id);

		// Set the permissions.
		let permissions = std::fs::Permissions::from_mode(0o555);
		std::fs::set_permissions(path, permissions).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to set permissions"),
		)?;

		// Increment the progress.
		state.progress.increment("artifacts", 1);

		Ok(dependencies)
	}

	fn checkout_internal_file(
		&self,
		state: &mut State,
		path: &Path,
		item: &Item,
		node: &tg::graph::data::File,
	) -> tg::Result<Vec<Item>> {
		let Item { id, graph, .. } = item;

		let mut dependencies = Vec::new();
		let mut visited = HashSet::<tg::artifact::Id, tg::id::BuildHasher>::default();
		for dependency in node.dependencies.values() {
			let Some(dependency) = dependency else {
				continue;
			};

			// Get the edge.
			let mut edge = match dependency.node.clone() {
				Some(tg::graph::data::Edge::Pointer(graph)) => {
					tg::graph::data::Edge::Pointer(graph)
				},
				Some(tg::graph::data::Edge::Object(id)) => match id.try_into() {
					Ok(id) => tg::graph::data::Edge::Object(id),
					Err(_) => continue,
				},
				None => continue,
			};

			// Update the graph if necessary.
			if let tg::graph::data::Edge::Pointer(pointer) = &mut edge
				&& pointer.graph.is_none()
			{
				pointer.graph = graph.clone();
			}

			// Get the node.
			let item = self
				.checkout_internal_get_item(edge)
				.map_err(|error| tg::error!(!error, "failed to get the item"))?;

			// Collect the dependency if it is not the root artifact.
			if item.id != state.artifact && visited.insert(item.id.clone()) {
				dependencies.push(item);
			}
		}

		let mut done = false;
		let contents = node
			.contents
			.as_ref()
			.ok_or_else(|| tg::error!("missing contents"))?;

		let src = &self.server.checkout_path().join(id.to_string());
		let dst = path;

		// Attempt to hard link the file.
		let hard_link_prohibited = if cfg!(target_os = "macos") {
			dst.to_str()
				.ok_or_else(|| tg::error!("invalid path"))?
				.contains(".app/Contents")
		} else {
			false
		};
		if !hard_link_prohibited {
			let result = std::fs::hard_link(src, dst);
			if result.is_ok()
				|| result.is_err_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists)
			{
				let len = std::fs::symlink_metadata(dst)
					.map_err(|error| tg::error!(!error, "failed to get the metadata"))?
					.len();
				state.progress.increment("bytes", len);

				done = true;
			}
		}

		// Otherwise, write the file.
		if !done {
			let mut reader =
				crate::read::Reader::new_sync(self, tg::Blob::with_id(contents.clone()))
					.map_err(|error| tg::error!(!error, "failed to create the reader"))?;
			let mut reader = InspectReader::new(&mut reader, {
				|buffer| {
					let len = buffer.len().to_u64().unwrap();
					state.progress.increment("bytes", len);
				}
			});
			let mut file = std::fs::File::create(path)
				.map_err(|error| tg::error!(!error, ?path, "failed to create the file"))?;
			std::io::copy(&mut reader, &mut file)
				.map_err(|error| tg::error!(!error, ?path, "failed to write to the file"))?;

			// Set the dependencies attr.
			let dependencies_ = node.dependencies.keys().cloned().collect::<Vec<_>>();
			if !dependencies_.is_empty() {
				let dependencies = serde_json::to_vec(&dependencies_)
					.map_err(|error| tg::error!(!error, "failed to serialize the dependencies"))?;
				xattr::set(path, tg::file::DEPENDENCIES_XATTR_NAME, &dependencies)
					.map_err(|error| tg::error!(!error, "failed to write the dependencies attr"))?;
			}

			// Set the module xattr.
			if let Some(module) = &node.module {
				let module = module.to_string();
				xattr::set(path, tg::file::MODULE_XATTR_NAME, module.as_bytes())
					.map_err(|error| tg::error!(!error, "failed to write the module xattr"))?;
			}

			// Set the permissions.
			let mode = if node.executable { 0o555 } else { 0o444 };
			let permissions = std::fs::Permissions::from_mode(mode);
			std::fs::set_permissions(path, permissions)
				.map_err(|error| tg::error!(!error, "failed to set the permissions"))?;
		}

		// Increment the progress.
		state.progress.increment("artifacts", 1);

		Ok(dependencies)
	}

	fn checkout_internal_symlink(
		&self,
		state: &mut State,
		path: &Path,
		item: &Item,
		node: &tg::graph::data::Symlink,
	) -> tg::Result<Vec<Item>> {
		let Item { graph, .. } = item;

		// Collect the dependency.
		let mut dependencies = Vec::new();

		// Render the target.
		let target = if let Some(mut edge) = node.artifact.clone() {
			let mut target = PathBuf::new();

			// Update the graph if necessary.
			if let tg::graph::data::Edge::Pointer(pointer) = &mut edge
				&& pointer.graph.is_none()
			{
				pointer.graph = graph.clone();
			}

			// Get the dependency node.
			let item = self
				.checkout_internal_get_item(edge)
				.map_err(|error| tg::error!(!error, "failed to get the item"))?;

			if item.id == state.artifact {
				// If the symlink's artifact is the root artifact, then use the root path.
				target.push(&state.path);
			} else {
				let dependency_id = item.id.clone();

				// Collect the dependency.
				dependencies.push(item);

				// Update the target.
				target.push(state.path.parent().unwrap().join(dependency_id.to_string()));
			}

			// Add the path if it is set.
			if let Some(path) = &node.path {
				target.push(path);
			}

			// Diff the path.
			let src = path
				.parent()
				.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
			let dst = &target;
			tangram_util::path::diff(src, dst)
				.map_err(|error| tg::error!(!error, "failed to diff the paths"))?
				.ok_or_else(|| tg::error!("expected the paths to differ"))?
		} else if let Some(path) = &node.path {
			path.clone()
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		std::os::unix::fs::symlink(target, path)
			.map_err(|error| tg::error!(!error, "failed to create the symlink"))?;

		// Increment the progress.
		state.progress.increment("artifacts", 1);

		Ok(dependencies)
	}

	fn checkout_internal_rename(
		&self,
		item: Item,
		temp: &Temp,
		dependencies: &[tg::artifact::Id],
	) -> tg::Result<tangram_index::checkout::put::Arg> {
		// Create the path.
		let path = self.server.checkout_path().join(item.id.to_string());

		// Rename the temp to the path.
		let result = tangram_util::fs::rename_noreplace_sync(temp, &path);
		let done = match result {
			Ok(()) => false,
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::AlreadyExists
						| std::io::ErrorKind::IsADirectory
						| std::io::ErrorKind::PermissionDenied
				) =>
			{
				true
			},
			Err(error) => {
				let src = temp.path().display();
				let dst = path.display();
				let error = tg::error!(!error, %src, %dst, "failed to rename to the checkout path");
				return Err(error);
			},
		};

		// Set the permissions.
		if !done && item.id.is_directory() {
			let permissions = std::fs::Permissions::from_mode(0o555);
			std::fs::set_permissions(&path, permissions).map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to set permissions"),
			)?;
		}

		// Set the modified time.
		if !done {
			let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
			filetime::set_symlink_file_times(&path, epoch, epoch).map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to set the modified time"),
			)?;
		}

		// Index the checkout.
		let touched_at = self.server.clock.unix_timestamp()?;
		let arg = tangram_index::checkout::put::Arg {
			dependencies: dependencies.iter().cloned().map(Into::into).collect(),
			id: item.id.into(),
			touched_at,
		};

		Ok(arg)
	}

	fn checkout_internal_get_item(
		&self,
		edge: tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::Result<Item> {
		match edge {
			tg::graph::data::Edge::Pointer(pointer) => {
				// Load the graph.
				let graph_id = pointer
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("missing graph"))?
					.clone();
				let (_size, data) = self
					.server
					.object_store
					.try_get_data_sync(&graph_id.clone().into())
					.map_err(|error| tg::error!(!error, "failed to get the graph data"))?
					.ok_or_else(|| tg::error!("failed to load the graph"))?;
				let graph_data: tg::graph::Data = data
					.try_into()
					.map_err(|_| tg::error!("expected graph data"))?;

				// Get the node.
				let node = graph_data
					.nodes
					.get(pointer.index)
					.ok_or_else(|| tg::error!("invalid graph node"))?
					.clone();

				// Compute the id.
				let data: tg::artifact::data::Artifact = match node.kind() {
					tg::artifact::Kind::Directory => {
						tg::directory::Data::Pointer(pointer.clone()).into()
					},
					tg::artifact::Kind::File => tg::file::Data::Pointer(pointer.clone()).into(),
					tg::artifact::Kind::Symlink => {
						tg::symlink::Data::Pointer(pointer.clone()).into()
					},
				};
				let bytes = data.serialize()?;
				let id = tg::artifact::Id::new(node.kind(), &bytes);

				let item = Item {
					id,
					node,
					graph: Some(graph_id),
				};

				Ok(item)
			},

			tg::graph::data::Edge::Object(object_id) => {
				// Load the object.
				let (_size, data) = self
					.server
					.object_store
					.try_get_data_sync(&object_id.clone().into())
					.map_err(|error| tg::error!(!error, "failed to get the object data"))?
					.ok_or_else(|| tg::error!("failed to load the object"))?;
				let data = data
					.try_into()
					.map_err(|_| tg::error!("expected artifact data"))?;

				match data {
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Pointer(
						pointer,
					))
					| tg::artifact::data::Artifact::File(tg::file::Data::Pointer(pointer))
					| tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Pointer(pointer)) => {
						// Load the graph.
						let graph_id = pointer
							.graph
							.as_ref()
							.ok_or_else(|| tg::error!("missing graph"))?
							.clone();
						let (_size, data) = self
							.server
							.object_store
							.try_get_data_sync(&graph_id.clone().into())
							.map_err(|error| tg::error!(!error, "failed to get the graph data"))?
							.ok_or_else(|| tg::error!("failed to load the graph"))?;
						let graph_data: tg::graph::Data = data
							.try_into()
							.map_err(|_| tg::error!("expected graph data"))?;

						// Get the node.
						let node = graph_data
							.nodes
							.get(pointer.index)
							.ok_or_else(|| tg::error!("invalid graph node"))?
							.clone();

						let item = Item {
							id: object_id,
							node,
							graph: Some(graph_id),
						};

						Ok(item)
					},

					tg::artifact::data::Artifact::Directory(tg::directory::Data::Node(node)) => {
						let item = Item {
							id: object_id,
							node: tg::graph::data::Node::Directory(node),
							graph: None,
						};
						Ok(item)
					},
					tg::artifact::data::Artifact::File(tg::file::Data::Node(node)) => {
						let item = Item {
							id: object_id,
							node: tg::graph::data::Node::File(node),
							graph: None,
						};
						Ok(item)
					},
					tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Node(node)) => {
						let item = Item {
							id: object_id,
							node: tg::graph::data::Node::Symlink(node),
							graph: None,
						};
						Ok(item)
					},
				}
			},
		}
	}

	fn checkout_internal_items_for_graph(
		graph_id: &tg::graph::Id,
		graph_data: &tg::graph::Data,
	) -> tg::Result<Vec<Item>> {
		// Collect node indices which have incoming file dependency or symlink artifact edges in the graph.
		let mut marks = HashSet::<usize, fnv::FnvBuildHasher>::default();
		for node in &graph_data.nodes {
			match node {
				tg::graph::data::Node::File(file) => {
					for dependency in file.dependencies.values().flatten() {
						if let Some(tg::graph::data::Edge::Pointer(pointer)) = &dependency.node
							&& pointer.graph.is_none()
						{
							marks.insert(pointer.index);
						}
					}
				},
				tg::graph::data::Node::Symlink(symlink) => {
					if let Some(tg::graph::data::Edge::Pointer(pointer)) = &symlink.artifact
						&& pointer.graph.is_none()
					{
						marks.insert(pointer.index);
					}
				},
				tg::graph::data::Node::Directory(_) => {},
			}
		}

		// Create items for nodes with incoming dependency edges.
		let mut items = Vec::new();
		for index in marks {
			let node = graph_data
				.nodes
				.get(index)
				.ok_or_else(|| tg::error!("invalid graph node index"))?
				.clone();

			let pointer = tg::graph::data::Pointer {
				graph: Some(graph_id.clone()),
				index,
				kind: node.kind(),
			};

			// Compute the artifact ID.
			let data: tg::artifact::data::Artifact = match node.kind() {
				tg::artifact::Kind::Directory => {
					tg::directory::Data::Pointer(pointer.clone()).into()
				},
				tg::artifact::Kind::File => tg::file::Data::Pointer(pointer.clone()).into(),
				tg::artifact::Kind::Symlink => tg::symlink::Data::Pointer(pointer.clone()).into(),
			};
			let bytes = data.serialize()?;
			let id = tg::artifact::Id::new(node.kind(), &bytes);

			items.push(Item {
				id,
				node,
				graph: Some(graph_id.clone()),
			});
		}

		Ok(items)
	}
}

impl Server {
	pub(crate) async fn remove_all_named_checkout_entries_with_lock(
		&self,
		_guard: &tokio::sync::MutexGuard<'_, ()>,
	) -> tg::Result<()> {
		if self.vfs.lock().unwrap().is_some() {
			return Ok(());
		}

		// Discard named entries before the backing directory becomes the physical store.
		let path = self.store_path();
		let mut entries = tokio::fs::read_dir(path)
			.await
			.map_err(|error| tg::error!(!error, "failed to read the store directory"))?;
		while let Some(entry) = entries
			.next_entry()
			.await
			.map_err(|error| tg::error!(!error, "failed to read the store directory"))?
		{
			let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
				continue;
			};
			if !matches!(
				tg::store::path::parse_component(&name),
				Ok(tg::store::path::Component::Tag { .. })
			) {
				continue;
			}
			tangram_util::fs::remove(entry.path())
				.await
				.map_err(|error| tg::error!(!error, "failed to remove a checkout entry"))?;
		}

		Ok(())
	}

	pub(crate) async fn materialize_named_checkout_entries_with_lock(
		&self,
		_guard: &tokio::sync::MutexGuard<'_, ()>,
		nodes: &[super::NamedNode],
		artifact: Option<&tg::artifact::Id>,
		suffix: Option<&str>,
	) -> tg::Result<()> {
		if self.vfs.lock().unwrap().is_some() {
			return Ok(());
		}
		for (index, node) in nodes.iter().enumerate() {
			let is_last = index + 1 == nodes.len();
			let suffix = is_last.then_some(suffix).flatten();
			let path = self
				.store_path()
				.join(Self::named_checkout_relative_path(node, suffix));
			if node.id.kind() == tg::id::Kind::Tag {
				let artifact = artifact.ok_or_else(
					|| tg::error!(id = %node.id, "the tag does not resolve to an artifact"),
				)?;
				Self::materialize_tag_checkout_entry(&path, &node.specifier, artifact, suffix)
					.await?;
			} else {
				Self::materialize_named_checkout_directory(&path).await?;
			}
		}

		Ok(())
	}

	pub(crate) async fn remove_named_checkout_entry_with_lock(
		&self,
		_guard: &tokio::sync::MutexGuard<'_, ()>,
		id: &tg::Id,
		specifier: &tg::Specifier,
	) -> tg::Result<()> {
		if self.vfs.lock().unwrap().is_some() {
			return Ok(());
		}
		let node = super::NamedNode {
			id: id.clone(),
			parent: None,
			specifier: specifier.clone(),
			target: None,
		};
		let path = self
			.store_path()
			.join(Self::named_checkout_relative_path(&node, None));
		if id.kind() != tg::id::Kind::Tag {
			return Self::remove_named_checkout_directory(&path).await;
		}

		Self::remove_tag_checkout_symlink(&path).await?;
		let parent = path.parent().unwrap();
		let prefix = format!("{}@module.", specifier.name());
		match tokio::fs::read_dir(parent).await {
			Ok(mut entries) => {
				while let Some(entry) = entries
					.next_entry()
					.await
					.map_err(|error| tg::error!(!error, "failed to read the store directory"))?
				{
					let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
						continue;
					};
					if name.starts_with(&prefix) {
						Self::remove_tag_checkout_symlink(&entry.path()).await?;
					}
				}
			},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
			Err(error) => {
				return Err(tg::error!(!error, "failed to read the store directory"));
			},
		}

		Ok(())
	}

	fn named_checkout_relative_path(node: &super::NamedNode, suffix: Option<&str>) -> PathBuf {
		let mut path = node
			.specifier
			.components()
			.map(String::from)
			.collect::<PathBuf>();
		if let Some(suffix) = suffix {
			let name = tg::store::path::module_component(node.specifier.name(), suffix);
			path.set_file_name(name);
		}
		path
	}

	async fn materialize_named_checkout_directory(path: &Path) -> tg::Result<()> {
		match tokio::fs::symlink_metadata(path).await {
			Ok(metadata) if metadata.is_dir() => return Ok(()),
			Ok(_) => tokio::fs::remove_file(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to remove a checkout entry"))?,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
			Err(error) => {
				return Err(tg::error!(!error, "failed to inspect a checkout entry"));
			},
		}
		tokio::fs::create_dir(path)
			.await
			.map_err(|error| tg::error!(!error, "failed to create a checkout directory"))?;

		Ok(())
	}

	async fn materialize_tag_checkout_entry(
		path: &Path,
		specifier: &tg::Specifier,
		artifact: &tg::artifact::Id,
		suffix: Option<&str>,
	) -> tg::Result<()> {
		match tokio::fs::symlink_metadata(path).await {
			Ok(metadata) if metadata.is_dir() => tokio::fs::remove_dir(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to remove a checkout directory"))?,
			Ok(_) => tokio::fs::remove_file(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to remove a checkout entry"))?,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
			Err(error) => {
				return Err(tg::error!(!error, "failed to inspect a checkout entry"));
			},
		}
		let mut target = PathBuf::new();
		for _ in 1..specifier.components().count() {
			target.push("..");
		}
		let suffix = suffix.unwrap_or_default();
		target.push(format!("{artifact}{suffix}"));
		tokio::fs::symlink(&target, path)
			.await
			.map_err(|error| tg::error!(!error, "failed to create a checkout entry"))?;

		Ok(())
	}

	async fn remove_named_checkout_directory(path: &Path) -> tg::Result<()> {
		match tokio::fs::symlink_metadata(path).await {
			Ok(metadata) if metadata.is_dir() => tokio::fs::remove_dir(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to remove a checkout directory"))?,
			Ok(metadata) if metadata.is_symlink() => tokio::fs::remove_file(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to remove a checkout entry"))?,
			Ok(_) => {
				return Err(
					tg::error!(path = %path.display(), "the checkout entry is not a directory"),
				);
			},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
			Err(error) => {
				return Err(tg::error!(!error, "failed to inspect a checkout entry"));
			},
		}

		Ok(())
	}

	async fn remove_tag_checkout_symlink(path: &Path) -> tg::Result<()> {
		match tokio::fs::symlink_metadata(path).await {
			Ok(metadata) if metadata.is_symlink() => tokio::fs::remove_file(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to remove a checkout entry"))?,
			Ok(_) => (),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
			Err(error) => {
				return Err(tg::error!(!error, "failed to inspect a checkout entry"));
			},
		}

		Ok(())
	}
}
