use {
	crate::{Session, watch::Watch},
	futures::{FutureExt as _, Stream, StreamExt as _},
	indexmap::IndexMap,
	indoc::indoc,
	num::ToPrimitive as _,
	std::{
		panic::AssertUnwindSafe,
		path::{Path, PathBuf},
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_ignore as ignore,
	tracing::Instrument as _,
};

mod artifact;
mod blob;
mod cache;
mod fixup;
mod graph;
mod index;
mod input;
mod lock;
mod path;
mod solve;
mod store;

pub use self::{graph::Graph, solve::Solutions};

pub type Tasks = tangram_futures::task::Map<
	crate::checkin::TaskKey,
	(),
	crate::progress::Handle<crate::checkin::TaskOutput>,
	fnv::FnvBuildHasher,
>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TaskKey {
	pub options: tg::checkin::Options,
	pub principal: tg::Principal,
	pub root: PathBuf,
	pub updates: Vec<tg::specifier::Pattern>,
}

#[derive(Clone)]
pub struct TaskOutput {
	pub graph: Graph,
	pub path: PathBuf,
}

type IndexObjectArgs =
	IndexMap<tg::object::Id, tangram_index::object::put::Arg, tg::id::BuildHasher>;

type IndexCacheEntryArgs = Vec<tangram_index::cache::put::Arg>;

type StoreArgs = IndexMap<tg::object::Id, crate::object::store::PutArg, tg::id::BuildHasher>;

type GraphData = IndexMap<tg::graph::Id, tg::graph::Data, tg::id::BuildHasher>;

#[derive(Clone, Copy)]
enum WatchObservation {
	Compatible { id: crate::watch::Id, version: u64 },
	Incompatible { id: crate::watch::Id, version: u64 },
	Vacant,
}

impl Session {
	#[tracing::instrument(
		fields(path = ?arg.path, root = arg.options.root),
		level = "trace",
		name = "checkin",
		skip_all
	)]
	pub(crate) async fn checkin(
		&self,
		mut arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + use<>,
	> {
		// Validate the arg.
		if arg.options.watch && !self.server.config.advanced.single_process {
			return Err(tg::error!(
				"the watch option is not supported in multi-process mode"
			));
		}

		arg.path = self.host_path_for_guest_path(&arg.path)?;

		// Validate and canonicalize the path.
		if !arg.path.is_absolute() {
			return Err(tg::error!(path = ?arg.path, "the path must be absolute"));
		}
		arg.path = tangram_util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|error| tg::error!(!error, path = %&arg.path.display(), "failed to canonicalize the path's parent"))?;

		// Handle paths in the cache directory.
		if let Ok(path) = arg.path.strip_prefix(self.server.cache_path()) {
			let progress = crate::progress::Handle::new();
			let output = self
				.checkin_cache_path(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to check in the cache path"))?;
			progress.output(output);
			return Ok(progress.stream().left_stream());
		}

		// Create the ignorer and determine the root.
		let ignorer = arg
			.options
			.ignore
			.then(Self::checkin_create_ignorer)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to create the ignorer"))?;
		let (root, ignorer) = if arg.options.root {
			(arg.path.clone(), ignorer)
		} else {
			self.checkin_find_root_path(&arg.path, ignorer)
				.await
				.map_err(|error| tg::error!(!error, "failed to find the root path"))?
		};

		// Get or spawn the checkin task for the root.
		let key = TaskKey {
			options: arg.options.clone(),
			principal: self.context.principal.clone(),
			root: root.clone(),
			updates: arg.updates.clone(),
		};
		let root_task = self.server.checkin_tasks.get_or_spawn_with_context(
			key,
			crate::progress::Handle::new,
			|progress, _stop| {
				let session = self.clone();
				let arg = arg.clone();
				let root = root.clone();
				async move {
					let result =
						AssertUnwindSafe(session.checkin_task(arg, &root, ignorer, &progress))
							.catch_unwind()
							.await;
					match result {
						Ok(Ok(output)) => {
							crate::checkpoint!(
								session.server,
								"checkin.progress.output",
								path = %root.display(),
							)
							.await;
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
							let error = tg::error!(?message, "the task panicked");
							progress.error(error);
						},
					}
				}
				.instrument(tracing::Span::current())
			},
		);

		// Get the root task's progress handle.
		let root_progress = root_task.context().clone();

		// Create the progress.
		let progress = crate::progress::Handle::new();

		// Spawn the task.
		let now = self.server.clock.unix_timestamp()?;
		let path = arg.path.clone();
		let task = Task::spawn({
			let session = self.clone();
			let progress = progress.clone();
			move |_| async move {
				// Forward events from the root progress stream.
				let mut output = None;
				crate::checkpoint!(
					session.server,
					"checkin.progress.subscribe",
					path = %path.display(),
				)
				.await;
				let mut stream = std::pin::pin!(root_progress.stream());
				crate::checkpoint!(
					session.server,
					"checkin.progress.subscribed",
					path = %path.display(),
				)
				.await;
				while let Some(event) = stream.next().await {
					if let Some(output_) = progress.forward(event) {
						output = Some(output_);
					}
				}
				let Some(output) = output else {
					progress.error(tg::error!("failed to get the output"));
					return;
				};

				// Look up the path in the graph.
				let Some(index) = output.graph.paths.get(&path).copied() else {
					progress.error(tg::error!("failed to get the node"));
					return;
				};

				// Get the node.
				let node = output.graph.nodes.get(&index).unwrap();

				// Determine the id.
				let id = if path != output.path
					&& let tg::graph::data::Edge::Pointer(pointer) = node.edge.as_ref().unwrap()
				{
					// If the path differs from the output path and the edge is a pointer, then store and index a pointer artifact for the path.
					let result = session
						.checkin_store_and_index_pointer_artifact(node, pointer)
						.await;
					match result {
						Ok(id) => id,
						Err(error) => {
							progress.error(error);
							return;
						},
					}
				} else {
					node.id.as_ref().unwrap().clone().try_into().unwrap()
				};

				// Create and send the output.
				let mut options = tg::referent::Options::with_path(path);
				let expires_at = now
					+ session
						.server
						.config
						.object
						.grant_time_to_live
						.as_secs()
						.to_i64()
						.unwrap();
				let token = match session.create_token(
					tg::grant::Resource::Id(id.clone().into()),
					vec![tg::grant::Permission::Object(
						tg::grant::permission::object::Permission::Subtree,
					)],
					expires_at,
				) {
					Ok(token) => token,
					Err(error) => {
						progress.error(error);
						return;
					},
				};
				if let Some(token) = token {
					options.tokens.insert_local(token);
				}
				let referent = tg::Referent { node: id, options };
				let output = tg::checkin::Output { artifact: referent };
				progress.output(output);
			}
		});

		let stream = progress
			.stream()
			.attach(task)
			.attach(root_task)
			.right_stream();

		Ok(stream)
	}

	async fn checkin_cache_path(&self, path: &Path) -> tg::Result<tg::checkin::Output> {
		let id = path
			.components()
			.next()
			.map(|component| {
				let std::path::Component::Normal(name) = component else {
					return Err(tg::error!("invalid path"));
				};
				name.to_str().ok_or_else(|| tg::error!("non-utf8 path"))
			})
			.ok_or_else(|| tg::error!("cannot check in the cache directory"))??
			.parse::<tg::artifact::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the artifact id"))?;

		let resource = tg::grant::Resource::Id(id.clone().into());
		let permission =
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree);
		if !self
			.authorize(resource, permission)
			.await?
			.is_some_and(|permissions| permissions.contains(permission))
		{
			return Err(tg::error!("unauthorized"));
		}

		if path.components().count() == 1 {
			let mut artifact = tg::Referent::with_node(id);
			if let Some(token) = self.create_artifact_token(&artifact.node)? {
				artifact.options.tokens.insert_local(token);
			}
			let output = tg::checkin::Output { artifact };
			return Ok(output);
		}

		let subpath = path.components().skip(1).collect::<PathBuf>();
		let artifact = tg::Artifact::with_id(id);
		let directory = artifact
			.try_unwrap_directory()
			.ok()
			.ok_or_else(|| tg::error!("invalid path"))?;
		let artifact = directory
			.get_with_handle(self, subpath)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the artifact from the cache"))?;

		let id = artifact.id();
		let mut referent = tg::Referent::with_node(id);
		if let Some(token) = self.create_artifact_token(&referent.node)? {
			referent.options.tokens.insert_local(token);
		}
		let output = tg::checkin::Output { artifact: referent };

		Ok(output)
	}

	fn create_artifact_token(
		&self,
		id: &tg::artifact::Id,
	) -> tg::Result<Option<tg::authorization::Token>> {
		let now = self.server.clock.unix_timestamp()?;
		let expires_at = now
			+ self
				.server
				.config
				.object
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		self.create_token(
			tg::grant::Resource::Id(id.clone().into()),
			vec![tg::grant::Permission::Object(
				tg::grant::permission::object::Permission::Subtree,
			)],
			expires_at,
		)
	}

	// Check in the artifact.
	async fn checkin_task(
		&self,
		arg: tg::checkin::Arg,
		root: &Path,
		ignorer: Option<ignore::Ignorer>,
		progress: &crate::progress::Handle<TaskOutput>,
	) -> tg::Result<TaskOutput> {
		// Validate the arg.
		if arg.options.destructive && arg.options.ignore {
			return Err(tg::error!("ignore is forbidden for destructive checkins"));
		}

		// Try to find the artifacts path.
		let artifacts_path = root.join(".tangram/artifacts");
		let artifacts_path = if tokio::fs::try_exists(&artifacts_path)
			.await
			.is_ok_and(|exists| exists)
		{
			Some(artifacts_path)
		} else {
			None
		};
		let watch_key = crate::watch::Key {
			path: root.to_owned(),
			principal: self.context.principal.clone(),
		};

		// Attempt to get the graph, lock, and solutions from a watcher.
		let watch = if arg.options.watch {
			self.server.watches.get(&watch_key)
		} else {
			None
		};
		let (mut graph, lock, mut solutions, watch_observation) = if let Some(watch) = watch {
			let compatible = watch.value().options() == &arg.options;
			let id = watch.value().id();
			let snapshot = watch.value().get();
			drop(watch);
			match snapshot.await {
				Ok(snapshot) => {
					if compatible {
						// Rebuild the graph when explicit updates invalidate its solved nodes.
						let (graph, solutions) = if arg.updates.is_empty() {
							(snapshot.graph, snapshot.solutions)
						} else {
							(Graph::default(), Solutions::default())
						};
						let lock = snapshot.lock;
						let watch_observation = WatchObservation::Compatible {
							id: snapshot.id,
							version: snapshot.version,
						};
						(graph, lock, solutions, watch_observation)
					} else {
						let graph = Graph::default();
						let lock = None;
						let solutions = Solutions::default();
						let watch_observation = WatchObservation::Incompatible {
							id: snapshot.id,
							version: snapshot.version,
						};
						(graph, lock, solutions, watch_observation)
					}
				},
				Err(error) => {
					let removed = self
						.server
						.watches
						.remove_if(&watch_key, |_, watch| watch.id() == id)
						.is_some();
					if !removed {
						return Err(tg::error!(!error, "the watch changed during indexing"));
					}
					let graph = Graph::default();
					let lock = None;
					let solutions = Solutions::default();
					let watch_observation = WatchObservation::Vacant;
					(graph, lock, solutions, watch_observation)
				},
			}
		} else {
			let graph = Graph::default();
			let lock = None;
			let solutions = Solutions::default();
			let watch_observation = WatchObservation::Vacant;
			(graph, lock, solutions, watch_observation)
		};
		if arg.options.watch {
			let updates = arg
				.updates
				.iter()
				.map(ToString::to_string)
				.collect::<Vec<_>>()
				.join(",");
			crate::checkpoint!(
				self.server,
				"checkin.watch.snapshot",
				path = %root.display(),
				solve = arg.options.solve,
				updates,
			)
			.await;
		}

		// Read the lock if it was not retrieved from the watcher and the lock option is set.
		let lock = if let Some(lock) = lock {
			Some(lock)
		} else if arg.options.lock.is_some() {
			Self::checkin_try_read_lock(root)
				.map_err(|error| tg::error!(!error, "failed to read the lock"))?
				.map(Arc::new)
		} else {
			None
		};

		// Get the next node index.
		let next = graph.next;

		// Spawn the fixup task.
		let (fixup_task, fixup_sender) = if arg.options.destructive {
			let (sender, receiver) = std::sync::mpsc::channel();
			let task = tokio::task::spawn_blocking(move || Self::checkin_fixup_task(&receiver))
				.map(|result| {
					result
						.map_err(|error| tg::error!(!error, "the fixup task panicked"))
						.and_then(|result| result)
				});
			(Some(task), Some(sender))
		} else {
			(None, None)
		};

		// Collect input.
		let mut graph = tokio::task::spawn_blocking({
			let session = self.clone();
			let arg = arg.clone();
			let artifacts_path = artifacts_path.clone();
			let lock = lock.clone();
			let progress = progress.clone();
			let root = root.to_owned();
			move || {
				let arg = input::CheckinInputArg {
					arg: &arg,
					artifacts_path: artifacts_path.as_deref(),
					fixup_sender,
					graph: &mut graph,
					ignorer,
					lock: lock.as_deref(),
					next,
					progress,
					root: &root,
				};
				session.checkin_input(arg)?;
				Ok::<_, tg::Error>(graph)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "the checkin input task panicked"))??;

		// Solve.
		if arg.options.solve {
			let solve_arg = solve::CheckinSolveArg {
				arg: &arg,
				graph: &mut graph,
				next,
				lock: lock.clone(),
				solutions: &mut solutions,
				root,
				progress,
			};
			self.checkin_solve(solve_arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to solve dependencies"))?;
		}

		// Get reference path edges.
		let paths = self
			.checkin_path_get_edges(&graph, next)
			.await
			.map_err(|error| tg::error!(!error, "failed to get reference path edges"))?;

		// Set the touch time.
		let touched_at = self.server.clock.unix_timestamp()?;

		// Create the output collections.
		let mut store_args = IndexMap::default();
		let mut index_object_args = IndexMap::default();
		let mut index_cache_entry_args = Vec::new();
		let mut graph_data = IndexMap::default();

		// Create blobs.
		let create_blobs_arg = blob::CheckinCreateBlobsArg {
			arg: &arg,
			graph: &mut graph,
			next,
			store_args: &mut store_args,
			index_object_args: &mut index_object_args,
			touched_at,
			progress,
		};
		self.checkin_create_blobs(create_blobs_arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to create blobs"))?;

		// Create artifacts.
		let create_artifacts_arg = artifact::CheckinCreateArtifactsArg {
			config: &self.server.config.checkin,
			arg: &arg,
			graph: &mut graph,
			paths: &paths,
			next,
			store_args: &mut store_args,
			index_object_args: &mut index_object_args,
			index_cache_entry_args: &mut index_cache_entry_args,
			graph_data: &mut graph_data,
			root,
			time_to_touch: self.server.config.object.time_to_touch,
			touched_at,
		};
		Self::checkin_create_artifacts(create_artifacts_arg)?;

		// Cache.
		if arg.options.cache_pointers {
			if let Some(task) = fixup_task {
				task.await
					.map_err(|error| tg::error!(!error, "failed to run the fixup task"))?;
			}
			let cache_arg = cache::CheckinCacheArg {
				arg: &arg,
				graph: &graph,
				next,
				root,
				index_cache_entry_args: &index_cache_entry_args,
				graph_data: &mut graph_data,
				progress,
			};
			self.checkin_cache(cache_arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to cache"))?;
		}

		// Store.
		self.checkin_store(store_args.into_values().collect(), progress)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the objects to the store"))?;

		// Write the lock.
		let reserve_lock_write = || match watch_observation {
			WatchObservation::Compatible { id, version }
			| WatchObservation::Incompatible { id, version } => {
				let watch = self
					.server
					.watches
					.get(&watch_key)
					.filter(|watch| watch.id() == id)
					.ok_or_else(|| tg::error!("the watch changed during checkin"))?;
				let lock_write_guard = watch
					.try_reserve_lock_write(&graph, version)
					.ok_or_else(|| tg::error!("files were modified during checkin"))?;

				Ok(Some(lock_write_guard))
			},
			WatchObservation::Vacant => Ok(None),
		};
		progress.spinner("locking", "locking");
		let (lock, lock_write_guard) = self
			.checkin_write_lock(&arg, &graph, next, lock, root, reserve_lock_write)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the lock"))?;
		progress.finish("locking");

		// Create the index batch.
		let account = self.usage_account(&self.context.principal).await?;
		let mut index_arg = self.checkin_index(
			&arg,
			&graph,
			index_object_args,
			index_cache_entry_args,
			root,
			touched_at,
		)?;
		if let Some(account) = account {
			let index = graph.paths.get(root).unwrap();
			let object = graph.nodes.get(index).unwrap().id.as_ref().unwrap().clone();
			index_arg
				.items
				.push(tangram_index::batch::Item::PutAccountObject(
					tangram_index::usage::storage::put::ObjectArg {
						account,
						object,
						touched_at,
					},
				));
		}

		// Create or update the watcher and spawn its index task.
		if self
			.server
			.config()
			.watch
			.as_ref()
			.is_some_and(|_| arg.options.watch)
		{
			let updates = arg
				.updates
				.iter()
				.map(ToString::to_string)
				.collect::<Vec<_>>()
				.join(",");
			crate::checkpoint!(
				self.server,
				"checkin.watch.publish",
				nodes = graph.nodes.len(),
				path = %root.display(),
				solve = arg.options.solve,
				updates,
			)
			.await;

			// Verify that the lock has the expected contents.
			if lock_write_guard.is_some() {
				let actual_lock = Self::checkin_try_read_lock(root)
					.map_err(|error| tg::error!(!error, "failed to read the lock"))?;
				if lock.as_deref() != actual_lock.as_ref() {
					return Err(tg::error!("the lock was modified during checkin"));
				}
			}

			// Create or update the watcher.
			let entry = self.server.watches.entry(watch_key.clone());
			match (entry, watch_observation) {
				(dashmap::Entry::Occupied(entry), WatchObservation::Compatible { id, version })
					if entry.get().id() == id && entry.get().options() == &arg.options =>
				{
					// Verify the version.
					let watch = entry.get();

					// Update the watch.
					let update_arg = crate::watch::UpdateArg {
						graph: graph.clone(),
						key: &watch_key,
						lock,
						lock_write_guard,
						next,
						server: &self.server,
						solutions,
						version,
					};
					let success = watch.update(update_arg, || {
						self.checkin_index_task(index_arg, &arg, root)
					});
					if !success {
						return Err(tg::error!("files were modified during checkin"));
					}
				},
				(
					dashmap::Entry::Occupied(mut entry),
					WatchObservation::Incompatible { id, version },
				) if entry.get().id() == id => {
					// Replace the incompatible watcher.
					let new_arg = crate::watch::NewArg {
						graph: graph.clone(),
						lock,
						next,
						options: arg.options.clone(),
						solutions,
						spawn_index_task: || self.checkin_index_task(index_arg, &arg, root),
					};
					let success = entry.get_mut().replace_if_version(
						&graph,
						version,
						lock_write_guard,
						|| {
							Watch::new(&self.server, &watch_key, new_arg)
								.map_err(|error| tg::error!(!error, "failed to create the watch"))
						},
					)?;
					if !success {
						return Err(tg::error!("files were modified during checkin"));
					}
				},
				(dashmap::Entry::Vacant(entry), WatchObservation::Vacant) => {
					let new_arg = crate::watch::NewArg {
						graph: graph.clone(),
						lock,
						next,
						options: arg.options.clone(),
						solutions,
						spawn_index_task: || self.checkin_index_task(index_arg, &arg, root),
					};
					let watch = Watch::new(&self.server, &watch_key, new_arg)
						.map_err(|error| tg::error!(!error, "failed to create the watch"))?;
					entry.insert(watch);
				},
				_ => return Err(tg::error!("the watch changed during checkin")),
			}

			// Spawn a task to clean nodes with no referrers.
			tokio::task::spawn_blocking({
				let session = self.clone();
				let root = root.to_owned();
				let watch_key = watch_key.clone();
				let next = graph.next;
				move || {
					if let Some(watch) = session.server.watches.get(&watch_key) {
						watch.clean(&root, next);
					}
				}
			});
		} else {
			self.server
				.index_batch(index_arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the checkin"))?;
		}

		let output = TaskOutput {
			graph,
			path: arg.path,
		};

		Ok(output)
	}

	pub(crate) async fn checkin_find_root_path(
		&self,
		path: &Path,
		mut ignorer: Option<ignore::Ignorer>,
	) -> tg::Result<(PathBuf, Option<ignore::Ignorer>)> {
		let path = path.to_owned();
		let output = tokio::task::spawn_blocking(move || {
			let mut output = None;
			for ancestor in path.ancestors() {
				let metadata = std::fs::symlink_metadata(ancestor).map_err(
					|error| tg::error!(!error, path = %path.display(), "failed to get the metadata"),
				)?;
				if metadata.is_dir()
					&& tg::module::try_get_root_module_file_name_sync(ancestor)?.is_some()
					&& ignorer
						.as_mut()
						.map(|ignorer| ignorer.matches(Some(ancestor), &path, None))
						.transpose()
						.map_err(|error| {
							tg::error!(!error, "failed to check if the path is ignored")
						})?
						.is_none_or(|ignore| !ignore)
				{
					output.replace(ancestor.to_owned());
				}
			}
			let output = output.unwrap_or(path);
			Ok::<_, tg::Error>((output, ignorer))
		})
		.await
		.map_err(|error| tg::error!(!error, "the checkin root task panicked"))??;
		Ok(output)
	}

	pub(crate) fn checkin_create_ignorer() -> tg::Result<ignore::Ignorer> {
		let file_names = vec![".tangramignore".into(), ".gitignore".into()];
		let global = indoc!(
			"
				.DS_Store
				.git
				.tangram
				tangram.lock
			"
		);
		ignore::Ignorer::new(file_names, Some(global))
			.map_err(|error| tg::error!(!error, "failed to create the matcher"))
	}

	pub(crate) async fn checkin_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
			.checkin(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the checkin"))?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
