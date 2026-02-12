use {
	crate::{Context, Server, watch::Watch},
	futures::{FutureExt as _, Stream, StreamExt as _},
	indexmap::IndexMap,
	indoc::indoc,
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

pub(crate) use self::{graph::Graph, solve::Solutions};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TaskKey {
	pub options: tg::checkin::Options,
	pub root: PathBuf,
	pub updates: Vec<tg::tag::Pattern>,
}

#[derive(Clone)]
pub struct TaskOutput {
	pub graph: Graph,
	pub path: PathBuf,
}

type IndexObjectArgs = IndexMap<tg::object::Id, tangram_index::PutObjectArg, tg::id::BuildHasher>;

type IndexCacheEntryArgs = Vec<tangram_index::PutCacheEntryArg>;

type StoreArgs = IndexMap<tg::object::Id, crate::store::PutObjectArg, tg::id::BuildHasher>;

impl Server {
	#[tracing::instrument(fields(path = ?arg.path), level = "trace", name = "checkin", skip_all)]
	pub(crate) async fn checkin_with_context(
		&self,
		context: &Context,
		mut arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + use<>,
	> {
		// Handle host path conversion.
		if let Some(process) = &context.process {
			arg.path = process.host_path_for_guest_path(arg.path.clone());
		}

		// Guard against concurrent cleans.
		let _clean_guard = self.try_acquire_clean_guard()?;

		// Validate and canonicalize the path.
		if !arg.path.is_absolute() {
			return Err(tg::error!(path = ?arg.path, "the path must be absolute"));
		}
		arg.path = tangram_util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, path = %&arg.path.display(), "failed to canonicalize the path's parent"))?;

		// Handle paths in the cache directory.
		if let Ok(path) = arg.path.strip_prefix(self.cache_path()) {
			let progress = crate::progress::Handle::new();
			let output = self
				.checkin_cache_path(path)
				.await
				.map_err(|source| tg::error!(!source, "failed to check in the cache path"))?;
			progress.output(output);
			return Ok(progress.stream().left_stream());
		}

		// Create the ignorer and find the root.
		let ignorer = arg
			.options
			.ignore
			.then(Self::checkin_create_ignorer)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to create the ignorer"))?;
		let (root, ignorer) = self
			.checkin_find_root_path(&arg.path, ignorer)
			.await
			.map_err(|source| tg::error!(!source, "failed to find the root path"))?;

		// Get or spawn the checkin task for the root.
		let key = TaskKey {
			options: arg.options.clone(),
			root: root.clone(),
			updates: arg.updates.clone(),
		};
		let root_task = self.checkin_tasks.get_or_spawn_with_context(
			key,
			crate::progress::Handle::new,
			|progress, _stop| {
				let server = self.clone();
				let arg = arg.clone();
				let root = root.clone();
				async move {
					let result =
						AssertUnwindSafe(server.checkin_task(arg, &root, ignorer, &progress))
							.catch_unwind()
							.await;
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
		let path = arg.path.clone();
		let task = Task::spawn({
			let server = self.clone();
			let progress = progress.clone();
			move |_| async move {
				// Forward events from the root progress stream.
				let mut output = None;
				let mut stream = std::pin::pin!(root_progress.stream());
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
					progress.error(tg::error!("failed to get the item"));
					return;
				};

				// Get the node.
				let node = output.graph.nodes.get(&index).unwrap();

				// Determine the id.
				let id = if path != output.path
					&& let tg::graph::data::Edge::Pointer(pointer) = node.edge.as_ref().unwrap()
				{
					// If the path differs from the output path and the edge is a pointer, then store and index a pointer artifact for the path.
					let result = server
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
				let options = tg::referent::Options::with_path(path);
				let referent = tg::Referent { item: id, options };
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
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the artifact id"))?;
		if path.components().count() == 1 {
			let output = tg::checkin::Output {
				artifact: tg::Referent::with_item(id),
			};
			return Ok(output);
		}
		let subpath = path.components().skip(1).collect::<PathBuf>();
		let artifact = tg::Artifact::with_id(id);
		let directory = artifact
			.try_unwrap_directory()
			.ok()
			.ok_or_else(|| tg::error!("invalid path"))?;
		let artifact = directory
			.get(self, subpath)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the artifact from the cache"))?;
		let id = artifact.id();
		let referent = tg::Referent::with_item(id);
		let output = tg::checkin::Output { artifact: referent };
		Ok(output)
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

		// Attempt to get the graph, lock, and solutions from a watcher.
		let (mut graph, lock, mut solutions, version) = if arg.options.watch
			&& let Some(watch) = self.watches.get(root)
			&& watch.value().options() == &arg.options
		{
			let snapshot = watch.value().get();
			let graph = snapshot.graph;
			let lock = snapshot.lock;
			let solutions = snapshot.solutions;
			let version = Some(snapshot.version);
			(graph, lock, solutions, version)
		} else {
			let graph = Graph::default();
			let lock = None;
			let solutions = Solutions::default();
			let version = None;
			(graph, lock, solutions, version)
		};

		// Read the lock if it was not retrieved from the watcher and the lock option is set.
		let lock = if let Some(lock) = lock {
			Some(lock)
		} else if arg.options.lock.is_some() {
			Self::checkin_try_read_lock(root)
				.map_err(|source| tg::error!(!source, "failed to read the lock"))?
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
						.map_err(|source| tg::error!(!source, "the fixup task panicked"))
						.and_then(|result| result)
				});
			(Some(task), Some(sender))
		} else {
			(None, None)
		};

		// Collect input.
		let mut graph = tokio::task::spawn_blocking({
			let server = self.clone();
			let arg = arg.clone();
			let artifacts_path = artifacts_path.clone();
			let lock = lock.clone();
			let progress = progress.clone();
			let root = root.to_owned();
			move || {
				server.checkin_input(
					&arg,
					artifacts_path.as_deref(),
					fixup_sender,
					&mut graph,
					ignorer,
					lock.as_deref(),
					next,
					progress,
					&root,
				)?;
				Ok::<_, tg::Error>(graph)
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "the checkin input task panicked"))??;

		// Solve.
		if arg.options.solve {
			self.checkin_solve(
				&arg,
				&mut graph,
				next,
				lock.clone(),
				&mut solutions,
				root,
				progress,
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to solve dependencies"))?;
		}

		// Get reference path edges.
		let paths = self
			.checkin_path_get_edges(&graph, next)
			.await
			.map_err(|source| tg::error!(!source, "failed to get reference path edges"))?;

		// Set the touch time.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Create the output collections.
		let mut store_args = IndexMap::default();
		let mut index_object_args = IndexMap::default();
		let mut index_cache_entry_args = Vec::new();

		// Create blobs.
		self.checkin_create_blobs(
			&arg,
			&mut graph,
			next,
			&mut store_args,
			&mut index_object_args,
			touched_at,
			progress,
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create blobs"))?;

		// Create artifacts.
		Self::checkin_create_artifacts(
			&self.config.checkin,
			&arg,
			&mut graph,
			&paths,
			next,
			&mut store_args,
			&mut index_object_args,
			&mut index_cache_entry_args,
			root,
			touched_at,
		)?;

		// Cache.
		if arg.options.cache_pointers {
			if let Some(task) = fixup_task {
				task.await
					.map_err(|source| tg::error!(!source, "failed to run the fixup task"))?;
			}
			self.checkin_cache(&arg, &graph, next, root, progress)
				.await
				.map_err(|source| tg::error!(!source, "failed to cache"))?;
		}

		// Store.
		self.checkin_store(store_args.into_values().collect(), progress)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the objects to the store"))?;

		// Write the lock.
		self.checkin_write_lock(&arg, &graph, next, lock.as_deref(), root, progress)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the lock"))?;

		// If the watch option is enabled, then create or update the watcher, verify the version, and then spawn a task to clean nodes with no referrers.
		if self
			.config()
			.watch
			.as_ref()
			.is_some_and(|_| arg.options.watch)
		{
			// Create or update the watcher.
			let entry = self.watches.entry(root.to_owned());
			match entry {
				dashmap::Entry::Occupied(entry) => {
					// Verify the version.
					let watch = entry.get();

					// Update the watch.
					let success =
						watch.update(self, root, graph.clone(), lock, solutions, version, next);

					// If the update was not successful, then files were modified during checkin.
					if !success {
						return Err(tg::error!("files were modified during checkin"));
					}
				},
				dashmap::Entry::Vacant(entry) => {
					let watch = Watch::new(
						self,
						root,
						graph.clone(),
						lock,
						arg.options.clone(),
						solutions,
						next,
					)
					.map_err(|source| tg::error!(!source, "failed to create the watch"))?;
					entry.insert(watch);
				},
			}

			// Spawn a task to clean nodes with no referrers.
			tokio::task::spawn_blocking({
				let server = self.clone();
				let root = root.to_owned();
				let next = graph.next;
				move || {
					if let Some(watch) = server.watches.get(&root) {
						watch.clean(&root, next);
					}
				}
			});
		}

		// Spawn the index task.
		self.index_tasks
			.spawn({
				let server = self.clone();
				let arg = arg.clone();
				let graph = graph.clone();
				let root = root.to_owned();
				move |_| {
					async move {
						server
							.checkin_index(
								&arg,
								&graph,
								index_object_args,
								index_cache_entry_args,
								&root,
								touched_at,
							)
							.await
					}
					.map(|result| {
						if let Err(error) = result {
							tracing::error!(error = %error.trace(), "the index task failed");
						}
					})
				}
			})
			.detach();

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
					|source| tg::error!(!source, path = %path.display(), "failed to get the metadata"),
				)?;
				if metadata.is_dir()
					&& tg::module::try_get_root_module_file_name_sync(ancestor)?.is_some()
					&& ignorer
						.as_mut()
						.map(|ignorer| ignorer.matches(Some(ancestor), &path, None))
						.transpose()
						.map_err(|source| {
							tg::error!(!source, "failed to check if the path is ignored")
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
		.map_err(|source| tg::error!(!source, "the checkin root task panicked"))??;
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
			.map_err(|source| tg::error!(!source, "failed to create the matcher"))
	}

	pub(crate) async fn handle_checkin_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
			.checkin_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the checkin"))?;

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
