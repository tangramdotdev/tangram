use tracing::Instrument as _;

use {
	crate::{Context, Server, watch::Watch},
	futures::{FutureExt as _, Stream, StreamExt as _},
	indexmap::IndexMap,
	indoc::indoc,
	std::{
		panic::AssertUnwindSafe,
		path::{Path, PathBuf},
		sync::Arc,
		time::Instant,
	},
	tangram_client::prelude::*,
	tangram_futures::stream::Ext as _,
	tangram_http::{Body, request::Ext as _},
	tangram_ignore as ignore,
	tokio_util::task::AbortOnDropHandle,
};

mod artifact;
mod blob;
mod cache;
mod fixup;
mod graph;
mod index;
mod input;
mod lock;
mod solve;
mod store;

pub(crate) use self::{graph::Graph, solve::Solutions};

type StoreArgs = IndexMap<tg::object::Id, crate::store::PutArg, tg::id::BuildHasher>;

type IndexObjectMessages =
	IndexMap<tg::object::Id, crate::index::message::PutObject, tg::id::BuildHasher>;

type IndexCacheEntryMessages = Vec<crate::index::message::PutCacheEntry>;

impl Server {
	#[tracing::instrument(fields(path = ?arg.path), name = "checkin", skip_all)]
	pub(crate) async fn checkin_with_context(
		&self,
		context: &Context,
		mut arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + use<>,
	> {
		if let Some(process) = &context.process {
			arg.path = process.host_path_for_guest_path(arg.path.clone());
		}
		let progress = crate::progress::Handle::new();
		let task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				let result = AssertUnwindSafe(server.checkin_inner(arg, &progress))
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
						progress.error(tg::error!(?message, "the task panicked"));
					},
				}
			}
			.instrument(tracing::Span::current())
		}));
		let stream = progress.stream().attach(task);
		Ok(stream)
	}

	// Check in the artifact.
	async fn checkin_inner(
		&self,
		mut arg: tg::checkin::Arg,
		progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<tg::checkin::Output> {
		// Validate the arg.
		if arg.options.destructive && arg.options.ignore {
			return Err(tg::error!("ignore is forbidden for destructive checkins"));
		}

		// Canonicalize the path's parent.
		arg.path = tangram_util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, path = %&arg.path.display(), "failed to canonicalize the path's parent"))?;

		// If this is a checkin of a path in the cache directory, then retrieve the corresponding artifact.
		if let Ok(path) = arg.path.strip_prefix(self.cache_path()) {
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
				.parse()?;
			if path.components().count() == 1 {
				let output = tg::checkin::Output {
					artifact: tg::Referent::with_item(id),
				};
				return Ok(output);
			}
			let path = path.components().skip(1).collect::<PathBuf>();
			let artifact = tg::Artifact::with_id(id);
			let directory = artifact
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?;
			let artifact = directory.get(self, path).await?;
			let id = artifact.id();
			let referent = tg::Referent::with_item(id);
			let output = tg::checkin::Output { artifact: referent };
			return Ok(output);
		}

		// Create the ignorer.
		let ignorer = arg
			.options
			.ignore
			.then(Self::checkin_create_ignorer)
			.transpose()?;

		// Find the root.
		let (root, ignorer) = self.checkin_find_root_path(&arg.path, ignorer).await?;

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
			&& let Some(watch) = self.watches.get(&root)
			&& let state = watch.value().state.lock().unwrap()
			&& state.options == arg.options
		{
			let graph = state.graph.clone();
			let lock = state.lock.clone();
			let solutions = state.solutions.clone();
			let version = Some(state.version);
			(graph, lock, solutions, version)
		} else {
			let graph = Graph::default();
			let lock = None;
			let solutions = Solutions::default();
			let version = None;
			(graph, lock, solutions, version)
		};

		// Read the lock if it was not retrieved from the watcher.
		let lock = if let Some(lock) = lock {
			Some(lock)
		} else {
			Self::checkin_try_read_lock(&root)
				.map_err(|source| tg::error!(!source, "failed to read the lock"))?
				.map(Arc::new)
		};

		// Get the next node index.
		let next = graph.next;

		// Spawn the fixup task.
		let (fixup_task, fixup_sender) = if arg.options.destructive {
			let (sender, receiver) = std::sync::mpsc::channel();
			let task = tokio::task::spawn_blocking(move || Self::checkin_fixup_task(&receiver))
				.map(|result| result.unwrap());
			(Some(task), Some(sender))
		} else {
			(None, None)
		};

		// Collect input.
		let start = Instant::now();
		let mut graph = tokio::task::spawn_blocking({
			let server = self.clone();
			let arg = arg.clone();
			let artifacts_path = artifacts_path.clone();
			let lock = lock.clone();
			let progress = progress.clone();
			let root = root.clone();
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
		.unwrap()?;
		tracing::trace!(elapsed = ?start.elapsed(), "collect input");

		// Solve.
		if arg.options.solve {
			let start = Instant::now();
			self.checkin_solve(&arg, &mut graph, next, lock.clone(), &mut solutions, &root)
				.await?;
			tracing::trace!(elapsed = ?start.elapsed(), "solve");
		}

		// Set the touch time.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Create the output collections.
		let mut store_args = IndexMap::default();
		let mut object_messages = IndexMap::default();
		let mut cache_entry_messages = Vec::new();

		// Create blobs.
		let start = Instant::now();
		self.checkin_create_blobs(
			&mut graph,
			next,
			&mut store_args,
			&mut object_messages,
			touched_at,
		)
		.await?;
		tracing::trace!(elapsed = ?start.elapsed(), "create blobs");

		// Create artifacts.
		let start = Instant::now();
		Self::checkin_create_artifacts(
			&arg,
			&mut graph,
			next,
			&mut store_args,
			&mut object_messages,
			&mut cache_entry_messages,
			&root,
			touched_at,
		)?;
		tracing::trace!(elapsed = ?start.elapsed(), "create objects");

		// Cache.
		if let Some(task) = fixup_task {
			task.await?;
		}
		let start = Instant::now();
		self.checkin_cache(&arg, &graph, next, &root)
			.await
			.map_err(|source| tg::error!(!source, "failed to cache"))?;
		tracing::trace!(elapsed = ?start.elapsed(), "cache");

		// Store.
		let start = Instant::now();
		self.checkin_store(store_args.into_values().collect())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the objects to the store"))?;
		tracing::trace!(elapsed = ?start.elapsed(), "write objects to store");

		// Write the lock.
		let start = Instant::now();
		self.checkin_write_lock(&arg, &graph, next, lock.as_deref(), &root)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the lock"))?;
		tracing::trace!(elapsed = ?start.elapsed(), "create lock");

		// If the watch option is enabled, then create or update the watcher, verify the version, and then spawn a task to clean nodes with no referrers.
		if arg.options.watch {
			// Create or update the watcher.
			let entry = self.watches.entry(root.clone());
			match entry {
				dashmap::Entry::Occupied(entry) => {
					let mut state = entry.get().state.lock().unwrap();
					if let Some(version) = version {
						let current = state.version;
						if current != version {
							return Err(tg::error!("files were modified during checkin"));
						}
					}
					state.graph = graph.clone();
					state.lock = lock;
				},
				dashmap::Entry::Vacant(entry) => {
					let watch =
						Watch::new(&root, graph.clone(), lock, arg.options.clone(), solutions)
							.map_err(|source| tg::error!(!source, "failed to create the watch"))?;
					entry.insert(watch);
				},
			}

			// On linux, watch the new paths.

			// Spawn a task to clean nodes with no referrers.
			tokio::task::spawn_blocking({
				let server = self.clone();
				let root = root.clone();
				let next = graph.next;
				move || {
					// Get the graph.
					let Some(watch) = server.watches.get(&root) else {
						return;
					};
					let mut state = watch.state.lock().unwrap();
					let graph = &mut state.graph;

					// Only clean if the graph has not been modified.
					if graph.next != next {
						return;
					}

					// Clean the graph.
					graph.clean(&root);

					// Update paths on darwin.
					#[cfg(target_os = "macos")]
					state.update_paths();
				}
			});
		}

		// Spawn the index task.
		self.tasks.spawn({
			let server = self.clone();
			let arg = arg.clone();
			let graph = graph.clone();
			let root = root.clone();
			move |_| {
				async move {
					server
						.checkin_index(
							&arg,
							&graph,
							object_messages,
							cache_entry_messages,
							&root,
							touched_at,
						)
						.await
				}
				.map(|result| {
					if let Err(error) = result {
						tracing::error!(?error, "the index task failed");
					}
				})
			}
		});

		// Find the item.
		let node = graph
			.paths
			.get(&arg.path)
			.copied()
			.ok_or_else(|| tg::error!("failed to get the item"))?;

		// Create the referent.
		let item = graph
			.nodes
			.get(&node)
			.unwrap()
			.id
			.as_ref()
			.unwrap()
			.clone()
			.try_into()
			.unwrap();
		let options = tg::referent::Options::with_path(arg.path.clone());
		let referent = tg::Referent { item, options };

		// Create the output.
		let output = tg::checkin::Output { artifact: referent };

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
					&& tg::package::try_get_root_module_file_name_sync(ancestor)?.is_some()
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
		.unwrap()?;
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
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = self.checkin_with_context(context, arg).await?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
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
