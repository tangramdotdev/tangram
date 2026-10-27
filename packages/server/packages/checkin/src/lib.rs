use {
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture},
	indexmap::IndexMap,
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
	tangram_server_core::{
		Session, checkin,
		checkin::{
			Contents, Directory, File, Graph, Node, ObjectOptions, Petgraph, Referrer, Solution,
			Solutions, Symlink, TaskOutput, Variant,
		},
		checkpoint,
		watch::{self, Watch},
	},
	tracing::Instrument as _,
};

mod artifact;
mod blob;
mod checkout;
mod fixup;
mod index;
mod input;
mod lock;
mod path;
mod solve;
mod store;

type IndexObjectArgs =
	IndexMap<tg::object::Id, tangram_index::object::put::Arg, tg::id::BuildHasher>;

type IndexCheckoutArgs = Vec<tangram_index::checkout::put::Arg>;

type CacheArgs = IndexMap<tg::object::Id, tangram_cache::object::put::Arg, tg::id::BuildHasher>;

type GraphData = IndexMap<tg::graph::Id, tg::graph::data::Leaf, tg::id::BuildHasher>;

type Tasks =
	tangram_futures::task::Map<TaskKey, (), tg::progress::Handle<TaskOutput>, fnv::FnvBuildHasher>;

/// The checkin component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	tasks: Tasks,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TaskKey {
	options: tg::checkin::Options,
	principal: tg::Principal,
	root: PathBuf,
	updates: Vec<tg::specifier::Pattern>,
}

#[derive(Clone, Copy)]
enum WatchObservation {
	Compatible { id: watch::Id, version: u64 },
	Incompatible { id: watch::Id, version: u64 },
	Vacant,
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		// Create the checkin tasks.
		let tasks = tangram_futures::task::Map::default();
		let state = State { tasks };

		Self(Arc::new(state))
	}
}

impl Component {
	#[tracing::instrument(
		fields(path = ?arg.path, root = arg.options.root),
		level = "trace",
		name = "checkin",
		skip_all
	)]
	pub(crate) async fn checkin(
		&self,
		session: &Session,
		mut arg: tg::checkin::Arg,
	) -> tg::Result<checkin::Stream> {
		if !session.server.checkouts_enabled() {
			return Err(tg::error!("checkouts are disabled"));
		}

		// Validate the arg.
		if session.server.config.advanced.multi_server {
			return Err(tg::error!("check-in is not supported in multi-server mode"));
		}

		// Canonicalize against the mounted store, whose parents exist even without a physical checkout.
		arg.path = session
			.host_path_for_guest_path_with_store_path(&arg.path, &session.server.store_path())?;
		if !arg.path.is_absolute() {
			return Err(tg::error!(path = ?arg.path, "the path must be absolute"));
		}
		arg.path = tangram_util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|error| tg::error!(!error, path = %arg.path.display(), "failed to canonicalize the path's parent"))?;

		// Recognize the store path after resolving parent symlinks and traversal components.
		if let Some(output) = self.try_checkin_store_path(session, &arg.path).await? {
			let capacity = session.server.config.progress.channel_capacity;
			let progress = tg::progress::Handle::new(capacity);
			progress.output(output);
			return Ok(progress
				.stream(session.server.config.progress.interval)
				.boxed());
		}

		// Create the ignorer and determine the root.
		let ignorer = arg
			.options
			.ignore
			.then(tangram_server_checkin_common::create_ignorer)
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
			principal: session.context.principal.clone(),
			root: root.clone(),
			updates: arg.updates.clone(),
		};
		let capacity = session.server.config.progress.channel_capacity;
		let root_task = self.tasks.get_or_spawn_with_context(
			key,
			|| tg::progress::Handle::new(capacity),
			|progress, _stop| {
				let component = self.clone();
				let session = session.clone();
				let arg = arg.clone();
				let root = root.clone();
				async move {
					let result = AssertUnwindSafe(
						component.checkin_task(&session, arg, &root, ignorer, &progress),
					)
					.catch_unwind()
					.boxed()
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
		let capacity = session.server.config.progress.channel_capacity;
		let progress = tg::progress::Handle::new(capacity);

		// Spawn the task.
		let now = session.server.clock.unix_timestamp();
		let path = arg.path.clone();
		let task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
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
				let mut stream =
					std::pin::pin!(root_progress.stream(session.server.config.progress.interval));
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
				let object_permissions = node
					.id
					.as_ref()
					.filter(|id| output.graph.ids.contains_key(*id))
					.map_or(node.permissions, |id| output.graph.object_permissions(id));

				// Determine the id.
				let id =
					if let tg::graph::data::Edge::Pointer(pointer) = node.edge.as_ref().unwrap() {
						// A reused watch may not have stored a pointer artifact for this graph member.
						let result = component
							.checkin_store_and_index_pointer_artifact(
								&session,
								node,
								pointer,
								object_permissions,
							)
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
				let permission = if object_permissions
					.contains(tg::authorization::permission::object::Set::SUBTREE)
				{
					tg::authorization::permission::object::Permission::Subtree
				} else {
					tg::authorization::permission::object::Permission::Node
				};
				let expires_at = now
					+ session
						.server
						.config
						.object
						.permission_time_to_live
						.as_secs()
						.to_i64()
						.unwrap();
				let token = match session.server.components.authorization.create_token(
					&session,
					id.clone().into(),
					vec![tg::authorization::Permission::Object(permission)],
					expires_at,
				) {
					Ok(token) => token,
					Err(error) => {
						progress.error(error);
						return;
					},
				};
				if let Some(token) = token {
					options.tokens.insert_local_authorization(token);
				}
				let referent = tg::Referent { node: id, options };
				let output = tg::checkin::Output { artifact: referent };
				progress.output(output);
			}
		});

		let stream = progress
			.stream(session.server.config.progress.interval)
			.attach(task)
			.attach(root_task)
			.boxed();

		Ok(stream)
	}

	pub(crate) fn checkin_merge_object_token(
		session: &Session,
		graph: &mut Graph,
		next: usize,
		id: &tg::object::Id,
		token: &tg::authorization::Token,
	) {
		if token.body.resource != tg::Id::from(id.clone())
			|| !session
				.server
				.components
				.authorization
				.verify_token(session, token)
		{
			return;
		}

		let mut object_permissions = tg::authorization::permission::object::Set::empty();
		for permission in &token.body.permissions {
			let tg::authorization::Permission::Object(permission) = permission else {
				continue;
			};
			object_permissions.insert(tg::authorization::permission::object::Set::from_permission(
				*permission,
			));
		}
		Self::checkin_merge_object_permissions(graph, next, id, object_permissions);
	}

	fn checkin_merge_object_permissions(
		graph: &mut Graph,
		next: usize,
		id: &tg::object::Id,
		new_permissions: tg::authorization::permission::object::Set,
	) -> usize {
		let index = graph.get_or_create_object_node(id, next);
		let permissions = &mut graph.nodes.get_mut(&index).unwrap().permissions;
		permissions.insert(new_permissions);
		if permissions.contains(tg::authorization::permission::object::Set::SUBTREE) {
			permissions.insert(tg::authorization::permission::object::Set::NODE);
		}

		index
	}

	fn checkin_record_object_children(
		graph: &mut Graph,
		next: usize,
		id: &tg::object::Id,
		children: impl FnOnce(&mut std::collections::BTreeSet<tg::object::Id>),
	) {
		let mut new_children = std::collections::BTreeSet::new();
		children(&mut new_children);
		let parent = graph.get_or_create_object_node(id, next);
		for id in new_children {
			let child = graph.get_or_create_object_node(&id, next);
			graph.add_object_edge(parent, child);
		}
	}

	fn checkin_record_object_data(
		graph: &mut Graph,
		next: usize,
		id: &tg::object::Id,
		data: &tg::object::Data,
	) {
		let index = graph.get_or_create_object_node(id, next);
		if data.is_graph() || graph.nodes.get(&index).unwrap().object_complete {
			return;
		}
		Self::checkin_record_object_children(graph, next, id, |children| {
			data.children(children);
		});
		graph.nodes.get_mut(&index).unwrap().object_complete = true;
	}

	// Check in the artifact.
	async fn checkin_task(
		&self,
		session: &Session,
		arg: tg::checkin::Arg,
		root: &Path,
		ignorer: Option<ignore::Ignorer>,
		progress: &tg::progress::Handle<TaskOutput>,
	) -> tg::Result<TaskOutput> {
		// Validate the arg.
		if arg.options.destructive && arg.options.ignore {
			return Err(tg::error!("ignore is forbidden for destructive checkins"));
		}

		// Try to find the store path.
		let store_path = root.join(".tangram/store");
		let store_path = if tokio::fs::try_exists(&store_path)
			.await
			.is_ok_and(|exists| exists)
		{
			Some(store_path)
		} else {
			None
		};
		let watch_key = watch::Key {
			path: root.to_owned(),
			principal: session.context.principal.clone(),
		};

		// Attempt to get the graph, lock, and solutions from a watcher.
		let watch = if arg.options.watch {
			session.server.components.watch.watches().get(&watch_key)
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
					let removed = session
						.server
						.components
						.watch
						.watches()
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
				session.server,
				"checkin.watch.snapshot",
				path = %root.display(),
				solve = arg.options.solve,
				updates,
			)
			.await;
		}

		// Revalidate the lock before reusing a watched graph because file events may still be queued.
		let lock = if arg.options.lock.is_some() {
			let current = checkin::try_read_lock(root)
				.map_err(|error| tg::error!(!error, "failed to read the lock"))?;
			if lock.as_deref() != current.as_ref() {
				graph = Graph::default();
				solutions = Solutions::default();
			}
			current.map(Arc::new)
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
			let session = session.clone();
			let arg = arg.clone();
			let store_path = store_path.clone();
			let lock = lock.clone();
			let progress = progress.clone();
			let root = root.to_owned();
			move || {
				let arg = input::CheckinInputArg {
					arg: &arg,
					fixup_sender,
					graph: &mut graph,
					ignorer,
					lock: lock.as_deref(),
					next,
					progress,
					root: &root,
					store_path: store_path.as_deref(),
				};
				Self::checkin_input(&session, arg)?;
				Ok::<_, tg::Error>(graph)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "the checkin input task panicked"))??;

		// Return immediately when a compatible watched checkin has no changes.
		if let WatchObservation::Compatible { id, version } = watch_observation
			&& graph.next == next
		{
			let current = session
				.server
				.components
				.watch
				.watches()
				.get(&watch_key)
				.filter(|watch| watch.id() == id && watch.options() == &arg.options)
				.is_some_and(|watch| watch.can_reuse(&graph, version));
			if !current {
				return Err(tg::error!("files were modified during checkin"));
			}
			let output = TaskOutput { graph };

			return Ok(output);
		}

		// Solve.
		if arg.options.solve {
			let solve_arg = solve::CheckinSolveArg {
				arg: &arg,
				graph: &mut graph,
				lock: lock.clone(),
				next,
				progress,
				root,
				solutions: &mut solutions,
			};
			self.checkin_solve(session, solve_arg)
				.boxed()
				.await
				.map_err(|error| tg::error!(!error, "failed to solve dependencies"))?;
		}
		// Get reference path edges.
		let paths = self
			.checkin_path_get_edges(session, &graph, next)
			.await
			.map_err(|error| tg::error!(!error, "failed to get reference path edges"))?;

		// Set the touch time.
		let touched_at = session.server.clock.unix_timestamp();

		// Create the output collections.
		let mut cache_args = IndexMap::default();
		let mut index_object_args = IndexMap::default();
		let mut index_checkout_args = Vec::new();
		let mut graph_data = IndexMap::default();

		// Create blobs.
		let create_blobs_arg = blob::CheckinCreateBlobsArg {
			arg: &arg,
			graph: &mut graph,
			next,
			cache_args: &mut cache_args,
			index_object_args: &mut index_object_args,
			touched_at,
			progress,
		};
		self.checkin_create_blobs(session, create_blobs_arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to create blobs"))?;

		// Create artifacts.
		let create_artifacts_arg = artifact::CheckinCreateArtifactsArg {
			arg: &arg,
			graph: &mut graph,
			paths: &paths,
			next,
			cache_args: &mut cache_args,
			index_object_args: &mut index_object_args,
			index_checkout_args: &mut index_checkout_args,
			graph_data: &mut graph_data,
			root,
			time_to_touch: session.server.config.object.time_to_touch,
			touched_at,
		};
		Self::checkin_create_artifacts(create_artifacts_arg)?;

		// Compute the permissions for the new objects.
		Self::checkin_compute_permissions(&mut graph, &index_object_args, next)?;

		// Check out.
		if arg.options.checkout_pointers {
			if let Some(task) = fixup_task {
				task.await
					.map_err(|error| tg::error!(!error, "failed to run the fixup task"))?;
			}
			let checkout_arg = checkout::CheckinCheckoutArg {
				arg: &arg,
				graph: &graph,
				graph_data: &mut graph_data,
				index_checkout_args: &index_checkout_args,
				next,
				progress,
				root,
			};
			self.checkin_checkout(session, checkout_arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to check out"))?;
		}

		// Store.
		self.checkin_store(session, cache_args.into_values().collect(), progress)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the objects to the cache"))?;

		// Write the lock.
		let reserve_lock_write = || match watch_observation {
			WatchObservation::Compatible { id, version }
			| WatchObservation::Incompatible { id, version } => {
				let watch = session
					.server
					.components
					.watch
					.watches()
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
		let account = session
			.server
			.components
			.usage
			.usage_account(session, &session.context.principal)
			.await?;
		let index_arg = index::CheckinIndexArg {
			arg: &arg,
			graph: &graph,
			index_checkout_args,
			index_object_args,
			root,
			touched_at,
		};
		let mut index_arg = Self::checkin_index(session, index_arg)?;
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
		if session
			.server
			.config
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
				session.server,
				"checkin.watch.publish",
				nodes = graph.nodes.len(),
				path = %root.display(),
				solve = arg.options.solve,
				updates,
			)
			.await;

			// Verify that the lock has the expected contents.
			if lock_write_guard.is_some() {
				let actual_lock = checkin::try_read_lock(root)
					.map_err(|error| tg::error!(!error, "failed to read the lock"))?;
				if lock.as_deref() != actual_lock.as_ref() {
					return Err(tg::error!("the lock was modified during checkin"));
				}
			}

			// Create or update the watcher.
			let entry = session
				.server
				.components
				.watch
				.watches()
				.entry(watch_key.clone());
			match (entry, watch_observation) {
				(dashmap::Entry::Occupied(entry), WatchObservation::Compatible { id, version })
					if entry.get().id() == id && entry.get().options() == &arg.options =>
				{
					// Verify the version.
					let watch = entry.get();

					// Update the watch.
					let update_arg = watch::UpdateArg {
						graph: graph.clone(),
						key: &watch_key,
						lock,
						lock_write_guard,
						next,
						solutions,
						version,
					};
					let success = watch.update(update_arg, || {
						Self::checkin_index_task(session, index_arg, &arg, root)
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
					let clock = session.server.clock.clone();
					let config = session.server.config.watch.clone().unwrap_or_default();
					let id = session.server.components.watch.create_watch_id();
					let expire = Self::checkin_watch_expire(session, &watch_key, id);
					let new_arg = watch::NewArg {
						clock,
						config,
						expire,
						graph: graph.clone(),
						id,
						lock,
						next,
						options: arg.options.clone(),
						solutions,
						spawn_index_task: || {
							Self::checkin_index_task(session, index_arg, &arg, root)
						},
					};
					let success = entry.get_mut().replace_if_version(
						&graph,
						version,
						lock_write_guard,
						|| {
							Watch::new(&watch_key, new_arg)
								.map_err(|error| tg::error!(!error, "failed to create the watch"))
						},
					)?;
					if !success {
						return Err(tg::error!("files were modified during checkin"));
					}
				},
				(dashmap::Entry::Vacant(entry), WatchObservation::Vacant) => {
					let clock = session.server.clock.clone();
					let config = session.server.config.watch.clone().unwrap_or_default();
					let id = session.server.components.watch.create_watch_id();
					let expire = Self::checkin_watch_expire(session, &watch_key, id);
					let new_arg = watch::NewArg {
						clock,
						config,
						expire,
						graph: graph.clone(),
						id,
						lock,
						next,
						options: arg.options.clone(),
						solutions,
						spawn_index_task: || {
							Self::checkin_index_task(session, index_arg, &arg, root)
						},
					};
					let watch = Watch::new(&watch_key, new_arg)
						.map_err(|error| tg::error!(!error, "failed to create the watch"))?;
					entry.insert(watch);
				},
				_ => return Err(tg::error!("the watch changed during checkin")),
			}
		} else {
			session
				.server
				.components
				.indexer
				.index_batch(&session.server, index_arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the checkin"))?;
		}

		let output = TaskOutput { graph };

		Ok(output)
	}

	fn checkin_watch_expire(session: &Session, key: &watch::Key, id: watch::Id) -> watch::Expire {
		let key = key.clone();
		let server = session.server.clone();
		Arc::new(move || {
			let key = key.clone();
			let server = server.clone();
			async move {
				server
					.components
					.watch
					.watches()
					.remove_if(&key, |_, watch| watch.id == id);
				crate::checkpoint!(server, "watch.removed").await;
			}
			.boxed()
		})
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

	pub(crate) async fn checkin_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
			.checkin(session, arg)
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
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

impl Default for Component {
	fn default() -> Self {
		Self::new()
	}
}

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {
	fn stop<'a>(&'a self, _server: &'a tangram_server_core::Server) -> BoxFuture<'a, ()> {
		async move {
			// Abort the checkin tasks.
			self.tasks.abort_all();
			let results = self.tasks.wait().await;
			for result in results {
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "a checkin task panicked");
				}
			}
			tracing::trace!("checkin tasks");
		}
		.boxed()
	}

	fn abort(&self) {
		self.tasks.abort_all();
	}
}

impl tangram_server_core::checkin::Component for Component {
	fn checkin<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::checkin::Arg,
	) -> BoxFuture<'a, tg::Result<checkin::Stream>> {
		self.checkin(session, arg).boxed()
	}

	fn checkin_request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.checkin_request(session, request).boxed()
	}
}
