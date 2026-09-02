use {
	crate::Server,
	notify::Watcher as _,
	std::{
		collections::HashSet,
		os::unix::fs::MetadataExt as _,
		path::{Path, PathBuf},
		sync::{Arc, Mutex, atomic::Ordering},
	},
	tangram_client::prelude::*,
	tangram_futures::task::{Shared, Task},
};

pub mod delete;
pub mod list;
pub mod touch;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Id(u64);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Key {
	pub path: PathBuf,
	pub principal: tg::Principal,
}

pub struct Watch {
	id: Id,
	options: tg::checkin::Options,
	state: Arc<Mutex<State>>,
	#[expect(dead_code)]
	task: Task<()>,
}

pub struct NewArg<F> {
	pub graph: crate::checkin::Graph,
	pub lock: Option<Arc<tg::graph::Data>>,
	pub next: usize,
	pub options: tg::checkin::Options,
	pub solutions: crate::checkin::Solutions,
	pub spawn_index_task: F,
}

pub struct LockWriteGuard {
	state: Option<Arc<Mutex<State>>>,
	version: u64,
}

struct State {
	affected_paths: HashSet<PathBuf, fnv::FnvBuildHasher>,
	graph: crate::checkin::Graph,
	index_task: Shared<tg::Result<()>>,
	invalidated_paths: HashSet<PathBuf, fnv::FnvBuildHasher>,
	lock: Option<Arc<tg::graph::Data>>,
	pending_lock_write_version: Option<u64>,
	#[cfg(target_os = "macos")]
	paths: HashSet<PathBuf, fnv::FnvBuildHasher>,
	revalidation_version: u64,
	sender: tokio::sync::mpsc::Sender<Message>,
	solutions: crate::checkin::Solutions,
	timeout_task: Option<Task<()>>,
	version: u64,
	watcher: notify::RecommendedWatcher,
}

pub struct Snapshot {
	pub graph: crate::checkin::Graph,
	pub id: Id,
	pub lock: Option<Arc<tg::graph::Data>>,
	pub solutions: crate::checkin::Solutions,
	pub version: u64,
}

pub struct UpdateArg<'a> {
	pub graph: crate::checkin::Graph,
	pub key: &'a Key,
	pub lock: Option<Arc<tg::graph::Data>>,
	pub lock_write_guard: Option<LockWriteGuard>,
	pub next: usize,
	pub server: &'a Server,
	pub solutions: crate::checkin::Solutions,
	pub version: u64,
}

struct Message {
	event: notify::Event,
	sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Id {
	#[must_use]
	fn new(server: &Server) -> Self {
		Self(server.next_watch_id.fetch_add(1, Ordering::Relaxed))
	}
}

impl LockWriteGuard {
	fn commit(mut self) {
		self.state = None;
	}
}

impl Watch {
	pub fn new<F>(server: &Server, key: &Key, arg: NewArg<F>) -> tg::Result<Self>
	where
		F: FnOnce() -> Shared<tg::Result<()>>,
	{
		#[cfg_attr(not(target_os = "linux"), expect(unused_variables))]
		let NewArg {
			graph,
			lock,
			next,
			options,
			solutions,
			spawn_index_task,
		} = arg;
		let id = Id::new(server);

		// Create the watcher.
		let config = notify::Config::default();
		let (sender, mut receiver) = tokio::sync::mpsc::channel::<Message>(1024);
		let handler = {
			let sender = sender.clone();
			move |result| match result {
				Ok(event) => {
					sender
						.blocking_send(Message {
							event,
							sender: None,
						})
						.ok();
				},
				Err(error) => {
					tracing::error!(?error);
				},
			}
		};
		let watcher = notify::RecommendedWatcher::new(handler, config)
			.map_err(|error| tg::error!(!error, "failed to create the watcher"))?;

		// Create the state.
		let index_task = spawn_index_task();
		let state = State {
			affected_paths: HashSet::default(),
			graph,
			index_task,
			invalidated_paths: HashSet::default(),
			lock,
			pending_lock_write_version: None,
			#[cfg(target_os = "macos")]
			paths: HashSet::default(),
			revalidation_version: 0,
			sender,
			solutions,
			timeout_task: None,
			version: 0,
			watcher,
		};
		let state = Arc::new(Mutex::new(state));

		// On Linux, add the paths.
		#[cfg(target_os = "linux")]
		state.lock().unwrap().add_paths_linux(next);

		// Spawn the task.
		let lockfile_path = key.path.join(tg::module::LOCKFILE_FILE_NAME);
		let uses_lock = options.lock.is_some();
		let task = Task::spawn({
			let lockfile_path = lockfile_path.clone();
			let state = state.clone();
			let root = key.path.clone();
			move |_| async move {
				while let Some(message) = receiver.recv().await {
					// Get the paths.
					let event_paths = message.event.paths.clone();
					let mut paths = Self::changes(&message.event);
					let lockfile_changed = paths.remove(lockfile_path.as_path());
					let only_lockfile_changed = message
						.event
						.paths
						.iter()
						.all(|path| path == &lockfile_path);
					if lockfile_changed && only_lockfile_changed {
						paths.remove(root.as_path());
					}
					let lock_changed = uses_lock && lockfile_changed;
					let read_lock =
						lock_changed && state.lock().unwrap().pending_lock_write_version.is_none();
					let lock = read_lock.then(|| crate::Session::checkin_try_read_lock(&root));

					// Lock the state.
					let mut state = state.lock().unwrap();

					// Update the lock unless an internal write is pending.
					let mut lock_modified = false;
					let mut modified = false;
					if state.pending_lock_write_version.is_none()
						&& let Some(lock) = lock
					{
						match lock {
							Ok(lock) => {
								let lock = lock.map(Arc::new);
								if state.lock != lock {
									state.lock = lock;
									lock_modified = true;
									modified = true;
								}
							},
							Err(error) => {
								tracing::error!(%error, "failed to read the lock");
								state.lock.take();
								lock_modified = true;
								modified = true;
							},
						}
					}

					// Update the nodes for the affected paths along with their ancestors.
					for path in paths {
						if state.affected_paths.contains(path) {
							modified = true;
						}
						let Some(index) = state.graph.paths.get(path).copied() else {
							continue;
						};
						modified = true;
						let mut queue = vec![index];
						let mut visited = HashSet::<usize, fnv::FnvBuildHasher>::default();
						while let Some(index) = queue.pop() {
							if !visited.insert(index) {
								continue;
							}

							// On linux, unwatch the path.
							#[cfg(target_os = "linux")]
							state.remove_path_linux(path);

							// Remove the node.
							let node = *state.graph.nodes.remove(&index).unwrap();
							tracing::trace!(path = ?node.path, edge = ?node.edge, "removed");
							if let Some(artifact) = &node.artifact {
								state.graph.artifacts.remove(artifact).unwrap();
							}
							if let Some(edge) = &node.edge
								&& let Some(id) = edge.try_unwrap_object_ref().ok()
								&& let Some(nodes) = state.graph.ids.get_mut(id)
							{
								nodes.retain(|i| *i != index);
								if nodes.is_empty() {
									state.graph.ids.remove(id);
								} else {
									let index = *nodes.last().unwrap();
									state
										.graph
										.nodes
										.get_mut(&index)
										.unwrap()
										.permissions
										.insert(node.permissions);
								}
							}
							if let Some(id) = &node.id
								&& let Some(nodes) = state.graph.ids.get_mut(id)
								&& nodes.contains(&index)
							{
								nodes.retain(|i| *i != index);
								if nodes.is_empty() {
									state.graph.ids.remove(id);
								} else {
									let index = *nodes.last().unwrap();
									state
										.graph
										.nodes
										.get_mut(&index)
										.unwrap()
										.permissions
										.insert(node.permissions);
								}
							}
							if let Some(path) = &node.path {
								state.graph.paths.remove(path).unwrap();
								state.affected_paths.insert(path.clone());
							}

							// Remove solutions that reference this node.
							state.solutions.remove_by_node(index);

							// Remove the node from its children's referrers and enqueue its children with no more referrers and no path.
							for child_index in node
								.children()
								.into_iter()
								.chain(node.object_children.iter().copied())
							{
								if let Some(child) = state.graph.nodes.get_mut(&child_index) {
									child.referrers.retain(|index_| *index_ != index);
									if child.referrers.is_empty() && child.path.is_none() {
										queue.push(child_index);
									}
								}
							}

							// Enqueue the node's referrers.
							for referrer in node.referrers {
								queue.push(referrer);
							}
						}
					}

					// Record the event and increment the version if the lock or any nodes changed.
					if modified {
						state.version += 1;
						if lock_modified {
							state.invalidated_paths.clear();
							state.revalidation_version = state.version;
						} else {
							state.invalidated_paths.extend(event_paths);
						}
					}

					// Notify any tasks waiting that the message has been received.
					if let Some(sender) = message.sender {
						sender.send(()).ok();
					}
				}
			}
		});

		// Spawn the timeout task.
		let timeout = Self::spawn_timeout_task(server, key, id);
		state.lock().unwrap().timeout_task.replace(timeout);

		let watch = Self {
			id,
			options,
			state,
			task,
		};

		Ok(watch)
	}

	pub fn id(&self) -> Id {
		self.id
	}

	pub fn options(&self) -> &tg::checkin::Options {
		&self.options
	}

	#[must_use]
	pub fn can_reuse(&self, graph: &crate::checkin::Graph, version: u64) -> bool {
		let state = self.state.lock().unwrap();
		state.pending_lock_write_version.is_none() && state.can_publish(graph, version)
	}

	pub fn replace_if_version<F>(
		&mut self,
		graph: &crate::checkin::Graph,
		version: u64,
		lock_write_guard: Option<LockWriteGuard>,
		replace: F,
	) -> tg::Result<bool>
	where
		F: FnOnce() -> tg::Result<Self>,
	{
		let state = self.state.clone();
		let mut state = state.lock().unwrap();
		let version = lock_write_guard
			.as_ref()
			.map_or(version, |lock_write_guard| lock_write_guard.version);
		if !state.lock_write_guard_matches(lock_write_guard.as_ref())
			|| !state.can_publish(graph, version)
		{
			drop(state);
			return Ok(false);
		}
		let watch = match replace() {
			Ok(watch) => watch,
			Err(error) => {
				drop(state);
				return Err(error);
			},
		};
		state.pending_lock_write_version.take();
		*self = watch;
		if let Some(lock_write_guard) = lock_write_guard {
			lock_write_guard.commit();
		}

		Ok(true)
	}

	pub fn try_reserve_lock_write(
		&self,
		graph: &crate::checkin::Graph,
		version: u64,
	) -> Option<LockWriteGuard> {
		let mut state = self.state.lock().unwrap();
		if state.pending_lock_write_version.is_some() || !state.can_publish(graph, version) {
			return None;
		}
		let version = state.version;
		state.pending_lock_write_version = Some(version);
		let lock_write_guard = LockWriteGuard {
			state: Some(self.state.clone()),
			version,
		};

		Some(lock_write_guard)
	}

	pub fn get(&self) -> impl Future<Output = tg::Result<Snapshot>> + Send + use<> {
		let id = self.id;
		let state = self.state.clone();
		async move {
			loop {
				let (index_task, version) = {
					let state = state.lock().unwrap();
					(state.index_task.clone(), state.version)
				};
				let result = index_task
					.wait()
					.await
					.map_err(|error| tg::error!(!error, "the indexing task panicked"))
					.and_then(|result| result);
				let state = state.lock().unwrap();
				if state.version != version {
					continue;
				}
				result?;
				let snapshot = Snapshot {
					graph: state.graph.clone(),
					id,
					lock: state.lock.clone(),
					solutions: state.solutions.clone(),
					version: state.version,
				};

				return Ok(snapshot);
			}
		}
	}

	pub fn get_unindexed(&self) -> Snapshot {
		let state = self.state.lock().unwrap();
		Snapshot {
			graph: state.graph.clone(),
			id: self.id,
			lock: state.lock.clone(),
			solutions: state.solutions.clone(),
			version: state.version,
		}
	}

	pub fn update<F>(&self, arg: UpdateArg<'_>, spawn_index_task: F) -> bool
	where
		F: FnOnce() -> Shared<tg::Result<()>>,
	{
		let UpdateArg {
			graph,
			key,
			lock,
			lock_write_guard,
			next,
			server,
			solutions,
			version,
		} = arg;
		let mut state = self.state.lock().unwrap();

		let version = lock_write_guard
			.as_ref()
			.map_or(version, |lock_write_guard| lock_write_guard.version);
		if !state.lock_write_guard_matches(lock_write_guard.as_ref())
			|| !state.can_publish(&graph, version)
		{
			drop(state);
			return false;
		}

		// Update the state.
		let index_task = spawn_index_task();
		state.affected_paths.clear();
		state.graph = graph;
		state.index_task = index_task;
		state.invalidated_paths.clear();
		state.lock = lock;
		state.pending_lock_write_version.take();
		state.solutions = solutions;
		state.version += 1;
		state.revalidation_version = state.version;

		// Reset the timeout task.
		state
			.timeout_task
			.replace(Self::spawn_timeout_task(server, key, self.id));

		// On Linux, add the new paths.
		#[cfg(target_os = "linux")]
		state.add_paths_linux(next);
		#[cfg(not(target_os = "linux"))]
		let _ = next;
		if let Some(lock_write_guard) = lock_write_guard {
			lock_write_guard.commit();
		}

		true
	}

	pub fn clean(&self, root: &Path, next: usize) {
		let mut state = self.state.lock().unwrap();

		// Only clean if the graph has not been modified.
		if state.graph.next != next {
			return;
		}

		// Clean the graph.
		#[cfg_attr(not(target_os = "linux"), expect(unused_variables))]
		let removed_paths = state.graph.clean(root);

		// Unwatch removed paths on Linux.
		#[cfg(target_os = "linux")]
		state.remove_paths_linux(&removed_paths);

		// Update paths on macOS.
		#[cfg(target_os = "macos")]
		state.update_paths_darwin();
	}

	fn changes(event: &notify::Event) -> HashSet<&Path, fnv::FnvBuildHasher> {
		let mut changes = HashSet::default();
		match &event.kind {
			notify::EventKind::Create(_)
			| notify::EventKind::Modify(notify::event::ModifyKind::Name(_))
			| notify::EventKind::Remove(_) => {
				for path in &event.paths {
					changes.insert(path.as_path());
					if let Some(parent) = path.parent() {
						changes.insert(parent);
					}
				}
			},
			notify::EventKind::Modify(
				notify::event::ModifyKind::Data(_) | notify::event::ModifyKind::Metadata(_) | _,
			)
			| notify::EventKind::Any
			| notify::EventKind::Other => {
				changes.extend(event.paths.iter().map(PathBuf::as_path));
			},
			notify::EventKind::Access(_) => (),
		}
		changes
	}

	fn spawn_timeout_task(server: &Server, key: &Key, id: Id) -> Task<()> {
		Task::spawn({
			let ttl = server.config.watch.clone().unwrap_or_default().ttl;
			let key = key.clone();
			let server = server.clone();
			async move |_stop| {
				// Wait for the TTL to expire.
				tokio::time::sleep(ttl).await;

				// Delete the watch.
				server.watches.remove_if(&key, |_, watch| watch.id == id);
			}
		})
	}
}

impl State {
	fn can_publish(&self, graph: &crate::checkin::Graph, version: u64) -> bool {
		if self.version == version {
			return true;
		}
		if version < self.revalidation_version || version > self.version {
			return false;
		}

		self.invalidated_paths
			.iter()
			.all(|path| Self::graph_matches_path(graph, path))
	}

	fn graph_matches_path(graph: &crate::checkin::Graph, path: &Path) -> bool {
		let graph_metadata = graph.paths.get(path).map(|index| {
			graph
				.nodes
				.get(index)
				.and_then(|node| node.path_metadata.as_ref())
		});
		let path_metadata = std::fs::symlink_metadata(path);
		match (graph_metadata, path_metadata) {
			(Some(Some(graph_metadata)), Ok(path_metadata)) => {
				Self::metadata_matches(graph_metadata, &path_metadata)
			},
			(None, Err(error))
				if matches!(
					error.kind(),
					std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
				) =>
			{
				true
			},
			_ => false,
		}
	}

	fn metadata_matches(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
		left.ctime() == right.ctime()
			&& left.ctime_nsec() == right.ctime_nsec()
			&& left.dev() == right.dev()
			&& left.gid() == right.gid()
			&& left.ino() == right.ino()
			&& left.len() == right.len()
			&& left.mode() == right.mode()
			&& left.mtime() == right.mtime()
			&& left.mtime_nsec() == right.mtime_nsec()
			&& left.nlink() == right.nlink()
			&& left.rdev() == right.rdev()
			&& left.uid() == right.uid()
	}

	fn lock_write_guard_matches(&self, lock_write_guard: Option<&LockWriteGuard>) -> bool {
		match (self.pending_lock_write_version, lock_write_guard) {
			(Some(version), Some(lock_write_guard)) => version == lock_write_guard.version,
			(None, None) => true,
			_ => false,
		}
	}

	#[cfg(target_os = "macos")]
	fn update_paths_darwin(&mut self) {
		// Get the new paths.
		let paths = self.graph.paths.roots();

		// Add the new paths.
		let mut watcher_paths = self.watcher.paths_mut();
		for path in &paths {
			if !self.paths.contains(path) {
				tracing::trace!(?path, "watched");
				let result = watcher_paths.add(path, notify::RecursiveMode::Recursive);
				if let Err(error) = result {
					tracing::error!(%error, ?path, "failed to watch the path");
				}
				self.paths.insert(path.clone());
			}
		}

		// Remove paths that are no longer in the graph.
		for path in self.paths.clone() {
			if !paths.contains(&path) {
				tracing::trace!(?path, "unwatched");
				let result = watcher_paths.remove(&path);
				if let Err(error) = result {
					tracing::error!(%error, ?path, "failed to unwatch the path");
				}
				self.paths.remove(&path);
			}
		}

		// Commit.
		let result = watcher_paths.commit();
		if let Err(error) = result {
			tracing::error!(%error, "failed to watch the paths");
		}
	}

	#[cfg(target_os = "linux")]
	fn add_paths_linux(&mut self, next: usize) {
		let mut paths = self.watcher.paths_mut();
		for path in self
			.graph
			.nodes
			.range(next..)
			.filter_map(|(_, node)| node.path.as_ref())
		{
			tracing::trace!(path = %path.display(), "watched");
			paths.add(path, notify::RecursiveMode::NonRecursive).ok();
		}
		paths.commit().ok();
	}

	#[cfg(target_os = "linux")]
	fn remove_path_linux(&mut self, path: &Path) {
		tracing::trace!(path = %path.display(), "unwatched");
		self.watcher.unwatch(path).ok();
	}

	#[cfg(target_os = "linux")]
	fn remove_paths_linux(&mut self, paths: &HashSet<PathBuf, fnv::FnvBuildHasher>) {
		let mut watcher_paths = self.watcher.paths_mut();
		for path in paths {
			tracing::trace!(path = %path.display(), "unwatched");
			watcher_paths.remove(path).ok();
		}
		watcher_paths.commit().ok();
	}
}

impl Drop for LockWriteGuard {
	fn drop(&mut self) {
		let Some(state) = self.state.take() else {
			return;
		};
		let mut state = state.lock().unwrap();
		if state.pending_lock_write_version == Some(self.version) {
			state.lock.take();
			state.pending_lock_write_version.take();
			state.version += 1;
			state.invalidated_paths.clear();
			state.revalidation_version = state.version;
		}
	}
}
