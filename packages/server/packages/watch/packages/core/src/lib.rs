use {
	futures::future::BoxFuture,
	notify::Watcher as _,
	std::{
		collections::HashSet, os::unix::fs::MetadataExt as _, path::Path, path::PathBuf, sync::Arc,
		sync::Mutex, sync::atomic::AtomicBool, sync::atomic::Ordering, time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::{task::Shared, task::Task},
	tangram_server_checkin_core::{Graph, Solutions, Variant, lockfile_path, try_read_lock},
	tangram_server_clock_core::Clock,
};

#[cfg(test)]
mod tests;

pub type Checkpoint = Arc<dyn Fn(PathBuf) -> BoxFuture<'static, ()> + Send + Sync>;

pub type Expire = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

#[cfg(target_os = "linux")]
type Paths = std::collections::BTreeMap<PathBuf, Inode>;
#[cfg(target_os = "macos")]
type Paths = HashSet<PathBuf, fnv::FnvBuildHasher>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Id(u64);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Key {
	pub path: PathBuf,
	pub principal: tg::Principal,
}

pub struct Watch {
	clock: Clock,

	expire: Expire,

	pub id: Id,

	pub options: tg::checkin::Options,

	pub state: Arc<Mutex<State>>,

	#[expect(dead_code)]
	task: Task<()>,

	time_to_live: Duration,
}

pub struct NewArg<F> {
	pub checkpoint: Option<Checkpoint>,
	pub clock: Clock,
	pub config: tangram_server_config::Watch,
	pub expire: Expire,
	pub graph: Graph,
	pub id: Id,
	pub lock: Option<Arc<tg::graph::data::Leaf>>,
	pub next: usize,
	pub options: tg::checkin::Options,
	pub solutions: Solutions,
	pub spawn_index_task: F,
}

pub struct LockWriteGuard {
	pub state: Option<Arc<Mutex<State>>>,
	pub version: u64,
}

pub struct State {
	pub affected_paths: HashSet<PathBuf, fnv::FnvBuildHasher>,

	pub graph: Graph,

	pub index_task: Shared<tg::Result<()>>,

	pub invalidated_paths: HashSet<PathBuf, fnv::FnvBuildHasher>,

	pub lock: Option<Arc<tg::graph::data::Leaf>>,

	pub overflowed: Arc<AtomicBool>,

	#[cfg(any(target_os = "linux", target_os = "macos"))]
	pub paths: Paths,

	pub pending_lock_write_version: Option<u64>,

	pub revalidation_version: u64,

	pub sender: tokio::sync::mpsc::Sender<Message>,

	pub solutions: Solutions,

	pub timeout_task: Option<Task<()>>,

	pub version: u64,

	pub watcher: Option<notify::RecommendedWatcher>,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Inode {
	pub device: u64,
	pub number: u64,
}

pub struct Snapshot {
	pub graph: Graph,
	pub id: Id,
	pub lock: Option<Arc<tg::graph::data::Leaf>>,
	pub solutions: Solutions,
	pub version: u64,
}

pub struct UpdateArg<'a> {
	pub graph: Graph,
	pub key: &'a Key,
	pub lock: Option<Arc<tg::graph::data::Leaf>>,
	pub lock_write_guard: Option<LockWriteGuard>,
	pub next: usize,
	pub solutions: Solutions,
	pub version: u64,
}

pub struct Message {
	pub event: notify::Event,
	pub sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Id {
	#[must_use]
	pub fn new(id: u64) -> Self {
		Self(id)
	}
}

impl LockWriteGuard {
	fn commit(mut self) {
		self.state = None;
	}
}

impl Watch {
	pub fn new<F>(key: &Key, arg: NewArg<F>) -> Self
	where
		F: FnOnce() -> Shared<tg::Result<()>>,
	{
		let NewArg {
			checkpoint,
			clock,
			config,
			expire,
			mut graph,
			id,
			lock,
			next,
			options,
			solutions,
			spawn_index_task,
		} = arg;

		// Clean the graph.
		graph.clean(&key.path, next);

		// Create the channel.
		let (sender, mut receiver) = tokio::sync::mpsc::channel::<Message>(config.channel_capacity);

		// Create the state.
		let index_task = spawn_index_task();
		let state = State {
			affected_paths: HashSet::default(),

			graph,

			index_task,

			invalidated_paths: HashSet::default(),

			lock,

			overflowed: Arc::new(AtomicBool::new(false)),

			#[cfg(any(target_os = "linux", target_os = "macos"))]
			paths: Paths::default(),

			pending_lock_write_version: None,

			revalidation_version: 0,

			sender,

			solutions,

			timeout_task: None,

			version: 0,

			watcher: None,
		};
		let state = Arc::new(Mutex::new(state));

		// Watch the paths.
		state.lock().unwrap().watch_paths(next);

		// Get the path of the lockfile, which a watch without a lock does not have, nor a root whose lockfile path is the root itself.
		let lockfile_path = options
			.lock
			.is_some()
			.then(|| lockfile_path(&key.path))
			.filter(|lockfile_path| lockfile_path != &key.path);

		// Spawn the task.
		let task = Task::spawn({
			let lockfile_path = lockfile_path.clone();
			let state = state.clone();
			let root = key.path.clone();
			move |_| async move {
				while let Some(message) = receiver.recv().await {
					// Get the paths.
					let event_paths = message.event.paths.clone();
					let mut paths = Self::changes(&message.event);
					let replaced_paths = Self::replacements(&message.event);
					let lockfile_changed = lockfile_path
						.as_ref()
						.is_some_and(|lockfile_path| paths.remove(lockfile_path.as_path()));
					let only_lockfile_changed = message
						.event
						.paths
						.iter()
						.all(|path| lockfile_path.as_ref() == Some(path));
					if lockfile_changed
						&& only_lockfile_changed
						&& let Some(parent) = lockfile_path.as_ref().and_then(|path| path.parent())
					{
						paths.remove(parent);
					}
					let read_lock = lockfile_changed
						&& state.lock().unwrap().pending_lock_write_version.is_none();
					let lock = read_lock.then(|| try_read_lock(&root));

					// Apply the event to the state.
					let changed_paths = {
						// Lock the state.
						let mut state = state.lock().unwrap();

						// Discard the graph if the channel or the watcher dropped events, so that the next checkin reads every path.
						let overflowed = state.overflowed.swap(false, Ordering::SeqCst);
						let reset = overflowed || message.event.need_rescan();
						if reset {
							state.reset();
						}

						// Retain the graph's lock so the next checkin detects the change and rebuilds it.
						let mut lock_modified = false;
						let mut modified = false;
						if state.pending_lock_write_version.is_none()
							&& let Some(lock) = lock
						{
							match lock {
								Ok(lock) => {
									let lock = lock.map(Arc::new);
									if state.lock != lock {
										lock_modified = true;
										modified = true;
									}
								},
								Err(error) => {
									tracing::error!(%error, "failed to read the lock");
									lock_modified = true;
									modified = true;
								},
							}
						}

						// Update the nodes for the affected paths along with their ancestors.
						let mut root_ignored = false;
						for path in paths {
							// Ignore delayed root notifications that match the metadata after the lock write.
							if message.sender.is_none()
								&& path == root.as_path()
								&& State::graph_matches_path(&state.graph, path)
							{
								root_ignored = true;
								continue;
							}
							if state.affected_paths.contains(path) {
								modified = true;
							}
							if state.remove_path(path) {
								modified = true;
							}
						}

						// Remove the nodes beneath each created, renamed, or removed path, because no event reports a change made beneath a path while it is elsewhere, and the watcher stops watching the paths beneath it.
						for path in replaced_paths {
							if root_ignored && path == root.as_path() {
								continue;
							}
							if state.remove_descendants(path) {
								modified = true;
							}
						}

						// Record the event and increment the version if the lock or any nodes changed.
						if modified {
							state.version += 1;
							if lock_modified {
								state.invalidated_paths.clear();
								state.revalidation_version = state.version;
							} else {
								state.invalidated_paths.extend(event_paths.iter().cloned());
							}
						}

						// Collect the paths that the event changed, including the root if the graph was discarded.
						let mut changed_paths = Vec::new();
						if reset {
							changed_paths.push(root.clone());
						}
						if modified {
							changed_paths.extend(event_paths);
						}

						changed_paths
					};

					// Notify any tasks waiting that the message has been received.
					if let Some(sender) = message.sender {
						sender.send(()).ok();
					}

					// Pass the checkpoint for each changed path, after the watch has applied the event.
					if let Some(checkpoint) = &checkpoint {
						for path in changed_paths {
							checkpoint(path).await;
						}
					}
				}
			}
		});

		// Spawn the timeout task.
		let time_to_live = config.time_to_live;
		let timeout = Self::spawn_timeout_task(&clock, time_to_live, &expire);
		state.lock().unwrap().timeout_task.replace(timeout);

		Self {
			clock,
			expire,
			id,
			options,
			state,
			task,
			time_to_live,
		}
	}

	pub fn invalidate_objects(&self, objects: &[tg::object::Id]) {
		let mut state = self.state.lock().unwrap();
		if !objects.iter().any(|id| state.graph.ids.contains_key(id)) {
			return;
		}

		state.reset();
	}

	#[must_use]
	pub fn id(&self) -> Id {
		self.id
	}

	#[must_use]
	pub fn options(&self) -> &tg::checkin::Options {
		&self.options
	}

	#[must_use]
	pub fn can_reuse(&self, graph: &Graph, version: u64) -> bool {
		let state = self.state.lock().unwrap();
		state.pending_lock_write_version.is_none() && state.can_publish(graph, version)
	}

	pub fn replace_if_version<F>(
		&mut self,
		graph: &Graph,
		version: u64,
		lock_write_guard: Option<LockWriteGuard>,
		replace: F,
	) -> bool
	where
		F: FnOnce() -> Self,
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
			return false;
		}
		let watch = replace();
		state.pending_lock_write_version.take();
		*self = watch;
		if let Some(lock_write_guard) = lock_write_guard {
			lock_write_guard.commit();
		}

		true
	}

	#[must_use]
	pub fn try_reserve_lock_write(&self, graph: &Graph, version: u64) -> Option<LockWriteGuard> {
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
				let snapshot = {
					let state = state.lock().unwrap();
					if state.version != version {
						continue;
					}
					result?;
					state.snapshot(id)
				};

				// Find the paths that changed and whose events have not arrived, without holding the lock.
				let graph = snapshot.graph.clone();
				let paths = tokio::task::spawn_blocking(move || State::changed_paths(&graph, 0))
					.await
					.map_err(|error| tg::error!(!error, "the changed paths task panicked"))?;
				if paths.is_empty() {
					return Ok(snapshot);
				}

				// Remove the nodes for the paths, unless the state changed in the meantime.
				let mut state = state.lock().unwrap();
				if state.version != snapshot.version {
					continue;
				}
				state.remove_changed_paths(&paths);
				let snapshot = state.snapshot(id);

				return Ok(snapshot);
			}
		}
	}

	pub fn update<F>(&self, arg: UpdateArg<'_>, spawn_index_task: F) -> bool
	where
		F: FnOnce() -> Shared<tg::Result<()>>,
	{
		let UpdateArg {
			mut graph,
			key,
			lock,
			lock_write_guard,
			next,
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
		let mut removed_paths = graph.clean(&key.path, next);

		// Update the state, and collect the paths that invalidation removed from the graph.
		let index_task = spawn_index_task();
		removed_paths.extend(state.affected_paths.drain());
		state.graph = graph;
		state.index_task = index_task;
		state.invalidated_paths.clear();
		state.lock = lock;
		state.pending_lock_write_version.take();
		state.solutions = solutions;
		state.version += 1;
		state.revalidation_version = state.version;

		// Reset the timeout task.
		state.timeout_task.replace(Self::spawn_timeout_task(
			&self.clock,
			self.time_to_live,
			&self.expire,
		));

		// Update the watched paths.
		#[cfg(target_os = "linux")]
		state.unwatch_paths_linux(&removed_paths);
		state.watch_paths(next);
		if let Some(lock_write_guard) = lock_write_guard {
			lock_write_guard.commit();
		}

		true
	}

	pub fn reset_timeout(&self) {
		let timeout = Self::spawn_timeout_task(&self.clock, self.time_to_live, &self.expire);
		self.state.lock().unwrap().timeout_task.replace(timeout);
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

	fn replacements(event: &notify::Event) -> &[PathBuf] {
		match &event.kind {
			notify::EventKind::Create(_)
			| notify::EventKind::Modify(notify::event::ModifyKind::Name(_))
			| notify::EventKind::Remove(_) => &event.paths,
			notify::EventKind::Access(_)
			| notify::EventKind::Any
			| notify::EventKind::Modify(_)
			| notify::EventKind::Other => &[],
		}
	}

	fn spawn_timeout_task(clock: &Clock, time_to_live: Duration, expire: &Expire) -> Task<()> {
		Task::spawn({
			let clock = clock.clone();
			let expire = expire.clone();
			async move |_stop| {
				// Wait for the ttl to expire.
				clock.sleep(time_to_live).await;

				// Expire the watch.
				expire().await;
			}
		})
	}
}

impl State {
	fn snapshot(&self, id: Id) -> Snapshot {
		Snapshot {
			graph: self.graph.clone(),
			id,
			lock: self.lock.clone(),
			solutions: self.solutions.clone(),
			version: self.version,
		}
	}

	fn can_publish(&self, graph: &Graph, version: u64) -> bool {
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

	fn graph_matches_path(graph: &Graph, path: &Path) -> bool {
		let graph_index = graph.paths.get(path);
		let graph_metadata = graph_index.and_then(|index| {
			graph
				.nodes
				.get(index)
				.and_then(|node| node.path_metadata.as_ref())
		});
		let path_metadata = std::fs::symlink_metadata(path);
		match (graph_metadata, path_metadata) {
			(Some(graph_metadata), Ok(path_metadata)) => {
				Self::metadata_matches(graph_metadata, &path_metadata)
			},
			(None, Err(error))
				if graph_index.is_none()
					&& matches!(
						error.kind(),
						std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
					) =>
			{
				true
			},
			(None, Ok(_)) if graph_index.is_none() => Self::graph_ignores_path(graph, path),
			_ => false,
		}
	}

	// Whether the graph ignores a path, which is the case if the nearest directory above the path that is in the graph ignored the entry that leads to the path.
	fn graph_ignores_path(graph: &Graph, path: &Path) -> bool {
		let mut child = path;
		while let Some(parent) = child.parent() {
			let Some(index) = graph.paths.get(parent) else {
				child = parent;
				continue;
			};
			let Some(name) = child.file_name().and_then(|name| name.to_str()) else {
				return false;
			};
			let node = graph.nodes.get(index).unwrap();
			return match &node.variant {
				Variant::Directory(directory) => directory.ignored.contains(name),
				Variant::File(_) | Variant::Object | Variant::Symlink(_) => false,
			};
		}

		false
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

	fn watch_paths(&mut self, next: usize) {
		// Discard the graph if any of its paths is not watched, so that the next checkin reads every path.
		let next = match self.try_watch_paths(next) {
			Ok(next) => next,
			Err(error) => {
				tracing::warn!(%error, "failed to watch the paths");
				self.reset();
				return;
			},
		};

		// Remove the nodes for paths that changed after the checkin read them and before they were watched, because no event reports such a change.
		let paths = Self::changed_paths(&self.graph, next);
		self.remove_changed_paths(&paths);
	}

	fn try_watch_paths(&mut self, next: usize) -> tg::Result<usize> {
		// Create the watcher if there is none, and watch every path with a new watcher.
		let next = if self.watcher.is_some() {
			next
		} else {
			self.watcher.replace(self.create_watcher()?);
			0
		};

		// Watch the paths.
		#[cfg(target_os = "linux")]
		self.update_paths_linux(next)?;
		#[cfg(target_os = "macos")]
		self.update_paths_darwin()?;

		Ok(next)
	}

	// Find the paths of the nodes from `next` whose metadata no longer matches the graph.
	fn changed_paths(graph: &Graph, next: usize) -> Vec<PathBuf> {
		graph
			.nodes
			.range(next..)
			.filter_map(|(_, node)| node.path.as_ref())
			.filter(|path| !Self::graph_matches_path(graph, path))
			.cloned()
			.collect()
	}

	// Remove the nodes for paths that changed and invalidate any checkins that read the paths before the change.
	fn remove_changed_paths(&mut self, paths: &[PathBuf]) {
		if paths.is_empty() {
			return;
		}
		for path in paths {
			tracing::trace!(path = %path.display(), "changed before its event");
			self.remove_path(path);
		}
		self.version += 1;
		self.invalidated_paths.extend(paths.iter().cloned());
	}

	fn remove_descendants(&mut self, path: &Path) -> bool {
		let mut removed = false;
		for path in self.graph.paths.descendants(path) {
			removed |= self.remove_path(&path);
		}
		removed
	}

	fn remove_path(&mut self, path: &Path) -> bool {
		// Get the path's node.
		let Some(index) = self.graph.paths.get(path).copied() else {
			return false;
		};

		// Remove the node along with its referrers and its children with no more referrers and no path.
		let mut queue = vec![index];
		let mut visited = HashSet::<usize, fnv::FnvBuildHasher>::default();
		while let Some(index) = queue.pop() {
			if !visited.insert(index) {
				continue;
			}

			// Remove the node.
			let node = *self.graph.nodes.remove(&index).unwrap();
			tracing::trace!(path = ?node.path, edge = ?node.edge, "removed");
			if let Some(artifact) = &node.artifact {
				self.graph.artifacts.remove(artifact).unwrap();
			}
			if let Some(id) = &node.id {
				self.graph.remove_id(index, id, node.permissions);
			}
			if let Some(path) = &node.path {
				self.graph.paths.remove(path).unwrap();
				self.affected_paths.insert(path.clone());
			}

			// Remove solutions that reference this node.
			self.solutions.remove_by_node(index);

			// Remove the node from its children's referrers and enqueue its children with no more referrers and no path.
			for child_index in node
				.children()
				.into_iter()
				.chain(node.object_children.iter().copied())
			{
				if let Some(child) = self.graph.nodes.get_mut(&child_index) {
					child.referrers.remove(&index);
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

		true
	}

	fn create_watcher(&self) -> tg::Result<notify::RecommendedWatcher> {
		let handler = {
			let overflowed = self.overflowed.clone();
			let sender = self.sender.clone();
			move |result| match result {
				Ok(event) => {
					// Keep the send nonblocking because this watch's task may be waiting for this thread to watch or unwatch a path.
					let message = Message {
						event,
						sender: None,
					};
					let result = sender.try_send(message);
					if let Err(tokio::sync::mpsc::error::TrySendError::Full(message)) = result {
						Self::resend_after_overflow(&overflowed, &sender, message);
					}
				},
				Err(error) => {
					tracing::error!(?error);
				},
			}
		};
		let watcher = notify::RecommendedWatcher::new(handler, notify::Config::default())
			.map_err(|error| tg::error!(!error, "failed to create the watcher"))?;

		Ok(watcher)
	}

	fn resend_after_overflow(
		overflowed: &AtomicBool,
		sender: &tokio::sync::mpsc::Sender<Message>,
		message: Message,
	) {
		// Retry after setting the flag so that either this message or a queued message makes the task observe the overflow.
		overflowed.store(true, Ordering::SeqCst);
		sender.try_send(message).ok();
	}

	fn reset(&mut self) {
		// Discard the graph and invalidate any checkins that are still using its previous version.
		self.affected_paths.clear();
		self.graph = Graph::default();
		self.invalidated_paths.clear();
		self.solutions = Solutions::default();
		self.version += 1;
		self.revalidation_version = self.version;

		// Drop the watcher, which unwatches every path and releases its resources, and replace its overflow flag, so that events it drops while it stops do not reset the watch again.
		self.watcher.take();
		self.overflowed = Arc::new(AtomicBool::new(false));
		#[cfg(any(target_os = "linux", target_os = "macos"))]
		self.paths.clear();
	}

	#[cfg(target_os = "macos")]
	fn update_paths_darwin(&mut self) -> tg::Result<()> {
		// Get the new paths.
		let paths = self.graph.paths.roots();

		// Add the new paths.
		let watcher = self.watcher.as_mut().unwrap();
		let mut watcher_paths = watcher.paths_mut();
		for path in &paths {
			if !self.paths.contains(path) {
				tracing::trace!(?path, "watched");
				watcher_paths
					.add(path, notify::RecursiveMode::Recursive)
					.map_err(
						|error| tg::error!(!error, path = %path.display(), "failed to watch the path"),
					)?;
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
		watcher_paths
			.commit()
			.map_err(|error| tg::error!(!error, "failed to start watching the paths"))?;

		Ok(())
	}

	#[cfg(target_os = "linux")]
	fn unwatch_paths_linux(&mut self, removed_paths: &HashSet<PathBuf, fnv::FnvBuildHasher>) {
		// Unwatch the removed paths that are no longer in the graph, including paths removed by invalidation.
		let Some(watcher) = &mut self.watcher else {
			return;
		};
		let mut watcher_paths = watcher.paths_mut();
		for path in removed_paths {
			if self.graph.paths.get(path).is_some() || self.paths.remove(path).is_none() {
				continue;
			}
			tracing::trace!(path = %path.display(), "unwatched");
			watcher_paths.remove(path).ok();
		}
		watcher_paths.commit().ok();
	}

	#[cfg(target_os = "linux")]
	fn update_paths_linux(&mut self, next: usize) -> tg::Result<()> {
		// Add the new paths, stopping at the first path that cannot be watched.
		let watcher = self.watcher.as_mut().unwrap();
		let mut watcher_paths = watcher.paths_mut();
		for (_, node) in self.graph.nodes.range(next..) {
			let Some(path) = &node.path else {
				continue;
			};
			tracing::trace!(path = %path.display(), "watched");
			let result = Self::add_path_linux(&mut *watcher_paths, &mut self.paths, path);
			let Err(error) = result else {
				continue;
			};

			// Skip a symlink whose target cannot be watched, such as a dangling symlink, because the watch on its parent directory observes the symlink itself.
			let symlink = node
				.path_metadata
				.as_ref()
				.is_some_and(|metadata| metadata.file_type().is_symlink());
			let parent = path
				.parent()
				.is_some_and(|parent| self.graph.paths.get(parent).is_some());
			if symlink && parent {
				continue;
			}

			return Err(tg::error!(!error, path = %path.display(), "failed to watch the path"));
		}

		// Commit.
		watcher_paths
			.commit()
			.map_err(|error| tg::error!(!error, "failed to watch the paths"))?;

		Ok(())
	}

	#[cfg(target_os = "linux")]
	fn add_path_linux(
		watcher: &mut dyn notify::PathsMut,
		paths: &mut Paths,
		path: &Path,
	) -> tg::Result<()> {
		// Follow symlinks to identify the inode that inotify watches.
		let inode = std::fs::metadata(path).map(|metadata| Inode {
			device: metadata.dev(),
			number: metadata.ino(),
		});

		// Remove the old inode's watch before registering a replacement at the same path.
		if let Some(previous) = paths.get(path)
			&& inode.as_ref().ok() != Some(previous)
		{
			watcher.remove(path).ok();
			paths.remove(path);
		}
		let inode = inode.map_err(|error| tg::error!(!error, "failed to get the metadata"))?;
		watcher
			.add(path, notify::RecursiveMode::NonRecursive)
			.map_err(|error| tg::error!(!error, "failed to watch the path"))?;
		paths.insert(path.to_owned(), inode);

		Ok(())
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
