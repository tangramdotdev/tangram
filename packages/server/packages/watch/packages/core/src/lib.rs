use {
	futures::future::BoxFuture,
	notify::Watcher as _,
	std::{
		collections::HashSet, os::unix::fs::MetadataExt as _, path::Path, path::PathBuf, sync::Arc,
		sync::Mutex, sync::atomic::AtomicBool, sync::atomic::Ordering, time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::{task::Shared, task::Task},
	tangram_server_checkin_core::{Graph, Solutions, try_read_lock},
	tangram_server_clock_core::Clock,
};

#[cfg(test)]
mod tests;

pub type Expire = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

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

	pub overflow_notify: Arc<tokio::sync::Notify>,

	pub overflowed: Arc<AtomicBool>,

	#[cfg(target_os = "macos")]
	pub paths: HashSet<PathBuf, fnv::FnvBuildHasher>,

	pub pending_lock_write_version: Option<u64>,

	pub revalidation_version: u64,

	pub sender: tokio::sync::mpsc::Sender<Message>,

	pub solutions: Solutions,

	pub timeout_task: Option<Task<()>>,

	pub version: u64,

	pub watcher: Option<notify::RecommendedWatcher>,
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
		let overflow_notify = Arc::new(tokio::sync::Notify::new());

		// Create the state.
		let index_task = spawn_index_task();
		let state = State {
			affected_paths: HashSet::default(),

			graph,

			index_task,

			invalidated_paths: HashSet::default(),

			lock,

			overflow_notify: overflow_notify.clone(),

			overflowed: Arc::new(AtomicBool::new(false)),

			#[cfg(target_os = "macos")]
			paths: HashSet::default(),

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

		// Spawn the task.
		let lockfile_path = key.path.join(tg::module::LOCKFILE_FILE_NAME);
		let uses_lock = options.lock.is_some();
		let task = Task::spawn({
			let lockfile_path = lockfile_path.clone();
			let state = state.clone();
			let root = key.path.clone();
			move |_| async move {
				while let Some(message) = Self::recv(&state, &mut receiver, &overflow_notify).await
				{
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
					let lock = read_lock.then(|| try_read_lock(&root));

					// Lock the state.
					let mut state = state.lock().unwrap();

					// Discard the graph if the channel or the watcher dropped events, so that the next checkin reads every path.
					let overflowed = state.overflowed.swap(false, Ordering::SeqCst);
					if overflowed || message.event.need_rescan() {
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
					for path in paths {
						// Ignore delayed root notifications that match the metadata after the lock write.
						if message.sender.is_none()
							&& path == root.as_path()
							&& State::graph_matches_path(&state.graph, path)
						{
							continue;
						}
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
							if let Some(id) = &node.id {
								state.graph.remove_id(index, id, node.permissions);
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

	async fn recv(
		state: &Mutex<State>,
		receiver: &mut tokio::sync::mpsc::Receiver<Message>,
		overflow_notify: &tokio::sync::Notify,
	) -> Option<Message> {
		loop {
			tokio::select! {
				biased;
				() = overflow_notify.notified() => {
					// Discard the graph even if the event channel drained before the overflow was recorded.
					let mut state = state.lock().unwrap();
					if state.overflowed.swap(false, Ordering::SeqCst) {
						state.reset();
					}
				},
				message = receiver.recv() => return message,
			}
		}
	}

	pub fn invalidate_objects(&self, objects: &[tg::object::Id]) {
		let mut state = self.state.lock().unwrap();
		if !objects.iter().any(|id| state.graph.ids.contains_key(id)) {
			return;
		}

		// Remove the watched paths before discarding the cached graph.
		#[cfg(target_os = "linux")]
		{
			let paths = state
				.graph
				.nodes
				.values()
				.filter_map(|node| node.path.clone())
				.collect();
			state.remove_paths_linux(&paths);
		}

		// Invalidate the graph and any checkins that are still using its previous version.
		state.affected_paths.clear();
		state.graph = Graph::default();
		state.invalidated_paths.clear();
		state.solutions = Solutions::default();
		state.version += 1;
		state.revalidation_version = state.version;
		#[cfg(target_os = "macos")]
		state.update_paths_darwin().ok();
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
		#[cfg_attr(not(target_os = "linux"), expect(unused_variables))]
		let removed_paths = graph.clean(&key.path, next);

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
		state.timeout_task.replace(Self::spawn_timeout_task(
			&self.clock,
			self.time_to_live,
			&self.expire,
		));

		// Update the watched paths.
		#[cfg(target_os = "linux")]
		state.remove_paths_linux(&removed_paths);
		state.watch_paths(next);
		if let Some(lock_write_guard) = lock_write_guard {
			lock_write_guard.commit();
		}

		true
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

	fn watch_paths(&mut self, next: usize) {
		// Discard the graph if any of its paths is not watched, so that the next checkin reads every path.
		if let Err(error) = self.try_watch_paths(next) {
			tracing::warn!(%error, "failed to watch the paths");
			self.reset();
		}
	}

	fn try_watch_paths(&mut self, next: usize) -> tg::Result<()> {
		// Create the watcher if there is none, and watch every path with a new watcher.
		#[cfg_attr(
			not(target_os = "linux"),
			expect(
				unused_variables,
				reason = "only linux watches the new nodes individually"
			)
		)]
		let next = if self.watcher.is_some() {
			next
		} else {
			self.watcher.replace(self.create_watcher()?);
			0
		};

		// Watch the paths.
		#[cfg(target_os = "linux")]
		self.add_paths_linux(next)?;
		#[cfg(target_os = "macos")]
		self.update_paths_darwin()?;

		Ok(())
	}

	fn create_watcher(&self) -> tg::Result<notify::RecommendedWatcher> {
		let handler = {
			let overflow_notify = self.overflow_notify.clone();
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
						overflow_notify.notify_one();
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
		#[cfg(target_os = "macos")]
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
	fn add_paths_linux(&mut self, next: usize) -> tg::Result<()> {
		// Add the new paths, stopping at the first path that cannot be watched.
		let watcher = self.watcher.as_mut().unwrap();
		let mut watcher_paths = watcher.paths_mut();
		for (_, node) in self.graph.nodes.range(next..) {
			let Some(path) = &node.path else {
				continue;
			};
			tracing::trace!(path = %path.display(), "watched");
			let Err(error) = watcher_paths.add(path, notify::RecursiveMode::NonRecursive) else {
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
	fn remove_path_linux(&mut self, path: &Path) {
		let Some(watcher) = &mut self.watcher else {
			return;
		};
		tracing::trace!(path = %path.display(), "unwatched");
		watcher.unwatch(path).ok();
	}

	#[cfg(target_os = "linux")]
	fn remove_paths_linux(&mut self, paths: &HashSet<PathBuf, fnv::FnvBuildHasher>) {
		let Some(watcher) = &mut self.watcher else {
			return;
		};
		let mut watcher_paths = watcher.paths_mut();
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
