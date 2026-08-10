use {
	crate::Server,
	notify::Watcher as _,
	std::{
		collections::HashSet,
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

struct State {
	graph: crate::checkin::Graph,
	index_task: Shared<tg::Result<()>>,
	lock: Option<Arc<tg::graph::Data>>,
	#[cfg(target_os = "macos")]
	paths: HashSet<PathBuf, fnv::FnvBuildHasher>,
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
			graph,
			index_task,
			lock,
			#[cfg(target_os = "macos")]
			paths: HashSet::default(),
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
		let task = Task::spawn({
			let state = state.clone();
			let root = key.path.clone();
			move |_| async move {
				while let Some(message) = receiver.recv().await {
					// Get the paths.
					let paths = Self::changes(&message.event);

					// Lock the state.
					let mut state = state.lock().unwrap();

					// Update the nodes for the affected paths along with their ancestors.
					let mut removed = false;
					for path in paths {
						// If the affected file is the lockfile, then clear it.
						if path == root.join("tangram.lock") {
							state.lock.take();
						}

						let Some(index) = state.graph.paths.get(path).copied() else {
							continue;
						};
						removed = true;
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
								}
							}
							if let Some(path) = &node.path {
								state.graph.paths.remove(path).unwrap();
							}

							// Remove solutions that reference this node.
							state.solutions.remove_by_node(index);

							// Remove the node from its children's referrers and enqueue its children with no more referrers and no path.
							for child_index in node.children() {
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

					// Increment the version if any nodes were removed.
					if removed {
						state.version += 1;
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
			next,
			server,
			solutions,
			version,
		} = arg;
		let mut state = self.state.lock().unwrap();

		if state.version != version {
			return false;
		}

		// Update the state.
		let index_task = spawn_index_task();
		state.graph = graph;
		state.index_task = index_task;
		state.lock = lock;
		state.solutions = solutions;
		state.version += 1;

		// Reset the timeout task.
		state
			.timeout_task
			.replace(Self::spawn_timeout_task(server, key, self.id));

		// On Linux, add the new paths.
		#[cfg(target_os = "linux")]
		state.add_paths_linux(next);
		#[cfg(not(target_os = "linux"))]
		let _ = next;

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
