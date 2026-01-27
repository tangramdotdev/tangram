use {
	notify::Watcher as _,
	std::{
		collections::HashSet,
		path::{Path, PathBuf},
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

pub mod delete;
pub mod list;
pub mod touch;

pub struct Watch {
	options: tg::checkin::Options,
	state: Arc<Mutex<State>>,
	#[expect(dead_code)]
	task: Task<()>,
}

struct State {
	graph: crate::checkin::Graph,
	lock: Option<Arc<tg::graph::Data>>,
	#[cfg(target_os = "macos")]
	paths: HashSet<PathBuf, fnv::FnvBuildHasher>,
	sender: tokio::sync::mpsc::Sender<Message>,
	solutions: crate::checkin::Solutions,
	version: u64,
	watcher: notify::RecommendedWatcher,
}

pub struct Snapshot {
	pub graph: crate::checkin::Graph,
	pub lock: Option<Arc<tg::graph::Data>>,
	pub solutions: crate::checkin::Solutions,
	pub version: u64,
}

struct Message {
	event: notify::Event,
	sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Watch {
	pub fn new(
		root: &Path,
		graph: crate::checkin::Graph,
		lock: Option<Arc<tg::graph::Data>>,
		options: tg::checkin::Options,
		solutions: crate::checkin::Solutions,
		#[cfg_attr(not(target_os = "linux"), expect(unused_variables))] next: usize,
	) -> tg::Result<Self> {
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
			.map_err(|source| tg::error!(!source, "failed to create the watcher"))?;

		// Create the state.
		let state = State {
			graph,
			lock,
			#[cfg(target_os = "macos")]
			paths: HashSet::default(),
			sender,
			solutions,
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
			let root = root.to_owned();
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

		let watch = Self {
			options,
			state,
			task,
		};

		Ok(watch)
	}

	pub fn options(&self) -> &tg::checkin::Options {
		&self.options
	}

	pub fn get(&self) -> Snapshot {
		let state = self.state.lock().unwrap();
		Snapshot {
			graph: state.graph.clone(),
			lock: state.lock.clone(),
			solutions: state.solutions.clone(),
			version: state.version,
		}
	}

	pub fn update(
		&self,
		graph: crate::checkin::Graph,
		lock: Option<Arc<tg::graph::Data>>,
		solutions: crate::checkin::Solutions,
		version: Option<u64>,
		#[cfg_attr(not(target_os = "linux"), expect(unused_variables))] next: usize,
	) -> bool {
		let mut state = self.state.lock().unwrap();

		if let Some(version) = version
			&& state.version != version
		{
			return false;
		}

		// Update the state.
		state.graph = graph;
		state.lock = lock;
		state.solutions = solutions;

		// On Linux, add the new paths.
		#[cfg(target_os = "linux")]
		state.add_paths_linux(next);

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
			notify::EventKind::Create(_) => {
				for path in &event.paths {
					changes.insert(path.as_path());
					if let Some(parent) = path.parent() {
						changes.insert(parent);
					}
				}
			},
			notify::EventKind::Modify(notify::event::ModifyKind::Name(_))
			| notify::EventKind::Remove(_) => {
				for path in &event.paths {
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
					tracing::error!(?error, ?path, "failed to watch the path");
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
					tracing::error!(?error, ?path, "failed to unwatch the path");
				}
				self.paths.remove(&path);
			}
		}

		// Commit.
		let result = watcher_paths.commit();
		if let Err(error) = result {
			tracing::error!(?error, "failed to watch the paths");
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
