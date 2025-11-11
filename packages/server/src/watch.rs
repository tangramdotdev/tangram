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

pub struct Watch {
	pub state: Arc<Mutex<State>>,
	#[expect(dead_code)]
	task: Task<()>,
}

pub struct State {
	pub graph: crate::checkin::Graph,
	pub lock: Option<Arc<tg::graph::Data>>,
	pub options: tg::checkin::Options,
	#[cfg(target_os = "macos")]
	pub paths: HashSet<PathBuf, fnv::FnvBuildHasher>,
	pub solutions: crate::checkin::Solutions,
	pub version: u64,
	pub watcher: notify::RecommendedWatcher,
}

impl Watch {
	pub fn new(
		root: &Path,
		graph: crate::checkin::Graph,
		lock: Option<Arc<tg::graph::Data>>,
		options: tg::checkin::Options,
		solutions: crate::checkin::Solutions,
	) -> tg::Result<Self> {
		// Create the watcher.
		let config = notify::Config::default();
		let (sender, mut receiver) = tokio::sync::mpsc::channel::<notify::Event>(1024);
		let handler = {
			move |result| match result {
				Ok(event) => {
					sender.blocking_send(event).ok();
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
			options,
			#[cfg(target_os = "macos")]
			paths: HashSet::default(),
			solutions,
			version: 0,
			watcher,
		};
		let state = Arc::new(Mutex::new(state));

		// Spawn the task.
		let task = Task::spawn({
			let state = state.clone();
			let root = root.to_owned();
			move |_| async move {
				while let Some(event) = receiver.recv().await {
					// Get the paths.
					let paths = Self::changes(&event);

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
							{
								tracing::trace!(path = %path.display(), "unwatched");
								state.watcher.unwatch(path).ok();
							}

							// Remove the node.
							let node = *state.graph.nodes.remove(&index).unwrap();
							tracing::trace!(path = ?node.path, id = ?node.id, "removed");
							if let Some(artifact) = &node.artifact {
								state.graph.artifacts.remove(artifact).unwrap();
							}
							if let Some(id) = &node.id
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
				}
			}
		});

		let watch = Self { state, task };

		Ok(watch)
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
	pub fn update_paths(&mut self) {
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
}
