use {
	notify::Watcher as _,
	std::{
		collections::HashSet,
		path::{Path, PathBuf},
		sync::{Arc, Mutex},
	},
	tangram_client as tg,
};

pub struct Watch {
	pub state: Arc<Mutex<State>>,
	#[allow(dead_code)]
	watcher: notify::RecommendedWatcher,
}

pub struct State {
	pub graph: crate::checkin::Graph,
	pub lock: Option<Arc<tg::graph::Data>>,
	pub options: tg::checkin::Options,
	pub solutions: crate::checkin::Solutions,
	pub version: u64,
}

impl Watch {
	pub fn new(
		root: &Path,
		graph: crate::checkin::Graph,
		lock: Option<Arc<tg::graph::Data>>,
		options: tg::checkin::Options,
		solutions: crate::checkin::Solutions,
	) -> tg::Result<Self> {
		let state = State {
			graph,
			lock,
			options,
			solutions,
			version: 0,
		};
		let state = Arc::new(Mutex::new(state));
		let config = notify::Config::default();
		let handler = {
			let state = state.clone();
			let root = root.to_owned();
			move |result: notify::Result<notify::Event>| {
				// Handle an error.
				let event = match result {
					Ok(event) => event,
					Err(error) => {
						tracing::error!(?error);
						return;
					},
				};

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

						// Remove the node.
						let node = *state.graph.nodes.remove(&index).unwrap();
						tracing::trace!(path = ?node.path, id = ?node.id, "deleting");
						if let Some(id) = &node.id {
							state.graph.ids.remove(id);
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
		};
		let mut watcher = notify::RecommendedWatcher::new(handler, config)
			.map_err(|source| tg::error!(!source, "failed to create the watcher"))?;
		watcher
			.watch(root.as_ref(), notify::RecursiveMode::Recursive)
			.map_err(|source| tg::error!(!source, "failed to add the watch path"))?;
		let watch = Self { state, watcher };
		Ok(watch)
	}

	pub fn changes(event: &notify::Event) -> HashSet<&Path, fnv::FnvBuildHasher> {
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
			notify::EventKind::Access(_) => {},
			notify::EventKind::Modify(
				notify::event::ModifyKind::Data(_) | notify::event::ModifyKind::Metadata(_) | _,
			)
			| notify::EventKind::Any
			| notify::EventKind::Other => {
				changes.extend(event.paths.iter().map(PathBuf::as_path));
			},
		}
		changes
	}
}
