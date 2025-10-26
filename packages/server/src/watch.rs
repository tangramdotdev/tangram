use {
	notify::Watcher as _,
	std::{
		collections::HashSet,
		path::Path,
		sync::{
			Arc, Mutex,
			atomic::{AtomicU64, Ordering},
		},
	},
	tangram_client as tg,
};

pub struct Watch {
	pub state: Arc<Mutex<crate::checkin::State>>,
	pub version: Arc<AtomicU64>,
	#[allow(dead_code)]
	pub watcher: notify::RecommendedWatcher,
}

impl Watch {
	pub fn new(path: &Path, state: crate::checkin::State) -> tg::Result<Self> {
		let version = Arc::new(AtomicU64::new(0));
		let state = Arc::new(Mutex::new(state));
		let notify_config = notify::Config::default();
		let handler = {
			let state = state.clone();
			let version = version.clone();
			move |result: notify::Result<notify::Event>| {
				// Handle an error.
				let event = match result {
					Ok(event) => event,
					Err(error) => {
						tracing::error!(?error);
						return;
					},
				};

				// Lock the state.
				let mut state = state.lock().unwrap();

				// Update the nodes for the affected paths along with their ancestors.
				let mut update = false;
				for path in event.paths {
					let Some(index) = state.graph.paths.get(&path).copied() else {
						continue;
					};
					update = true;
					let mut queue = vec![index];
					let mut visited = HashSet::<usize, fnv::FnvBuildHasher>::default();
					while let Some(index) = queue.pop() {
						if !visited.insert(index) {
							continue;
						}
						let node = state.graph.nodes.remove(&index).unwrap();
						if let Some(path) = &node.path {
							state.graph.paths.remove(path).unwrap();
						}
						if let Some(id) = &node.id {
							state.objects.remove(id);
						}
						for referrer in node.referrers {
							queue.push(referrer);
						}
					}
				}

				// Increment the version if any nodes were removed.
				if update {
					version.fetch_add(1, Ordering::SeqCst);
				}
			}
		};
		let mut watcher = notify::RecommendedWatcher::new(handler, notify_config)
			.map_err(|source| tg::error!(!source, "failed to create the watcher"))?;
		watcher
			.watch(path.as_ref(), notify::RecursiveMode::Recursive)
			.map_err(|source| tg::error!(!source, "failed to add the watch path"))?;
		let watch = Self {
			state,
			version,
			watcher,
		};
		Ok(watch)
	}
}
