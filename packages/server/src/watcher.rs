use {
	crate::Server,
	notify::Watcher as _,
	std::{collections::HashSet, path::Path},
	tangram_client as tg,
};

impl Server {
	pub async fn watcher_task(&self, _config: &crate::config::Watcher) -> tg::Result<()> {
		// Create a channel for receiving file system events.
		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

		// Create the watcher.
		let notify_config = notify::Config::default();
		let handler = move |result| {
			if let Ok(event) = result {
				sender.send(event).ok();
			}
		};
		let watcher = notify::RecommendedWatcher::new(handler, notify_config)
			.map_err(|source| tg::error!(!source, "failed to create the watcher"))?;

		// Store the watcher in the server.
		self.watcher.lock().unwrap().replace(watcher);

		// Handle events.
		while let Some(event) = receiver.recv().await {
			for path in event.paths {
				for mut entry in self.checkins.iter_mut() {
					let Some(state) = entry.value_mut().as_mut() else {
						continue;
					};
					if let Some(index) = state.graph.paths.get(&path).copied() {
						let mut queue = vec![index];
						let mut visited = HashSet::<usize, fnv::FnvBuildHasher>::default();
						while let Some(index) = queue.pop() {
							if !visited.insert(index) {
								continue;
							}
							state.graph.nodes[index].dirty = true;
							for &index in &state.graph.nodes[index].referrers {
								queue.push(index);
							}
						}
					}
				}
			}
		}

		Ok(())
	}

	pub fn watcher_add_path(&self, path: impl AsRef<Path>) -> tg::Result<()> {
		let mut watcher = self.watcher.lock().unwrap();
		let watcher = watcher
			.as_mut()
			.ok_or_else(|| tg::error!("the watcher is not running"))?;
		watcher
			.watch(path.as_ref(), notify::RecursiveMode::Recursive)
			.map_err(|source| tg::error!(!source, "failed to add the watch path"))?;
		Ok(())
	}

	pub fn watcher_remove_path(&self, path: impl AsRef<Path>) -> tg::Result<()> {
		let mut watcher = self.watcher.lock().unwrap();
		let watcher = watcher
			.as_mut()
			.ok_or_else(|| tg::error!("the watcher is not running"))?;
		watcher
			.unwatch(path.as_ref())
			.map_err(|source| tg::error!(!source, "failed to remove the watch path"))?;
		Ok(())
	}
}
