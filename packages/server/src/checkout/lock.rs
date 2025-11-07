use {crate::Server, tangram_client::prelude::*};

impl Server {
	pub(super) fn checkout_write_lock(state: &mut super::State) -> tg::Result<()> {
		// Convert the lock nodes from Option to required.
		let nodes: Vec<_> = state
			.lock_nodes
			.iter()
			.enumerate()
			.map(|(index, node)| {
				node.clone().ok_or_else(
					|| tg::error!(%node = index, "invalid graph, failed to create lock node"),
				)
			})
			.collect::<tg::Result<_>>()?;

		// Do not write the lock if it is empty.
		if nodes.is_empty() {
			return Ok(());
		}

		// Create the lock.
		let lock = tg::graph::Data { nodes };

		// Strip the lock.
		let lock = Self::strip_lock(lock, &state.lock_ids);

		// Do not write the lock if it is empty after stripping.
		if lock.nodes.is_empty() {
			return Ok(());
		}

		// Write the lock.
		if state.artifact.is_directory() {
			let contents = serde_json::to_vec_pretty(&lock)
				.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
			let lockfile_path = state.path.join(tg::package::LOCKFILE_FILE_NAME);
			std::fs::write(&lockfile_path, &contents).map_err(
				|source| tg::error!(!source, %path = lockfile_path.display(), "failed to write the lockfile"),
			)?;
		} else if state.artifact.is_file() {
			let contents = serde_json::to_vec(&lock)
				.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
			xattr::set(&state.path, tg::file::LOCKATTR_XATTR_NAME, &contents)
				.map_err(|source| tg::error!(!source, "failed to write the lockattr"))?;
		}

		Ok(())
	}
}
