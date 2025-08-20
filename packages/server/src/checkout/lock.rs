use super::State;
use crate::Server;
use tangram_client as tg;

impl Server {
	pub(super) fn checkout_write_lock(
		&self,
		id: tg::artifact::Id,
		state: &mut State,
	) -> tg::Result<()> {
		// Create the lock.
		let lock = self
			.checkout_create_lock(&id, state.arg.dependencies)
			.map_err(|source| tg::error!(!source, "failed to create the lock"))?;

		// Do not write the lock if it is empty.
		if lock.nodes.is_empty() {
			return Ok(());
		}

		// Write the lock.
		let artifact = tg::Artifact::with_id(id);
		if artifact.is_directory() {
			let contents = serde_json::to_vec_pretty(&lock)
				.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
			let lockfile_path = state.path.join(tg::package::LOCKFILE_FILE_NAME);
			std::fs::write(&lockfile_path, &contents).map_err(
				|source| tg::error!(!source, %path = lockfile_path.display(), "failed to write the lockfile"),
			)?;
		} else if artifact.is_file() {
			let contents = serde_json::to_vec(&lock)
				.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
			xattr::set(&state.path, tg::file::LOCKATTR_XATTR_NAME, &contents)
				.map_err(|source| tg::error!(!source, "failed to write the lockattr"))?;
		}

		Ok(())
	}

	fn checkout_create_lock(
		&self,
		_artifact: &tg::artifact::Id,
		_dependencies: bool,
	) -> tg::Result<tg::graph::Data> {
		todo!()
	}
}
