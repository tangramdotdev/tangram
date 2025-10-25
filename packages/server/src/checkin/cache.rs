use {
	super::state::State,
	crate::{Server, temp::Temp},
	std::{os::unix::fs::PermissionsExt as _, sync::Arc},
	tangram_client as tg,
};

impl Server {
	pub(super) async fn checkin_cache(&self, state: Arc<State>) -> tg::Result<()> {
		if state.arg.options.destructive {
			self.checkin_cache_task_destructive(state).await?;
		} else {
			tokio::task::spawn_blocking({
				let server = self.clone();
				let state = state.clone();
				move || server.checkin_cache_task_inner(&state)
			})
			.await
			.unwrap()
			.map_err(|source| tg::error!(!source, "the checkin cache task failed"))?;
		}
		Ok(())
	}

	pub(super) async fn checkin_cache_task_destructive(&self, state: Arc<State>) -> tg::Result<()> {
		let node = state.graph.nodes.get(&0).unwrap();
		let id = node.object_id.as_ref().unwrap();
		let src = node.path.as_ref().unwrap();
		let dst = &self.cache_path().join(id.to_string());
		if id.is_directory() {
			let permissions = std::fs::Permissions::from_mode(0o755);
			std::fs::set_permissions(src, permissions).map_err(
				|source| tg::error!(!source, %path = src.display(), "failed to set permissions"),
			)?;
		}
		let done = match tokio::fs::rename(src, dst).await {
			Ok(()) => false,
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::AlreadyExists
						| std::io::ErrorKind::DirectoryNotEmpty
						| std::io::ErrorKind::PermissionDenied
				) =>
			{
				true
			},
			Err(source) => {
				return Err(tg::error!(!source, "failed to rename the root"));
			},
		};
		if !done && id.is_directory() {
			let permissions = std::fs::Permissions::from_mode(0o555);
			tokio::fs::set_permissions(dst, permissions).await.map_err(
				|source| tg::error!(!source, %path = dst.display(), "failed to set permissions"),
			)?;
		}
		if !done {
			let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
			filetime::set_symlink_file_times(dst, epoch, epoch).map_err(
				|source| tg::error!(!source, %path = dst.display(), "failed to set the modified time"),
			)?;
		}
		Ok(())
	}

	fn checkin_cache_task_inner(&self, state: &State) -> tg::Result<()> {
		for (_, node) in &state.graph.nodes {
			let Some(path) = node.path.as_ref() else {
				continue;
			};
			let metadata = node.path_metadata.as_ref().unwrap();
			if !metadata.is_file() {
				continue;
			}
			let id = node.object_id.as_ref().unwrap();

			// Copy the file to a temp.
			let src = path;
			let temp = Temp::new(self);
			let dst = temp.path();
			std::fs::copy(src, dst)
				.map_err(|source| tg::error!(!source, "failed to copy the file"))?;

			// Set its permissions.
			if !metadata.is_symlink() {
				let executable = metadata.permissions().mode() & 0o111 != 0;
				let mode = if executable { 0o555 } else { 0o444 };
				let permissions = std::fs::Permissions::from_mode(mode);
				std::fs::set_permissions(dst, permissions).map_err(
					|source| tg::error!(!source, %path = dst.display(), "failed to set permissions"),
				)?;
			}

			// Rename the temp to the cache directory.
			let src = temp.path();
			let dst = &self.cache_path().join(id.to_string());
			let done = match std::fs::rename(src, dst) {
				Ok(()) => false,
				Err(source) => {
					return Err(tg::error!(!source, "failed to rename the file"));
				},
			};

			// Set the file times.
			if !done {
				let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
				filetime::set_symlink_file_times(dst, epoch, epoch).map_err(
					|source| tg::error!(!source, %path = dst.display(), "failed to set the modified time"),
				)?;
			}
		}

		Ok(())
	}
}
