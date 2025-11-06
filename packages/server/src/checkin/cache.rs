use {
	crate::{Server, checkin::Graph, temp::Temp},
	futures::stream::{self, StreamExt as _, TryStreamExt as _},
	std::{
		os::unix::fs::PermissionsExt as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
	tangram_util::iter::Ext as _,
};

const BATCH_SIZE: usize = 128;
const CONCURRENCY: usize = 8;

impl Server {
	pub(super) async fn checkin_cache(
		&self,
		arg: &tg::checkin::Arg,
		graph: &Graph,
		next: usize,
		root: &Path,
	) -> tg::Result<()> {
		if arg.options.destructive {
			self.checkin_cache_destructive(graph, root).await?;
		} else {
			let batches = graph
				.nodes
				.range(next..)
				.filter_map(|(_, node)| {
					let path = node.path.as_ref()?.clone();
					let metadata = node.path_metadata.as_ref()?.clone();
					if !metadata.is_file() {
						return None;
					}
					let id = node.id.as_ref()?.clone();
					Some((path, metadata, id))
				})
				.batches(BATCH_SIZE)
				.map(|batch| {
					let server = self.clone();
					async move {
						tokio::task::spawn_blocking(move || server.checkin_cache_inner(batch))
							.await
							.unwrap()
					}
				})
				.collect::<Vec<_>>();
			stream::iter(batches)
				.buffer_unordered(CONCURRENCY)
				.try_collect::<()>()
				.await
				.map_err(|source| tg::error!(!source, "the checkin cache task failed"))?;
		}
		Ok(())
	}

	async fn checkin_cache_destructive(&self, graph: &Graph, root: &Path) -> tg::Result<()> {
		let index = graph.paths.get(root).unwrap();
		let node = graph.nodes.get(index).unwrap();
		let id = node.id.as_ref().unwrap();
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

	fn checkin_cache_inner(
		&self,
		batch: Vec<(PathBuf, std::fs::Metadata, tg::object::Id)>,
	) -> tg::Result<()> {
		for (path, metadata, id) in batch {
			// If the file is already cached, then continue.
			let cache_path = self.cache_path().join(id.to_string());
			if cache_path.exists() {
				continue;
			}

			// Copy the file to a temp.
			let src = &path;
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
			let dst = &cache_path;
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
