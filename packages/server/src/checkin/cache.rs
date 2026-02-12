use {
	crate::{
		Server,
		checkin::{Graph, IndexCacheEntryArgs, graph::Variant},
		temp::Temp,
	},
	futures::stream::{self, StreamExt as _, TryStreamExt as _},
	std::{
		collections::BTreeSet,
		os::unix::fs::PermissionsExt as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
	tangram_index::Index,
	tangram_util::{iter::Ext as _, path},
};

impl Server {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn checkin_cache(
		&self,
		arg: &tg::checkin::Arg,
		graph: &Graph,
		next: usize,
		root: &Path,
		index_cache_entry_args: &IndexCacheEntryArgs,
		progress: &crate::progress::Handle<super::TaskOutput>,
	) -> tg::Result<()> {
		if arg.options.destructive {
			progress.spinner("checking", "checking");
			self.checkin_ensure_dependencies_are_cached(graph, root, index_cache_entry_args)
				.await?;
			progress.finish("checking");
			progress.spinner("copying", "copying");
			self.checkin_cache_destructive(graph, root).await?;
			progress.finish("copying");
		} else {
			let files = graph
				.nodes
				.range(next..)
				.filter_map(|(_, node)| {
					let path = node.path.as_ref()?.clone();
					let metadata = node.path_metadata.as_ref()?.clone();
					if !metadata.is_file() {
						return None;
					}
					let id = node.id.as_ref()?.clone();
					let size = metadata.len();
					Some((path, metadata, id, size))
				})
				.collect::<Vec<_>>();

			// Start the progress indicator.
			progress.spinner("copying", "copying");
			let total = files.iter().map(|(_, _, _, size)| size).sum::<u64>();
			progress.start(
				"bytes".to_owned(),
				"bytes".to_owned(),
				tg::progress::IndicatorFormat::Bytes,
				Some(0),
				Some(total),
			);

			let batches = files
				.into_iter()
				.batches(self.config.checkin.cache.batch_size)
				.map(|batch| {
					let server = self.clone();
					let batch_bytes: u64 = batch.iter().map(|(_, _, _, size)| size).sum();
					let batch: Vec<_> = batch
						.into_iter()
						.map(|(path, metadata, id, _)| (path, metadata, id))
						.collect();
					async move {
						tokio::task::spawn_blocking(move || server.checkin_cache_inner(batch))
							.await
							.map_err(|source| {
								tg::error!(!source, "the checkin cache task panicked")
							})??;
						progress.increment("bytes", batch_bytes);
						Ok::<_, tg::Error>(())
					}
				})
				.collect::<Vec<_>>();

			stream::iter(batches)
				.buffer_unordered(self.config.checkin.cache.concurrency)
				.try_collect::<()>()
				.await
				.map_err(|source| tg::error!(!source, "the checkin cache task failed"))?;

			progress.finish("copying");
			progress.finish("bytes");
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
				|source| tg::error!(!source, path = %src.display(), "failed to set permissions"),
			)?;
		}
		let done = match tangram_util::fs::rename_noreplace(src, dst).await {
			Ok(()) => false,
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::AlreadyExists
						| std::io::ErrorKind::IsADirectory
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
				|source| tg::error!(!source, path = %dst.display(), "failed to set permissions"),
			)?;
		}
		if !done {
			let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
			filetime::set_symlink_file_times(dst, epoch, epoch).map_err(
				|source| tg::error!(!source, path = %dst.display(), "failed to set the modified time"),
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
					|source| tg::error!(!source, path = %dst.display(), "failed to set permissions"),
				)?;
			}

			// Rename the temp to the cache directory.
			let src = temp.path();
			let dst = &cache_path;
			let done = match tangram_util::fs::rename_noreplace_sync(src, dst) {
				Ok(()) => false,
				Err(error)
					if matches!(
						error.kind(),
						std::io::ErrorKind::AlreadyExists
							| std::io::ErrorKind::IsADirectory
							| std::io::ErrorKind::PermissionDenied
					) =>
				{
					true
				},
				Err(source) => {
					return Err(tg::error!(!source, "failed to rename the file"));
				},
			};

			// Set the file times.
			if !done {
				let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
				filetime::set_symlink_file_times(dst, epoch, epoch).map_err(
					|source| tg::error!(!source, path = %dst.display(), "failed to set the modified time"),
				)?;
			}
		}

		Ok(())
	}

	async fn checkin_ensure_dependencies_are_cached(
		&self,
		graph: &Graph,
		path: &Path,
		index_cache_entry_args: &IndexCacheEntryArgs,
	) -> tg::Result<()> {
		let root = graph
			.paths
			.get(path)
			.copied()
			.ok_or_else(|| tg::error!("expected a node"))?;
		let will_cache = index_cache_entry_args
			.iter()
			.map(|arg| arg.id.clone())
			.collect::<BTreeSet<_>>();
		let mut visited = BTreeSet::new();
		let mut stack = vec![root];
		let root_is_dir = graph
			.nodes
			.get(&root)
			.is_some_and(|node| node.variant.is_directory());

		let mut artifacts = Vec::new();
		while let Some(index) = stack.pop() {
			if !visited.insert(index) {
				continue;
			}

			let Some(node) = graph.nodes.get(&index) else {
				continue;
			};

			// Make sure we're not missing local path dependencies.
			if let Some(node_path) = &node.path
				&& index != root
			{
				if !root_is_dir {
					return Err(tg::error!(
						"cannot destructively checkin files or symlinks with path dependencies"
					));
				}
				if path::diff(path, node_path).is_ok_and(|path| path.starts_with("..")) {
					return Err(tg::error!(
						"cannot destructively check in an artifact with external path dependencies"
					));
				}
			}

			match &node.variant {
				Variant::Directory(directory) => {
					for edge in directory.entries.values() {
						match edge {
							tg::graph::data::Edge::Object(id) => {
								if let Some(index) = graph.artifacts.get(id) {
									if !will_cache.contains(id) {
										return Err(tg::error!("missing cache dependency"));
									}
									stack.push(*index);
								}
								artifacts.push(id.clone());
							},
							tg::graph::data::Edge::Pointer(pointer) => {
								if let Some(id) = &pointer.graph {
									let artifact = tg::Artifact::with_pointer(tg::graph::Pointer {
										graph: Some(tg::Graph::with_id(id.clone())),
										index: pointer.index,
										kind: pointer.kind,
									})
									.id();
									if will_cache.contains(&artifact) {
										continue;
									}
									let data = graph.graphs.get(id);
									let ids = self.graph_ids(id, data).await.map_err(|source| {
										tg::error!(!source, "failed to get the graph ids")
									})?;
									artifacts.extend(ids);
								} else {
									stack.push(pointer.index);
								}
							},
						}
					}
				},
				Variant::File(file) => {
					for dependency in file.dependencies.values().flatten() {
						if let Some(edge) = &dependency.item {
							match edge {
								tg::graph::data::Edge::Object(id) => {
									let Ok(id) = tg::artifact::Id::try_from(id.clone()) else {
										continue;
									};
									if let Some(index) = graph.artifacts.get(&id) {
										if !will_cache.contains(&id) {
											return Err(tg::error!("missing cache dependency"));
										}
										stack.push(*index);
									}
									artifacts.push(id.clone());
								},
								tg::graph::data::Edge::Pointer(pointer) => {
									if let Some(id) = &pointer.graph {
										let artifact =
											tg::Artifact::with_pointer(tg::graph::Pointer {
												graph: Some(tg::Graph::with_id(id.clone())),
												index: pointer.index,
												kind: pointer.kind,
											})
											.id();
										if will_cache.contains(&artifact) {
											continue;
										}
										let data = graph.graphs.get(id);
										let ids =
											self.graph_ids(id, data).await.map_err(|source| {
												tg::error!(!source, "failed to get the graph ids")
											})?;
										artifacts.extend(ids);
									} else {
										stack.push(pointer.index);
									}
								},
							}
						}
					}
				},
				Variant::Symlink(symlink) => {
					if let Some(edge) = &symlink.artifact {
						match edge {
							tg::graph::data::Edge::Object(id) => {
								if let Some(index) = graph.artifacts.get(id) {
									if !will_cache.contains(id) {
										return Err(tg::error!("missing cache dependency"));
									}
									stack.push(*index);
								}
								artifacts.push(id.clone());
							},
							tg::graph::data::Edge::Pointer(pointer) => {
								if let Some(id) = &pointer.graph {
									let artifact = tg::Artifact::with_pointer(tg::graph::Pointer {
										graph: Some(tg::Graph::with_id(id.clone())),
										index: pointer.index,
										kind: pointer.kind,
									})
									.id();
									if will_cache.contains(&artifact) {
										continue;
									}
									let data = graph.graphs.get(id);
									let ids = self.graph_ids(id, data).await.map_err(|source| {
										tg::error!(!source, "failed to get the graph ids")
									})?;
									artifacts.extend(ids);
								} else {
									stack.push(pointer.index);
								}
							},
						}
					}
				},
			}
		}
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let all_cached = self
			.index
			.touch_cache_entries(&artifacts, touched_at)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to get the cache entries from the index")
			})?
			.into_iter()
			.all(|entry| entry.is_some());
		if !all_cached {
			return Err(tg::error!("missing cache dependency"));
		}
		Ok(())
	}

	pub(crate) async fn graph_ids(
		&self,
		graph: &tg::graph::Id,
		data: Option<&tg::graph::Data>,
	) -> tg::Result<Vec<tg::artifact::Id>> {
		let data = if let Some(data) = data {
			data.clone()
		} else {
			tg::Graph::with_id(graph.clone())
				.data(self)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the graph data"))?
		};
		let graph = tg::Graph::with_id(graph.clone());
		let mut nodes = Vec::with_capacity(data.nodes.len());
		for (index, node) in data.nodes.into_iter().enumerate() {
			let artifact = tg::Artifact::with_pointer(tg::graph::Pointer {
				graph: Some(graph.clone()),
				index,
				kind: node.kind(),
			});
			nodes.push(artifact.id());
		}
		Ok(nodes)
	}
}
