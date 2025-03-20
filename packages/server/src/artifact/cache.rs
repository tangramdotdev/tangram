use crate::{Server, temp::Temp};
use futures::FutureExt as _;
use reflink_copy::reflink;
use std::{collections::HashMap, os::unix::fs::PermissionsExt as _, path::PathBuf};
use tangram_client as tg;
use tangram_either::Either;

#[cfg(test)]
mod tests;

struct State {
	artifact: tg::artifact::Id,
	depth: usize,
	graphs: HashMap<tg::graph::Id, tg::graph::Data, fnv::FnvBuildHasher>,
	path: PathBuf,
	progress: crate::progress::Handle<tg::artifact::checkout::Output>,
	visited: im::HashSet<tg::artifact::Id, fnv::FnvBuildHasher>,
}

impl Server {
	pub(crate) async fn cache_artifact(
		&self,
		artifact: &tg::artifact::Id,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<()> {
		let server = self.clone();
		let id = artifact.clone();
		let artifact = Either::Right(artifact.clone());
		let visited = im::HashSet::default();
		let progress = progress.clone();
		let task = self
			.cache_task_map
			.get_or_spawn_blocking(id.clone(), move |_| {
				server.cache_dependency_inner(&id, artifact, visited, &progress)
			});
		let future = task.wait().map(|result| match result {
			Ok(result) => Ok(result),
			Err(error) if error.is_cancelled() => Ok(Err(tg::error!("the task was canceled"))),
			Err(error) => Err(error),
		});
		future.await.unwrap()?;
		Ok(())
	}

	fn cache_dependency(
		&self,
		id: &tg::artifact::Id,
		artifact: Either<(tg::graph::Id, usize), tg::artifact::Id>,
		visited: &im::HashSet<tg::artifact::Id, fnv::FnvBuildHasher>,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<()> {
		let server = self.clone();
		let id = id.clone();
		let mut visited = visited.clone();
		let progress = progress.clone();
		if visited.insert(id.clone()).is_some() {
			return Ok(());
		}
		let task = self
			.cache_task_map
			.get_or_spawn_blocking(id.clone(), move |_| {
				server.cache_dependency_inner(&id, artifact, visited, &progress)
			});
		let future = task.wait().map(|result| match result {
			Ok(result) => Ok(result),
			Err(error) if error.is_cancelled() => Ok(Err(tg::error!("the task was canceled"))),
			Err(error) => Err(error),
		});
		tokio::runtime::Handle::current()
			.block_on(future)
			.unwrap()?;
		Ok(())
	}

	fn cache_dependency_inner(
		&self,
		id: &tg::artifact::Id,
		artifact: Either<(tg::graph::Id, usize), tg::artifact::Id>,
		visited: im::HashSet<tg::artifact::Id, fnv::FnvBuildHasher>,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<()> {
		// Create the temp.
		let temp = Temp::new(self);

		// Create the state.
		let mut state = State {
			artifact: id.clone(),
			depth: 0,
			graphs: HashMap::default(),
			path: temp.path().to_owned(),
			progress: progress.clone(),
			visited,
		};

		// Cache the artifact.
		self.cache_inner(&mut state, artifact, temp.path().to_owned())?;

		// Create the path.
		let path = self.cache_path().join(id.to_string());

		// Rename the temp to the path.
		let src = temp.path();
		let dst = &path;
		let result = std::fs::rename(src, dst);
		match result {
			Ok(()) => {},
			Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {},
			Err(source) => {
				let src = src.display();
				let dst = dst.display();
				let error = tg::error!(!source, %src, %dst, "failed to rename to the cache path");
				return Err(error);
			},
		}

		// Set the modified time to the epoch.
		let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
		filetime::set_symlink_file_times(&path, epoch, epoch).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
		)?;

		Ok(())
	}

	fn cache_inner(
		&self,
		state: &mut State,
		artifact: Either<(tg::graph::Id, usize), tg::artifact::Id>,
		path: PathBuf,
	) -> tg::Result<()> {
		// Get the graph or artifact's data.
		let data = match artifact {
			// If the artifact refers to a graph, then add it to the state.
			Either::Left((graph, node)) => {
				if !state.graphs.contains_key(&graph) {
					let data = match &self.store {
						crate::Store::Lmdb(store) => {
							store.try_get_object_data(&graph.clone().into())?
						},
						crate::Store::Memory(store) => {
							store.try_get_object_data(&graph.clone().into())?
						},
						_ => return Err(tg::error!("not yet implemented")),
					}
					.ok_or_else(|| tg::error!("failed to get the value"))?
					.try_into()
					.map_err(|_| tg::error!("expected a graph"))?;
					state.graphs.insert(graph.clone(), data);
				};
				Either::Left((graph, node))
			},

			// Otherwise, get the artifact's data.
			Either::Right(id) => {
				let data = match &self.store {
					crate::Store::Lmdb(store) => store.try_get_object_data(&id.into())?,
					crate::Store::Memory(store) => store.try_get_object_data(&id.into())?,
					_ => return Err(tg::error!("not yet implemented")),
				}
				.ok_or_else(|| tg::error!("failed to get the value"))?;
				let data = tg::artifact::Data::try_from(data)?;
				Either::Right(data)
			},
		};

		// Cache the artifact.
		match data {
			Either::Left((graph, node)) => {
				self.cache_inner_graph(state, path, &graph, node)?;
			},
			Either::Right(data) => {
				self.cache_inner_data(state, path, data)?;
			},
		}

		Ok(())
	}

	fn cache_inner_graph(
		&self,
		state: &mut State,
		path: PathBuf,
		graph: &tg::graph::Id,
		node: usize,
	) -> tg::Result<()> {
		let node = state
			.graphs
			.get(graph)
			.ok_or_else(|| tg::error!("expected the graph to exist"))?
			.nodes
			.get(node)
			.ok_or_else(|| tg::error!("expected the node to exist"))?
			.clone();
		match node {
			tg::graph::data::Node::Directory(directory) => {
				self.cache_inner_graph_directory(state, path, graph, &directory)?;
			},
			tg::graph::data::Node::File(file) => {
				self.cache_inner_graph_file(state, path, graph, file)?;
			},
			tg::graph::data::Node::Symlink(symlink) => {
				self.cache_inner_graph_symlink(state, path, graph, symlink)?;
			},
		}
		Ok(())
	}

	#[allow(clippy::needless_pass_by_value)]
	fn cache_inner_graph_directory(
		&self,
		state: &mut State,
		path: PathBuf,
		graph: &tg::graph::Id,
		directory: &tg::graph::data::Directory,
	) -> tg::Result<()> {
		std::fs::create_dir_all(&path).unwrap();
		for (name, artifact) in &directory.entries {
			let artifact = artifact.clone().map_left(|node| (graph.clone(), node));
			let path = path.join(name);
			state.depth += 1;
			self.cache_inner(state, artifact, path)?;
			state.depth -= 1;
		}
		Ok(())
	}

	fn cache_inner_graph_file(
		&self,
		state: &mut State,
		path: PathBuf,
		graph: &tg::graph::Id,
		file: tg::graph::data::File,
	) -> tg::Result<()> {
		let tg::graph::data::File {
			contents,
			dependencies,
			executable,
		} = file;

		// Cache the dependencies.
		for referent in dependencies.values() {
			let id = match referent.item.clone() {
				Either::Left(node) => {
					let kind = state
						.graphs
						.get(graph)
						.unwrap()
						.nodes
						.get(node)
						.ok_or_else(|| tg::error!("expected the node to exist"))?
						.kind();
					let graph = graph.clone();
					let data: tg::artifact::Data = match kind {
						tg::artifact::Kind::Directory => tg::directory::Data::Graph {
							graph: graph.clone(),
							node,
						}
						.into(),
						tg::artifact::Kind::File => tg::file::Data::Graph {
							graph: graph.clone(),
							node,
						}
						.into(),
						tg::artifact::Kind::Symlink => tg::symlink::Data::Graph {
							graph: graph.clone(),
							node,
						}
						.into(),
					};
					let bytes = data.serialize()?;
					tg::artifact::Id::new(kind, &bytes)
				},
				Either::Right(id) => match tg::artifact::Id::try_from(id.clone()) {
					Ok(id) => id,
					Err(_) => continue,
				},
			};
			if id != state.artifact {
				let artifact = match &referent.item {
					Either::Left(node) => Either::Left((graph.clone(), *node)),
					Either::Right(id) => match tg::artifact::Id::try_from(id.clone()) {
						Ok(id) => Either::Right(id),
						Err(_) => continue,
					},
				};
				self.cache_dependency(&id, artifact, &state.visited, &state.progress)?;
			}
		}

		// Copy the file.
		let src = &self.blobs_path().join(contents.to_string());
		let dst = &path;
		let mut done = false;
		let mut error = None;
		let hard_link_prohibited = if cfg!(target_os = "macos") {
			dst.to_str()
				.ok_or_else(|| tg::error!("invalid path"))?
				.contains(".app/Contents")
		} else {
			false
		};
		if !done && !hard_link_prohibited {
			let result = std::fs::hard_link(src, dst);
			match result {
				Ok(()) => {
					done = true;
				},
				Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
					done = true;
				},
				Err(error_) => {
					error = Some(error_);
				},
			}
		}
		if !done {
			let result = reflink(src, dst);
			match result {
				Ok(()) => {
					done = true;
				},
				Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
					done = true;
				},
				Err(error_) => {
					error = Some(error_);
				},
			}
		}
		if !done {
			let server = self.clone();
			let path = path.clone();
			let future = async move {
				let _permit = server.file_descriptor_semaphore.acquire().await.unwrap();
				let mut reader = tg::Blob::with_id(contents)
					.read(&server, tg::blob::read::Arg::default())
					.await
					.map_err(|source| tg::error!(!source, "failed to create the reader"))?;
				let mut file_ = tokio::fs::File::create(&path)
					.await
					.map_err(|source| tg::error!(!source, ?path, "failed to create the file"))?;
				tokio::io::copy(&mut reader, &mut file_).await.map_err(
					|source| tg::error!(!source, ?path = path, "failed to write to the file"),
				)?;
				Ok::<_, tg::Error>(())
			};
			let result = tokio::runtime::Handle::current().block_on(future);
			match result {
				Ok(()) => {
					done = true;
				},
				Err(error_) => {
					error = Some(std::io::Error::other(error_));
				},
			}
		}
		if !done {
			return Err(tg::error!(?error, "failed to copy the file"));
		}

		// Set the file's permissions.
		if executable {
			let permissions = std::fs::Permissions::from_mode(0o755);
			std::fs::set_permissions(path, permissions)
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}

		Ok(())
	}

	fn cache_inner_graph_symlink(
		&self,
		state: &mut State,
		path: PathBuf,
		graph: &tg::graph::Id,
		symlink: tg::graph::data::Symlink,
	) -> tg::Result<()> {
		match symlink {
			tg::graph::data::Symlink::Target { target } => {
				std::os::unix::fs::symlink(target, path)
					.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			},
			tg::graph::data::Symlink::Artifact { artifact, subpath } => {
				// Cache the artifact.
				let id = match artifact.clone() {
					Either::Left(node) => {
						let kind = state
							.graphs
							.get(graph)
							.unwrap()
							.nodes
							.get(node)
							.ok_or_else(|| tg::error!("expected the node to exist"))?
							.kind();
						let graph = graph.clone();
						let data: tg::artifact::Data = match kind {
							tg::artifact::Kind::Directory => tg::directory::Data::Graph {
								graph: graph.clone(),
								node,
							}
							.into(),
							tg::artifact::Kind::File => tg::file::Data::Graph {
								graph: graph.clone(),
								node,
							}
							.into(),
							tg::artifact::Kind::Symlink => tg::symlink::Data::Graph {
								graph: graph.clone(),
								node,
							}
							.into(),
						};
						let bytes = data.serialize()?;
						tg::artifact::Id::new(kind, &bytes)
					},
					Either::Right(id) => id.clone(),
				};
				if id != state.artifact {
					let artifact = artifact.clone().map_left(|node| (graph.clone(), node));
					self.cache_dependency(&id, artifact, &state.visited, &state.progress)?;
				}

				// Render the target.
				let mut target = if id == state.artifact {
					// If the symlink's artifact is the root artifact, then use the root path.
					state.path.clone()
				} else {
					// Otherwise, use the artifact's path.
					state.path.parent().unwrap().join(artifact.to_string())
				};
				if let Some(subpath) = subpath {
					target = target.join(subpath);
				}
				let src = path
					.parent()
					.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
				let dst = &target;
				let target = crate::util::path::diff(src, dst)?;

				// Create the symlink.
				std::os::unix::fs::symlink(target, path)
					.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			},
		}
		Ok(())
	}

	fn cache_inner_data(
		&self,
		state: &mut State,
		path: PathBuf,
		data: tg::artifact::Data,
	) -> tg::Result<()> {
		match data {
			tg::artifact::Data::Directory(directory) => {
				self.cache_inner_data_directory(state, path, directory)?;
			},
			tg::artifact::Data::File(file) => {
				self.cache_inner_data_file(state, path, file)?;
			},
			tg::artifact::Data::Symlink(symlink) => {
				self.cache_inner_data_symlink(state, path, symlink)?;
			},
		}
		Ok(())
	}

	fn cache_inner_data_directory(
		&self,
		state: &mut State,
		path: PathBuf,
		directory: tg::directory::Data,
	) -> tg::Result<()> {
		match directory {
			tg::directory::Data::Graph { graph, node } => {
				let artifact = Either::Left((graph, node));
				self.cache_inner(state, artifact, path)?;
			},
			tg::directory::Data::Normal { entries } => {
				std::fs::create_dir_all(&path).unwrap();
				for (name, id) in entries {
					let artifact = Either::Right(id.clone());
					let path = path.join(name);
					state.depth += 1;
					self.cache_inner(state, artifact, path)?;
					state.depth -= 1;
				}
			},
		}
		Ok(())
	}

	fn cache_inner_data_file(
		&self,
		state: &mut State,
		path: PathBuf,
		file: tg::file::Data,
	) -> tg::Result<()> {
		match file {
			tg::file::Data::Graph { graph, node } => {
				let artifact = Either::Left((graph, node));
				self.cache_inner(state, artifact, path)?;
			},
			tg::file::Data::Normal {
				contents,
				dependencies,
				executable,
			} => {
				// Cache the dependencies.
				for referent in dependencies.values() {
					let Ok(id) = tg::artifact::Id::try_from(referent.item.clone()) else {
						continue;
					};
					if id != state.artifact {
						let artifact = Either::Right(id.clone());
						self.cache_dependency(&id, artifact, &state.visited, &state.progress)?;
					}
				}

				// Copy the file.
				let src = &self.blobs_path().join(contents.to_string());
				let dst = &path;
				let mut done = false;
				let mut error = None;
				let hard_link_prohibited = if cfg!(target_os = "macos") {
					dst.to_str()
						.ok_or_else(|| tg::error!("invalid path"))?
						.contains(".app/Contents")
				} else {
					false
				};
				if !done && !hard_link_prohibited {
					let result = std::fs::hard_link(src, dst);
					match result {
						Ok(()) => {
							done = true;
						},
						Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
							done = true;
						},
						Err(error_) => {
							error = Some(error_);
						},
					}
				}
				if !done {
					let result = reflink(src, dst);
					match result {
						Ok(()) => {
							done = true;
						},
						Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
							done = true;
						},
						Err(error_) => {
							error = Some(error_);
						},
					}
				}
				if !done {
					let server = self.clone();
					let path = path.clone();
					let future = async move {
						let _permit = server.file_descriptor_semaphore.acquire().await.unwrap();
						let mut reader = tg::Blob::with_id(contents)
							.read(&server, tg::blob::read::Arg::default())
							.await
							.map_err(|source| tg::error!(!source, "failed to create the reader"))?;
						let mut file = tokio::fs::File::create(&path).await.map_err(|source| {
							tg::error!(!source, ?path, "failed to create the file")
						})?;
						tokio::io::copy(&mut reader, &mut file).await.map_err(
							|source| tg::error!(!source, ?path = path, "failed to write to the file"),
						)?;
						Ok::<_, tg::Error>(())
					};
					let result = tokio::runtime::Handle::current().block_on(future);
					match result {
						Ok(()) => {
							done = true;
						},
						Err(error_) => {
							error = Some(std::io::Error::other(error_));
						},
					}
				}
				if !done {
					return Err(tg::error!(?error, "failed to copy the file"));
				}

				// Set the file's permissions.
				if executable {
					let permissions = std::fs::Permissions::from_mode(0o755);
					std::fs::set_permissions(path, permissions)
						.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
				}
			},
		}
		Ok(())
	}

	fn cache_inner_data_symlink(
		&self,
		state: &mut State,
		path: PathBuf,
		symlink: tg::symlink::Data,
	) -> tg::Result<()> {
		match symlink {
			tg::symlink::Data::Graph { graph, node } => {
				let artifact = Either::Left((graph, node));
				self.cache_inner(state, artifact, path)?;
			},
			tg::symlink::Data::Target { target } => {
				std::os::unix::fs::symlink(target, path)
					.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			},
			tg::symlink::Data::Artifact { artifact, subpath } => {
				// Cache the artifact.
				if artifact != state.artifact {
					self.cache_dependency(
						&artifact,
						Either::Right(artifact.clone()),
						&state.visited,
						&state.progress,
					)?;
				}

				// Render the target.
				let mut target = if artifact == state.artifact {
					// If the symlink's artifact is the root artifact, then use the root path.
					state.path.clone()
				} else {
					// Otherwise, use the artifact's path.
					state.path.parent().unwrap().join(artifact.to_string())
				};
				if let Some(subpath) = subpath {
					target = target.join(subpath);
				}
				let src = path
					.parent()
					.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
				let dst = &target;
				let target = crate::util::path::diff(src, dst)?;

				// Create the symlink.
				std::os::unix::fs::symlink(target, path)
					.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			},
		}
		Ok(())
	}
}
