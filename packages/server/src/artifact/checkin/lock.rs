use std::collections::BTreeSet;

use super::graph;
use crate::Server;
use either::Either;
use tangram_client as tg;
use tokio::io::AsyncWriteExt as _;

impl Server {
	pub(super) async fn get_or_create_lock(
		&self,
		arg: &tg::artifact::checkin::Arg,
	) -> tg::Result<(tg::Lock, usize)> {
		// Get the lock file path.
		let lockfilepath = tg::artifact::module::try_get_lock_path_for_path(arg.path.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to get lockfile path"))?
			.ok_or_else(|| tg::error!(%path = arg.path, "invalid root module path"))?;

		// Resolve local path dependencies.
		let (graph, root) = self
			.create_graph_for_path(&arg.path, arg.locked)
			.await
			.map_err(
				|source| tg::error!(!source, %path = arg.path, "failed to resolve path dependencies"),
			)?;

		// Check for an existing lockfile
		let lock = self
			.try_read_lockfile(&lockfilepath)
			.await
			.map_err(|source| tg::error!(!source, %path = arg.path, "failed to read lockfile"))?;
		if let Some(lock) = lock {
			// Add path dependenices back to the lock.
			let lock = self
				.add_path_references_to_lock(&lock, 0, &arg.path.clone().parent(), arg.locked)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to add path dependencies to lockfile")
				})?;

			// Create a graph for the lock.
			let (graph_, root_) = self
				.create_graph_for_lockfile(&lock)
				.await
				.map_err(|source| tg::error!(!source, "failed to create graph for lockfile"))?;

			// Validate.
			let mut visited = BTreeSet::new();
			if self
				.check_graph(&graph, &root, &graph_, &root_, &mut visited)
				.await
				.map_err(|source| tg::error!(!source, "failed to check if lock is out of date"))?
			{
				return self
					.create_lock(&graph_, &root_)
					.await
					.map_err(|source| tg::error!(!source, "failed to create lock"));
			} else if arg.locked {
				return Err(tg::error!("lock is out of date"));
			}
		}

		// Walk the package graph to fill in tag dependencies.
		let graph = self
			.walk_package_graph(graph, &root)
			.await
			.map_err(|source| tg::error!(!source, "failed to walk package graph"))?;
		graph.validate(self)?;

		// Create the package.
		let (lock, root) = self
			.create_lock(&graph, &root)
			.await
			.map_err(|source| tg::error!(!source, "failed to create lock"))?;

		// Remove path references from the package.
		let lockfile = self.remove_path_references_from_lock(&lock, root).await?;
		self.write_lockfile(&lockfilepath, &lockfile).await?;

		Ok((lock, root))
	}

	async fn add_path_references_to_lock(
		&self,
		package: &tg::Lock,
		node: usize,
		path: &tg::Path,
		locked: bool,
	) -> tg::Result<tg::Lock> {
		let mut object = package.object(self).await?.as_ref().clone();
		let mut stack = vec![(node, path.clone())];
		let mut visited = BTreeSet::new();
		while let Some((index, path)) = stack.pop() {
			if visited.contains(&index) {
				continue;
			}
			visited.insert(index);
			let node = &mut object.nodes[index];
			let arg = tg::artifact::checkin::Arg {
				path: path.clone(),
				destructive: false,
				locked,
				dependencies: true,
			};
			let object_ = self
				.check_in_artifact_without_progress(arg)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to check in artifact"))?
				.into();

			node.object.replace(object_);

			for (reference, value) in &mut node.dependencies.iter_mut().flatten() {
				let Some(path_) = reference
					.path()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| reference.query()?.path.as_ref())
				else {
					continue;
				};

				let path = path.clone().parent().join(path_.clone()).normalize();
				let root_module_path =
					tg::artifact::module::try_get_root_module_path_for_path(path.as_ref())
						.await
						.ok()
						.flatten();
				let path = root_module_path
					.map(|p| path.clone().join(p))
					.unwrap_or(path);

				match value {
					Either::Left(index) => {
						stack.push((*index, path));
					},
					Either::Right(object_) => {
						if let tg::Object::File(file) = object_ {
							let mut file = file.object(self).await?.as_ref().clone();
							match &file.dependencies {
								Some(tg::file::Dependencies::Set(_)) | None => (),
								Some(tg::file::Dependencies::Map(_dependencies)) => {
									todo!()
								},
								Some(tg::file::Dependencies::Lock(lock, node)) => {
									let lock =
										Box::pin(self.add_path_references_to_lock(
											lock, *node, &path, locked,
										))
										.await?;
									file.dependencies
										.replace(tg::file::Dependencies::Lock(lock, *node));
									*object_ = tg::File::with_object(file).into();
									continue;
								},
							}
						}
						let arg = tg::artifact::checkin::Arg {
							path: path.clone(),
							destructive: false,
							locked,
							dependencies: true,
						};
						*object_ = self
							.check_in_artifact_without_progress(arg)
							.await
							.map_err(
								|source| tg::error!(!source, %path, "failed to check in artifact"),
							)?
							.into();
					},
				}
			}
		}
		Ok(tg::Lock::with_object(object))
	}

	async fn remove_path_references_from_lock(
		&self,
		lock: &tg::Lock,
		node: usize,
	) -> tg::Result<tg::Lock> {
		let mut object = lock.object(self).await?.as_ref().clone();
		let mut stack = vec![node];
		let mut visited = BTreeSet::new();
		while let Some(index) = stack.pop() {
			if visited.contains(&index) {
				continue;
			}
			visited.insert(index);
			object.nodes[index].object.take();
			for (reference, either) in object.nodes[index].dependencies.iter_mut().flatten() {
				let Some(_) = reference
					.path()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| reference.query().and_then(|q| q.path.as_ref()))
				else {
					continue;
				};
				match either {
					Either::Left(index) => stack.push(*index),
					Either::Right(tg::Object::File(file)) => {
						let mut object = file.object(self).await?.as_ref().clone();
						let Some(dependencies) = &object.dependencies else {
							continue;
						};
						match dependencies {
							tg::file::Dependencies::Set(_) => continue,
							tg::file::Dependencies::Map(_dependencies) => {
								todo!()
							},
							tg::file::Dependencies::Lock(lock, node) => {
								let lock =
									Box::pin(self.remove_path_references_from_lock(lock, *node))
										.await?;
								object
									.dependencies
									.replace(tg::file::Dependencies::Lock(lock, *node));
								*file = tg::File::with_object(object);
							},
						}
					},
					Either::Right(_object) => continue,
				}
			}
		}
		Ok(tg::Lock::with_object(object))
	}

	async fn check_graph(
		&self,
		package_graph: &graph::Graph,
		package_node: &graph::Id,
		lock_graph: &graph::Graph,
		lock_node: &graph::Id,
		visited: &mut BTreeSet<graph::Id>,
	) -> tg::Result<bool> {
		if visited.contains(package_node) {
			return Ok(true);
		};
		visited.insert(package_node.clone());
		let package_node = package_graph.nodes.get(package_node).unwrap();
		let lock_node = lock_graph.nodes.get(lock_node).unwrap();
		if package_node.outgoing.len() != lock_node.outgoing.len() {
			return Ok(false);
		}

		for (reference, package_node) in &package_node.outgoing {
			// If the package graph doesn't contain a node it means it hasn't been solved, so skip.
			if !package_graph.nodes.contains_key(package_node) {
				continue;
			}
			let Some(lock_node) = lock_node.outgoing.get(reference) else {
				return Ok(false);
			};
			if !Box::pin(self.check_graph(
				package_graph,
				package_node,
				lock_graph,
				lock_node,
				visited,
			))
			.await?
			{
				return Ok(false);
			}
		}
		Ok(true)
	}

	async fn try_read_lockfile(&self, path: &tg::Path) -> tg::Result<Option<tg::Lock>> {
		let bytes = match tokio::fs::read(&path).await {
			Ok(bytes) => bytes,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(source) => {
				return Err(tg::error!(!source, %path, "failed to read the lockfile"));
			},
		};
		let data = serde_json::from_slice::<tg::lock::Data>(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the lockfile"))?;
		let object: tg::lock::Object = data.try_into()?;
		let lock = tg::Lock::with_object(object);
		Ok(Some(lock))
	}

	async fn write_lockfile(&self, path: &tg::Path, lock: &tg::Lock) -> tg::Result<()> {
		let path = path.clone().join(tg::artifact::module::LOCKFILE_FILE_NAME);
		let data = lock.data(self).await?;
		let bytes = serde_json::to_vec_pretty(&data)
			.map_err(|source| tg::error!(!source, "failed to serialize data"))?;
		let mut file = tokio::fs::File::options()
			.create(true)
			.truncate(true)
			.append(false)
			.write(true)
			.open(&path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to open lockfile for writing"))?;
		file.write_all(&bytes)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to write lockfile"))?;
		Ok(())
	}
}
