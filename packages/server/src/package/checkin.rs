use crate::Server;
use either::Either;
use std::collections::BTreeSet;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tokio::io::AsyncWriteExt;
mod graph;

impl Server {
	pub async fn check_in_package(
		&self,
		arg: tg::package::checkin::Arg,
	) -> tg::Result<tg::package::checkin::Output> {
		let tg::package::checkin::Arg {
			path,
			locked,
			remote,
		} = arg;

		// Handle the remote.
		if let Some(remote) = remote.as_ref() {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!(%remote, "remote does not exist"))?;
			let arg = tg::package::checkin::Arg {
				path,
				locked: false,
				remote: None,
			};
			return remote.check_in_package(arg).await;
		};

		// Get the root module path.
		let root_module_path = tg::package::get_root_module_path_for_path(path.as_ref())
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to get the root module path"))?;

		// Resolve path dependencies to create the initial package object.
		let (graph, root) = self
			.create_graph_for_path(&path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to create package graph"))?;

		// Read and validate against an existing lockfile if it exists.
		let lockfile = self
			.try_read_lockfile(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read lockfile"))?;
		if let Some(lockfile) = lockfile {
			// Add the path dependencies back to the package.
			let package = self
				.add_path_references_to_package(
					&lockfile,
					&path.clone().join(root_module_path.clone()),
				)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to add path dependencies to lockfile")
				})?;

			let (graph_, root_) = self.create_graph_for_lockfile(&package).await?;

			let mut visited = BTreeSet::new();
			if self.check_graph(&graph, &root, &graph_, &root_, &mut visited) {
				let object = graph_.into_package_object(&root_);
				let package = tg::Package::with_object(object).id(self).await?;
				let output = tg::package::checkin::Output { package };
				return Ok(output);
			} else if locked {
				return Err(tg::error!("lockfile is out of date"));
			}
		}

		// Walk the package graph to fill in tag dependencies.
		let graph = self
			.walk_package_graph(graph, &root)
			.await
			.map_err(|source| tg::error!(!source, "failed to walk package graph"))?;

		// Create the package.
		let object = graph.into_package_object(&root);
		let package = tg::Package::with_object(object);

		// Remove path references from the package.
		let lockfile = self.remove_path_references_from_package(&package).await?;
		self.write_lockfile(&path, &lockfile).await?;

		// Create the output.
		let output = tg::package::checkin::Output {
			package: package.id(self).await?,
		};

		Ok(output)
	}

	fn check_graph(
		&self,
		package_graph: &graph::Graph,
		package_node: &graph::Id,
		lock_graph: &graph::Graph,
		lock_node: &graph::Id,
		visited: &mut BTreeSet<graph::Id>,
	) -> bool {
		if visited.contains(package_node) {
			return true;
		};
		visited.insert(package_node.clone());
		let package_node = package_graph.nodes.get(package_node).unwrap();
		let lock_node = lock_graph.nodes.get(lock_node).unwrap();
		if package_node.outgoing.len() != lock_node.outgoing.len() {
			return false;
		}
		for (reference, package_node) in package_node.outgoing.iter() {
			// If the package graph doesn't contain a node it means it hasn't been solved, so skip.
			if !package_graph.nodes.contains_key(package_node) {
				continue;
			}
			let Some(lock_node) = lock_node.outgoing.get(reference) else {
				return false;
			};
			if !self.check_graph(package_graph, package_node, lock_graph, lock_node, visited) {
				return false;
			}
		}
		true
	}

	async fn try_read_lockfile(&self, path: &tg::Path) -> tg::Result<Option<tg::Package>> {
		let path = path.clone().join(tg::package::LOCKFILE_FILE_NAME);
		let bytes = match tokio::fs::read(&path).await {
			Ok(bytes) => bytes,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(source) => {
				return Err(tg::error!(!source, %path, "failed to read the lockfile"));
			},
		};
		let data: tg::package::Data = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the lockfile"))?;
		let object: tg::package::Object = data.try_into()?;
		let package = tg::Package::with_object(object);
		Ok(Some(package))
	}

	async fn write_lockfile(&self, path: &tg::Path, package: &tg::Package) -> tg::Result<()> {
		let path = path.clone().join(tg::package::LOCKFILE_FILE_NAME);
		let data = package.data(self).await?;
		let bytes = serde_json::to_vec_pretty(&data)
			.map_err(|source| tg::error!(!source, "failed to serialize data"))?;
		tokio::fs::File::options()
			.create(true)
			.append(false)
			.write(true)
			.open(&path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to open lockfile for writing"))?
			.write_all(&bytes)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to write lockfile"))?;
		Ok(())
	}

	async fn add_path_references_to_package(
		&self,
		package: &tg::Package,
		path: &tg::Path,
	) -> tg::Result<tg::Package> {
		let mut object = package.object(self).await?.as_ref().clone();
		let mut stack = vec![(object.root, path.clone())];
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
			};
			let object_ = tg::Artifact::check_in(self, arg)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to check in artifact"))?
				.into();
			node.object.replace(object_);

			for (reference, value) in &mut node.dependencies {
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
					tg::package::try_get_root_module_path_for_path(path.as_ref())
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
					Either::Right(tg::Object::Package(package)) => {
						*package =
							Box::pin(self.add_path_references_to_package(package, &path)).await?;
					},
					Either::Right(object_) => {
						let arg = tg::artifact::checkin::Arg {
							path: path.clone(),
							destructive: false,
						};
						*object_ = tg::Artifact::check_in(self, arg)
							.await
							.map_err(
								|source| tg::error!(!source, %path, "failed to check in artifact"),
							)?
							.into();
					},
				}
			}
		}
		Ok(tg::Package::with_object(object))
	}

	async fn remove_path_references_from_package(
		&self,
		package: &tg::Package,
	) -> tg::Result<tg::Package> {
		let mut object = package.object(self).await?.as_ref().clone();
		let mut stack = vec![(object.root)];
		let mut visited = BTreeSet::new();
		while let Some(index) = stack.pop() {
			if visited.contains(&index) {
				continue;
			}
			visited.insert(index);
			object.nodes[index].object.take();
			for (reference, package) in &mut object.nodes[index].dependencies {
				let Some(_) = reference
					.path()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| reference.query().and_then(|q| q.path.as_ref()))
				else {
					continue;
				};
				match package {
					Either::Left(index) => stack.push(*index),
					Either::Right(tg::Object::Package(package)) => {
						*package =
							Box::pin(self.remove_path_references_from_package(&package)).await?;
					},
					Either::Right(_object) => continue,
				}
			}
		}
		Ok(tg::Package::with_object(object))
	}
}

impl Server {
	pub(crate) async fn handle_check_in_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.check_in_package(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
