use std::collections::BTreeSet;

use super::graph;
use crate::Server;
use tangram_client as tg;
use tokio::io::AsyncWriteExt as _;

pub(super) struct Output {
	pub stripped: tg::Lock,
	pub non_stripped: tg::Lock,
	pub root_node_index: usize,
}

impl Server {
	// pub(super) async fn get_or_create_lock(
	// 	&self,
	// 	arg: &tg::artifact::checkin::Arg,
	// 	state: &super::State,
	// ) -> tg::Result<Output> {
	// 	// Get the lock file path.
	// 	let lockfilepath = tg::artifact::module::try_get_lock_path_for_path(arg.path.as_ref())
	// 		.await
	// 		.map_err(|source| tg::error!(!source, "failed to get lockfile path"))?
	// 		.ok_or_else(|| tg::error!(%path = arg.path, "invalid root module path"))?;

	// 	// Resolve local path dependencies.
	// 	let (graph, root) = self
	// 		.create_graph_for_path(&arg.path, arg.locked, state)
	// 		.await
	// 		.map_err(
	// 			|source| tg::error!(!source, %path = arg.path, "failed to resolve path dependencies"),
	// 		)?;

	// 	// Check for an existing lockfile
	// 	let lock = self
	// 		.try_read_lockfile(&lockfilepath)
	// 		.await
	// 		.map_err(|source| tg::error!(!source, %path = arg.path, "failed to read lockfile"))?;
	// 	if let Some(lock) = lock {
	// 		// Create a graph for the lock.
	// 		let (graph_, root_) = self
	// 			.create_graph_for_lockfile(&lock)
	// 			.await
	// 			.map_err(|source| tg::error!(!source, "failed to create graph for lockfile"))?;

	// 		// Validate.
	// 		let mut visited = BTreeSet::new();
	// 		if self
	// 			.check_graph(&graph, &root, &graph_, &root_, &mut visited)
	// 			.await
	// 			.map_err(|source| tg::error!(!source, "failed to check if lock is out of date"))?
	// 		{
	// 			return self
	// 				.create_lock(&graph_, &root_)
	// 				.await
	// 				.map_err(|source| tg::error!(!source, "failed to create lock"));
	// 		} else if arg.locked {
	// 			return Err(tg::error!("lock is out of date"));
	// 		}
	// 	}

	// 	// Unify any unresolved dependencies.
	// 	let graph = self
	// 		.unify_dependencies(graph, &root)
	// 		.await
	// 		.map_err(|source| tg::error!(!source, "failed to walk package graph"))?;

	// 	graph.validate(self)?;

	// 	// Create the package.
	// 	let (lock, root) = self
	// 		.create_lock(&graph, &root)
	// 		.await
	// 		.map_err(|source| tg::error!(!source, "failed to create lock"))?;

	// 	// Mark that the lock needs to be written to disk.
	// 	state.visited.lock().unwrap().get_mut(&arg.path).ok_or_else(|| tg::error!("invalid graph"))?.lock.replace(super::Lock::File);

	// 	Ok((lock, root))
	// }

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
