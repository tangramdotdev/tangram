use {
	crate::{Component, Graph, Variant},
	std::{
		collections::{BTreeMap, HashSet, VecDeque},
		path::Path,
		sync::Arc,
	},
	tangram_client::prelude::*,
};

impl Component {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn checkin_write_lock(
		&self,
		arg: &tg::checkin::Arg,
		graph: &Graph,
		next: usize,
		lock: Option<Arc<tg::graph::data::Leaf>>,
		root: &Path,
		reserve_lock_write: impl FnOnce() -> tg::Result<Option<crate::watch::LockWriteGuard>>,
	) -> tg::Result<(
		Option<Arc<tg::graph::data::Leaf>>,
		Option<crate::watch::LockWriteGuard>,
	)> {
		// Get the root node.
		let root_index = graph.paths.get(root).unwrap();
		let root_node = graph.nodes.get(root_index).unwrap();

		// Do not write a lock if the lock arg is not set.
		if arg.options.lock.is_none() {
			return Ok((lock, None));
		}

		// Do not write a lock if this is a destructive checkin.
		if arg.options.destructive {
			return Ok((lock, None));
		}

		// If the root is not solvable, then remove an existing lock and return.
		if !root_node.solvable {
			if arg.options.locked && lock.is_some() {
				return Err(tg::error!("the lock is out of date"));
			}
			let lock_write_guard = reserve_lock_write()?;
			match root_node.variant {
				Variant::Directory(_) => {
					let lockfile_path = root.join(tg::module::LOCKFILE_FILE_NAME);
					tangram_util::fs::remove(&lockfile_path).await.ok();
				},
				Variant::File(_) => {
					let lockfile_path = root.with_extension("lock");
					tangram_util::fs::remove(&lockfile_path).await.ok();
					tg::file::xattrs::remove_lock(root).ok();
				},
				Variant::Object => unreachable!(),
				Variant::Symlink(_) => (),
			}
			return Ok((None, lock_write_guard));
		}

		// Do not write a lock if the lock was not changed during solving.
		if let Some(lock) = &lock {
			let changed = Self::checkin_lock_changed(graph, next, lock);
			if !changed {
				return Ok((Some(lock.clone()), None));
			}
		}

		// Create the lock.
		let new_lock = Self::checkin_create_lock(graph, root, arg.options.source_dependencies);

		// If this is a locked checkin, then verify the lock is unchanged.
		if arg.options.locked
			&& lock
				.as_ref()
				.is_some_and(|existing| new_lock.nodes != existing.nodes)
		{
			return Err(tg::error!("the lock is out of date"));
		}

		let lock = Arc::new(new_lock);
		let expected = (!lock.nodes.is_empty()).then(|| lock.clone());
		let lock_write_guard = reserve_lock_write()?;

		// If the root is a directory, then write a lockfile. Otherwise, write a file lock.
		match root_node.variant {
			Variant::Directory(_) => {
				// Determine the lockfile path.
				let lockfile_path = root.join(tg::module::LOCKFILE_FILE_NAME);

				// Remove an existing lockfile.
				tangram_util::fs::remove(&lockfile_path).await.ok();

				// Do not write an empty lock.
				if lock.nodes.is_empty() {
					return Ok((expected, lock_write_guard));
				}

				// Serialize the lock.
				let contents = serde_json::to_vec_pretty(&lock)
					.map_err(|error| tg::error!(!error, "failed to serialize the lock"))?;

				// Write the lockfile.
				tokio::fs::write(&lockfile_path, contents)
					.await
					.map_err(|error| tg::error!(!error, "failed to write the lock"))?;
			},

			Variant::File(_) => {
				let lock_kind = arg.options.lock.unwrap();
				let lock_kind = if matches!(lock_kind, tg::checkin::Lock::Auto) {
					let lockfile_path = root.with_extension("lock");
					if tokio::fs::metadata(&lockfile_path)
						.await
						.is_ok_and(|metadata| metadata.is_file())
					{
						tg::checkin::Lock::File
					} else {
						tg::checkin::Lock::Attr
					}
				} else {
					lock_kind
				};
				match lock_kind {
					tg::checkin::Lock::Attr => {
						// Get the lockfile path.
						let lockfile_path = root.with_extension("lock");

						// Remove an existing lockfile.
						tangram_util::fs::remove(&lockfile_path).await.ok();

						// Remove an existing lockattr.
						tg::file::xattrs::remove_lock(root).ok();

						// Do not write an empty lock.
						if lock.nodes.is_empty() {
							return Ok((expected, lock_write_guard));
						}

						// Serialize the lock.
						let contents = serde_json::to_vec(&lock)
							.map_err(|error| tg::error!(!error, "failed to serialize the lock"))?;

						// Write the lockattr.
						tg::file::xattrs::write_lock(root, &contents)
							.map_err(|error| tg::error!(!error, "failed to write the lockattr"))?;
					},

					tg::checkin::Lock::File => {
						// Remove an existing lockattr.
						tg::file::xattrs::remove_lock(root).ok();

						// Get the lockfile path.
						let lockfile_path = root.with_extension("lock");

						// Remove an existing lockfile.
						tangram_util::fs::remove(&lockfile_path).await.ok();

						// Do not write an empty lock.
						if lock.nodes.is_empty() {
							return Ok((expected, lock_write_guard));
						}

						// Serialize the lock.
						let contents = serde_json::to_vec_pretty(&lock)
							.map_err(|error| tg::error!(!error, "failed to serialize the lock"))?;

						// Write the lockfile.
						tokio::fs::write(&lockfile_path, contents)
							.await
							.map_err(|error| tg::error!(!error, "failed to write the lock"))?;
					},

					tg::checkin::Lock::Auto => unreachable!(),
				}
			},
			Variant::Object => unreachable!(),

			Variant::Symlink(_) => {},
		}

		Ok((expected, lock_write_guard))
	}

	fn checkin_lock_changed(graph: &Graph, next: usize, lock: &tg::graph::data::Leaf) -> bool {
		graph.nodes.range(next..).any(|(_, node)| {
			if let Some(lock_index) = node.lock_index {
				let lock_node = lock.nodes.get(lock_index).unwrap();
				match (lock_node, &node.variant) {
					// If a directory removed an entry that was present in the lock, then the lock changed.
					(tg::graph::data::Node::Directory(lock), Variant::Directory(node)) => {
						let lock_leaf = lock
							.try_unwrap_leaf_ref()
							.expect("lock directories must be leaves");
						for name in lock_leaf.entries.keys() {
							if !node.entries.contains_key(name) {
								return true;
							}
						}
					},
					(tg::graph::data::Node::File(lock), Variant::File(node)) => {
						let dependencies = node
							.dependencies
							.iter()
							.map(|(reference, dependency)| {
								let reference =
									reference_without_location_and_tokens(reference.clone());
								let dependency = dependency
									.clone()
									.map(tg::graph::data::Dependency::without_location_and_tokens);
								(reference, dependency)
							})
							.collect::<BTreeMap<_, _>>();
						if dependencies.iter().any(|(reference, option)| {
							if !reference.is_solvable() {
								return false;
							}
							if let Some(lock) = lock.dependencies.get(reference) {
								// If a file had a dependency change, then the lock changed.
								if lock.as_ref().map(|lock| &lock.options)
									!= option.as_ref().map(|referent| &referent.options)
								{
									return true;
								}
							} else {
								// If a file added a solvable dependency, then the lock changed.
								return true;
							}
							false
						}) {
							return true;
						}

						// If a file removed a dependency that was present in the lock, then the lock changed.
						for reference in lock.dependencies.keys() {
							if !dependencies.contains_key(reference) {
								return true;
							}
						}
					},
					(tg::graph::data::Node::Symlink(_), Variant::Symlink(_)) => (),
					_ => {
						return true;
					},
				}
			} else if let Variant::File(file) = &node.variant {
				// If a file with solvable dependencies was added, then the lock changed.
				if file
					.dependencies
					.iter()
					.any(|(reference, _)| reference.is_solvable())
				{
					return true;
				}
			}
			false
		})
	}

	fn checkin_create_lock(
		graph: &Graph,
		root: &Path,
		source_dependencies: bool,
	) -> tg::graph::data::Leaf {
		// Get the root node index.
		let root_index = *graph.paths.get(root).unwrap();

		let mut nodes = Vec::with_capacity(graph.nodes.len());
		let mut visited = HashSet::new();
		let mut queue = VecDeque::new();
		let mut mapping = BTreeMap::new();

		queue.push_back(root_index);

		while let Some(index) = queue.pop_front() {
			// Skip if already visited.
			if !visited.insert(index) {
				continue;
			}

			// Record the mapping from graph index to lock index.
			mapping.insert(index, nodes.len());

			// Get the node.
			let node = &graph.nodes.get(&index).unwrap();

			// Create the lock node.
			let (lock_node, children) = match &node.variant {
				Variant::Directory(directory) => {
					let mut entries = BTreeMap::new();
					let mut children = Vec::new();
					for (name, edge) in &directory.entries {
						entries.insert(name.clone(), edge.clone());
						if let Ok(target_index) = edge.try_unwrap_index_ref()
							&& !visited.contains(target_index)
						{
							children.push(*target_index);
						}
					}
					let leaf = tg::graph::data::DirectoryLeaf { entries };
					let data = tg::graph::data::Directory::Leaf(leaf);
					(tg::graph::data::Node::Directory(data), children)
				},

				Variant::File(file) => {
					let mut dependencies = BTreeMap::new();
					let mut children = Vec::new();
					for (reference, option) in &file.dependencies {
						if let Some(dependency) = option
							&& let Some(edge) = &dependency.0.node
							&& let Ok(target_index) = edge.try_unwrap_index_ref()
							&& !visited.contains(target_index)
						{
							children.push(*target_index);
						}
						let reference = reference_without_location_and_tokens(reference.clone());
						let dependency = option
							.clone()
							.map(tg::graph::data::Dependency::without_location_and_tokens);
						dependencies.insert(reference, dependency);
					}
					let data = tg::graph::data::File {
						contents: None,
						dependencies,
						executable: file.executable,
						module: file.module,
					};
					(tg::graph::data::Node::File(data), children)
				},

				Variant::Object => unreachable!(),

				Variant::Symlink(symlink) => {
					let mut children = Vec::new();
					if let Some(edge) = &symlink.artifact
						&& let Ok(target_index) = edge.try_unwrap_index_ref()
						&& !visited.contains(target_index)
					{
						children.push(*target_index);
					}
					let data = tg::graph::data::Symlink {
						artifact: symlink.artifact.clone(),
						path: None,
					};
					(tg::graph::data::Node::Symlink(data), children)
				},
			};

			// Add the node to the lock.
			nodes.push(lock_node);

			// Add the children to the queue.
			for child in children {
				queue.push_back(child);
			}
		}

		// Remap from graph indices to lock indices.
		for node in &mut nodes {
			match node {
				tg::graph::data::Node::Directory(directory) => {
					if let tg::graph::data::Directory::Leaf(leaf) = directory {
						for edge in leaf.entries.values_mut() {
							if let tg::graph::data::Edge::Index(target_index) = edge
								&& let Some(lock_index) = mapping.get(&*target_index)
							{
								*target_index = *lock_index;
							}
						}
					}
				},
				tg::graph::data::Node::File(file) => {
					for dependency in file.dependencies.values_mut().flatten() {
						if let Some(tg::graph::data::Edge::Index(target_index)) =
							&mut dependency.node && let Some(lock_index) =
							mapping.get(&*target_index)
						{
							*target_index = *lock_index;
						}
					}
				},
				tg::graph::data::Node::Symlink(symlink) => {
					if let Some(tg::graph::data::Edge::Index(target_index)) = &mut symlink.artifact
						&& let Some(lock_index) = mapping.get(&*target_index)
					{
						*target_index = *lock_index;
					}
				},
			}
		}

		// Create the lock.
		let lock = tg::graph::data::Leaf { nodes };

		// Strip the lock.
		tangram_server_checkin_common::strip_lock(lock, source_dependencies)
	}
}

fn reference_without_location_and_tokens(mut reference: tg::Reference) -> tg::Reference {
	let mut options = reference.options().clone();
	options.location.take();
	options.tokens.clear();
	reference.set_options(options);

	reference
}
