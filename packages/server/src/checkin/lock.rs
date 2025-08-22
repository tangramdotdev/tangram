use {
	super::state::{State, Variant},
	crate::Server,
	std::{
		collections::{BTreeMap, BTreeSet},
		path::{Path, PathBuf},
	},
	tangram_client as tg,
	tangram_either::Either,
};

impl Server {
	pub(crate) fn checkin_try_read_lock(path: &Path) -> tg::Result<Option<tg::graph::Data>> {
		// Attempt to read a lockattr.
		let contents = 'a: {
			let Ok(Some(contents)) = xattr::get(path, tg::file::LOCKATTR_XATTR_NAME) else {
				break 'a None;
			};
			Some(contents)
		};

		// Attempt to read a lockfile.
		let contents = 'a: {
			if let Some(contents) = contents {
				break 'a Some(contents);
			}
			let lock_path = path.join(tg::package::LOCKFILE_FILE_NAME);
			let contents = match std::fs::read(&lock_path) {
				Ok(contents) => contents,
				Err(error)
					if matches!(
						error.kind(),
						std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
					) =>
				{
					break 'a None;
				},
				Err(source) => {
					return Err(
						tg::error!(!source, %path = lock_path.display(), "failed to read the lockfile"),
					);
				},
			};
			Some(contents)
		};

		let Some(contents) = contents else {
			return Ok(None);
		};

		// Deserialize the lock.
		let lock = serde_json::from_slice::<tg::graph::Data>(&contents).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to deserialize the lock"),
		)?;

		Ok(Some(lock))
	}

	pub(crate) fn checkin_get_lock_paths(
		path: &std::path::Path,
		lock: &tg::graph::Data,
	) -> radix_trie::Trie<PathBuf, usize> {
		let mut paths = radix_trie::Trie::new();
		let mut stack = Vec::new();
		if !lock.nodes.is_empty() {
			stack.push((path.to_owned(), 0));
		}
		let mut visited = BTreeSet::new();
		while let Some((path, index)) = stack.pop() {
			if !visited.insert(index) {
				continue;
			}
			paths.insert(path.clone(), index);
			let node = &lock.nodes[index];
			match node {
				tg::graph::data::Node::Directory(directory) => {
					for (name, edge) in &directory.entries {
						if let Ok(reference) = edge.try_unwrap_reference_ref() {
							let path = path.join(name);
							stack.push((path, reference.node));
						}
					}
				},
				tg::graph::data::Node::File(file) => {
					for referent in file.dependencies.values() {
						if referent.tag().is_none()
							&& let Some(referent_path) = referent.path()
							&& let Ok(reference) = referent.item.try_unwrap_reference_ref()
						{
							let path = path.join(referent_path);
							stack.push((path, reference.node));
						}
					}
				},
				tg::graph::data::Node::Symlink(_) => {},
			}
		}
		paths
	}

	pub(super) async fn checkin_write_lock(&self, state: &State) -> tg::Result<()> {
		// Do not create a lock if this is a destructive checkin or the user did not request one.
		if state.arg.destructive || !state.arg.lock {
			return Ok(());
		}

		// Create the lock.
		let lock = Self::checkin_create_lock(state);

		// If this is a locked checkin, then verify the lock is unchanged.
		if state.arg.locked
			&& state
				.lock
				.as_ref()
				.is_some_and(|existing| existing.nodes != lock.nodes)
		{
			return Err(tg::error!("the lock is out of date"));
		}

		// If the root is a directory, then write a lockfile. Otherwise, write a lockattr.
		match state.graph.nodes[0].variant {
			Some(Variant::Directory(_)) => {
				// Determine the lockfile path.
				let lockfile_path = state.arg.path.join(tg::package::LOCKFILE_FILE_NAME);

				// Remove an existing lockfile.
				crate::util::fs::remove(&lockfile_path).await.ok();

				// Do nothing if the lock is empty.
				if lock.nodes.is_empty() {
					return Ok(());
				}

				// Serialize the lock.
				let contents = serde_json::to_vec_pretty(&lock)
					.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;

				// Write the lockfile.
				tokio::fs::write(&lockfile_path, contents)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the lock"))?;
			},

			Some(Variant::File(_)) => {
				// Serialize the lock.
				let contents = serde_json::to_vec(&lock)
					.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;

				// Write the lockattr.
				xattr::set(&state.arg.path, tg::file::LOCKATTR_XATTR_NAME, &contents)
					.map_err(|source| tg::error!(!source, "failed to write the lockatttr"))?;
			},

			_ => {},
		}

		Ok(())
	}

	fn checkin_create_lock(state: &State) -> tg::graph::Data {
		// Create the nodes.
		let mut nodes = Vec::with_capacity(state.graph.nodes.len());
		for node in &state.graph.nodes {
			let node = match &node.variant {
				Some(Variant::Directory(directory)) => {
					let mut entries = BTreeMap::new();
					for (name, node) in directory.entries.clone() {
						let reference = tg::graph::data::Reference { graph: None, node };
						let edge = tg::graph::data::Edge::Reference(reference);
						entries.insert(name, edge);
					}
					let data = tg::graph::data::Directory { entries };
					tg::graph::data::Node::Directory(data)
				},

				Some(Variant::File(file)) => {
					let contents = match &file.blob {
						Some(Either::Right(id)) => Some(id.clone()),
						_ => None,
					};
					let mut dependencies = BTreeMap::new();
					for (reference, referent) in &file.dependencies {
						if let Some(referent) = referent {
							let item = {
								let node = *referent.item();
								let reference = tg::graph::data::Reference { graph: None, node };
								tg::graph::data::Edge::Reference(reference)
							};
							let referent = referent.clone().map(|_| item);
							dependencies.insert(reference.clone(), referent);
						}
					}
					let executable = file.executable;
					let data = tg::graph::data::File {
						contents,
						dependencies,
						executable,
					};
					tg::graph::data::Node::File(data)
				},

				Some(Variant::Symlink(symlink)) => {
					let artifact = symlink.artifact.map(|artifact| {
						let reference = tg::graph::data::Reference {
							graph: None,
							node: artifact,
						};
						tg::graph::data::Edge::Reference(reference)
					});
					let data = tg::graph::data::Symlink {
						artifact,
						path: symlink.path.clone(),
					};
					tg::graph::data::Node::Symlink(data)
				},

				None => tg::graph::data::Node::Directory(tg::graph::data::Directory {
					entries: BTreeMap::new(),
				}),
			};

			nodes.push(node);
		}

		// Create the lock.
		let lock = tg::graph::Data { nodes };

		// Strip the lock.
		Self::strip_lock(lock)
	}

	fn strip_lock(lock: tg::graph::Data) -> tg::graph::Data {
		// Mark.
		let mut marks = vec![false; lock.nodes.len()];
		let mut visited = BTreeSet::new();
		Self::strip_lock_mark(&lock, &mut marks, &mut visited, 0, false);

		// Create the nodes and map.
		let mut nodes = Vec::new();
		let mut map = BTreeMap::new();
		for (index, (mark, node)) in std::iter::zip(&marks, lock.nodes).enumerate() {
			if *mark {
				map.insert(index, nodes.len());
				nodes.push(node);
			}
		}

		// Update indexes and remove unmarked edges.
		for node in &mut nodes {
			match node {
				tg::graph::data::Node::Directory(directory) => {
					directory.entries.retain(|_name, edge| {
						edge.try_unwrap_reference_ref()
							.is_ok_and(|reference| marks[reference.node])
					});
					for edge in directory.entries.values_mut() {
						if let tg::graph::data::Edge::Reference(reference) = edge
							&& reference.graph.is_none()
						{
							let node = map.get(&reference.node).copied().unwrap();
							reference.node = node;
						}
					}
				},
				tg::graph::data::Node::File(file) => {
					file.dependencies.retain(|_name, referent| {
						referent
							.item
							.try_unwrap_reference_ref()
							.is_ok_and(|reference| marks[reference.node])
					});
					for referent in file.dependencies.values_mut() {
						if let tg::graph::data::Edge::Reference(reference) = &mut referent.item
							&& reference.graph.is_none()
						{
							let node = map.get(&reference.node).copied().unwrap();
							reference.node = node;
						}
					}
				},
				tg::graph::data::Node::Symlink(symlink) => {
					if let Some(tg::graph::data::Edge::Reference(reference)) = &mut symlink.artifact
						&& reference.graph.is_none()
					{
						let node = map.get(&reference.node).copied().unwrap();
						reference.node = node;
					}
				},
			}
		}

		tg::graph::Data { nodes }
	}

	fn strip_lock_mark(
		lock: &tg::graph::Data,
		marks: &mut [bool],
		visited: &mut BTreeSet<usize>,
		index: usize,
		tagged: bool,
	) -> bool {
		if !visited.insert(index) {
			return marks[index];
		}
		let node = &lock.nodes[index];
		let mut mark = tagged;
		match node {
			tg::graph::data::Node::Directory(directory) => {
				for edge in directory.entries.values() {
					if let Ok(reference) = edge.try_unwrap_reference_ref() {
						mark = mark
							|| Self::strip_lock_mark(lock, marks, visited, reference.node, false);
					}
				}
			},
			tg::graph::data::Node::File(file) => {
				for referent in file.dependencies.values() {
					if let Ok(reference) = referent.item.try_unwrap_reference_ref() {
						mark = mark
							|| Self::strip_lock_mark(
								lock,
								marks,
								visited,
								reference.node,
								referent.tag().is_some(),
							);
					}
				}
			},
			tg::graph::data::Node::Symlink(symlink) => {
				if let Some(edge) = &symlink.artifact {
					if let Ok(reference) = edge.try_unwrap_reference_ref() {
						mark = mark
							|| Self::strip_lock_mark(lock, marks, visited, reference.node, false);
					}
				}
			},
		}
		marks[index] = mark;
		mark
	}
}
