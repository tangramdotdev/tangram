use crate::Server;
use futures::{stream::FuturesUnordered, TryStreamExt};
use std::{
	collections::{BTreeMap, BTreeSet, VecDeque},
	path::{Path, PathBuf},
	sync::{Arc, RwLock},
};
use tangram_client as tg;
use tangram_either::Either;

#[derive(Clone)]
struct FindInLockfileArg<'a> {
	current_node_path: PathBuf,
	current_node: usize,
	current_package_node: usize,
	current_package_path: PathBuf,
	lockfile: &'a tg::Lockfile,
	search: Either<usize, &'a Path>,
}

pub(crate) struct LockfileNode {
	pub node: usize,
	pub package: PathBuf,
	pub path: PathBuf,
}

pub async fn try_get_lockfile_node_for_module_path(
	path: &Path,
) -> tg::Result<Option<(tg::Lockfile, usize)>> {
	if !tg::package::is_module_path(path) || !path.is_absolute() {
		return Err(tg::error!(%path = path.display(), "expected an absolute module path"));
	}

	let ancestors = path.ancestors().skip(1);

	for ancestor in ancestors {
		let lockfile_path = ancestor.join(tg::package::LOCKFILE_FILE_NAME);
		let exists = tokio::fs::try_exists(&lockfile_path).await.map_err(|source| tg::error!(!source, %path = lockfile_path.display(), "failed to check if the lockfile exists"))?;
		if exists {
			let lockfile = tg::Lockfile::try_read(&lockfile_path).await?.ok_or_else(
				|| tg::error!(%path = lockfile_path.display(), "failed to read lockfile"),
			)?;
			if let Some(node) =
				try_get_node_for_module_path(path, &lockfile, &lockfile_path).await?
			{
				return Ok(Some((lockfile, node)));
			}
		}
	}
	Ok(None)
}

pub async fn try_get_node_for_module_path(
	path: &Path,
	lockfile: &tg::Lockfile,
	lockfile_path: &Path,
) -> tg::Result<Option<usize>> {
	if lockfile.nodes.is_empty() {
		return Ok(None);
	}

	let root_path = lockfile_path
		.parent()
		.ok_or_else(|| tg::error!("expected a lockfile path"))?
		.to_owned();

	path.strip_prefix(&root_path).map_err(|source| tg::error!(!source, %path = path.display(), %lockfile = lockfile_path.display(), "expected path to be a child of the lockfile's directory"))?;

	let mut queue: VecDeque<_> = vec![(0, root_path)].into();
	let mut visited = BTreeSet::new();

	while let Some((node_index, node_path)) = queue.pop_front() {
		if read_link(&node_path).await? == path {
			return Ok(Some(node_index));
		}
		if visited.contains(&node_index) {
			continue;
		}
		visited.insert(node_index);
		let Some(node) = lockfile.nodes.get(node_index) else {
			continue;
		};
		match node {
			tg::lockfile::Node::Directory { entries } => {
				let children = entries.iter().filter_map(|(name, either)| {
					let index = *either.as_ref().left()?;
					let path = node_path.join(name);
					Some((index, path))
				});
				queue.extend(children);
			},
			tg::lockfile::Node::File { dependencies, .. } => {
				let children = dependencies.iter().filter_map(|(reference, referent)| {
					if referent.tag.is_some() {
						return None;
					}
					let path = reference
						.item()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| reference.options()?.path.as_ref())?;
					let path = node_path.join(path);
					let index = *referent.item.as_ref().left()?;
					Some((index, path))
				});
				queue.extend(children);
			},
			tg::lockfile::Node::Symlink { .. } => continue,
		}
	}

	Ok(None)
}

async fn read_link(path: &Path) -> tg::Result<PathBuf> {
	match tokio::fs::read_link(path).await {
		Ok(path) => Ok(path),
		Err(error) if error.raw_os_error() == Some(libc::EINVAL) => Ok(path.to_owned()),
		Err(source) => Err(tg::error!(!source, %path = path.display(), "failed to readlink")),
	}
}

pub async fn _create_artifact_for_lockfile_node(
	server: &Server,
	lockfile: &tg::Lockfile,
	node: usize,
) -> tg::Result<tg::Artifact> {
	// Strip any unused nodes from the lockfile, which ensures that it is complete.
	let lockfile = _filter_lockfile(lockfile, node).await?;

	// Create artifacts for all the nodes of the lockfile.
	let artifacts = server._create_artifact_data_for_lockfile(&lockfile).await?;

	// Pull out the new root artifact.
	let artifact = artifacts
		.get(&0)
		.ok_or_else(|| tg::error!("invalid lockfile"))?;

	// Convert the data into an object.
	let artifact = match artifact.clone() {
		tg::artifact::Data::Directory(data) => {
			let object: tg::directory::Object = data.try_into()?;
			tg::Directory::with_object(Arc::new(object)).into()
		},
		tg::artifact::Data::File(data) => {
			let object: tg::file::Object = data.try_into()?;
			tg::File::with_object(Arc::new(object)).into()
		},
		tg::artifact::Data::Symlink(data) => {
			let object: tg::symlink::Object = data.try_into()?;
			tg::Symlink::with_object(Arc::new(object)).into()
		},
	};
	Ok(artifact)
}

async fn _filter_lockfile(lockfile: &tg::Lockfile, node: usize) -> tg::Result<tg::Lockfile> {
	let visited = RwLock::new(BTreeMap::new());
	let nodes = RwLock::new(Vec::new());
	_filter_lockfile_inner(lockfile, node, &visited, &nodes).await?;
	Ok(tg::Lockfile {
		nodes: nodes.into_inner().unwrap(),
	})
}

async fn _filter_lockfile_inner(
	lockfile: &tg::Lockfile,
	node: usize,
	visited: &RwLock<BTreeMap<usize, usize>>,
	nodes: &RwLock<Vec<tg::lockfile::Node>>,
) -> tg::Result<usize> {
	if let Some(index) = visited.read().unwrap().get(&node) {
		return Ok(*index);
	};

	let new_node = match &lockfile.nodes[node] {
		tg::lockfile::Node::Directory { .. } => tg::lockfile::Node::Directory {
			entries: BTreeMap::new(),
		},
		tg::lockfile::Node::File {
			contents,
			executable,
			..
		} => tg::lockfile::Node::File {
			contents: contents.clone(),
			dependencies: BTreeMap::new(),
			executable: *executable,
		},
		tg::lockfile::Node::Symlink { subpath, .. } => tg::lockfile::Node::Symlink {
			artifact: None,
			subpath: subpath.clone(),
		},
	};

	let index = {
		let mut nodes = nodes.write().unwrap();
		let index = nodes.len();
		nodes.push(new_node);
		index
	};
	visited.write().unwrap().insert(node, index);

	match &lockfile.nodes[node] {
		tg::lockfile::Node::Directory { entries } => {
			let entries_ = entries
				.iter()
				.map(|(name, entry)| {
					let name = name.clone();
					let entry = entry.clone();
					async move {
						let entry = match entry {
							Either::Left(index) => Either::Left(
								Box::pin(_filter_lockfile_inner(lockfile, index, visited, nodes))
									.await?,
							),
							Either::Right(id) => Either::Right(id.clone()),
						};
						Ok::<_, tg::Error>((name, entry))
					}
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;

			let mut nodes = nodes.write().unwrap();
			let tg::lockfile::Node::Directory { entries, .. } = &mut nodes[index] else {
				unreachable!()
			};
			*entries = entries_;
		},
		tg::lockfile::Node::File { dependencies, .. } => {
			let dependencies_ = dependencies
				.iter()
				.map(|(reference, referent)| {
					let reference = reference.clone();
					let item = referent.item.clone();
					async move {
						let item = match item {
							Either::Left(index) => Either::Left(
								Box::pin(_filter_lockfile_inner(lockfile, index, visited, nodes))
									.await?,
							),
							Either::Right(id) => Either::Right(id.clone()),
						};

						let dependency = tg::Referent {
							item,
							subpath: referent.subpath.clone(),
							tag: referent.tag.clone(),
						};

						Ok::<_, tg::Error>((reference, dependency))
					}
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;
			let mut nodes = nodes.write().unwrap();
			let tg::lockfile::Node::File { dependencies, .. } = &mut nodes[index] else {
				unreachable!()
			};
			*dependencies = dependencies_;
		},
		tg::lockfile::Node::Symlink {
			artifact: Some(entry),
			..
		} => {
			let entry = match entry {
				Either::Left(index) => Either::Left(
					Box::pin(_filter_lockfile_inner(lockfile, *index, visited, nodes)).await?,
				),
				Either::Right(id) => Either::Right(id.clone()),
			};
			let mut nodes = nodes.write().unwrap();
			let tg::lockfile::Node::Symlink { artifact, .. } = &mut nodes[index] else {
				unreachable!()
			};
			artifact.replace(entry);
		},
		tg::lockfile::Node::Symlink { .. } => (),
	}

	Ok(index)
}

impl Server {
	pub(crate) async fn find_node_in_lockfile(
		&self,
		search: Either<usize, &Path>,
		lockfile_path: &Path,
		lockfile: &tg::Lockfile,
	) -> tg::Result<LockfileNode> {
		let current_package_path = lockfile_path.parent().unwrap().to_owned();
		let current_package_node = 0;
		let mut visited = vec![false; lockfile.nodes.len()];

		let arg = FindInLockfileArg {
			current_node_path: current_package_path.clone(),
			current_node: current_package_node,
			current_package_path,
			current_package_node,
			lockfile,
			search,
		};

		self.find_node_in_lockfile_inner(arg.clone(), &mut visited)
			.await?
			.ok_or_else(
				|| tg::error!(%lockfile = lockfile_path.display(), ?search = arg.search, "failed to find node in lockfile"),
			)
	}

	pub(crate) async fn find_node_index_in_lockfile(
		&self,
		path: &Path,
		lockfile_path: &Path,
		lockfile: &tg::Lockfile,
	) -> tg::Result<usize> {
		let result = self
			.find_node_in_lockfile(Either::Right(path), lockfile_path, lockfile)
			.await?;
		Ok(result.node)
	}

	pub(crate) async fn find_path_in_lockfile(
		&self,
		node: usize,
		lockfile_path: &Path,
		lockfile: &tg::Lockfile,
	) -> tg::Result<PathBuf> {
		let result = self
			.find_node_in_lockfile(Either::Left(node), lockfile_path, lockfile)
			.await?;
		Ok(result.path)
	}

	async fn find_node_in_lockfile_inner(
		&self,
		mut arg: FindInLockfileArg<'_>,
		visited: &mut [bool],
	) -> tg::Result<Option<LockfileNode>> {
		// If this is the node we're searching for, return.
		match arg.search {
			Either::Left(node) if node == arg.current_node => {
				let result = LockfileNode {
					node: arg.current_node,
					package: arg.current_package_path,
					path: arg.current_node_path,
				};
				return Ok(Some(result));
			},
			Either::Right(path) if path == arg.current_node_path => {
				let result = LockfileNode {
					node: arg.current_node,
					package: arg.current_package_path,
					path: arg.current_node_path,
				};
				return Ok(Some(result));
			},
			_ => (),
		}

		// Check if this node has been visited and update the visited set.
		if visited[arg.current_node] {
			return Ok(None);
		}
		visited[arg.current_node] = true;

		match &arg.lockfile.nodes[arg.current_node] {
			tg::lockfile::Node::Directory { entries } => {
				// If this is a directory with a root module, update the current package path/node.
				if entries
					.keys()
					.any(|name| tg::package::is_root_module_path(name.as_ref()))
				{
					arg.current_package_path = arg.current_node_path.clone();
					arg.current_package_node = arg.current_node;
				}

				// Recurse over the entries.
				for (name, entry) in entries {
					let current_node_path = arg.current_node_path.join(name);
					let current_node = *entry.as_ref().unwrap_left();

					let arg = FindInLockfileArg {
						current_node_path,
						current_node,
						..arg.clone()
					};
					let result = Box::pin(self.find_node_in_lockfile_inner(arg, visited)).await?;
					if let Some(result) = result {
						return Ok(Some(result));
					}
				}
			},
			tg::lockfile::Node::File { dependencies, .. } => {
				for (reference, dependency) in dependencies {
					// Skip dependencies that are not contained in the lockfile.
					let Some(dependency_package_node) = dependency.item.as_ref().left().copied()
					else {
						continue;
					};

					// Skip dependencies contained within the same package, since the traversal is guaranteed to reach them.
					if dependency_package_node == arg.current_package_node {
						continue;
					}

					// Compute the canonical path of the import.
					let path = reference
						.item()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| reference.options()?.path.as_ref())
						.ok_or_else(|| tg::error!(%reference, "expected a path reference"))?;
					let path = reference
						.options()
						.and_then(|options| options.subpath.as_ref())
						.map_or_else(|| path.to_owned(), |subpath| path.join(subpath));
					let path = arg.current_node_path.parent().unwrap().join(path);
					let path = tokio::fs::canonicalize(&path).await.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the path"),
					)?;

					// Strip the subpath from the canoncial path to get the package path.
					let subpath = dependency.subpath.as_deref().unwrap_or("".as_ref());
					let current_package_node = dependency_package_node;
					let current_package_path = strip_subpath(&path, subpath)?;

					// Recurse on the dependency's package.
					let arg = FindInLockfileArg {
						current_node: current_package_node,
						current_node_path: current_package_path.clone(),
						current_package_node,
						current_package_path,
						..arg.clone()
					};
					let result = Box::pin(self.find_node_in_lockfile_inner(arg, visited)).await?;

					if let Some(path) = result {
						return Ok(Some(path));
					}
				}
			},
			tg::lockfile::Node::Symlink { artifact, subpath } => {
				// Get the referent artifact.
				let Some(Either::Left(artifact)) = artifact else {
					return Ok(None);
				};

				// If the artifact is in the same package, skip it because it will eventually be discovered.
				if *artifact == arg.current_package_node {
					return Ok(None);
				}

				// Compute the canonical path of the target.
				let target = tokio::fs::read_link(&arg.current_node_path).await.map_err(
					|source| tg::error!(!source, %path = arg.current_node_path.display(), "failed to readlink"),
				)?;
				let path = arg.current_node_path.join(target);
				let path = tokio::fs::canonicalize(&path).await.map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the path"),
				)?;

				// Strip the subpath.
				let subpath = subpath.as_deref().unwrap_or("".as_ref());
				let current_package_path = strip_subpath(&path, subpath)?;
				let current_package_node = *artifact;

				// Recurse on the package.
				let arg = FindInLockfileArg {
					current_node: current_package_node,
					current_node_path: current_package_path.clone(),
					current_package_node,
					current_package_path,
					..arg.clone()
				};
				let result = Box::pin(self.find_node_in_lockfile_inner(arg, visited)).await?;
				if let Some(result) = result {
					return Ok(Some(result));
				}
			},
		}

		Ok(None)
	}
}

// Remove a subpath from a base path.
fn strip_subpath(base: &Path, subpath: &Path) -> tg::Result<PathBuf> {
	if subpath.is_absolute() {
		return Err(tg::error!("invalid subpath"));
	}
	if !base.ends_with(subpath) {
		return Err(
			tg::error!(%base = base.display(), %subpath = subpath.display(), "cannot remove subpath from base"),
		);
	}
	let path = base
		.components()
		.take(base.components().count() - subpath.components().count())
		.collect();
	Ok(path)
}
