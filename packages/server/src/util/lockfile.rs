use crate::Server;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt,
};
use std::{
	collections::{BTreeMap, BTreeSet, VecDeque},
	path::{Path, PathBuf},
	sync::{Arc, RwLock},
};
use tangram_client as tg;
use tangram_either::Either;

pub async fn try_get_lockfile_node_for_module_path(
	path: &Path,
) -> tg::Result<Option<(tg::Lockfile, usize)>> {
	if !tg::package::is_module_path(path) || !path.is_absolute() {
		return Err(tg::error!(%path = path.display(), "expected an absolute module path"));
	}

	let ancestors = path
		.parent()
		.ok_or_else(|| tg::error!(%path = path.display(), "expected a non-root absolute path"))?
		.ancestors()
		.skip(1);

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
		let node = &lockfile.nodes[node_index];
		match node {
			tg::lockfile::Node::Directory { entries } => {
				let children = entries.iter().filter_map(|(name, either)| {
					let index = *either.as_ref()?.as_ref().left()?;
					let path = node_path.join(name);
					Some((index, path))
				});
				queue.extend(children);
			},
			tg::lockfile::Node::File { dependencies, .. } => {
				let children = dependencies
					.iter()
					.filter_map(|(reference, dependency)| {
						if dependency.tag.is_some() {
							return None;
						}
						let path = reference
							.path()
							.try_unwrap_path_ref()
							.ok()
							.or_else(|| reference.query()?.path.as_ref())?;
						let path = node_path.join(path);
						let index = *dependency.object.as_ref()?.as_ref().left()?;
						let follow = reference.query().and_then(|q| q.follow).unwrap_or(false);
						Some((index, path, follow))
					})
					.map(|(index, path, follow)| async move {
						let path = if follow {
							read_link(&path).await?
						} else {
							path
						};
						Ok::<_, tg::Error>((index, path))
					})
					.collect::<FuturesOrdered<_>>()
					.try_collect::<Vec<_>>()
					.await?;
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

pub async fn create_artifact_for_lockfile_node(
	server: &Server,
	lockfile: &tg::Lockfile,
	node: usize,
) -> tg::Result<tg::Artifact> {
	// Strip any unused nodes from the lockfile, which ensures that it is complete.
	let lockfile = filter_lockfile(lockfile, node).await?;

	// Create artifacts for all the nodes of the lockfile.
	let artifacts = server.create_artifact_data_for_lockfile(&lockfile).await?;

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

async fn filter_lockfile(lockfile: &tg::Lockfile, node: usize) -> tg::Result<tg::Lockfile> {
	let visited = RwLock::new(BTreeMap::new());
	let nodes = RwLock::new(Vec::new());
	filter_lockfile_inner(lockfile, node, &visited, &nodes).await?;
	Ok(tg::Lockfile {
		paths: BTreeMap::new(),
		nodes: nodes.into_inner().unwrap(),
	})
}

async fn filter_lockfile_inner(
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
		tg::lockfile::Node::Symlink { path, .. } => tg::lockfile::Node::Symlink {
			artifact: None,
			path: path.clone(),
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
						let Some(entry) = entry else {
							return Err(tg::error!("incomplete lockfile"));
						};
						let entry = match entry {
							Either::Left(index) => Either::Left(
								Box::pin(filter_lockfile_inner(lockfile, index, visited, nodes))
									.await?,
							),
							Either::Right(id) => Either::Right(id.clone()),
						};
						Ok::<_, tg::Error>((name, Some(entry)))
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
				.map(|(reference, dependency)| {
					let reference = reference.clone();
					let entry = dependency.object.clone();
					async move {
						let Some(entry) = entry else {
							return Err(tg::error!("incomplete lockfile"));
						};
						let entry = match entry {
							Either::Left(index) => Either::Left(
								Box::pin(filter_lockfile_inner(lockfile, index, visited, nodes))
									.await?,
							),
							Either::Right(id) => Either::Right(id.clone()),
						};

						let dependency = tg::lockfile::Dependency {
							object: Some(entry),
							tag: dependency.tag.clone(),
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
			let entry = entry
				.as_ref()
				.ok_or_else(|| tg::error!("incomplete lockfile"))?;
			let entry = match entry {
				Either::Left(index) => Either::Left(
					Box::pin(filter_lockfile_inner(lockfile, *index, visited, nodes)).await?,
				),
				Either::Right(id) => Either::Right(id.clone()),
			};
			let mut nodes = nodes.write().unwrap();
			let tg::lockfile::Node::Symlink { artifact, .. } = &mut nodes[index] else {
				unreachable!()
			};
			artifact.replace(Some(entry));
		},
		tg::lockfile::Node::Symlink { .. } => (),
	}

	Ok(index)
}
