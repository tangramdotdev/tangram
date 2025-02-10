use crate::Server;
use itertools::Itertools;
use std::{
	collections::BTreeMap,
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_either::Either;

/// Represents a lockfile that's been parsed, containing all of its objects and paths to artifacts that it references.
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct ParsedLockfile {
	nodes: Vec<tg::lockfile::Node>,
	paths: Vec<Option<PathBuf>>,
	path: PathBuf,
}

#[derive(Clone)]
struct FindInLockfileArg<'a> {
	current_node_path: PathBuf,
	current_node: usize,
	current_package_node: usize,
	current_package_path: PathBuf,
	nodes: &'a [tg::lockfile::Node],
	search: Either<usize, &'a Path>,
}

pub(crate) struct LockfileNode {
	pub node: usize,
	pub package: PathBuf,
	pub path: PathBuf,
}

impl Server {
	pub(crate) async fn find_node_in_lockfile(
		&self,
		search: Either<usize, &Path>,
		lockfile_path: &Path,
		lockfile: &tg::Lockfile,
	) -> tg::Result<LockfileNode> {
		self.find_node_in_lockfile_nodes(search, lockfile_path, &lockfile.nodes)
			.await
	}

	pub(crate) async fn find_node_in_lockfile_nodes(
		&self,
		search: Either<usize, &Path>,
		lockfile_path: &Path,
		nodes: &[tg::lockfile::Node],
	) -> tg::Result<LockfileNode> {
		let current_package_path = lockfile_path.parent().unwrap().to_owned();
		let current_package_node = 0;
		if nodes.is_empty() {
			return Err(tg::error!("invalid lockfile"));
		}
		let mut visited = vec![false; nodes.len()];

		let arg = FindInLockfileArg {
			current_node_path: current_package_path.clone(),
			current_node: current_package_node,
			current_package_path,
			current_package_node,
			nodes,
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

		match &arg.nodes[arg.current_node] {
			tg::lockfile::Node::Directory(tg::lockfile::Directory { entries, .. }) => {
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

			tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) => {
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

					let current_package_node = dependency_package_node;
					let current_package_path = path.clone();

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

			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target { .. }) => {
				return Ok(None);
			},

			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
				artifact,
				subpath,
				..
			}) => {
				// Get the referent artifact.
				let Either::Left(artifact) = artifact else {
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
	let subpath = subpath.strip_prefix("./").unwrap_or(subpath.as_ref());
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

pub type ResolvedDependency = Option<Either<PathBuf, tg::object::Id>>;
impl ParsedLockfile {
	pub fn try_resolve_dependency(
		&self,
		node_path: &Path,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::Referent<ResolvedDependency>>> {
		let node = self.get_node_for_path(node_path)?;

		// Dependency resolution is only valid for files.
		let tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) = &self.nodes[node]
		else {
			return Err(tg::error!(%path = node_path.display(), "expected a file node"))?;
		};

		// Lookup the dependency.
		let Some(referent) = dependencies.get(reference) else {
			return Ok(None);
		};

		// Resolve the item.
		let item = match &referent.item {
			Either::Left(index) => {
				if let Some(path) = self.paths[*index].clone() {
					Some(Either::Left(path))
				} else if referent.tag.is_some() {
					None
				} else {
					return Ok(None);
				}
			},
			Either::Right(object) => Some(Either::Right(object.clone())),
		};

		// Construct the referent.
		let referent = tg::Referent {
			item,
			tag: referent.tag.clone(),
			path: referent.path.clone(),
			subpath: referent.subpath.clone(),
		};

		Ok(Some(referent))
	}

	pub fn get_path_for_node(&self, node: usize) -> tg::Result<PathBuf> {
		self.paths[node]
			.clone()
			.ok_or_else(|| tg::error!("expected a node path"))
	}

	pub fn get_node_for_path(&self, node_path: &Path) -> tg::Result<usize> {
		// Linear search, which should be faster than looking in a btreemap. Replace with a btreemap if this is too slow.
		self
			.paths
			.iter()
			.position(|path| path.as_deref() == Some(node_path))
			.ok_or_else(|| tg::error!(%path = node_path.display(), %lockfile = self.path.display(), "failed to find in lockfile"))
	}

	pub fn get_file_dependencies(
		&self,
		path: &Path,
	) -> tg::Result<Vec<(tg::Reference, tg::Referent<ResolvedDependency>)>> {
		// Find the node index of this path.
		let node = self.get_node_for_path(path)?;

		// Get the corresponding file node.
		let tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) = &self.nodes[node]
		else {
			return Err(tg::error!(%path = path.display(), "expected a file"));
		};

		// Resolve the dependencies.
		let mut dependencies_ = Vec::with_capacity(dependencies.len());
		for (reference, referent) in dependencies {
			// Resolve the item.
			let item = match &referent.item {
				Either::Left(index) => self.paths[*index].clone().map(Either::Left),
				Either::Right(object) => Some(Either::Right(object.clone())),
			};
			let referent = tg::Referent {
				item,
				path: referent.path.clone(),
				subpath: referent.subpath.clone(),
				tag: referent.tag.clone(),
			};
			dependencies_.push((reference.clone(), referent));
		}

		Ok(dependencies_)
	}
}

impl Server {
	pub async fn try_parse_lockfile(&self, path: &Path) -> tg::Result<Option<ParsedLockfile>> {
		// First try and read the lockfile from the file's xattrs.
		let contents_and_root = 'a: {
			// Read the lockfile's xattrs.
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let Ok(Some(contents)) = xattr::get(path, tg::file::XATTR_LOCK_NAME) else {
				break 'a None;
			};
			Some((contents, path))
		};

		// If not available in the xattrs, try and read the file.
		let contents_and_root = 'a: {
			if let Some(contents) = contents_and_root {
				break 'a Some(contents);
			}

			// If this is not a lockfile path, break.
			if path.file_name().and_then(|name| name.to_str())
				!= Some(tg::package::LOCKFILE_FILE_NAME)
			{
				break 'a None;
			}

			// Read the lockfile from disk.
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let contents = tokio::fs::read(path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to read lockfile"),
			)?;

			Some((contents, path.parent().unwrap()))
		};

		let Some((contents, root)) = contents_and_root else {
			return Ok(None);
		};

		// Deserialize the lockfile.
		let lockfile = serde_json::from_slice::<tg::Lockfile>(&contents).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to deserialize lockfile"),
		)?;

		// Get any referenced objects that are not contained within the lockfile.
		let mut object_ids: Vec<tg::object::Id> = Vec::new();
		for node in &lockfile.nodes {
			match node {
				tg::lockfile::Node::Directory(tg::lockfile::Directory { entries, .. }) => {
					let it = entries
						.values()
						.filter_map(|value| value.as_ref().right().cloned());
					object_ids.extend(it);
				},
				tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) => {
					let it =
						dependencies
							.values()
							.filter_map(|referent| match referent.item.clone() {
								Either::Right(tg::object::Id::Directory(id)) => Some(id.into()),
								Either::Right(tg::object::Id::File(id)) => Some(id.into()),
								Either::Right(tg::object::Id::Symlink(id)) => Some(id.into()),
								_ => None,
							});
					object_ids.extend(it);
				},
				tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
					artifact, ..
				}) => {
					let it = artifact.as_ref().right().cloned();
					object_ids.extend(it);
				},
				tg::lockfile::Node::Symlink(_) => (),
			}
		}
		let artifact_ids: Vec<tg::artifact::Id> = object_ids
			.into_iter()
			.map(tg::artifact::Id::try_from)
			.try_collect()?;

		// Get the artifacts path.
		let mut artifacts_path = None;
		for path in path.ancestors().skip(1) {
			let path = path.join(".tangram/artifacts");
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			if matches!(tokio::fs::try_exists(&path).await, Ok(true)) {
				artifacts_path.replace(path);
				break;
			}
		}
		let artifacts_path = artifacts_path.unwrap_or_else(|| self.artifacts_path());

		// Find paths for any artifacts.
		let mut artifacts = BTreeMap::new();
		for id in artifact_ids {
			// Check if the file exists.
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let path = artifacts_path.join(id.to_string());
			if matches!(tokio::fs::try_exists(&path).await, Ok(true)) {
				artifacts.insert(id, path);
			}
		}

		// Get the paths for the lockfile nodes.
		let paths = get_paths(&artifacts_path, root, &lockfile).await?;

		// Create the parsed lockfile.
		let lockfile = ParsedLockfile {
			nodes: lockfile.nodes,
			paths,
			path: path.to_owned(),
		};

		Ok(Some(lockfile))
	}
}

// Given a lockfile, get the paths of all the nodes.
async fn get_paths(
	artifacts_path: &Path,
	root_path: &Path,
	lockfile: &tg::Lockfile,
) -> tg::Result<Vec<Option<PathBuf>>> {
	async fn get_paths_inner(
		artifacts_path: &Path,
		lockfile: &tg::Lockfile,
		node_path: &Path,
		node: usize,
		visited: &mut Vec<Option<PathBuf>>,
	) -> tg::Result<()> {
		// Check if the node has been visited.
		if visited[node].is_some() {
			return Ok(());
		}

		// Check if the file system object exists. If it doesn't, leave the node empty.
		if !matches!(tokio::fs::try_exists(node_path).await, Ok(true)) {
			return Ok(());
		}

		// Update the visited set.
		visited[node].replace(node_path.to_owned());

		// Recurse.
		match &lockfile.nodes[node] {
			tg::lockfile::Node::Directory(tg::lockfile::Directory { entries, .. }) => {
				for (name, entry) in entries {
					let Either::Left(index) = entry else {
						continue;
					};
					let node_path = node_path.join(name);
					Box::pin(get_paths_inner(
						artifacts_path,
						lockfile,
						&node_path,
						*index,
						visited,
					))
					.await?;
				}
			},
			tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) => {
				'outer: for (reference, referent) in dependencies {
					let Either::Left(index) = &referent.item else {
						continue;
					};

					// Try and follow a relative path.
					'a: {
						let Some(path) = reference
							.item()
							.try_unwrap_path_ref()
							.ok()
							.or_else(|| reference.options()?.path.as_ref())
						else {
							break 'a;
						};
						let path = node_path.parent().unwrap().join(path);
						if !matches!(tokio::fs::try_exists(&path).await, Ok(true)) {
							break 'a;
						}
						let Ok(node_path) = crate::util::fs::canonicalize_parent(&path).await
						else {
							break 'a;
						};
						if Box::pin(get_paths_inner(
							artifacts_path,
							lockfile,
							&node_path,
							*index,
							visited,
						))
						.await
						.is_ok()
						{
							continue 'outer;
						}
					};

					// Try and get the path to the object.
					let Some(id) = lockfile.nodes[*index].id() else {
						continue 'outer;
					};
					let node_path = artifacts_path.join(id.to_string());
					Box::pin(get_paths_inner(
						artifacts_path,
						lockfile,
						&node_path,
						*index,
						visited,
					))
					.await?;
				}
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
				artifact: Either::Left(index),
				..
			}) => {
				// Try and get the path to the object.
				let object = &lockfile.nodes[*index]
					.id()
					.ok_or_else(|| tg::error!("expected an object ID"))?;
				let node_path = artifacts_path.join(object.to_string());
				Box::pin(get_paths_inner(
					artifacts_path,
					lockfile,
					&node_path,
					*index,
					visited,
				))
				.await?;
			},
			tg::lockfile::Node::Symlink(_) => (),
		}

		Ok(())
	}

	if lockfile.nodes.is_empty() {
		return Ok(Vec::new());
	}

	let mut visited = vec![None; lockfile.nodes.len()];
	let node = 0;
	Box::pin(get_paths_inner(
		artifacts_path,
		lockfile,
		root_path,
		node,
		&mut visited,
	))
	.await?;
	Ok(visited)
}
