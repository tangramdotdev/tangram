use super::{input, ProgressState};
use crate::{tmp::Tmp, Server};
use futures::{stream::FuturesUnordered, TryStreamExt};
use indoc::formatdoc;
use itertools::Itertools;
use num::ToPrimitive;
use std::{
	collections::{BTreeMap, BTreeSet},
	os::unix::fs::MetadataExt,
	path::PathBuf,
	sync::{Arc, RwLock},
};
use tangram_client as tg;
use tangram_database::{self as db, Connection as _, Database as _, Query as _, Transaction as _};
use tangram_either::Either;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug)]
pub struct Graph {
	pub input: Arc<RwLock<input::Graph>>,
	pub id: tg::artifact::Id,
	pub data: tg::artifact::Data,
	pub lock_index: usize,
	pub weight: usize,
	pub dependencies: BTreeMap<tg::Reference, Arc<RwLock<Self>>>,
}

struct State {
	root: tg::Path,
	artifacts: BTreeMap<usize, tg::artifact::Data>,
	visited: BTreeMap<tg::Path, Arc<RwLock<Graph>>>,
	lockfile: tg::Lockfile,
}

impl Server {
	pub async fn compute_count_and_weight(
		&self,
		output: Arc<RwLock<Graph>>,
		lockfile: &tg::Lockfile,
	) -> tg::Result<BTreeMap<tg::artifact::Id, (usize, usize)>> {
		// Get the output in reverse-topological order. TODO: is this really necessary?
		let mut stack = vec![output];
		let mut visited = BTreeSet::new();
		let mut order = Vec::new();
		while let Some(output) = stack.pop() {
			let id = output.read().unwrap().data.id()?;
			if visited.contains(&id) {
				continue;
			}
			visited.insert(id);
			order.push(output.clone());
			stack.extend(output.read().unwrap().dependencies.values().cloned());
		}
		order.reverse();

		// Compute the count and weight.
		let mut count_and_weight = BTreeMap::new();
		let mut visited = BTreeMap::new();
		'outer: for output in &order {
			let node = output.read().unwrap().lock_index;
			let id = output.read().unwrap().data.id()?;
			let mut count = 1usize;
			let mut weight = output.read().unwrap().weight;
			for child in lockfile.nodes[node].children() {
				match child {
					Either::Left(node) => {
						let Some((c, w)) = visited.get(&node) else {
							continue 'outer;
						};
						count += *c;
						weight += *w;
					},
					Either::Right(_) => {
						continue 'outer;
					},
				}
			}
			visited.insert(node, (count, weight));
			count_and_weight.insert(id, (count, weight));
		}

		Ok(count_and_weight)
	}

	pub async fn write_output_to_database(
		&self,
		output: Arc<RwLock<Graph>>,
		lockfile: &tg::Lockfile,
	) -> tg::Result<()> {
		// Compute count and weight.
		let count_and_weight = self
			.compute_count_and_weight(output.clone(), lockfile)
			.await?;

		// Get a database connection/transaction.
		let mut connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to create a transaction"))?;

		// Get the output in reverse-topological order. TODO: is this really necessary?
		let mut stack = vec![output];
		let mut visited = BTreeSet::new();
		while let Some(output) = stack.pop() {
			// Check if we've visited this node yet.
			let data = output.read().unwrap().data.clone();
			let id = data.id()?;
			if visited.contains(&id) {
				continue;
			}
			visited.insert(id.clone());

			// Write to the database.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, count, weight, complete, touched_at)
					values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6)
					on conflict (id) do update set touched_at = {p}4;
				"
			);
			let (count, weight, complete) = if let Some((count, weight)) = count_and_weight.get(&id)
			{
				(Some(*count), Some(*weight), 1)
			} else {
				(None, None, 0)
			};
			let bytes = data.serialize()?;
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			let params = db::params![id, bytes, count, weight, complete, now];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to put the artifact into the database")
				})?;

			stack.extend(output.read().unwrap().dependencies.values().cloned());
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		Ok(())
	}
}

// Output
impl Server {
	pub(super) async fn collect_output(
		&self,
		input: Arc<RwLock<input::Graph>>,
		lockfile: tg::Lockfile,
		progress: &super::ProgressState,
	) -> tg::Result<Arc<RwLock<Graph>>> {
		let root = input.read().unwrap().arg.path.clone();
		let artifacts = self.create_artifact_data(&lockfile).await?;
		let mut state = State {
			root,
			artifacts,
			lockfile,
			visited: BTreeMap::new(),
		};
		self.collect_output_inner(input, &mut state, progress).await
	}

	async fn collect_output_inner(
		&self,
		input: Arc<RwLock<input::Graph>>,
		state: &mut State,
		progress: &super::ProgressState,
	) -> tg::Result<Arc<RwLock<Graph>>> {
		let path = input.read().unwrap().arg.path.clone();
		let path = path.diff(&state.root).unwrap();
		if let Some(output) = state.visited.get(&path) {
			return Ok(output.clone());
		}

		// Find the entry in the lockfile.
		let lock_index = *state
			.lockfile
			.paths
			.get(&path)
			.ok_or_else(|| tg::error!(%path, "missing path in lockfile"))?;

		// Get the data.
		let data = state
			.artifacts
			.get(&lock_index)
			.ok_or_else(|| tg::error!("missing artifact data"))?
			.clone();

		// Compute the ID.
		let id = data.id()?;

		// Update the total bytes that will be copied.
		if input.read().unwrap().metadata.is_file() {
			let size = input.read().unwrap().metadata.size().to_u64().unwrap();
			progress.update_output_total(size);
		}

		// Create the output
		let output = Arc::new(RwLock::new(Graph {
			input: input.clone(),
			id,
			data,
			weight: 0,
			lock_index,
			dependencies: BTreeMap::new(),
		}));
		state.visited.insert(path.clone(), output.clone());

		// Recurse.
		let mut output_dependencies = BTreeMap::new();
		let input_dependencies = input
			.read()
			.unwrap()
			.edges
			.iter()
			.filter_map(|edge| {
				let child = edge.graph.clone()?;
				Some((edge.reference.clone(), child))
			})
			.collect::<Vec<_>>();
		for (reference, input) in input_dependencies {
			let output = Box::pin(self.collect_output_inner(input, state, progress)).await?;
			output_dependencies.insert(reference, output);
		}

		output.write().unwrap().dependencies = output_dependencies;
		Ok(output)
	}
}

impl Server {
	pub async fn create_artifact_data(
		&self,
		lockfile: &tg::lockfile::Lockfile,
	) -> tg::Result<BTreeMap<usize, tg::artifact::Data>> {
		let mut graphs = Vec::new();
		let mut indices = BTreeMap::new();

		for (graph_index, scc) in petgraph::algo::tarjan_scc(GraphImpl(lockfile))
			.into_iter()
			.enumerate()
		{
			let mut nodes = Vec::with_capacity(scc.len());

			// Create new indices for each node.
			for (node_index, lockfile_index) in scc.iter().copied().enumerate() {
				indices.insert(lockfile_index, (graph_index, node_index));
			}

			// Convert nodes.
			for lockfile_index in scc {
				match &lockfile.nodes[lockfile_index] {
					tg::lockfile::Node::Directory { entries } => {
						let node = self
							.create_directory_node(entries.clone(), &graphs, &indices)
							.await?;
						nodes.push(tg::graph::data::Node::Directory(node));
					},
					tg::lockfile::Node::File {
						contents,
						dependencies,
						executable,
						..
					} => {
						let node = self
							.create_file_node(
								contents.clone(),
								dependencies.clone(),
								*executable,
								&graphs,
								&indices,
							)
							.await?;
						nodes.push(tg::graph::data::Node::File(node));
					},
					tg::lockfile::Node::Symlink { artifact, path } => {
						let node = self
							.create_symlink_node(artifact.clone(), path.clone(), &graphs, &indices)
							.await?;
						nodes.push(tg::graph::data::Node::Symlink(node));
					},
				}
			}

			// Create the graph.
			let graph = tg::graph::data::Data { nodes };

			// Store the graph.
			self.put_object(
				&graph.id()?.into(),
				tg::object::put::Arg {
					bytes: graph.serialize()?,
				},
			)
			.await?;

			graphs.push(graph);
		}

		// Create artifact data.
		let mut artifacts = BTreeMap::new();
		for (lockfile_index, (graph_index, node_index)) in indices {
			let graph = &graphs[graph_index];
			let graph_id = graph.id()?;
			let data = match &graph.nodes[node_index] {
				tg::graph::data::Node::Directory(directory) => {
					if graph.nodes.len() == 1 {
						let entries = directory
							.entries
							.iter()
							.map(|(name, either)| (name.clone(), either.clone().unwrap_right()))
							.collect();
						tg::directory::Data::Normal { entries }.into()
					} else {
						tg::directory::Data::Graph {
							graph: graph_id,
							node: node_index,
						}
						.into()
					}
				},
				tg::graph::data::Node::File(file) => {
					if graph.nodes.len() == 1 {
						let dependencies = file
							.dependencies
							.iter()
							.map(|(reference, dependency)| {
								let object = match &dependency.object {
									Either::Left(_node) => {
										return Err(tg::error!("invalid graph"));
									},
									Either::Right(object) => object.clone(),
								};
								let dependency = tg::file::data::Dependency {
									object,
									tag: dependency.tag.clone(),
								};
								Ok::<_, tg::Error>((reference.clone(), dependency))
							})
							.try_collect()?;
						tg::file::Data::Normal {
							contents: file.contents.clone(),
							dependencies,
							executable: file.executable,
						}
						.into()
					} else {
						tg::file::Data::Graph {
							graph: graph_id,
							node: node_index,
						}
						.into()
					}
				},
				tg::graph::data::Node::Symlink(symlink) => {
					if graph.nodes.len() == 1 {
						let artifact = symlink.artifact.clone().map(Either::unwrap_right);
						tg::symlink::Data::Normal {
							artifact,
							path: symlink.path.clone(),
						}
						.into()
					} else {
						tg::symlink::Data::Graph {
							graph: graph_id,
							node: node_index,
						}
						.into()
					}
				},
			};
			artifacts.insert(lockfile_index, data);
		}

		Ok(artifacts)
	}

	pub async fn create_directory_node(
		&self,
		entries: BTreeMap<String, tg::lockfile::Entry>,
		graphs: &[tg::graph::Data],
		indices: &BTreeMap<usize, (usize, usize)>,
	) -> tg::Result<tg::graph::data::node::Directory> {
		let mut entries_ = BTreeMap::new();
		for (name, entry) in entries {
			let entry = entry.ok_or_else(|| tg::error!("incomplete lockfile"))?;
			let entry = self
				.resolve_lockfile_dependency(entry, graphs, indices)
				.await?
				.map_right(|object| match object {
					tg::object::Id::Directory(id) => id.into(),
					tg::object::Id::File(id) => id.into(),
					tg::object::Id::Symlink(id) => id.into(),
					_ => unreachable!(),
				});
			entries_.insert(name, entry);
		}
		Ok(tg::graph::data::node::Directory { entries: entries_ })
	}

	async fn create_file_node(
		&self,
		contents: Option<tg::blob::Id>,
		dependencies: BTreeMap<tg::Reference, tg::lockfile::Dependency>,
		executable: bool,
		graphs: &[tg::graph::Data],
		indices: &BTreeMap<usize, (usize, usize)>,
	) -> tg::Result<tg::graph::data::node::File> {
		let contents = contents.ok_or_else(|| tg::error!("incomplete lockfile"))?;
		let dependencies = dependencies
			.into_iter()
			.map(|(reference, dependency)| async move {
				let object = dependency
					.object
					.clone()
					.ok_or_else(|| tg::error!("incomplete lockfile"))?;
				let object = self
					.resolve_lockfile_dependency(object, graphs, indices)
					.await?;
				let dependency = tg::graph::data::node::Dependency {
					object,
					tag: dependency.tag,
				};
				Ok::<_, tg::Error>((reference, dependency))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(tg::graph::data::node::File {
			contents,
			dependencies,
			executable,
		})
	}

	pub async fn create_symlink_node(
		&self,
		artifact: Option<tg::lockfile::Entry>,
		path: Option<tg::Path>,
		graphs: &[tg::graph::Data],
		indices: &BTreeMap<usize, (usize, usize)>,
	) -> tg::Result<tg::graph::data::node::Symlink> {
		let artifact = if let Some(artifact) = artifact {
			let artifact = artifact.ok_or_else(|| tg::error!("incomplete lockfile"))?;
			let artifact = self
				.resolve_lockfile_dependency(artifact, graphs, indices)
				.await?
				.map_right(|object| match object {
					tg::object::Id::Directory(id) => id.into(),
					tg::object::Id::File(id) => id.into(),
					tg::object::Id::Symlink(id) => id.into(),
					_ => unreachable!(),
				});
			Some(artifact)
		} else {
			None
		};
		Ok(tg::graph::data::node::Symlink { artifact, path })
	}

	pub async fn resolve_lockfile_dependency(
		&self,
		dependency: Either<usize, tg::object::Id>,
		graphs: &[tg::graph::Data],
		indices: &BTreeMap<usize, (usize, usize)>,
	) -> tg::Result<Either<usize, tg::object::Id>> {
		let Either::Left(lockfile_index) = dependency else {
			return Ok(dependency);
		};
		let (graph_index, node_index) = indices.get(&lockfile_index).copied().unwrap();
		if graph_index == graphs.len() {
			return Ok(Either::Left(node_index));
		}

		let graph = &graphs[graph_index];
		if graph.nodes.len() == 1 {
			let id = match &graph.nodes[node_index] {
				tg::graph::data::Node::Directory(node) => tg::directory::Data::Normal {
					entries: node
						.entries
						.clone()
						.into_iter()
						.map(|(name, either)| {
							let object = either
								.right()
								.ok_or_else(|| tg::error!("expected an object id"))?;
							Ok::<_, tg::Error>((name, object))
						})
						.try_collect()?,
				}
				.id()?
				.into(),
				tg::graph::data::Node::File(node) => {
					let dependencies = node
						.dependencies
						.iter()
						.map(|(reference, dependency)| {
							let dependency = tg::file::data::Dependency {
								object: dependency
									.object
									.clone()
									.right()
									.ok_or_else(|| tg::error!("expected an object id"))?,
								tag: dependency.tag.clone(),
							};
							Ok::<_, tg::Error>((reference.clone(), dependency))
						})
						.try_collect()?;
					tg::file::Data::Normal {
						contents: node.contents.clone(),
						dependencies,
						executable: node.executable,
					}
					.id()?
					.into()
				},
				tg::graph::data::Node::Symlink(node) => tg::symlink::Data::Normal {
					artifact: node
						.artifact
						.clone()
						.map(|object| {
							object
								.right()
								.ok_or_else(|| tg::error!("expected an artifact ID"))
						})
						.transpose()?,
					path: node.path.clone(),
				}
				.id()?
				.into(),
			};
			return Ok(Either::Right(id));
		}

		let object_id = match &graph.nodes[node_index].kind() {
			tg::artifact::Kind::Directory => tg::directory::Data::Graph {
				graph: graph.id()?,
				node: node_index,
			}
			.id()?
			.into(),
			tg::artifact::Kind::File => tg::file::Data::Graph {
				graph: graph.id()?,
				node: node_index,
			}
			.id()?
			.into(),
			tg::artifact::Kind::Symlink => tg::symlink::Data::Graph {
				graph: graph.id()?,
				node: node_index,
			}
			.id()?
			.into(),
		};

		Ok(Either::Right(object_id))
	}
}

impl Server {
	pub async fn write_links(&self, output: Arc<RwLock<Graph>>) -> tg::Result<()> {
		let path = self
			.checkouts_path()
			.join(output.read().unwrap().id.to_string())
			.try_into()?;
		let mut visited = BTreeSet::new();
		self.write_links_inner(&path, output, &mut visited).await?;
		Ok(())
	}

	async fn write_links_inner(
		&self,
		path: &tg::Path,
		output: Arc<RwLock<Graph>>,
		visited: &mut BTreeSet<tg::Path>,
	) -> tg::Result<()> {
		if visited.contains(path) {
			return Ok(());
		};
		visited.insert(path.clone());
		let (data, id) = {
			let output = output.read().unwrap();
			(output.data.clone(), output.id.clone())
		};

		// If this is a file, we need to create a hardlink at checkouts/<file id> and create a symlink for its contents in blobs/<content id> -> ../checkouts/<file id>
		if let tg::artifact::Data::File(file) = data {
			// Get a file descriptor permit.
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();

			// Get the destination of the hardlink.
			let dst: tg::Path = self.checkouts_path().join(id.to_string()).try_into()?;

			// Create hard link to the file.
			match tokio::fs::hard_link(path, &dst).await {
				Ok(()) => (),
				Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
				Err(source) => {
					return Err(tg::error!(!source, %src = path, %dst, "failed to create hardlink"))
				},
			}

			// Get the contents' blob ID.
			let contents = match file {
				// If this is a normal file we have no work to do.
				tg::file::Data::Normal { contents, .. } => contents,

				// If the file is a pointer into a graph, we need to fetch the graph and convert its data.
				tg::file::Data::Graph { graph, node } => {
					let graph = tg::Graph::with_id(graph).data(self).await?;
					let tg::graph::data::Node::File(file) = &graph.nodes[node] else {
						return Err(tg::error!("expected a file"));
					};
					file.contents.clone()
				},
			};

			// Create a symlink to the file in the blobs directory.
			let symlink_target = PathBuf::from("../checkouts").join(id.to_string());
			let symlink_path = self.blobs_path().join(contents.to_string());
			match tokio::fs::symlink(&symlink_target, &symlink_path).await {
				Ok(()) => (),
				Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
				Err(source) => {
					return Err(
						tg::error!(!source, %src = symlink_target.display(), %dst = symlink_path.display(), "failed to create blob symlink"),
					)
				},
			}

			// Since every file is a child of a directory we do not need to recurse over file dependencies and can bail early.
			return Ok(());
		}

		// Recurse.
		let dependencies = output.read().unwrap().dependencies.clone();
		for (reference, child_output) in dependencies {
			let input = child_output.read().unwrap().input.clone();
			let path = if input.read().unwrap().is_root {
				self.checkouts_path()
					.join(child_output.read().unwrap().data.id()?.to_string())
					.try_into()?
			} else {
				let path_ = reference
					.path()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| reference.query()?.path.as_ref())
					.cloned()
					.ok_or_else(|| tg::error!("expected a path dependency"))?;
				// We could use metadata from the input, or the data, this avoids having to acquire a lock.
				if matches!(id, tg::artifact::Id::Directory(_)) {
					path.clone().join(path_)
				} else {
					path.clone().parent().join(path_).normalize()
				}
			};

			Box::pin(self.write_links_inner(&path, child_output, visited)).await?;
		}

		Ok(())
	}
}

#[derive(Copy, Clone)]
struct GraphImpl<'a>(&'a tg::Lockfile);

impl<'a> petgraph::visit::GraphBase for GraphImpl<'a> {
	type EdgeId = (usize, usize);
	type NodeId = usize;
}

impl<'a> petgraph::visit::GraphRef for GraphImpl<'a> {}

#[allow(clippy::needless_arbitrary_self_type)]
impl<'a> petgraph::visit::NodeIndexable for GraphImpl<'a> {
	fn from_index(self: &Self, i: usize) -> Self::NodeId {
		i
	}

	fn node_bound(self: &Self) -> usize {
		self.0.nodes.len()
	}

	fn to_index(self: &Self, a: Self::NodeId) -> usize {
		a
	}
}

impl<'a> petgraph::visit::IntoNeighbors for GraphImpl<'a> {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;
	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		match &self.0.nodes[a] {
			tg::lockfile::Node::Directory { entries } => Box::new(
				entries
					.values()
					.filter_map(|entry| entry.as_ref()?.as_ref().left().copied()),
			),
			tg::lockfile::Node::File { dependencies, .. } => Box::new(
				dependencies
					.values()
					.filter_map(|dependency| dependency.object.as_ref()?.as_ref().left().copied()),
			),
			tg::lockfile::Node::Symlink { artifact, .. } => Box::new(
				artifact
					.iter()
					.filter_map(|entry| entry.as_ref()?.as_ref().left().copied()),
			),
		}
	}
}

impl<'a> petgraph::visit::IntoNodeIdentifiers for GraphImpl<'a> {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.0.nodes.len()
	}
}

impl Server {
	pub(super) async fn copy_or_move_to_checkouts_directory(
		&self,
		output: Arc<RwLock<Graph>>,
		progress: &super::ProgressState,
	) -> tg::Result<()> {
		let mut visited = BTreeSet::new();
		let mut stack = vec![output];
		while let Some(output) = stack.pop() {
			let id = output.read().unwrap().id.clone();
			if visited.contains(&id) {
				continue;
			}
			visited.insert(id);

			if output.read().unwrap().input.read().unwrap().is_root {
				// Create a temp.
				let temp = Tmp::new(self);
				let temp: tg::Path = temp.path.clone().try_into()?;

				// Copy or move to the temp.
				let mut visited = BTreeSet::new();
				self.copy_or_move_all(temp.clone(), output.clone(), &mut visited, progress)
					.await?;

				// Write xattrs.
				self.write_xattrs(temp.clone(), output.clone()).await?;

				// Rename to the checkouts directory.
				let artifact = output.read().unwrap().data.id()?;
				let dest = self.checkouts_path().join(artifact.to_string());
				match tokio::fs::rename(&temp, &dest).await {
					Ok(()) => (),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
					Err(error) if error.raw_os_error() == Some(libc::ENOTEMPTY) => (),
					Err(source) => return Err(tg::error!(!source, "failed to rename")),
				}
			}

			// Recurse.
			stack.extend(output.read().unwrap().dependencies.values().cloned());
		}
		Ok(())
	}

	async fn copy_or_move_all(
		&self,
		dest: tg::Path,
		output: Arc<RwLock<Graph>>,
		visited: &mut BTreeSet<tg::Path>,
		progress: &ProgressState,
	) -> tg::Result<()> {
		let input = output.read().unwrap().input.read().unwrap().clone();
		if visited.contains(&input.arg.path) {
			return Ok(());
		}
		visited.insert(input.arg.path.clone());
		if input.metadata.is_dir() {
			if input.arg.destructive {
				match tokio::fs::rename(&input.arg.path, &dest).await {
					Ok(()) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::ENOTEMPTY) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::ENODEV) => (),
					Err(source) => return Err(tg::error!(!source, "failed to rename directory")),
				}
			}

			tokio::fs::create_dir_all(&dest)
				.await
				.map_err(|source| tg::error!(!source, "failed to create directory"))?;
			let dependencies = output.read().unwrap().dependencies.clone();
			for (_, output) in dependencies {
				if output.read().unwrap().input.read().unwrap().is_root {
					continue;
				}
				let diff = output
					.read()
					.unwrap()
					.input
					.read()
					.unwrap()
					.arg
					.path
					.diff(&input.arg.path)
					.unwrap();
				let dest = dest.clone().join(diff).normalize();
				Box::pin(self.copy_or_move_all(dest, output, visited, progress)).await?;
			}
		} else if input.metadata.is_symlink() {
			let target = tokio::fs::read_link(&input.arg.path)
				.await
				.map_err(|source| tg::error!(!source, "expected a symlink"))?;
			tokio::fs::symlink(&target, &dest)
				.await
				.map_err(|source| tg::error!(!source, "failed to create symlink"))?;
		} else if input.metadata.is_file() {
			if input.arg.destructive {
				match tokio::fs::rename(&input.arg.path, &dest).await {
					Ok(()) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::ENODEV) => (),
					Err(source) => {
						return Err(
							tg::error!(!source, %path = input.arg.path, "failed to rename file"),
						)
					},
				}
			}
			tokio::fs::copy(&input.arg.path, &dest).await.map_err(
				|source| tg::error!(!source, %path = input.arg.path, %dest, "failed to copy file"),
			)?;
		} else {
			return Err(tg::error!(%path = input.arg.path, "invalid file type"));
		}

		// Send a new progress report.
		if input.metadata.is_file() {
			progress.report_output_progress(input.metadata.size().to_u64().unwrap());
		}
		Ok(())
	}

	async fn write_xattrs(&self, dest: tg::Path, output: Arc<RwLock<Graph>>) -> tg::Result<()> {
		let mut visited = BTreeSet::new();
		self.write_xattrs_inner(dest, &output, &mut visited).await
	}

	async fn write_xattrs_inner(
		&self,
		dest: tg::Path,
		output: &Arc<RwLock<Graph>>,
		visited: &mut BTreeSet<tg::Path>,
	) -> tg::Result<()> {
		if visited.contains(&dest) {
			return Ok(());
		}
		visited.insert(dest.clone());

		// If this is a file, write xattrs.
		let data = output.read().unwrap().data.clone();
		if let tg::artifact::Data::File(file) = data {
			// Get the dependencies.
			let dependencies = match file {
				// If this is a normal file we have no work to do.
				tg::file::Data::Normal { dependencies, .. } => dependencies,

				// If the file is a pointer into a graph, we need to fetch the graph and convert its data.
				tg::file::Data::Graph { graph, node } => {
					let graph = tg::Graph::with_id(graph).data(self).await?;
					let tg::graph::data::Node::File(file) = &graph.nodes[node] else {
						return Err(tg::error!("expected a file"));
					};
					file.dependencies
						.iter()
						.map(|(reference, dependency)| {
							let object = match &dependency.object {
								Either::Left(node) => match graph.id_of_node(*node)? {
									tg::artifact::Id::Directory(id) => id.into(),
									tg::artifact::Id::File(id) => id.into(),
									tg::artifact::Id::Symlink(id) => id.into(),
								},
								Either::Right(id) => id.clone(),
							};
							let dependency = tg::file::data::Dependency {
								object,
								tag: dependency.tag.clone(),
							};
							Ok::<_, tg::Error>((reference.clone(), dependency))
						})
						.try_collect()?
				},
			};

			// Serialize the dependencies and write the xattr.
			if dependencies.is_empty() {
				return Ok(());
			}

			let json = serde_json::to_vec(&dependencies)
				.map_err(|source| tg::error!(!source, "failed to serialize dependencies"))?;
			xattr::set(&dest, tg::file::XATTR_NAME, &json).map_err(
				|source| tg::error!(!source, %path = dest, "failed to write dependencies as an xattr"),
			)?;

			// Exit early. Since every file is the child of a directory and all directories are walked, we don't need to recurse over file dependencies.
			return Ok(());
		}

		// Recurse over path dependencies.
		let dependencies = output
			.read()
			.unwrap()
			.dependencies
			.iter()
			.filter_map(|(reference, child)| {
				let path = reference
					.path()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| reference.query()?.path.as_ref())?;
				Some((path.clone(), child.clone()))
			})
			.collect::<Vec<_>>();

		for (subpath, output) in dependencies {
			let dest = dest.clone().join(subpath);
			Box::pin(self.write_xattrs(dest, output)).await?;
		}

		Ok(())
	}
}
