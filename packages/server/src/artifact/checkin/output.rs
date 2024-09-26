use super::{input, ProgressState};
use crate::{tmp::Tmp, Server};
use futures::{future, stream::FuturesUnordered, StreamExt, TryStreamExt as _};
use indoc::formatdoc;
use itertools::Itertools;
use num::ToPrimitive;
use std::{
	collections::{BTreeMap, BTreeSet},
	os::unix::fs::{MetadataExt as _, PermissionsExt as _},
	path::{Path, PathBuf},
	sync::{Arc, RwLock, Weak},
};
use tangram_client as tg;
use tangram_client::path::Ext as _;
use tangram_database::{self as db, Connection as _, Database as _, Query as _, Transaction as _};
use tangram_either::Either;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug)]
pub struct Graph {
	pub input: Arc<tokio::sync::RwLock<input::Graph>>,
	pub id: tg::artifact::Id,
	pub data: tg::artifact::Data,
	pub lock_index: usize,
	pub weight: usize,
	pub dependencies: BTreeMap<tg::Reference, Edge>,
}

#[derive(Clone, Debug)]
pub struct Edge(Either<Arc<RwLock<Graph>>, Weak<RwLock<Graph>>>);

struct State {
	root: PathBuf,
	artifacts: BTreeMap<usize, tg::artifact::Data>,
	visited: BTreeMap<PathBuf, Weak<RwLock<Graph>>>,
	lockfile: tg::Lockfile,
}

impl Server {
	pub async fn compute_count_and_weight(
		&self,
		output: Arc<RwLock<Graph>>,
		lockfile: &tg::Lockfile,
	) -> tg::Result<BTreeMap<tg::artifact::Id, (usize, usize)>> {
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
			stack.extend(output.read().unwrap().dependencies.values().map(Edge::node));
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

		// Get the output in reverse-topological order.
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

			stack.extend(output.read().unwrap().dependencies.values().map(Edge::node));
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		Ok(())
	}

	pub(super) async fn write_data_to_database(&self, data: tg::artifact::Data) -> tg::Result<()> {
		// Get a database connection/transaction.
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, complete, touched_at)
				values ({p}1, {p}2, {p}3, {p}4)
				on conflict (id) do update set touched_at = {p}4;
			"
		);

		let id = data.id()?;
		let bytes = data.serialize()?;
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![id, bytes, 0, now];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to put the artifact into the database")
			})?;

		Ok(())
	}
}

// Output.
impl Server {
	pub(super) async fn collect_output(
		&self,
		input: Arc<tokio::sync::RwLock<input::Graph>>,
		lockfile: tg::Lockfile,
		progress: &super::ProgressState,
	) -> tg::Result<Arc<RwLock<Graph>>> {
		let root = input.read().await.arg.path.clone();
		let artifacts = self.create_artifact_data_for_lockfile(&lockfile).await?;
		let mut state = State {
			root,
			artifacts,
			lockfile,
			visited: BTreeMap::new(),
		};
		let output = self
			.collect_output_inner(input, &mut state, progress)
			.await?
			.0
			.unwrap_left();
		Ok(output)
	}

	async fn collect_output_inner(
		&self,
		input: Arc<tokio::sync::RwLock<input::Graph>>,
		state: &mut State,
		progress: &super::ProgressState,
	) -> tg::Result<Edge> {
		let path = input.read().await.arg.path.clone();
		let path = path.diff(&state.root).unwrap();
		if let Some(output) = state.visited.get(&path) {
			let edge = Edge(Either::Right(output.clone()));
			return Ok(edge);
		}

		// Find the entry in the lockfile.
		let lock_index = *state
			.lockfile
			.paths
			.get(&path)
			.and_then(|nodes| nodes.first())
			.ok_or_else(|| tg::error!(%path = path.display(), "missing path in lockfile"))?;

		// Get the data.
		let data = state
			.artifacts
			.get(&lock_index)
			.ok_or_else(|| tg::error!("missing artifact data"))?
			.clone();

		// Compute the ID.
		let id = data.id()?;

		// Update the total bytes that will be copied.
		if input.read().await.metadata.is_file() {
			let size = input.read().await.metadata.size().to_u64().unwrap();
			progress.update_output_total(size);
		}

		// Create the output.
		let output = Arc::new(RwLock::new(Graph {
			input: input.clone(),
			id: id.clone(),
			data,
			weight: 0,
			lock_index,
			dependencies: BTreeMap::new(),
		}));
		state.visited.insert(path.clone(), Arc::downgrade(&output));

		// Recurse.
		let mut output_dependencies = BTreeMap::new();
		let input_dependencies = input
			.read()
			.await
			.edges
			.iter()
			.filter_map(|edge| {
				let child = edge.node()?;
				Some((edge.reference.clone(), child))
			})
			.collect::<Vec<_>>();
		for (reference, input) in input_dependencies {
			let output = Box::pin(self.collect_output_inner(input, state, progress)).await?;
			output_dependencies.insert(reference, output);
		}

		output.write().unwrap().dependencies = output_dependencies;
		Ok(Edge(Either::Left(output)))
	}
}

impl Server {
	pub(crate) async fn create_artifact_data_for_lockfile(
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

	async fn create_directory_node(
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

	async fn create_symlink_node(
		&self,
		artifact: Option<tg::lockfile::Entry>,
		path: Option<PathBuf>,
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
		let path = path
			.map(|path| {
				path.to_str()
					.map(ToOwned::to_owned)
					.ok_or_else(|| tg::error!(%path = path.display(), "invalid path"))
			})
			.transpose()?;
		Ok(tg::graph::data::node::Symlink { artifact, path })
	}

	async fn resolve_lockfile_dependency(
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
			.join(output.read().unwrap().id.to_string());
		let mut visited = BTreeSet::new();
		self.write_links_inner(&path, output, &mut visited).await?;
		Ok(())
	}

	async fn write_links_inner(
		&self,
		path: &PathBuf,
		output: Arc<RwLock<Graph>>,
		visited: &mut BTreeSet<PathBuf>,
	) -> tg::Result<()> {
		if visited.contains(path) {
			return Ok(());
		};
		visited.insert(path.clone());

		let (data, id) = {
			let output = output.read().unwrap();
			(output.data.clone(), output.id.clone())
		};

		// macOS disables hardlinking for some files within .app dirs, such as *.app/Contents/{PkgInfo,Resources/\*.lproj,_CodeSignature} and .DS_Store. Perform a copy instead in these cases. See <https://github.com/NixOS/nix/blob/95f2b2beabc1ab0447bb4683516598265d71b695/src/libstore/optimise-store.cc#L100>.
		let hardlink_prohibited = if cfg!(target_os = "macos") {
			static APP_DIR_RE: std::sync::LazyLock<regex::Regex> =
				std::sync::LazyLock::new(|| regex::Regex::new(r"\.app/Contents/.+$").unwrap());
			let path_string = path.normalize().display().to_string();
			APP_DIR_RE.is_match(&path_string)
		} else {
			false
		};

		// If this is a file, we need to create a hardlink at checkouts/<file id> and create a symlink for its contents in blobs/<content id> -> ../checkouts/<file id>.
		if let tg::artifact::Data::File(file) = data {
			// Get a file descriptor permit.
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();

			// Get the destination of the hardlink.
			let dst = self.checkouts_path().join(id.to_string());

			// Create hard link to the file or copy as needed.
			let result = if hardlink_prohibited {
				tokio::fs::copy(path, &dst).await.map(|_| ())
			} else {
				tokio::fs::hard_link(path, &dst).await
			};
			match result {
				Ok(()) => (),
				Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
				Err(source) => {
					let src = path.display();
					let dst = dst.display();
					let error = if hardlink_prohibited {
						tg::error!(!source, %src, %dst, "failed to copy file")
					} else {
						tg::error!(!source, %src, %dst, "failed to create hardlink")
					};
					return Err(error);
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
			let child_output = child_output.node();
			let input = child_output.read().unwrap().input.clone();
			let path = if input.read().await.is_root {
				self.checkouts_path()
					.join(child_output.read().unwrap().data.id()?.to_string())
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
					path.join(path_)
				} else {
					path.parent().unwrap().join(path_)
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

			let input = output.read().unwrap().input.clone();
			if input.read().await.is_root {
				// Create a temp.
				let temp = Tmp::new(self);

				// Copy or move to the temp.
				let mut visited = BTreeSet::new();
				self.copy_or_move_all(temp.path.clone(), output.clone(), &mut visited, progress)
					.await?;

				// Update the xattrs and file permissions.
				self.update_xattrs_and_permissions(temp.path.clone(), output.clone())
					.await?;

				// Reset the file times to epoch.
				self.set_file_times_to_epoch(&temp.path, true).await?;

				// Rename to the checkouts directory.
				let artifact = output.read().unwrap().data.id()?;
				let dest = self.checkouts_path().join(artifact.to_string());

				match tokio::fs::rename(&temp.path, &dest).await {
					Ok(()) => (),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
					Err(error) if error.raw_os_error() == Some(libc::ENOTEMPTY) => (),
					Err(source) => return Err(tg::error!(!source, "failed to rename")),
				}

				// Update hardlinks and xattrs.
				self.write_links(output.clone()).await?;

				// Reset the top-level object times to epoch post-rename.
				self.set_file_times_to_epoch(dest, false).await?;
			}

			// Recurse.
			stack.extend(output.read().unwrap().dependencies.values().map(Edge::node));
		}
		Ok(())
	}

	async fn copy_or_move_all(
		&self,
		dest: PathBuf,
		output: Arc<RwLock<Graph>>,
		visited: &mut BTreeSet<PathBuf>,
		progress: &ProgressState,
	) -> tg::Result<()> {
		let input = output.read().unwrap().input.clone();
		let input = input.read().await.clone();
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
				let output = output.node();
				let input_ = output.read().unwrap().input.clone();
				if input_.read().await.is_root {
					continue;
				}
				let diff = input_.read().await.arg.path.diff(&input.arg.path).unwrap();
				let dest = dest.join(diff).normalize();
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
							tg::error!(!source, %path = input.arg.path.display(), "failed to rename file"),
						)
					},
				}
			}
			tokio::fs::copy(&input.arg.path, &dest).await.map_err(
				|source| tg::error!(!source, %path = input.arg.path.display(), %dest = dest.display(), "failed to copy file"),
			)?;
		} else {
			return Err(tg::error!(%path = input.arg.path.display(), "invalid file type"));
		}

		// Send a new progress report.
		if input.metadata.is_file() {
			progress.report_output_progress(input.metadata.size().to_u64().unwrap());
		}
		Ok(())
	}

	async fn set_file_times_to_epoch(
		&self,
		dest: impl AsRef<Path>,
		recursive: bool,
	) -> tg::Result<()> {
		let dest = dest.as_ref();
		tokio::task::spawn_blocking({
			let dest = dest.to_path_buf();
			move || {
				set_file_times_to_epoch_inner(&dest, recursive)?;
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.map_err(|error| {
			tg::error!(
				source = error,
				"failed to set file times to epoch for {:?}",
				dest
			)
		})?
	}

	async fn update_xattrs_and_permissions(
		&self,
		dest: PathBuf,
		output: Arc<RwLock<Graph>>,
	) -> tg::Result<()> {
		let mut visited = BTreeSet::new();
		self.update_xattrs_and_permissions_inner(dest, &output, &mut visited)
			.await
	}

	async fn update_xattrs_and_permissions_inner(
		&self,
		dest: PathBuf,
		output: &Arc<RwLock<Graph>>,
		visited: &mut BTreeSet<PathBuf>,
	) -> tg::Result<()> {
		if visited.contains(&dest) {
			return Ok(());
		}
		visited.insert(dest.clone());

		// If this is a file, write xattrs.
		let data = output.read().unwrap().data.clone();
		match &data {
			tg::artifact::Data::File(file) => {
				let executable = match &file {
					tg::file::Data::Normal { executable, .. } => *executable,
					tg::file::Data::Graph { graph, node } => {
						tg::Graph::with_id(graph.clone()).object(self).await?.nodes[*node]
							.try_unwrap_file_ref()
							.map_err(|_| tg::error!("expected a file"))?
							.executable
					},
				};

				let permissions = if executable { 0o0755 } else { 0o0644 };
				tokio::fs::set_permissions(&dest, std::fs::Permissions::from_mode(permissions))
					.await
					.map_err(|source| tg::error!(!source, "failed to set file permissions"))?;

				let json = serde_json::to_vec(&file)
					.map_err(|source| tg::error!(!source, "failed to serialize file data"))?;
				xattr::set(&dest, tg::file::XATTR_NAME, &json).map_err(
					|source| tg::error!(!source, %path = dest.display(), "failed to write file data as an xattr"),
				)?;
			},
			tg::artifact::Data::Directory(_) => {
				let permissions = tokio::fs::metadata(&dest)
					.await
					.unwrap()
					.permissions()
					.mode();
				let mode = format!("{permissions:o}");
				tokio::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o0755))
					.await
					.map_err(
						|source| tg::error!(!source, %path = dest.display(), %mode, "failed to set directory permissions"),
					)?;
			},
			tg::artifact::Data::Symlink(_) => (),
		}

		// Recurse over path dependencies.
		let dependencies = output.read().unwrap().dependencies.clone();
		let dependencies = dependencies
			.into_iter()
			.map(|(reference, child)| async move {
				let child = child.node();
				let input = child.read().unwrap().input.clone();
				if input.read().await.is_root {
					return None;
				}
				let path = reference
					.path()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| reference.query()?.path.as_ref())?;
				Some((path.clone(), child.clone()))
			})
			.collect::<FuturesUnordered<_>>()
			.filter_map(future::ready)
			.collect::<Vec<_>>()
			.await;
		for (subpath, output_) in dependencies {
			let dest_ = if matches!(&data, tg::artifact::Data::File(_)) {
				dest.parent().unwrap().join(&subpath).normalize()
			} else {
				dest.join(subpath.clone())
			};
			Box::pin(self.update_xattrs_and_permissions_inner(dest_, &output_, visited)).await?;
		}

		Ok(())
	}
}

fn set_file_times_to_epoch_inner(path: &Path, recursive: bool) -> tg::Result<()> {
	let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
	if recursive && path.is_dir() {
		for entry in std::fs::read_dir(path)
			.map_err(|error| tg::error!(source = error, "could not read dir"))?
		{
			let entry =
				entry.map_err(|error| tg::error!(source = error, "could not read entry"))?;
			set_file_times_to_epoch_inner(&entry.path(), recursive)?;
		}
	}

	filetime::set_symlink_file_times(path, epoch, epoch).map_err(|source| {
		tg::error!(
			source = source,
			"failed to set the modified time for {:?}",
			path
		)
	})?;

	Ok(())
}

impl Edge {
	pub fn node(&self) -> Arc<RwLock<Graph>> {
		match &self.0 {
			Either::Left(strong) => strong.clone(),
			Either::Right(weak) => weak.upgrade().unwrap(),
		}
	}
}
