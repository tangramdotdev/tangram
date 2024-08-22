use super::{
	input::Input,
	unify::{Graph, Id},
};
use crate::Server;
use indoc::formatdoc;
use itertools::Itertools;
use std::{
	collections::{BTreeMap, BTreeSet},
	os::unix::fs::PermissionsExt,
	path::PathBuf,
	sync::{Arc, RwLock},
};
use tangram_client as tg;
use tangram_database::{self as db, Connection as _, Database as _, Query as _, Transaction as _};
use tangram_either::Either;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug)]
pub struct Output {
	pub data: tg::artifact::Data,
	pub lock_index: usize,
	pub dependencies: BTreeMap<tg::Reference, Arc<RwLock<Self>>>,
}

impl Server {
	pub async fn write_output_to_database(&self, output: Arc<RwLock<Output>>) -> tg::Result<()> {
		let mut connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to create a transaction"))?;

		let mut stack = vec![output];
		let mut visited = BTreeSet::new();
		while let Some(output) = stack.pop() {
			if visited.contains(&(Arc::as_ptr(&output) as usize)) {
				continue;
			}
			visited.insert(Arc::as_ptr(&output) as usize);

			// TODO: count/weight.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, complete, touched_at)
					values ({p}1, {p}2, {p}3, {p}4)
					on conflict (id) do update set touched_at = {p}4;
				"
			);
			let data = output.read().unwrap().data.clone();
			let id = data.id()?;
			let bytes = data.serialize()?;
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			let params = db::params![id, bytes, 1, now];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to put the artifact into the database")
				})?;

			stack.extend(output.read().unwrap().dependencies.values().cloned());
		}

		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		Ok(())
	}
}

impl Server {
	pub(super) async fn create_lockfile(
		&self,
		graph: &Graph,
		root: &Id,
	) -> tg::Result<tg::Lockfile> {
		let mut paths = BTreeMap::new();
		let mut output_graph_nodes = Vec::with_capacity(graph.nodes.len());
		let mut visited = BTreeMap::new();
		self.create_lockfile_inner(
			graph,
			root,
			&mut output_graph_nodes,
			&mut visited,
			&mut paths,
		)
		.await?
		.left()
		.ok_or_else(|| tg::error!("expected a the root to have an index"))?;
		let nodes = output_graph_nodes.into_iter().map(Option::unwrap).collect();
		Ok(tg::Lockfile { paths, nodes })
	}

	async fn create_lockfile_inner(
		&self,
		graph: &Graph,
		id: &Id,
		output_graph_nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut BTreeMap<Id, Either<usize, tg::object::Id>>,
		paths: &mut BTreeMap<tg::Path, usize>,
	) -> tg::Result<Either<usize, tg::object::Id>> {
		// Check if we've visited this node already.
		if let Some(either) = visited.get(id) {
			return Ok(either.clone());
		}

		// Get the graph node.
		let graph_node = graph.nodes.get(id).unwrap();

		// If this is an object inline it.
		let input = match &graph_node.object {
			Either::Left(input) => input.clone(),
			Either::Right(object) => {
				let lockfile_id = Either::Right(object.clone());
				visited.insert(id.clone(), lockfile_id.clone());
				return Ok(lockfile_id);
			},
		};

		// Get the artifact kind.
		let kind = match &graph_node.object {
			// If this is an artifact on disk,
			Either::Left(object) => {
				let input = object.read().unwrap();
				if input.metadata.is_dir() {
					tg::artifact::Kind::Directory
				} else if input.metadata.is_file() {
					tg::artifact::Kind::File
				} else {
					tg::artifact::Kind::Symlink
				}
			},

			// If this is an object without any dependencies, inline it.
			Either::Right(object) if graph_node.outgoing.is_empty() => {
				let either = Either::Right(object.clone());
				visited.insert(id.clone(), either.clone());
				return Ok(either);
			},

			// Otherwise, create a node.
			Either::Right(tg::object::Id::Directory(_)) => tg::artifact::Kind::Directory,
			Either::Right(tg::object::Id::File(_)) => tg::artifact::Kind::File,
			Either::Right(_) => return Err(tg::error!("invalid input graph")),
		};

		// Get the index of this node.
		let index = output_graph_nodes.len();
		output_graph_nodes.push(None);

		// Update the visited table.
		visited.insert(id.clone(), Either::Left(index));

		// Update the paths table.
		paths.insert(input.read().unwrap().arg.path.clone(), index);

		// Recurse over dependencies.
		let mut dependencies = BTreeMap::new();
		for (reference, input_graph_id) in &graph_node.outgoing {
			let output_graph_id = Box::pin(self.create_lockfile_inner(
				graph,
				input_graph_id,
				output_graph_nodes,
				visited,
				paths,
			))
			.await?;
			dependencies.insert(reference.clone(), output_graph_id);
		}

		// Create the node.
		let output_node = match kind {
			tg::artifact::Kind::Directory => {
				let entries = dependencies
					.into_iter()
					.map(|(reference, id)| {
						let name = reference
							.path()
							.try_unwrap_path_ref()
							.map_err(|_| tg::error!("invalid input graph"))?
							.components()
							.last()
							.ok_or_else(|| tg::error!("invalid input graph"))?
							.try_unwrap_normal_ref()
							.map_err(|_| tg::error!("invalid input graph"))?
							.clone();
						let id = match id {
							Either::Left(id) => Either::Left(id),
							Either::Right(tg::object::Id::Directory(id)) => {
								Either::Right(id.into())
							},
							Either::Right(tg::object::Id::File(id)) => Either::Right(id.into()),
							Either::Right(tg::object::Id::Symlink(id)) => Either::Right(id.into()),
							Either::Right(_) => return Err(tg::error!("invalid input graph")),
						};
						Ok::<_, tg::Error>((name, Some(id)))
					})
					.try_collect()?;
				tg::lockfile::Node::Directory { entries }
			},
			tg::artifact::Kind::File => {
				let input = input.read().unwrap().clone();
				self.get_file_data_from_input(input, dependencies).await?
			},
			tg::artifact::Kind::Symlink => {
				let input = input.read().unwrap().clone();
				self.get_symlink_data_from_input(input).await?
			},
		};

		// Update the node and return the index.
		output_graph_nodes[index].replace(output_node);
		Ok(Either::Left(index))
	}

	async fn get_file_data_from_input(
		&self,
		input: Input,
		dependencies: BTreeMap<tg::Reference, Either<usize, tg::object::Id>>,
	) -> tg::Result<tg::lockfile::Node> {
		let super::input::Input { arg, metadata, .. } = input;

		// Create the blob.
		let file = tokio::fs::File::open(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read file"))?;
		let output = self
			.create_blob(file)
			.await
			.map_err(|source| tg::error!(!source, "failed to create blob"))?;
		let dependencies = dependencies
			.into_iter()
			.map(|(k, v)| (k, Some(v)))
			.collect::<BTreeMap<_, _>>();

		// Create the data.
		Ok(tg::lockfile::Node::File {
			contents: Some(output.blob),
			dependencies: (!dependencies.is_empty()).then_some(dependencies),
			executable: metadata.permissions().mode() & 0o111 != 0,
		})
	}

	async fn get_symlink_data_from_input(&self, input: Input) -> tg::Result<tg::lockfile::Node> {
		let Input { arg, .. } = input;
		let path = arg.path;

		// Read the target from the symlink.
		let target = tokio::fs::read_link(&path).await.map_err(
			|source| tg::error!(!source, %path, r#"failed to read the symlink at path"#,),
		)?;

		// Unrender the target.
		let target = target
			.to_str()
			.ok_or_else(|| tg::error!("the symlink target must be valid UTF-8"))?;
		let artifacts_path = self.artifacts_path();
		let artifacts_path = artifacts_path
			.to_str()
			.ok_or_else(|| tg::error!("the artifacts path must be valid UTF-8"))?;
		let target = tg::template::Data::unrender(artifacts_path, target)?;

		// Get the artifact and path.
		let (artifact, path) = if target.components.len() == 1 {
			let path = target.components[0]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(None, Some(path))
		} else if target.components.len() == 2 {
			let artifact = target.components[0]
				.try_unwrap_artifact_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = target.components[1]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid sylink"))?
				.clone();
			let path = &path[1..];
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(Some(Some(Either::Right(artifact.into()))), Some(path))
		} else {
			return Err(tg::error!("invalid symlink"));
		};
		Ok(tg::lockfile::Node::Symlink { artifact, path })
	}
}

// Output
impl Server {
	pub async fn collect_output(
		&self,
		input: Arc<RwLock<Input>>,
		lockfile: &tg::Lockfile,
	) -> tg::Result<Arc<RwLock<Output>>> {
		let artifacts = self.create_artifacts_from_lockfile(lockfile).await?;
		let mut visited = BTreeMap::new();
		self.collect_output_inner(input, &artifacts, lockfile, &mut visited)
			.await
	}

	async fn collect_output_inner(
		&self,
		input: Arc<RwLock<Input>>,
		artifacts: &BTreeMap<usize, tg::artifact::Data>,
		lockfile: &tg::Lockfile,
		visited: &mut BTreeMap<tg::Path, Arc<RwLock<Output>>>,
	) -> tg::Result<Arc<RwLock<Output>>> {
		let path = input.read().unwrap().arg.path.clone();
		if let Some(output) = visited.get(&path) {
			return Ok(output.clone());
		}

		// Find the entry in the lockfile.
		let lock_index = *lockfile
			.paths
			.get(&path)
			.ok_or_else(|| tg::error!("missing path in lockfile"))?;

		// Get the data.
		let data = artifacts
			.get(&lock_index)
			.ok_or_else(|| tg::error!("missing artifact data"))?
			.clone();

		// Get the artifact ID.
		let id = data.id()?;

		// Copy or move the file.
		if input.read().unwrap().is_root {
			self.copy_or_move_to_checkouts_directory(input.clone(), id.clone())
				.await?;
		}

		// Create the output
		let output = Arc::new(RwLock::new(Output {
			data,
			lock_index,
			dependencies: BTreeMap::new(),
		}));
		visited.insert(path.clone(), output.clone());

		// Recurse.
		let mut output_dependencies = BTreeMap::new();
		let input_dependencies = input.read().unwrap().dependencies.clone();
		for (reference, input) in input_dependencies {
			let input = input.ok_or_else(|| tg::error!("invalid input graph"))?;
			let output =
				Box::pin(self.collect_output_inner(input, artifacts, lockfile, visited)).await?;
			output_dependencies.insert(reference, output);
		}
		output.write().unwrap().dependencies = output_dependencies;
		Ok(output)
	}
}

impl Server {
	pub async fn create_artifacts_from_lockfile(
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
			tg::Graph::with_object(Arc::new(graph.clone().try_into()?))
				.store(self)
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
						let dependencies = if let Some(dependencies) = &file.dependencies {
							let dependencies = match dependencies {
								Either::Left(dependencies) => Either::Left(
									dependencies
										.iter()
										.map(|either| either.clone().unwrap_right())
										.collect(),
								),
								Either::Right(dependencies) => Either::Right(
									dependencies
										.iter()
										.map(|(reference, either)| {
											(reference.clone(), either.clone().unwrap_right())
										})
										.collect(),
								),
							};
							Some(dependencies)
						} else {
							None
						};
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

	pub async fn create_file_node(
		&self,
		contents: Option<tg::blob::Id>,
		dependencies: Option<BTreeMap<tg::Reference, tg::lockfile::Entry>>,
		executable: bool,
		graphs: &[tg::graph::Data],
		indices: &BTreeMap<usize, (usize, usize)>,
	) -> tg::Result<tg::graph::data::node::File> {
		let dependencies = if let Some(dependencies) = dependencies {
			let mut dependencies_ = BTreeMap::new();
			for (reference, entry) in dependencies {
				let entry = entry.ok_or_else(|| tg::error!("incomplete lockfile"))?;
				let entry = self
					.resolve_lockfile_dependency(entry, graphs, indices)
					.await?;
				dependencies_.insert(reference, entry);
			}
			Some(Either::Right(dependencies_))
		} else {
			None
		};
		Ok(tg::graph::data::node::File {
			contents: contents.ok_or_else(|| tg::error!("incomplete lockfile"))?,
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

		let graph_id = graphs[graph_index].id()?;
		let object_id = match &graphs[graph_index].nodes[node_index].kind() {
			tg::artifact::Kind::Directory => tg::directory::Data::Graph {
				graph: graph_id,
				node: node_index,
			}
			.id()?
			.into(),
			tg::artifact::Kind::File => tg::file::Data::Graph {
				graph: graph_id,
				node: node_index,
			}
			.id()?
			.into(),
			tg::artifact::Kind::Symlink => tg::symlink::Data::Graph {
				graph: graph_id,
				node: node_index,
			}
			.id()?
			.into(),
		};

		Ok(Either::Right(object_id))
	}
}

impl Server {
	pub async fn write_hardlinks_and_xattrs(
		&self,
		input: Arc<RwLock<Input>>,
		output: Arc<RwLock<Output>>,
	) -> tg::Result<()> {
		let path = self
			.checkouts_path()
			.join(output.read().unwrap().data.id()?.to_string())
			.try_into()?;
		let mut visited = BTreeSet::new();
		self.write_hardlinks_and_xattrs_inner(&path, input, output, &mut visited)
			.await?;
		Ok(())
	}

	async fn write_hardlinks_and_xattrs_inner(
		&self,
		path: &tg::Path,
		input: Arc<RwLock<Input>>,
		output: Arc<RwLock<Output>>,
		visited: &mut BTreeSet<tg::Path>,
	) -> tg::Result<()> {
		if visited.contains(path) {
			return Ok(());
		};
		visited.insert(path.clone());
		let data = output.read().unwrap().data.clone();
		let id = data.id()?;
		if let tg::artifact::Data::File(tg::file::Data::Normal {
			contents,
			dependencies,
			..
		}) = data
		{
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();

			let dst: tg::Path = self.checkouts_path().join(id.to_string()).try_into()?;

			if let Some(_dependencies) = dependencies {
				todo!(); // write xattr
			}

			// Create hard link to the file.
			match tokio::fs::hard_link(path, &dst).await {
				Ok(()) => (),
				Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
				Err(source) => {
					return Err(tg::error!(!source, %src = path, %dst, "failed to create hardlink"))
				},
			}

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
		}

		let dependencies = input.read().unwrap().dependencies.clone();
		for (reference, child_input) in dependencies
			.iter()
			.filter_map(|(reference, input)| Some((reference, input.as_ref()?)))
		{
			let child_output = output
				.read()
				.unwrap()
				.dependencies
				.get(reference)
				.ok_or_else(
					|| tg::error!(%referrer = path, %reference, "missing output reference"),
				)?
				.clone();

			let path = if child_input.read().unwrap().is_root {
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

				if input.read().unwrap().metadata.is_dir() {
					path.clone().join(path_)
				} else {
					path.clone().parent().join(path_).normalize()
				}
			};

			Box::pin(self.write_hardlinks_and_xattrs_inner(
				&path,
				child_input.clone(),
				child_output,
				visited,
			))
			.await?;
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
			tg::lockfile::Node::File { dependencies, .. } => {
				Box::new(dependencies.iter().flat_map(|entries| {
					entries
						.values()
						.filter_map(|entry| entry.as_ref()?.as_ref().left().copied())
				}))
			},
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
		input: Arc<RwLock<Input>>,
		artifact: tg::artifact::Id,
	) -> tg::Result<()> {
		let dest = self
			.checkouts_path()
			.join(artifact.to_string())
			.try_into()?;
		let mut visited = BTreeSet::new();
		self.copy_or_move_to_checkouts_directory_inner(dest, input, &mut visited)
			.await
	}

	async fn copy_or_move_to_checkouts_directory_inner(
		&self,
		dest: tg::Path,
		input: Arc<RwLock<Input>>,
		visited: &mut BTreeSet<tg::Path>,
	) -> tg::Result<()> {
		let input = input.read().unwrap().clone();
		if visited.contains(&input.arg.path) {
			return Ok(());
		}
		visited.insert(input.arg.path.clone());

		if input.metadata.is_dir() {
			tokio::fs::create_dir_all(&dest)
				.await
				.map_err(|source| tg::error!(!source, "failed to create directory"))?;
			let children = input.dependencies.values().cloned();
			for input_ in children {
				let Some(input_) = input_ else {
					continue;
				};
				if input_.read().unwrap().is_root {
					continue;
				}
				let diff = input_
					.read()
					.unwrap()
					.arg
					.path
					.diff(&input.arg.path)
					.unwrap();
				let dest = dest.clone().join(diff).normalize();
				Box::pin(self.copy_or_move_to_checkouts_directory_inner(dest, input_, visited))
					.await?;
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
				|source| tg::error!(!source, %path = input.arg.path, "failed to copy file"),
			)?;
		} else {
			return Err(tg::error!(%path = input.arg.path, "invalid file type"));
		}
		Ok(())
	}
}
