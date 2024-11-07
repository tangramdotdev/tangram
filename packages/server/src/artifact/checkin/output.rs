use super::{input, object};
use crate::{temp::Temp, util::path::Ext as _, Server};
use futures::{future, stream::FuturesUnordered, FutureExt, StreamExt, TryStreamExt as _};
use indoc::formatdoc;
use itertools::Itertools;
use std::{
	collections::{BTreeMap, BTreeSet},
	ffi::OsStr,
	os::unix::fs::PermissionsExt as _,
	path::{Path, PathBuf},
	sync::RwLock,
};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use time::format_description::well_known::Rfc3339;

#[derive(Debug)]
pub struct Graph {
	pub nodes: Vec<Node>,
}

#[derive(Clone, Debug)]
pub struct Node {
	pub input: usize,
	pub id: tg::artifact::Id,
	pub data: tg::artifact::Data,
	pub metadata: tg::object::Metadata,
	pub edges: Vec<Edge>,
}

#[derive(Clone, Debug)]
pub struct Edge {
	pub reference: tg::Reference,
	pub subpath: Option<PathBuf>,
	pub node: usize,
}

struct State {
	nodes: Vec<Node>,
	visited: Vec<Option<usize>>,
}

impl Server {
	pub(super) async fn create_output_graph(
		&self,
		input: &input::Graph,
		object: &object::Graph,
	) -> tg::Result<Graph> {
		// Create the state.
		let state = RwLock::new(State {
			nodes: Vec::with_capacity(object.nodes.len()),
			visited: vec![None; object.nodes.len()],
		});

		// Recurse over the graph.
		self.create_output_graph_inner(input, object, 0, &state)
			.await?;

		// Create the output.
		let output = Graph {
			nodes: state.into_inner().unwrap().nodes,
		};

		Ok(output)
	}

	async fn create_output_graph_inner(
		&self,
		input: &input::Graph,
		object: &object::Graph,
		object_index: usize,
		state: &RwLock<State>,
	) -> tg::Result<usize> {
		// Check if this output node has been visited.
		if let Some(output_index) = state.read().unwrap().visited[object_index] {
			return Ok(output_index);
		}

		// Get the corresponding input node.
		let input_index = *object.nodes[object_index]
			.unify
			.object
			.as_ref()
			.unwrap_left();

		// Create the output node.
		let output_index = {
			let mut state = state.write().unwrap();
			let output_index = state.nodes.len();
			let node = Node {
				input: input_index,
				id: object.nodes[object_index]
					.id
					.clone()
					.unwrap()
					.try_into()
					.unwrap(),
				data: object.nodes[object_index].data.clone().unwrap(),
				metadata: object.nodes[object_index].metadata.clone().unwrap(),
				edges: Vec::new(),
			};
			state.nodes.push(node);
			state.visited[object_index].replace(output_index);
			output_index
		};

		// Create the output edges.
		let edges = object.nodes[object_index]
			.edges
			.iter()
			.filter_map(|edge| {
				if object.nodes[edge.index].unify.object.is_right() {
					return None;
				}
				let fut = Box::pin(
					self.create_output_graph_inner(input, object, edge.index, state),
				)
				.map(|node| {
					node.map(|node| Edge {
						reference: edge.reference.clone(),
						subpath: edge.subpath.clone(),
						node,
					})
				});
				Some(fut)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Update the output edges.
		state.write().unwrap().nodes[output_index].edges = edges;

		// Return the created node.
		Ok(output_index)
	}
}

impl Server {
	pub async fn write_output_to_database(&self, output: &Graph) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to create a transaction"))?;

		// Get the output in reverse-topological order.
		let mut stack = vec![0];
		let mut visited = vec![false; output.nodes.len()];
		while let Some(output_index) = stack.pop() {
			// Check if we've visited this node yet.
			if visited[output_index] {
				continue;
			}
			visited[output_index] = true;

			// Get the output data.
			let output = &output.nodes[output_index];

			// Write to the database.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, complete, count, depth, weight, touched_at)
					values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7)
					on conflict (id) do update set touched_at = {p}7;
				"
			);
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			let params: Vec<tangram_database::Value> = db::params![
				output.id,
				output.data.serialize()?,
				output.metadata.complete,
				output.metadata.count,
				output.metadata.depth,
				output.metadata.weight,
				now
			];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to put the artifact into the database")
				})?;

			// Insert the object's children into the database.
			output
				.data
				.children()
				.iter()
				.map(|child| {
					let id = output.id.clone();
					let transaction = &transaction;
					async move {
						let statement = formatdoc!(
							"
								insert into object_children (object, child)
								values ({p}1, {p}2)
								on conflict (object, child) do nothing;
							"
						);
						let params = db::params![id, child];
						transaction
							.execute(statement, params)
							.await
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to put the object children into the database"
								)
							})
							.ok()
					}
				})
				.collect::<FuturesUnordered<_>>()
				.collect::<Vec<_>>()
				.await;

			stack.extend(output.edges.iter().map(|edge| edge.node));
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}
}

impl Server {
	/// Write hardlinks in the cache directory.
	pub async fn write_links(
		&self,
		input: &input::Graph,
		output: &Graph,
		node: usize,
	) -> tg::Result<()> {
		let path = self.cache_path().join(output.nodes[node].id.to_string());
		let mut visited = vec![false; output.nodes.len()];
		self.write_links_inner(input, output, node, &path, &mut visited)
			.await?;
		Ok(())
	}

	async fn write_links_inner(
		&self,
		input: &input::Graph,
		output: &Graph,
		node: usize,
		path: &PathBuf,
		visited: &mut [bool],
	) -> tg::Result<()> {
		// Check if we've visited this node.
		if visited[node] {
			return Ok(());
		};
		visited[node] = true;

		// macOS disables hardlinking for some files within .app dirs, such as *.app/Contents/{PkgInfo,Resources/\*.lproj,_CodeSignature} and .DS_Store. Perform a copy instead in these cases. See <https://github.com/NixOS/nix/blob/95f2b2beabc1ab0447bb4683516598265d71b695/src/libstore/optimise-store.cc#L100>.
		let hardlink_prohibited = if cfg!(target_os = "macos") {
			static APP_DIR_RE: std::sync::LazyLock<regex::Regex> =
				std::sync::LazyLock::new(|| regex::Regex::new(r"\.app/Contents/.+$").unwrap());
			APP_DIR_RE.is_match(path.to_string_lossy().as_ref())
		} else {
			false
		};

		// If this is a file, we need to create a hardlink in the checkouts directory and create a symlink for its contents in the blobs directory that points to the corresponding entry in the checkouts directory.
		if let tg::artifact::Data::File(file) = &&output.nodes[node].data {
			// Create hard link to the file or copy as needed.
			let dst = self.cache_path().join(output.nodes[node].id.to_string());
			if hardlink_prohibited {
				let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
				let result = tokio::fs::copy(path, &dst).await.map(|_| ());
				match result {
					Ok(()) => (),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
					Err(source) => {
						let src = path.display();
						let dst = dst.display();
						return Err(tg::error!(!source, %src, %dst, "failed to copy"));
					},
				}
			} else {
				let result = tokio::fs::hard_link(path, &dst).await;
				match result {
					Ok(()) => (),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
					Err(source) => {
						let src = path.display();
						let dst = dst.display();
						return Err(tg::error!(!source, %src, %dst, "failed to hardlink"));
					},
				}
			};

			// Get the contents' ID.
			let contents = match file {
				// If this is a normal file we have no work to do.
				tg::file::Data::Normal { contents, .. } => contents.clone(),

				// If the file is a pointer into a graph, we need to fetch the graph and convert its data.
				tg::file::Data::Graph { graph, node } => {
					let graph = tg::Graph::with_id(graph.clone()).data(self).await?;
					let tg::graph::data::Node::File(file) = &graph.nodes[*node] else {
						return Err(tg::error!("expected a file"));
					};
					file.contents.clone()
				},
			};

			// Create a symlink to the file in the blobs directory.
			let dst = self.blobs_path().join(contents.to_string());
			if hardlink_prohibited {
				let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
				let result = tokio::fs::copy(path, &dst).await.map(|_| ());
				match result {
					Ok(()) => (),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
					Err(source) => {
						let src = path.display();
						let dst = dst.display();
						return Err(tg::error!(!source, %src, %dst, "failed to copy"));
					},
				}
			} else {
				let result = tokio::fs::hard_link(path, &dst).await;
				match result {
					Ok(()) => (),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
					Err(source) => {
						let src = path.display();
						let dst = dst.display();
						return Err(tg::error!(!source, %src, %dst, "failed to hardlink"));
					},
				}
			};

			// Since every file is a child of a directory we do not need to recurse over file dependencies and can bail early.
			return Ok(());
		}

		// Recurse.
		for edge in &output.nodes[node].edges {
			// Skip any children that are roots.
			let input_index = output.nodes[edge.node].input;
			if input.nodes[input_index].parent.is_none() {
				continue;
			}

			// Get the path of the dependency.
			let path_ = edge
				.reference
				.item()
				.try_unwrap_path_ref()
				.ok()
				.or_else(|| edge.reference.options()?.path.as_ref())
				.cloned()
				.ok_or_else(|| tg::error!("expected a path dependency"))?;

			// We could use metadata from the input, or the data, this avoids having to acquire a lock.
			let path = if matches!(&output.nodes[node].id, tg::artifact::Id::Directory(_)) {
				path.join(path_)
			} else {
				path.parent().unwrap().join(path_)
			};

			Box::pin(self.write_links_inner(input, output, edge.node, &path, visited)).await?;
		}

		Ok(())
	}
}

impl Server {
	pub(crate) async fn _create_artifact_data_for_lockfile(
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
							._create_directory_node(entries.clone(), &graphs, &indices)
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
							._create_file_node(
								contents.clone(),
								dependencies.clone(),
								*executable,
								&graphs,
								&indices,
							)
							.await?;
						nodes.push(tg::graph::data::Node::File(node));
					},
					tg::lockfile::Node::Symlink { artifact, subpath } => {
						let node = self
							._create_symlink_node(
								artifact.clone(),
								subpath.clone(),
								&graphs,
								&indices,
							)
							.await?;
						nodes.push(tg::graph::data::Node::Symlink(node));
					},
				}
			}

			// Create the graph.
			let graph = tg::graph::data::Data { nodes };

			// Store the graph.
			let id = graph.id()?;
			let bytes = graph.serialize()?;
			let arg = tg::object::put::Arg { bytes };
			self.put_object(&id.into(), arg).await?;

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
							.map(|(reference, referent)| {
								let item = match &referent.item {
									Either::Left(_node) => {
										return Err(tg::error!("invalid graph"));
									},
									Either::Right(object) => object.clone(),
								};
								let referent = tg::Referent {
									item,
									tag: referent.tag.clone(),
									subpath: referent.subpath.clone(),
								};
								Ok::<_, tg::Error>((reference.clone(), referent))
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
							subpath: symlink.subpath.clone(),
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

	async fn _create_directory_node(
		&self,
		entries: BTreeMap<String, tg::lockfile::Entry>,
		graphs: &[tg::graph::Data],
		indices: &BTreeMap<usize, (usize, usize)>,
	) -> tg::Result<tg::graph::data::node::Directory> {
		let mut entries_ = BTreeMap::new();
		for (name, entry) in entries {
			let entry = self
				._resolve_lockfile_dependency(entry, graphs, indices)
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

	async fn _create_file_node(
		&self,
		contents: Option<tg::blob::Id>,
		dependencies: BTreeMap<tg::Reference, tg::Referent<tg::lockfile::Entry>>,
		executable: bool,
		graphs: &[tg::graph::Data],
		indices: &BTreeMap<usize, (usize, usize)>,
	) -> tg::Result<tg::graph::data::node::File> {
		let contents = contents.ok_or_else(|| tg::error!("incomplete lockfile"))?;
		let dependencies = dependencies
			.into_iter()
			.map(|(reference, referent)| async move {
				let item = referent.item.clone();
				let item = self
					._resolve_lockfile_dependency(item, graphs, indices)
					.await?;
				let dependency = tg::Referent {
					item,
					subpath: referent.subpath,
					tag: referent.tag,
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

	async fn _create_symlink_node(
		&self,
		artifact: Option<tg::lockfile::Entry>,
		path: Option<PathBuf>,
		graphs: &[tg::graph::Data],
		indices: &BTreeMap<usize, (usize, usize)>,
	) -> tg::Result<tg::graph::data::node::Symlink> {
		let artifact = if let Some(artifact) = artifact {
			let artifact = self
				._resolve_lockfile_dependency(artifact, graphs, indices)
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
		let subpath = path;
		Ok(tg::graph::data::node::Symlink { artifact, subpath })
	}

	async fn _resolve_lockfile_dependency(
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
						.map(|(reference, referent)| {
							let referent = tg::Referent {
								item: referent
									.item
									.clone()
									.right()
									.ok_or_else(|| tg::error!("expected an object id"))?,
								tag: referent.tag.clone(),
								subpath: referent.subpath.clone(),
							};
							Ok::<_, tg::Error>((reference.clone(), referent))
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
					subpath: node.subpath.clone(),
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
					.filter_map(|entry| entry.as_ref().left().copied()),
			),
			tg::lockfile::Node::File { dependencies, .. } => Box::new(
				dependencies
					.values()
					.filter_map(|referent| referent.item.as_ref().left().copied()),
			),
			tg::lockfile::Node::Symlink { artifact, .. } => Box::new(
				artifact
					.iter()
					.filter_map(|entry| entry.as_ref().left().copied()),
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
		input: &input::Graph,
		output: &Graph,
		node: usize,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<()> {
		let mut stack = vec![node];
		let mut visited = vec![false; output.nodes.len()];
		while let Some(output_index) = stack.pop() {
			// Check if this node has been visited.
			if visited[output_index] {
				continue;
			}
			visited[output_index] = true;

			// Get the input index.
			let input_index = output.nodes[output_index].input;
			if input.nodes[input_index].parent.is_none() {
				// Create a temp.
				let temp = Temp::new(self);

				// Copy or move to the temp.
				let mut visited = BTreeSet::new();
				self.copy_or_move_all(
					input,
					output,
					output_index,
					temp.path.clone(),
					&mut visited,
					progress,
				)
				.await?;

				// Update the xattrs and file permissions.
				self.update_xattrs_and_permissions(input, output, output_index, temp.path.clone())
					.await?;

				// Reset the file times to epoch.
				self.set_file_times_to_epoch(&temp.path, true).await?;

				// Rename to the checkouts directory.
				let dest = self
					.cache_path()
					.join(output.nodes[output_index].id.to_string());
				match tokio::fs::rename(&temp.path, &dest).await {
					Ok(()) => (),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
					Err(error) if error.raw_os_error() == Some(libc::ENOTEMPTY) => (),
					Err(source) => return Err(tg::error!(!source, "failed to rename")),
				}

				// Update hardlinks and xattrs.
				self.write_links(input, output, output_index).await?;

				// Reset the top-level object times to epoch post-rename.
				self.set_file_times_to_epoch(dest, false).await?;
			}
			// Recurse.
			stack.extend(output.nodes[output_index].edges.iter().map(|e| e.node));
		}
		Ok(())
	}

	async fn copy_or_move_all(
		&self,
		input: &input::Graph,
		output: &Graph,
		node: usize,
		dest: PathBuf,
		visited: &mut BTreeSet<PathBuf>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<()> {
		// Check if we've visited this node.
		let input_index = output.nodes[node].input;
		let input_node = input.nodes[input_index].clone();
		if visited.contains(&input_node.arg.path) {
			return Ok(());
		}
		visited.insert(input_node.arg.path.clone());

		if input_node.metadata.is_dir() {
			if input_node.arg.destructive {
				match tokio::fs::rename(&input_node.arg.path, &dest).await {
					Ok(()) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::ENOTEMPTY) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::ENODEV) => (),
					Err(source) => return Err(tg::error!(!source, "failed to rename directory")),
				}
			}
			tokio::fs::create_dir_all(&dest)
				.await
				.map_err(|source| tg::error!(!source, "failed to create directory"))?;
			let dependencies = output.nodes[node].edges.clone();
			for edge in dependencies {
				let input_index = output.nodes[edge.node].input;
				let input_node_ = &input.nodes[input_index];
				let diff = input_node_.arg.path.diff(&input_node.arg.path).unwrap();
				let dest = dest.join(&diff);
				Box::pin(self.copy_or_move_all(input, output, edge.node, dest, visited, progress))
					.await?;
			}
		} else if input_node.metadata.is_symlink() {
			let target = self.get_symlink_target(input, output, node).await?;
			tokio::fs::symlink(&target, &dest)
				.await
				.map_err(|source| tg::error!(!source, "failed to create symlink"))?;
		} else if input_node.metadata.is_file() {
			if input_node.arg.destructive {
				match tokio::fs::rename(&input_node.arg.path, &dest).await {
					Ok(()) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::ENODEV) => (),
					Err(source) => {
						return Err(
							tg::error!(!source, %path = input_node.arg.path.display(), "failed to rename file"),
						)
					},
				}
			}
			tokio::fs::copy(&input_node.arg.path, &dest).await.map_err(
				|source| tg::error!(!source, %path = input_node.arg.path.display(), %dest = dest.display(), "failed to copy file"),
			)?;
		} else {
			return Err(tg::error!(%path = input_node.arg.path.display(), "invalid file type"));
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
		input: &input::Graph,
		output: &Graph,
		node: usize,
		dest: PathBuf,
	) -> tg::Result<()> {
		let mut visited = vec![false; output.nodes.len()];
		self.update_xattrs_and_permissions_inner(input, output, node, dest, &mut visited)
			.await
	}

	async fn update_xattrs_and_permissions_inner(
		&self,
		input: &input::Graph,
		output: &Graph,
		node: usize,
		dest: PathBuf,
		visited: &mut [bool],
	) -> tg::Result<()> {
		// Check if we've visited this node.
		if visited[node] {
			return Ok(());
		}
		visited[node] = true;

		// If this is a file, write xattrs.
		match &output.nodes[node].data {
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
					.map_err(
						|source| tg::error!(!source, %path = dest.display(), "failed to set file permissions"),
					)?;

				let json = serde_json::to_vec(&file)
					.map_err(|source| tg::error!(!source, "failed to serialize file data"))?;

				let metadata = tokio::fs::symlink_metadata(&dest).await.map_err(
					|source| tg::error!(!source, %dest = dest.display(), "failed to get metadata"),
				)?;
				if !metadata.is_file() {
					return Err(tg::error!(%path = dest.display(), "expected a file"));
				}
				xattr::set(&dest, tg::file::XATTR_DATA_NAME, &json).map_err(
					|source| tg::error!(!source, %path = dest.display(), "failed to write file data as an xattr"),
				)?;
				let metadata = serde_json::to_vec(&output.nodes[node].metadata)
					.map_err(|source| tg::error!(!source, "failed to serialize metadata"))?;
				xattr::set(&dest, tg::file::XATTR_METADATA_NAME, &metadata).map_err(
					|source| tg::error!(!source, %path = dest.display(), "failed to write file metadata as an xattr"),
				)?;
			},
			tg::artifact::Data::Directory(_) => {
				let permissions = tokio::fs::metadata(&dest)
					.await
					.map_err(
						|source| tg::error!(!source, %dest = dest.display(), "failed to get metadata"),
					)?
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
		let dependencies = output.nodes[node].edges.clone();
		let dependencies = dependencies
			.into_iter()
			.map(|edge| async move {
				// Skip roots.
				let input_index = output.nodes[edge.node].input;
				let input_node = &input.nodes[input_index];
				input_node.parent.as_ref()?;

				let path = edge
					.reference
					.item()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| edge.reference.options()?.path.as_ref())?;

				Some((path.clone(), edge.node))
			})
			.collect::<FuturesUnordered<_>>()
			.filter_map(future::ready)
			.collect::<Vec<_>>()
			.await;

		for (subpath, next_node) in dependencies {
			let dest = if matches!(
				&output.nodes[node].data,
				tg::artifact::Data::File(_) | tg::artifact::Data::Symlink(_)
			) {
				dest.parent().unwrap().join(&subpath)
			} else {
				dest.join(&subpath)
			};
			Box::pin(
				self.update_xattrs_and_permissions_inner(input, output, next_node, dest, visited),
			)
			.await?;
		}

		Ok(())
	}

	async fn get_symlink_target(
		&self,
		input: &input::Graph,
		output: &Graph,
		symlink: usize,
	) -> tg::Result<PathBuf> {
		// Find how deep this node is in the output tree.
		let mut input_index = output.nodes[symlink].input;
		let mut depth = 0;
		loop {
			let Some(parent) = input.nodes[input_index].parent else {
				break;
			};
			depth += 1;
			input_index = parent;
		}

		// Get the outgoing edge.
		let output_edge = output.nodes[symlink]
			.edges
			.first()
			.ok_or_else(|| tg::error!("invalid symlink node"))?
			.clone();

		// Create the target.
		let id = output.nodes[output_edge.node].id.to_string();
		let subpath = output_edge.subpath.unwrap_or_default();

		let components = (0..depth)
			.map(|_| std::path::Component::ParentDir)
			.chain(std::iter::once(std::path::Component::Normal(OsStr::new(
				&id,
			))))
			.chain(subpath.components());

		Ok(components.collect())
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
