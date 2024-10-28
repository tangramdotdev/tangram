use super::object;
use crate::{temp::Temp, util::path::Ext as _, Server};
use futures::{future, stream::FuturesUnordered, StreamExt, TryStreamExt as _};
use indoc::formatdoc;
use itertools::Itertools;
use std::{
	collections::{BTreeMap, BTreeSet},
	os::unix::fs::PermissionsExt as _,
	path::{Path, PathBuf},
	sync::{Arc, RwLock, Weak},
};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug)]
pub struct Graph {
	pub input: Arc<tokio::sync::RwLock<super::input::Graph>>,
	pub id: tg::artifact::Id,
	pub data: tg::artifact::Data,
	pub metadata: tg::object::Metadata,
	pub edges: Vec<Edge>,
}

#[derive(Clone, Debug)]
pub struct Edge {
	pub reference: tg::Reference,
	pub graph: Either<Arc<RwLock<Graph>>, Weak<RwLock<Graph>>>,
}

pub type Node = Either<Arc<RwLock<Graph>>, Weak<RwLock<Graph>>>;

impl Server {
	pub(super) async fn create_output_graph(
		&self,
		object_graph: &object::Graph,
	) -> tg::Result<Arc<RwLock<Graph>>> {
		let mut visited = BTreeMap::new();
		let output = self
			.create_output_graph_inner(object_graph, 0, &mut visited)
			.await?
			.unwrap_left();
		Ok(output)
	}

	async fn create_output_graph_inner(
		&self,
		graph: &object::Graph,
		node: usize,
		visited: &mut BTreeMap<usize, Weak<RwLock<Graph>>>,
	) -> tg::Result<Node> {
		if let Some(visited) = visited.get(&node) {
			return Ok(Either::Right(visited.clone()));
		}

		let input = graph.nodes[node]
			.unify
			.object
			.as_ref()
			.unwrap_left()
			.clone();
		let output = Arc::new(RwLock::new(Graph {
			input,
			id: graph.nodes[node].id.clone().unwrap().try_into().unwrap(),
			data: graph.nodes[node].data.clone().unwrap(),
			metadata: graph.nodes[node].metadata.unwrap(),
			edges: Vec::new(),
		}));
		visited.insert(node, Arc::downgrade(&output));

		let mut edges = Vec::new();
		for edge in &graph.nodes[node].edges {
			if graph.nodes[edge.index].unify.object.is_right() {
				continue;
			}
			let node = Box::pin(self.create_output_graph_inner(graph, edge.index, visited)).await?;
			let edge = Edge {
				reference: edge.reference.clone(),
				graph: node,
			};
			edges.push(edge);
		}

		output.write().unwrap().edges = edges;
		Ok(Either::Left(output))
	}
}

impl Server {
	pub async fn write_output_to_database(&self, output: Arc<RwLock<Graph>>) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to create a transaction"))?;

		// Get the output in reverse-topological order.
		let mut stack = vec![output];
		let mut visited = BTreeSet::new();
		while let Some(output) = stack.pop() {
			// Check if we've visited this node yet.
			let (id, data, metadata) = {
				let output = output.read().unwrap();
				(output.id.clone(), output.data.clone(), output.metadata)
			};
			if visited.contains(&id) {
				continue;
			}
			visited.insert(id.clone());

			// Write to the database.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, complete, count, depth, weight, touched_at)
					values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7)
					on conflict (id) do update set touched_at = {p}7;
				"
			);
			let bytes = data.serialize()?;
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			let params: Vec<tangram_database::Value> = db::params![
				id,
				bytes,
				metadata.complete,
				metadata.count,
				metadata.depth,
				metadata.weight,
				now
			];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to put the artifact into the database")
				})?;

			// Insert the object's children into the database.
			data.children()
				.iter()
				.map(|child| {
					let id = id.clone();
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

			stack.extend(output.read().unwrap().edges.iter().map(Edge::node));
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}

	pub(super) async fn _write_data_to_database(&self, data: tg::artifact::Data) -> tg::Result<()> {
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
					tg::lockfile::Node::Symlink { artifact, subpath } => {
						let node = self
							.create_symlink_node(
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

	async fn create_directory_node(
		&self,
		entries: BTreeMap<String, tg::lockfile::Entry>,
		graphs: &[tg::graph::Data],
		indices: &BTreeMap<usize, (usize, usize)>,
	) -> tg::Result<tg::graph::data::node::Directory> {
		let mut entries_ = BTreeMap::new();
		for (name, entry) in entries {
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
					.resolve_lockfile_dependency(item, graphs, indices)
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

	async fn create_symlink_node(
		&self,
		artifact: Option<tg::lockfile::Entry>,
		path: Option<PathBuf>,
		graphs: &[tg::graph::Data],
		indices: &BTreeMap<usize, (usize, usize)>,
	) -> tg::Result<tg::graph::data::node::Symlink> {
		let artifact = if let Some(artifact) = artifact {
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
		let subpath = path;
		Ok(tg::graph::data::node::Symlink { artifact, subpath })
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

impl Server {
	pub async fn write_links(&self, output: Arc<RwLock<Graph>>) -> tg::Result<()> {
		let path = self
			.cache_path()
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
			APP_DIR_RE.is_match(path.to_string_lossy().as_ref())
		} else {
			false
		};

		// If this is a file, we need to create a hardlink in the checkouts directory and create a symlink for its contents in the blobs directory that points to the corresponding entry in the checkouts directory.
		if let tg::artifact::Data::File(file) = data {
			// Create hard link to the file or copy as needed.
			let dst = self.cache_path().join(id.to_string());
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
		let edges = output.read().unwrap().edges.clone();
		for edge in edges {
			let child_output = edge.node();
			let input = child_output.read().unwrap().input.clone();

			// Skip any children that are roots.
			if input.read().await.root.is_none() {
				continue;
			}

			let path_ = edge
				.reference
				.item()
				.try_unwrap_path_ref()
				.ok()
				.or_else(|| edge.reference.options()?.path.as_ref())
				.cloned()
				.ok_or_else(|| tg::error!("expected a path dependency"))?;
			// We could use metadata from the input, or the data, this avoids having to acquire a lock.
			let path = if matches!(id, tg::artifact::Id::Directory(_)) {
				path.join(path_)
			} else {
				path.parent().unwrap().join(path_)
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
		output: Arc<RwLock<Graph>>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<()> {
		let mut visited = BTreeSet::new();
		let mut stack = vec![output];
		while let Some(output) = stack.pop() {
			let id = output.read().unwrap().id.clone();
			if visited.contains(&id) {
				continue;
			}
			visited.insert(id.clone());
			let input = output.read().unwrap().input.clone();
			if input.read().await.root.is_none() {
				// Create a temp.
				let temp = Temp::new(self);

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
				let dest = self.cache_path().join(artifact.to_string());

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
			stack.extend(output.read().unwrap().edges.iter().map(Edge::node));
		}
		Ok(())
	}

	async fn copy_or_move_all(
		&self,
		dest: PathBuf,
		output: Arc<RwLock<Graph>>,
		visited: &mut BTreeSet<PathBuf>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
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
			let dependencies = output.read().unwrap().edges.clone();
			for edge in dependencies {
				let output = edge.node();
				let input_ = output.read().unwrap().input.clone();
				if input_.read().await.root.is_none() {
					continue;
				}
				let diff = input_.read().await.arg.path.diff(&input.arg.path).unwrap();
				let dest = dest.join(&diff);
				Box::pin(self.copy_or_move_all(dest, output, visited, progress)).await?;
			}
		} else if input.metadata.is_symlink() {
			let target = 'a: {
				let edge = input.edges.first().unwrap();
				if let Some(dependency) = edge.node() {
					if dependency.read().await.root.is_none() && edge.reference.as_str() != "." {
						let id = output
							.read()
							.unwrap()
							.edges
							.first()
							.unwrap()
							.node()
							.read()
							.unwrap()
							.id
							.clone();
						let target = input
							.root
							.as_ref()
							.and_then(|root| input.arg.path.diff(root))
							.map_or_else(
								|| PathBuf::from(id.to_string()),
								|diff| {
									let mut buf = PathBuf::new();
									for component in diff.components() {
										if matches!(
											component,
											std::path::Component::ParentDir
												| std::path::Component::CurDir
										) {
											buf.push(std::path::Component::ParentDir);
											continue;
										}
										break;
									}
									buf.push(id.to_string());
									buf
								},
							);
						break 'a target;
					}
				}

				// Otherwise use the reference.
				edge.reference.item().unwrap_path_ref().clone()
			};

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
		visited: &mut BTreeSet<tg::artifact::Id>,
	) -> tg::Result<()> {
		if visited.contains(&output.read().unwrap().id) {
			return Ok(());
		}
		visited.insert(output.read().unwrap().id.clone());

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
				let metadata = serde_json::to_vec(&output.read().unwrap().metadata)
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
		let dependencies = output.read().unwrap().edges.clone();
		let dependencies = dependencies
			.into_iter()
			.map(|edge| async move {
				let child = edge.node();
				let input = child.read().unwrap().input.clone();
				input.read().await.root.as_ref()?;
				let path = edge
					.reference
					.item()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| edge.reference.options()?.path.as_ref())?;
				Some((path.clone(), child.clone()))
			})
			.collect::<FuturesUnordered<_>>()
			.filter_map(future::ready)
			.collect::<Vec<_>>()
			.await;
		for (subpath, output_) in dependencies {
			let input = output_.read().unwrap().input.clone();
			if input.read().await.root.is_none() {
				continue;
			}
			let dest = if matches!(
				&data,
				tg::artifact::Data::File(_) | tg::artifact::Data::Symlink(_)
			) {
				dest.parent().unwrap().join(&subpath)
			} else {
				dest.join(&subpath)
			};
			Box::pin(self.update_xattrs_and_permissions_inner(dest, &output_, visited)).await?;
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
		match &self.graph {
			Either::Left(strong) => strong.clone(),
			Either::Right(weak) => weak.upgrade().unwrap(),
		}
	}
}

impl Graph {
	#[allow(dead_code)]
	async fn print(self_: Arc<RwLock<Graph>>) {
		let mut stack = vec![(tg::Reference::with_path("."), Either::Left(self_), 0)];
		while let Some((reference, node, depth)) = stack.pop() {
			for _ in 0..depth {
				eprint!("  ");
			}
			match node {
				Either::Left(strong) => {
					eprintln!("* strong {reference} {:x?}", Arc::as_ptr(&strong));
					stack.extend(strong.read().unwrap().edges.iter().map(|edge| {
						let reference = edge.reference.clone();
						let graph = edge.graph.clone();
						let depth = depth + 1;
						(reference, graph, depth)
					}));
				},
				Either::Right(weak) => {
					eprintln!("* weak {reference} {:x?}", Weak::as_ptr(&weak));
				},
			}
		}
	}
}
