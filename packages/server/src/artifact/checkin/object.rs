use super::unify;
use crate::Server;
use num::ToPrimitive;
use std::{
	collections::BTreeMap,
	os::unix::fs::PermissionsExt as _,
	path::{Path, PathBuf},
};
use tangram_client::{self as tg, handle::Ext};
use tangram_either::Either;

#[derive(Debug)]
pub struct Graph {
	pub indices: BTreeMap<unify::Id, usize>,
	pub nodes: Vec<Node>,
	pub paths: BTreeMap<PathBuf, usize>,
	pub objects: BTreeMap<tg::object::Id, usize>,
}

#[derive(Debug)]
pub struct Node {
	pub data: Option<tg::artifact::Data>,
	pub id: Option<tg::object::Id>,
	pub edges: Vec<Edge>,
	pub metadata: Option<tg::object::Metadata>,
	pub unify: unify::Node,
}

#[derive(Debug)]
pub struct Edge {
	pub index: usize,
	pub reference: tg::Reference,
	pub subpath: Option<PathBuf>,
	pub tag: Option<tg::Tag>,
}

#[derive(Clone, Debug)]
struct RemappedEdge {
	pub id: Either<usize, tg::object::Id>,
	pub reference: tg::Reference,
	pub subpath: Option<PathBuf>,
	pub tag: Option<tg::Tag>,
}

impl Server {
	pub(super) async fn create_object_graph(
		&self,
		root: &unify::Id,
		unify: unify::Graph,
	) -> tg::Result<Graph> {
		let mut indices = BTreeMap::new();
		let mut paths: BTreeMap<PathBuf, usize> = BTreeMap::new();
		let mut nodes = Vec::with_capacity(unify.nodes.len());
		let objects = BTreeMap::new();

		self.create_object_graph_inner(root, &unify, &mut indices, &mut nodes, &mut paths)
			.await;

		let mut graph = Graph {
			indices,
			nodes,
			paths,
			objects,
		};
		self.create_objects(&mut graph).await?;

		Ok(graph)
	}

	async fn create_object_graph_inner(
		&self,
		node: &unify::Id,
		graph: &unify::Graph,
		indices: &mut BTreeMap<unify::Id, usize>,
		nodes: &mut Vec<Node>,
		paths: &mut BTreeMap<PathBuf, usize>,
	) -> usize {
		// Check if this node is visited.
		if let Some(index) = indices.get(node) {
			return *index;
		}

		// Compute the new index.
		let index = nodes.len();
		indices.insert(node.clone(), index);

		// Add the path if it exists.
		if let Some(input) = graph.nodes.get(node).unwrap().object.as_ref().left() {
			let path = input.read().await.arg.path.clone();
			paths.insert(path, index);
		}

		// Create the node.
		nodes.push(Node {
			id: None,
			data: None,
			edges: Vec::new(),
			metadata: None,
			unify: graph.nodes.get(node).unwrap().clone(),
		});

		// Recurse.
		for (reference, edge) in &graph.nodes.get(node).unwrap().edges {
			let dependency_index = Box::pin(self.create_object_graph_inner(
				&edge.referent,
				graph,
				indices,
				nodes,
				paths,
			))
			.await;
			let referrent = graph.nodes.get(&edge.referent).unwrap();
			let edge = Edge {
				index: dependency_index,
				reference: reference.clone(),
				subpath: edge.subpath.clone(),
				tag: referrent.tag.clone(),
			};
			nodes[index].edges.push(edge);
		}

		index
	}

	pub(super) async fn create_objects(&self, graph: &mut Graph) -> tg::Result<()> {
		let mut graph_metadata = BTreeMap::new();
		let mut file_metadata = BTreeMap::new();

		// Partition the graph into its strongly connected components.
		for scc in petgraph::algo::tarjan_scc(&*graph) {
			// Special case: the node is a bare object.
			if scc.len() == 1 && graph.nodes[scc[0]].unify.object.is_right() {
				let id = graph.nodes[scc[0]]
					.unify
					.object
					.as_ref()
					.unwrap_right()
					.clone();
				let metadata = self.get_object_metadata(&id).await.map_err(
					|source| tg::error!(!source, %object = id, "failed to get object metadata"),
				)?;
				graph.nodes[scc[0]].id.replace(id.clone());
				graph.nodes[scc[0]].metadata.replace(metadata);
				graph.objects.insert(id.clone(), scc[0]);
				continue;
			}

			// Assign node indices.
			let mut indices = BTreeMap::new();
			for (new_index, old_index) in scc.iter().copied().enumerate() {
				indices.insert(old_index, new_index);
			}

			// Create graph nodes.
			let mut nodes = Vec::new();
			for old_index in &scc {
				let node = self
					.create_graph_node_data(*old_index, graph, &indices, &mut file_metadata)
					.await?;
				nodes.push(node);
			}

			if nodes.len() == 1 {
				// If the SCC has one component, then create normal artifacts.
				let index = scc[0];
				let node = nodes[0].clone();
				let data = self.create_normal_artifact_data(node);

				// Get the metadata.
				let metadata = self.compute_object_metadata(
					graph,
					index,
					&data,
					&file_metadata,
					&graph_metadata,
				);

				// Update the graph.
				let id: tg::object::Id = data.id()?.into();
				graph.nodes[index].data.replace(data);
				graph.nodes[index].id.replace(id.clone());
				graph.nodes[index].metadata.replace(metadata);
				graph.objects.insert(id, index);
			} else {
				// Otherwise, construct an object graph.
				let object_graph = tg::graph::data::Data {
					nodes: nodes.clone(),
				};

				// Get the graph metadata.
				let metadata = self.compute_graph_metadata(graph, &object_graph);

				// Store the graph.
				let id = object_graph.id()?;
				let bytes = object_graph.serialize()?;
				let arg = tg::object::put::Arg { bytes };
				self.put_object(&id.clone().into(), arg).await?;

				graph_metadata.insert(id.clone(), metadata);

				for old_index in scc {
					// Get the index within the object graph.
					let new_index = indices.get(&old_index).copied().unwrap();

					// Create the artifact data.
					let data: tg::artifact::Data = match nodes[new_index].kind() {
						tg::artifact::Kind::Directory => tg::directory::Data::Graph {
							graph: id.clone(),
							node: new_index,
						}
						.into(),
						tg::artifact::Kind::File => tg::file::Data::Graph {
							graph: id.clone(),
							node: new_index,
						}
						.into(),
						tg::artifact::Kind::Symlink => tg::symlink::Data::Graph {
							graph: id.clone(),
							node: new_index,
						}
						.into(),
					};

					// Get the metadata.
					let metadata = self.compute_object_metadata(
						graph,
						old_index,
						&data,
						&file_metadata,
						&graph_metadata,
					);

					// Update the graph.
					let id: tg::object::Id = data.id()?.into();
					graph.nodes[old_index].data.replace(data);
					graph.nodes[old_index].id.replace(id.clone());
					graph.nodes[old_index].metadata.replace(metadata);
					graph.objects.insert(id, old_index);
				}
			}
		}

		Ok(())
	}

	async fn create_graph_node_data(
		&self,
		index: usize,
		graph: &mut Graph,
		indices: &BTreeMap<usize, usize>,
		file_metadata: &mut BTreeMap<usize, tg::object::Metadata>,
	) -> tg::Result<tg::graph::data::Node> {
		// Get the input metadata, or skip if the node is an object.
		let (path, metadata) = match graph.nodes[index].unify.object.clone() {
			Either::Left(input) => {
				let input = input.read().await;
				(input.arg.path.clone(), input.metadata.clone())
			},
			Either::Right(_) => {
				return Err(tg::error!("expected a node"));
			},
		};

		let edges = graph.nodes[index]
			.edges
			.iter()
			.map(|edge| {
				let id = if let Some(new_index) = indices.get(&edge.index) {
					Either::Left(*new_index)
				} else {
					let id = graph.nodes[edge.index].id.clone().unwrap();
					Either::Right(id)
				};
				RemappedEdge {
					reference: edge.reference.clone(),
					id,
					tag: edge.tag.clone(),
					subpath: edge.subpath.clone(),
				}
			})
			.collect::<Vec<_>>();

		// Create the node data.
		let node = if metadata.is_dir() {
			let entries = edges
				.into_iter()
				.map(|edge| {
					let name = edge
						.reference
						.item()
						.unwrap_path_ref()
						.components()
						.last()
						.unwrap()
						.as_os_str()
						.to_str()
						.unwrap()
						.to_owned();
					let id = edge.id.map_right(|id| id.try_into().unwrap());
					(name, id)
				})
				.collect();
			let directory = tg::graph::data::node::Directory { entries };
			tg::graph::data::Node::Directory(directory)
		} else if metadata.is_file() {
			let file = self
				.create_graph_file_node_data(path.as_ref(), index, metadata, edges, file_metadata)
				.await?;
			tg::graph::data::Node::File(file)
		} else if metadata.is_symlink() {
			let edge = edges.first().cloned().unwrap();
			let artifact = edge.id.map_right(|object| match object {
				tg::object::Id::Directory(a) => a.into(),
				tg::object::Id::File(a) => a.into(),
				tg::object::Id::Symlink(a) => a.into(),
				_ => unreachable!(),
			});
			let subpath = edge
				.subpath
				.map(|path| path.strip_prefix("./").unwrap_or(&path).to_owned());
			let symlink = tg::graph::data::node::Symlink {
				artifact: Some(artifact),
				subpath,
			};

			tg::graph::data::Node::Symlink(symlink)
		} else {
			return Err(tg::error!("invalid file type"));
		};

		Ok(node)
	}

	async fn create_graph_file_node_data(
		&self,
		path: &Path,
		index: usize,
		metadata: std::fs::Metadata,
		edges: Vec<RemappedEdge>,
		file_metadata: &mut BTreeMap<usize, tg::object::Metadata>,
	) -> tg::Result<tg::graph::data::node::File> {
		// Compute the dependencies, which will be shared in all cases.
		let dependencies = edges
			.into_iter()
			.map(|edge| {
				let dependency = tg::Referent {
					item: edge.id,
					tag: edge.tag,
					subpath: edge.subpath,
				};
				(edge.reference, dependency)
			})
			.collect();

		// Check if there is an xattr.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let data = xattr::get(path, tg::file::XATTR_DATA_NAME)
			.map_err(|source| tg::error!(!source, "failed to read xattr"))?;
		if let Some(data) = data {
			// Deserialize the data.
			let data = tg::file::Data::deserialize(&data.into())?;

			// Deserialize the metadata and update the state, if necessary.
			if let Some(metadata) = xattr::get(path, tg::file::XATTR_METADATA_NAME)
				.map_err(|source| tg::error!(!source, "failed to read xattr"))?
			{
				let mut metadata: tg::object::Metadata = serde_json::from_slice(&metadata)
					.map_err(|source| tg::error!(!source, "failed to deserialize metadata"))?;
				metadata.count = metadata.count.map(|count| count - 1);
				metadata.depth = metadata.depth.map(|depth| depth - 1);
				metadata.weight = metadata
					.weight
					.map(|weight| weight - data.serialize().unwrap().len().to_u64().unwrap());
				file_metadata.insert(index, metadata);
			}

			// Get the file's contents/executable bit.
			let (contents, executable) = match data {
				tg::file::Data::Graph { graph, node } => {
					let node = tg::Graph::with_id(graph).data(self).await?.nodes[node].clone();
					let tg::graph::data::Node::File(file) = node else {
						return Err(tg::error!("expected a file node"));
					};
					(file.contents, file.executable)
				},
				tg::file::Data::Normal {
					contents,
					executable,
					..
				} => (contents, executable),
			};

			// Create the file.
			let file = tg::graph::data::node::File {
				contents,
				dependencies,
				executable,
			};
			return Ok(file);
		}

		// Read the file contents.
		let file = tokio::fs::File::open(&path)
			.await
			.map_err(|source| tg::error!(!source, %path = path.display(), "failed to read file"))?;
		let output = self.create_blob_inner(file, None).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to create blob"),
		)?;
		drop(permit);

		// For files only, we need to keep track of the count, depth, and weight when reading the file.
		let file_metadata_ = tg::object::Metadata {
			complete: false,
			count: Some(output.count),
			depth: Some(output.depth),
			weight: Some(output.weight),
		};
		file_metadata.insert(index, file_metadata_);

		let contents = output.blob;
		let executable = metadata.permissions().mode() & 0o111 != 0;
		let file = tg::graph::data::node::File {
			contents,
			dependencies,
			executable,
		};
		Ok(file)
	}

	#[allow(clippy::unused_self)]
	fn create_normal_artifact_data(&self, node: tg::graph::data::Node) -> tg::artifact::Data {
		match node {
			tg::graph::data::Node::Directory(directory) => {
				let entries = directory
					.entries
					.into_iter()
					.map(|(name, entry)| (name, entry.unwrap_right()))
					.collect();
				tg::directory::Data::Normal { entries }.into()
			},
			tg::graph::data::Node::File(file) => {
				let tg::graph::data::node::File {
					contents,
					executable,
					dependencies,
				} = file;
				let dependencies = dependencies
					.into_iter()
					.map(|(reference, referent)| {
						let tg::Referent { item, tag, subpath } = referent;
						let item = item.unwrap_right();
						(reference, tg::Referent { item, subpath, tag })
					})
					.collect();
				tg::file::Data::Normal {
					contents,
					executable,
					dependencies,
				}
				.into()
			},
			tg::graph::data::Node::Symlink(symlink) => {
				let tg::graph::data::node::Symlink { artifact, subpath } = symlink;
				let artifact = artifact.map(Either::unwrap_right);
				tg::symlink::Data::Normal { artifact, subpath }.into()
			},
		}
	}

	#[allow(clippy::unused_self)]
	fn compute_graph_metadata(
		&self,
		graph: &Graph,
		data: &tg::graph::Data,
	) -> tg::object::Metadata {
		let mut complete = true;
		let mut count = 1;
		let mut depth = 1;
		let mut weight = data.serialize().unwrap().len().to_u64().unwrap();

		for node in &data.nodes {
			match node {
				tg::graph::data::Node::Directory(directory) => {
					for entry in directory.entries.values() {
						if let Either::Right(id) = &entry {
							let node = graph.objects.get(&id.clone().into()).unwrap();
							let metadata = graph.nodes[*node].metadata.clone().unwrap();
							complete &= metadata.complete;
							count += metadata.count.unwrap_or(0);
							depth = std::cmp::max(depth, metadata.depth.unwrap_or(0) + 1);
							weight += metadata.weight.unwrap_or(0);
						}
					}
				},
				tg::graph::data::Node::File(file) => {
					for referent in file.dependencies.values() {
						if let Either::Right(id) = &referent.item {
							let node = graph.objects.get(id).unwrap();
							let metadata = graph.nodes[*node].metadata.clone().unwrap();
							complete &= metadata.complete;
							count += metadata.count.unwrap_or(0);
							depth = std::cmp::max(depth, metadata.depth.unwrap_or(0) + 1);
							weight += metadata.weight.unwrap_or(0);
						}
					}
				},
				tg::graph::data::Node::Symlink(symlink) => {
					if let Some(Either::Right(id)) = &symlink.artifact {
						let node = graph.objects.get(&id.clone().into()).unwrap();
						let metadata = graph.nodes[*node].metadata.clone().unwrap();
						complete &= metadata.complete;
						count += metadata.count.unwrap_or(0);
						depth = std::cmp::max(depth, metadata.depth.unwrap_or(0) + 1);
						weight += metadata.weight.unwrap_or(0);
					}
				},
			}
		}

		tg::object::Metadata {
			complete,
			count: Some(count),
			depth: Some(depth),
			weight: Some(weight),
		}
	}

	#[allow(clippy::only_used_in_recursion)]
	fn compute_object_metadata(
		&self,
		graph: &Graph,
		index: usize,
		data: &tg::artifact::Data,
		file_metadata: &BTreeMap<usize, tg::object::Metadata>,
		graph_metadata: &BTreeMap<tg::graph::Id, tg::object::Metadata>,
	) -> tg::object::Metadata {
		if let Some(metadata) = graph.nodes[index].metadata.clone() {
			return metadata;
		}

		let mut complete = true;
		let (count, depth, weight) = match (file_metadata.get(&index), data) {
			(Some(existing), _) => (existing.count, existing.depth, existing.weight),
			(None, tg::artifact::Data::File(_)) => (None, None, None),
			_ => (Some(0), Some(0), Some(0)),
		};

		let mut count = count.map(|count| count + 1);
		let mut depth = depth.map(|depth| depth + 1);
		let data_size = data.serialize().unwrap().len().to_u64().unwrap();
		let mut weight = weight.map(|weight| weight + data_size);

		match data {
			tg::artifact::Data::Directory(tg::directory::Data::Graph { graph, .. })
			| tg::artifact::Data::File(tg::file::Data::Graph { graph, .. })
			| tg::artifact::Data::Symlink(tg::symlink::Data::Graph { graph, .. }) => {
				let metadata = graph_metadata.get(graph).unwrap();
				complete &= metadata.complete;
				if let Some(c) = metadata.count {
					count = count.map(|count| count + c);
				} else {
					count.take();
				}
				if let Some(d) = metadata.depth {
					depth = depth.map(|depth| std::cmp::max(depth, d + 1));
				} else {
					depth.take();
				}
				if let Some(w) = metadata.weight {
					weight = weight.map(|weight| weight + w);
				} else {
					weight.take();
				}
			},
			_ => {
				for edge in &graph.nodes[index].edges {
					let metadata = graph.nodes[edge.index].metadata.clone().unwrap_or_else(|| {
						let data = graph.nodes[edge.index].data.as_ref().unwrap();
						self.compute_object_metadata(
							graph,
							edge.index,
							data,
							file_metadata,
							graph_metadata,
						)
					});
					complete &= metadata.complete;
					if let Some(c) = metadata.count {
						count = count.map(|count| count + c);
					} else {
						count.take();
					}
					if let Some(d) = metadata.depth {
						depth = depth.map(|depth| std::cmp::max(depth, d + 1));
					} else {
						depth.take();
					}
					if let Some(w) = metadata.weight {
						weight = weight.map(|weight| weight + w);
					} else {
						weight.take();
					}
				}
			},
		}
		tg::object::Metadata {
			complete,
			count,
			depth,
			weight,
		}
	}
}

impl petgraph::visit::GraphBase for Graph {
	type EdgeId = (usize, usize);
	type NodeId = usize;
}

#[allow(clippy::needless_arbitrary_self_type)]
impl<'a> petgraph::visit::NodeIndexable for &'a Graph {
	fn from_index(self: &Self, i: usize) -> Self::NodeId {
		i
	}

	fn node_bound(self: &Self) -> usize {
		self.nodes.len()
	}

	fn to_index(self: &Self, a: Self::NodeId) -> usize {
		a
	}
}

impl<'a> petgraph::visit::IntoNeighbors for &'a Graph {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;
	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		let it = self.nodes[a].edges.iter().map(|edge| edge.index);
		Box::new(it)
	}
}

impl<'a> petgraph::visit::IntoNodeIdentifiers for &'a Graph {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.nodes.len()
	}
}
