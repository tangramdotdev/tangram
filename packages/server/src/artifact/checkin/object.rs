use super::{input, unify};
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
	pub path: Option<PathBuf>,
	pub reference: tg::Reference,
	pub subpath: Option<PathBuf>,
	pub tag: Option<tg::Tag>,
}

#[derive(Clone, Debug)]
struct RemappedEdge {
	pub id: Either<usize, tg::object::Id>,
	pub path: Option<PathBuf>,
	pub reference: tg::Reference,
	pub subpath: Option<PathBuf>,
	pub tag: Option<tg::Tag>,
}

impl Server {
	pub(super) async fn create_object_graph(
		&self,
		input: &input::Graph,
		unify: &unify::Graph,
		root: &unify::Id,
	) -> tg::Result<Graph> {
		let mut indices = BTreeMap::new();
		let mut paths: BTreeMap<PathBuf, usize> = BTreeMap::new();
		let mut nodes = Vec::with_capacity(unify.nodes.len());
		let objects = BTreeMap::new();

		self.create_object_graph_inner(input, unify, root, &mut indices, &mut nodes, &mut paths)
			.await;

		let mut graph = Graph {
			indices,
			nodes,
			paths,
			objects,
		};
		self.create_objects(input, &mut graph).await?;
		Ok(graph)
	}

	async fn create_object_graph_inner(
		&self,
		input: &input::Graph,
		unify: &unify::Graph,
		node: &unify::Id,
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
		if let Some(input_index) = unify.nodes.get(node).unwrap().object.as_ref().left() {
			let input_node = &input.nodes[*input_index];
			let path = input_node.arg.path.clone();
			paths.insert(path, index);
		}

		// Create the node.
		nodes.push(Node {
			id: None,
			data: None,
			edges: Vec::new(),
			metadata: None,
			unify: unify.nodes.get(node).unwrap().clone(),
		});

		// Recurse.
		for (reference, edge) in &unify.nodes.get(node).unwrap().edges {
			let dependency_index = Box::pin(self.create_object_graph_inner(
				input,
				unify,
				&edge.referent,
				indices,
				nodes,
				paths,
			))
			.await;
			let referrent = unify.nodes.get(&edge.referent).unwrap();
			let edge = Edge {
				index: dependency_index,
				path: edge.path.clone(),
				reference: reference.clone(),
				subpath: edge.subpath.clone(),
				tag: referrent.tag.clone(),
			};
			nodes[index].edges.push(edge);
		}

		index
	}

	pub(super) async fn create_objects(
		&self,
		input: &input::Graph,
		graph: &mut Graph,
	) -> tg::Result<()> {
		let mut graph_metadata = BTreeMap::new();
		let mut file_metadata = BTreeMap::new();

		// Partition the graph into its strongly connected components.
		for mut scc in petgraph::algo::tarjan_scc(&*graph) {
			// Iterate the scc in reverse order.
			scc.reverse();

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
					.create_graph_node_data(input, graph, *old_index, &indices, &mut file_metadata)
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
				let bytes = data.serialize()?;
				let id = tg::artifact::Id::new(data.kind(), &bytes);

				graph.nodes[index].data.replace(data);
				graph.nodes[index].id.replace(id.clone().into());
				graph.nodes[index].metadata.replace(metadata);
				graph.objects.insert(id.into(), index);
			} else {
				// Otherwise, construct an object graph.
				let object_graph = tg::graph::Data {
					nodes: nodes.clone(),
				};

				// Get the graph metadata.
				let metadata = self.compute_graph_metadata(graph, &object_graph);

				// Store the graph.
				let bytes = object_graph.serialize()?;
				let id = tg::graph::Id::new(&bytes);
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
					let bytes = data.serialize()?;
					let id = tg::artifact::Id::new(data.kind(), &bytes);
					graph.nodes[old_index].data.replace(data);
					graph.nodes[old_index].id.replace(id.clone().into());
					graph.nodes[old_index].metadata.replace(metadata);
					graph.objects.insert(id.into(), old_index);
				}
			}
		}

		Ok(())
	}

	async fn create_graph_node_data(
		&self,
		input: &input::Graph,
		graph: &mut Graph,
		index: usize,
		indices: &BTreeMap<usize, usize>,
		file_metadata: &mut BTreeMap<usize, tg::object::Metadata>,
	) -> tg::Result<tg::graph::data::Node> {
		// Get the input metadata, or skip if the node is an object.
		let (input_index, path, metadata) = match graph.nodes[index].unify.object.clone() {
			Either::Left(input_index) => {
				let input = &input.nodes[input_index];
				(input_index, input.arg.path.clone(), input.metadata.clone())
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
					id,
					path: edge.path.clone(),
					reference: edge.reference.clone(),
					subpath: edge.subpath.clone(),
					tag: edge.tag.clone(),
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
			let directory = tg::graph::data::Directory { entries };
			tg::graph::data::Node::Directory(directory)
		} else if metadata.is_file() {
			let file = self
				.create_graph_file_node_data(path.as_ref(), index, metadata, edges, file_metadata)
				.await?;
			tg::graph::data::Node::File(file)
		} else if metadata.is_symlink() {
			if let Some(edge) = edges.first().cloned() {
				let artifact = edge.id.map_right(|object| match object {
					tg::object::Id::Directory(a) => a.into(),
					tg::object::Id::File(a) => a.into(),
					tg::object::Id::Symlink(a) => a.into(),
					_ => unreachable!(),
				});
				let subpath = edge
					.subpath
					.map(|path| path.strip_prefix("./").unwrap_or(&path).to_owned());
				let symlink = tg::graph::data::Symlink::Artifact { artifact, subpath };
				tg::graph::data::Node::Symlink(symlink)
			} else {
				let target = input.nodes[input_index]
					.edges
					.first()
					.ok_or_else(|| tg::error!("invalid input graph"))?
					.reference
					.item()
					.try_unwrap_path_ref()
					.map_err(|_| tg::error!("expected a path item"))?;
				let symlink = tg::graph::data::Symlink::Target {
					target: target.clone(),
				};
				tg::graph::data::Node::Symlink(symlink)
			}
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
	) -> tg::Result<tg::graph::data::File> {
		// Compute the dependencies, which will be shared in all cases.
		let dependencies = edges
			.into_iter()
			.map(|edge| {
				let dependency = tg::Referent {
					item: edge.id,
					path: edge.path,
					tag: edge.tag,
					subpath: edge.subpath,
				};
				(edge.reference, dependency)
			})
			.collect();

		// Read the file contents.
		let file = tokio::fs::File::open(&path)
			.await
			.map_err(|source| tg::error!(!source, %path = path.display(), "failed to read file"))?;
		let output = self.create_blob_inner(file, None).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to create blob"),
		)?;

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
		let file = tg::graph::data::File {
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
				let tg::graph::data::File {
					contents,
					executable,
					dependencies,
				} = file;
				let dependencies = dependencies
					.into_iter()
					.map(|(reference, referent)| {
						let tg::Referent {
							item,
							path,
							tag,
							subpath,
						} = referent;
						let item = item.unwrap_right();
						(
							reference,
							tg::Referent {
								item,
								path,
								subpath,
								tag,
							},
						)
					})
					.collect();
				tg::file::Data::Normal {
					contents,
					executable,
					dependencies,
				}
				.into()
			},
			tg::graph::data::Node::Symlink(symlink) => match symlink {
				tg::graph::data::Symlink::Target { target } => {
					tg::symlink::Data::Target { target }.into()
				},
				tg::graph::data::Symlink::Artifact { artifact, subpath } => {
					let artifact = Either::unwrap_right(artifact);
					tg::symlink::Data::Artifact { artifact, subpath }.into()
				},
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
				tg::graph::data::Node::Symlink(tg::graph::data::Symlink::Target { .. }) => (),
				tg::graph::data::Node::Symlink(tg::graph::data::Symlink::Artifact {
					artifact,
					..
				}) => {
					if let Either::Right(id) = artifact {
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
