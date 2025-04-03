use super::{input, unify};
use crate::{Server, blob::create::Blob};
use indoc::formatdoc;
use num::ToPrimitive;
use std::{
	collections::{BTreeMap, BTreeSet},
	os::unix::fs::PermissionsExt as _,
	path::{Path, PathBuf},
	sync::Arc,
};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;

#[derive(Debug)]
pub struct Graph {
	pub graphs: BTreeMap<tg::graph::Id, (tg::graph::Data, Metadata)>,
	pub indices: BTreeMap<unify::Id, usize>,
	pub nodes: Vec<Node>,
	pub paths: BTreeMap<PathBuf, usize>,
	pub objects: BTreeMap<tg::object::Id, usize>,
}

#[derive(Debug)]
pub struct Node {
	pub blob: Option<Arc<Blob>>,
	pub data: Option<tg::artifact::Data>,
	pub id: Option<tg::object::Id>,
	pub edges: Vec<Edge>,
	pub metadata: Option<Metadata>,
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

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Metadata {
	pub complete: bool,
	pub count: Option<u64>,
	pub depth: Option<u64>,
	pub weight: Option<u64>,
}

#[derive(Clone, Debug)]
struct RemappedEdge {
	pub id: Either<usize, tg::object::Id>,
	pub _path: Option<PathBuf>,
	pub reference: tg::Reference,
	pub subpath: Option<PathBuf>,
	pub _tag: Option<tg::Tag>,
}

impl Server {
	pub(super) async fn create_object_graph(
		&self,
		input: &input::Graph,
		unify: &unify::Graph,
		root: &unify::Id,
		progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
	) -> tg::Result<Arc<Graph>> {
		let mut indices = BTreeMap::new();
		let mut paths: BTreeMap<PathBuf, usize> = BTreeMap::new();
		let mut nodes = Vec::with_capacity(unify.nodes.len());
		let objects = BTreeMap::new();

		self.create_object_graph_inner(input, unify, root, &mut indices, &mut nodes, &mut paths)
			.await;

		let mut graph = Graph {
			graphs: BTreeMap::new(),
			indices,
			nodes,
			paths,
			objects,
		};
		self.create_objects(input, &mut graph, progress).await?;
		Ok(Arc::new(graph))
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
			blob: None,
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
			let edge = Edge {
				index: dependency_index,
				path: None,
				reference: reference.clone(),
				subpath: edge.subpath.clone(),
				tag: None,
			};
			nodes[index].edges.push(edge);
		}
		index
	}

	pub(super) async fn create_objects(
		&self,
		input: &input::Graph,
		graph: &mut Graph,
		progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
	) -> tg::Result<()> {
		let mut file_metadata = BTreeMap::new();

		// Partition the graph into its strongly connected components.
		for mut scc in petgraph::algo::tarjan_scc(&*graph) {
			// Iterate the scc in reverse order.
			scc.reverse();

			// Special case: the node is not an artifact.
			if scc.len() == 1
				&& matches!(
					graph.nodes[scc[0]].unify.object,
					Either::Right(
						tg::object::Id::Branch(_)
							| tg::object::Id::Graph(_)
							| tg::object::Id::Leaf(_)
							| tg::object::Id::Command(_)
					)
				) {
				let id = graph.nodes[scc[0]]
					.unify
					.object
					.as_ref()
					.unwrap_right()
					.clone();
				graph.nodes[scc[0]].id.replace(id.clone());
				graph.objects.insert(id.clone(), scc[0]);
				if let Ok(metadata) = self
					.try_get_object_complete_metadata_local(&id)
					.await
					.and_then(|option| {
						option.ok_or_else(|| tg::error!("expected the object to exist"))
					}) {
					graph.nodes[scc[0]].metadata.replace(metadata);
				}
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

				// Update the graph.
				let bytes = data.serialize()?;
				let id = tg::artifact::Id::new(data.kind(), &bytes);
				graph.nodes[index].data.replace(data.clone());
				graph.nodes[index].id.replace(id.clone().into());
				graph.objects.insert(id.into(), index);

				// Update the graph.
				let bytes = data.serialize()?;
				let id = tg::artifact::Id::new(data.kind(), &bytes);
				graph.nodes[index].data.replace(data.clone());
				graph.nodes[index].id.replace(id.clone().into());

				// Get the metadata.
				let metadata = self.compute_object_metadata(graph, index, &data, &file_metadata);
				graph.nodes[index].metadata.replace(metadata);
				progress.increment("objects", 1);
			} else {
				// Otherwise, construct an object graph.
				let object_graph = tg::graph::Data {
					nodes: nodes.clone(),
				};

				// Store the graph.
				let bytes = object_graph.serialize()?;
				let id = tg::graph::Id::new(&bytes);
				let metadata =
					self.compute_graph_metadata(graph, &object_graph, &scc, &file_metadata);
				graph
					.graphs
					.insert(id.clone(), (object_graph.clone(), metadata));
				progress.increment("objects", 1);

				for old_index in scc.iter().copied() {
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

					// Update the graph.
					let bytes = data.serialize()?;
					let id = tg::artifact::Id::new(data.kind(), &bytes);
					graph.nodes[old_index].data.replace(data);
					graph.nodes[old_index].id.replace(id.clone().into());
					graph.objects.insert(id.into(), old_index);
					progress.increment("objects", 1);
				}

				// Update metadata.
				for old_index in scc {
					// Get the metadata.
					let data = graph.nodes[old_index].data.as_ref().unwrap();
					let metadata =
						self.compute_object_metadata(graph, old_index, data, &file_metadata);
					graph.nodes[old_index].metadata.replace(metadata);
				}
			}
		}

		Ok(())
	}

	async fn create_graph_node_data(
		&self,
		input: &input::Graph,
		graph: &mut Graph,
		node: usize,
		indices: &BTreeMap<usize, usize>,
		file_metadata: &mut BTreeMap<usize, Metadata>,
	) -> tg::Result<tg::graph::data::Node> {
		if graph.nodes[node].unify.object.is_left() {
			self.create_graph_node_data_from_input(input, graph, node, indices, file_metadata)
				.await
		} else {
			self.create_graph_node_data_from_object(graph, node, indices)
				.await
		}
	}

	async fn create_graph_node_data_from_input(
		&self,
		input: &input::Graph,
		graph: &mut Graph,
		index: usize,
		indices: &BTreeMap<usize, usize>,
		file_metadata: &mut BTreeMap<usize, Metadata>,
	) -> tg::Result<tg::graph::data::Node> {
		// Get the input metadata, or skip if the node is an object.
		let input_index = graph.nodes[index].unify.object.clone().unwrap_left();
		let (path, metadata) = (
			input.nodes[input_index].arg.path.clone(),
			input.nodes[input_index].metadata.clone(),
		);

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
					_path: edge.path.clone(),
					reference: edge.reference.clone(),
					subpath: edge.subpath.clone(),
					_tag: edge.tag.clone(),
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
						.next_back()
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
			let (file, blob) = self
				.create_graph_file_node_data(path.as_ref(), index, metadata, edges, file_metadata)
				.await?;
			graph.nodes[index].blob = Some(Arc::new(blob));
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

	async fn create_graph_node_data_from_object(
		&self,
		graph: &mut Graph,
		node: usize,
		indices: &BTreeMap<usize, usize>,
	) -> tg::Result<tg::graph::data::Node> {
		let original_id = graph.nodes[node].unify.object.clone().unwrap_right();

		let edges = graph.nodes[node]
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
					_path: edge.path.clone(),
					reference: edge.reference.clone(),
					subpath: edge.subpath.clone(),
					_tag: edge.tag.clone(),
				}
			})
			.collect::<Vec<_>>();

		let node = match original_id {
			tg::object::Id::Directory(_) => {
				let entries = edges
					.into_iter()
					.map(|edge| {
						let name = edge
							.reference
							.item()
							.unwrap_path_ref()
							.components()
							.next_back()
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
			},
			tg::object::Id::File(file) => {
				let data = tg::File::with_id(file.clone())
					.data(self)
					.await
					.map_err(|source| tg::error!(!source, %file, "missing file"))?;
				let (contents, executable) = match data {
					tg::file::Data::Graph { graph, node } => {
						let data = tg::Graph::with_id(graph.clone())
							.data(self)
							.await
							.map_err(|source| tg::error!(!source, %graph, "missing graph"))?
							.nodes[node]
							.clone();
						let tg::graph::data::Node::File(file) = data else {
							return Err(tg::error!(%graph, %node, "expected a file node"));
						};
						(file.contents, file.executable)
					},
					tg::file::Data::Normal {
						contents,
						executable,
						..
					} => (contents, executable),
				};
				// Compute the dependencies, which will be shared in all cases.
				let dependencies = edges
					.into_iter()
					.map(|edge| {
						let dependency = tg::Referent {
							item: edge.id,
							path: None,
							subpath: edge.subpath,
							tag: None,
						};
						(edge.reference, dependency)
					})
					.collect();
				tg::graph::data::Node::File(tg::graph::data::File {
					contents,
					dependencies,
					executable,
				})
			},
			tg::object::Id::Symlink(symlink) => {
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
					let data = tg::Symlink::with_id(symlink.clone())
						.data(self)
						.await
						.map_err(|source| tg::error!(!source, %symlink, "missing symlink"))?;
					let target = match data {
						tg::symlink::Data::Artifact { .. } => {
							return Err(tg::error!(%symlink, "expected a target symlink"));
						},
						tg::symlink::Data::Graph { graph, node } => {
							let data = tg::Graph::with_id(graph.clone())
								.data(self)
								.await
								.map_err(|source| tg::error!(!source, %graph, "missing graph"))?
								.nodes[node]
								.clone();
							let tg::graph::data::Node::Symlink(tg::graph::data::Symlink::Target {
								target,
							}) = data
							else {
								return Err(tg::error!(%graph, %node, "expected a target symlink"));
							};
							target
						},
						tg::symlink::Data::Target { target } => target,
					};
					tg::graph::data::Node::Symlink(tg::graph::data::Symlink::Target { target })
				}
			},
			_ => return Err(tg::error!(%id = original_id, "unexpected object in graph")),
		};
		Ok(node)
	}

	async fn create_graph_file_node_data(
		&self,
		path: &Path,
		index: usize,
		metadata: std::fs::Metadata,
		edges: Vec<RemappedEdge>,
		file_metadata: &mut BTreeMap<usize, Metadata>,
	) -> tg::Result<(tg::graph::data::File, Blob)> {
		// Compute the dependencies, which will be shared in all cases.
		let dependencies = edges
			.into_iter()
			.map(|edge| {
				let dependency = tg::Referent {
					item: edge.id,
					path: None,
					subpath: edge.subpath,
					tag: None,
				};
				(edge.reference, dependency)
			})
			.collect();

		// Read the file contents.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let mut file = tokio::fs::File::open(path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to open the file"),
		)?;
		let blob = self.create_blob_inner(&mut file, None).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to create the blob"),
		)?;
		drop(file);
		drop(permit);

		// For files only, we need to keep track of the count, depth, and weight when reading the file.
		let file_metadata_ = Metadata {
			complete: false,
			count: Some(blob.count),
			depth: Some(blob.depth),
			weight: Some(blob.weight),
		};
		file_metadata.insert(index, file_metadata_);

		let executable = metadata.permissions().mode() & 0o111 != 0;
		let file = tg::graph::data::File {
			contents: blob.id.clone(),
			dependencies,
			executable,
		};

		Ok((file, blob))
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
						let tg::Referent { item, subpath, .. } = referent;
						let item = item.unwrap_right();
						(
							reference,
							tg::Referent {
								item,
								path: None,
								subpath,
								tag: None,
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
		scc: &[usize],
		file_metadata: &BTreeMap<usize, Metadata>,
	) -> Metadata {
		let mut complete = true;
		let mut count = 1;
		let mut depth = 1;
		let mut weight = data.serialize().unwrap().len().to_u64().unwrap();

		let mut visited = BTreeSet::new();
		for (new_index, node) in data.nodes.iter().enumerate() {
			match node {
				tg::graph::data::Node::Directory(directory) => {
					for entry in directory.entries.values() {
						if let Either::Right(id) = &entry {
							if !visited.insert(id.clone().into()) {
								continue;
							}
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
					if let Some(metadata) = file_metadata.get(&scc[new_index]) {
						count += metadata.count.unwrap();
						depth = std::cmp::max(depth, metadata.depth.unwrap_or(0) + 1);
						weight += metadata.weight.unwrap();
					}
					for referent in file.dependencies.values() {
						if let Either::Right(id) = &referent.item {
							if !visited.insert(id.clone()) {
								continue;
							}
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
						if !visited.insert(id.clone().into()) {
							continue;
						}
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

		Metadata {
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
		file_metadata: &BTreeMap<usize, Metadata>,
	) -> Metadata {
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
			tg::artifact::Data::Directory(tg::directory::Data::Graph {
				graph: graph_id, ..
			})
			| tg::artifact::Data::File(tg::file::Data::Graph {
				graph: graph_id, ..
			})
			| tg::artifact::Data::Symlink(tg::symlink::Data::Graph {
				graph: graph_id, ..
			}) => {
				let metadata = &graph.graphs.get(graph_id).unwrap().1;
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
				let mut visited = BTreeSet::new();
				for edge in &graph.nodes[index].edges {
					let id = graph.nodes[edge.index].id.as_ref().unwrap();
					if !visited.insert(id) {
						continue;
					}

					let metadata = graph.nodes[edge.index].metadata.clone().unwrap_or_else(|| {
						if let Some(data) = graph.nodes[edge.index].data.as_ref() {
							self.compute_object_metadata(graph, edge.index, data, file_metadata)
						} else {
							Metadata {
								complete: false,
								count: None,
								depth: None,
								weight: None,
							}
						}
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
		Metadata {
			complete,
			count,
			depth,
			weight,
		}
	}

	pub(crate) async fn try_get_object_complete_metadata_local(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<Metadata>> {
		// Get an index connection.
		let connection = self
			.index
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the object metadata.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select complete, count, depth, weight
				from objects
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let output = connection
			.query_optional_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}
}

impl petgraph::visit::GraphBase for Graph {
	type EdgeId = (usize, usize);
	type NodeId = usize;
}

impl petgraph::visit::NodeIndexable for &Graph {
	fn from_index(&self, i: usize) -> Self::NodeId {
		i
	}

	fn node_bound(&self) -> usize {
		self.nodes.len()
	}

	fn to_index(&self, a: Self::NodeId) -> usize {
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

impl petgraph::visit::IntoNodeIdentifiers for &Graph {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.nodes.len()
	}
}
