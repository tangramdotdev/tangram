use super::{Blob, GraphObject, Object, State, Variant};
use crate::Server;
use futures::{StreamExt, TryStreamExt as _, stream};
use itertools::Itertools as _;
use std::collections::BTreeMap;
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(super) async fn checkin_create_blobs(&self, state: &mut State) -> tg::Result<()> {
		let server = self.clone();
		let nodes = state
			.graph
			.nodes
			.iter()
			.enumerate()
			.filter_map(|(index, node)| {
				if !node.variant.is_file() {
					return None;
				}
				let path = node.path.clone()?;
				Some((index, path))
			})
			.collect::<Vec<_>>();
		let blobs = stream::iter(nodes)
			.map(|(index, path)| {
				let server = server.clone();
				async move {
					let mut file = tokio::fs::File::open(path.as_ref()).await.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to open the file"),
					)?;
					let blob = server.create_blob_inner(&mut file, None).await.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to create the blob"),
					)?;
					Ok::<_, tg::Error>((index, blob))
				}
			})
			.buffer_unordered(8)
			.try_collect::<Vec<_>>()
			.await?;
		for (index, blob) in blobs {
			state.graph.nodes[index]
				.variant
				.unwrap_file_mut()
				.blob
				.replace(Blob::Create(blob));
		}
		Ok(())
	}

	pub(super) fn checkin_create_objects(state: &mut State) -> tg::Result<()> {
		// TODO: remove special case for normal artifacts.
		// Separate into sccs.
		let sccs = petgraph::algo::tarjan_scc(&state.graph);
		for mut scc in sccs {
			// Special case: no cycles.
			if scc.len() == 1 {
				Self::create_normal_object(state, scc[0])?;
				continue;
			}
			scc.reverse();

			let graph_indices = scc
				.iter()
				.copied()
				.enumerate()
				.map(|(local, global)| (global, local))
				.collect::<BTreeMap<_, _>>();

			// Create the graph object.
			let mut graph = tg::graph::Data {
				nodes: Vec::with_capacity(scc.len()),
			};
			for index in &scc {
				Self::add_graph_node(state, &graph_indices, &mut graph, *index)?;
			}

			// Create the graph obejct.
			let data = graph.serialize().unwrap();
			let id = tg::graph::Id::new(&data);
			let object = GraphObject {
				id,
				data: graph,
				bytes: data,
			};

			// Create the objects.
			for (local, global) in scc.iter().copied().enumerate() {
				let (kind, data) = match &state.graph.nodes[global].variant {
					Variant::Directory(_) => {
						let kind = tg::object::Kind::Directory;
						let data = tg::directory::data::Directory::Graph(tg::graph::data::Ref {
							graph: Some(object.id.clone()),
							node: local,
						});
						let data = tg::object::Data::from(data);
						(kind, data)
					},
					Variant::File(_) => {
						let kind = tg::object::Kind::File;
						let data = tg::file::data::File::Graph(tg::graph::data::Ref {
							graph: Some(object.id.clone()),
							node: local,
						});
						let data = tg::object::Data::from(data);
						(kind, data)
					},
					Variant::Symlink(_) => {
						let kind = tg::object::Kind::Symlink;
						let data = tg::symlink::data::Symlink::Graph(tg::graph::data::Ref {
							graph: Some(object.id.clone()),
							node: local,
						});
						let data = tg::object::Data::from(data);
						(kind, data)
					},
					Variant::Object => continue,
				};

				// Create the object.
				let bytes = data
					.serialize()
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				let id = tg::object::Id::new(kind, &bytes);
				let object = Object {
					bytes: Some(bytes),
					data: Some(data),
					id,
				};
				state.graph.nodes[global].object.replace(object);
			}

			// Store the graph.
			state.graph_objects.push(object);
		}
		Ok(())
	}

	fn add_graph_node(
		state: &mut State,
		graph_indices: &BTreeMap<usize, usize>,
		graph: &mut tg::graph::Data,
		index: usize,
	) -> tg::Result<()> {
		let node = match &state.graph.nodes[index].variant {
			Variant::Directory(directory) => {
				let entries = directory
					.entries
					.iter()
					.map(|(name, index)| {
						let name = name.clone();
						let item = graph_indices
							.get(index)
							.copied()
							.map(|node| {
								tg::graph::data::Edge::Graph(tg::graph::data::Ref {
									graph: None,
									node,
								})
							})
							.or_else(|| {
								state.graph.nodes[*index].object.as_ref().map(|object| {
									tg::graph::data::Edge::Object(
										object.id.clone().try_into().unwrap(),
									)
								})
							})
							.unwrap();
						(name, item)
					})
					.collect();
				let data = tg::graph::data::Directory { entries };
				tg::graph::data::Node::Directory(data)
			},
			Variant::File(file) => {
				let contents = match file.blob.as_ref().unwrap() {
					Blob::Create(blob) => blob.id.clone(),
					Blob::Id(id) => id.clone(),
				};
				let dependencies = file
					.dependencies
					.iter()
					.map(|(reference, referent)| {
						let referent = referent
							.as_ref()
							.ok_or_else(|| tg::error!(%reference, "unresolved reference"))?;
						let item: tangram_client::graph::data::Edge<tangram_client::object::Id> =
							match &referent.item {
								Either::Left(id) => tg::graph::data::Edge::Object(id.clone()),
								Either::Right(index) => graph_indices
									.get(index)
									.copied()
									.map(|node| {
										tg::graph::data::Edge::Graph(tg::graph::data::Ref {
											graph: None,
											node,
										})
									})
									.or_else(|| {
										state.graph.nodes[*index].object.as_ref().map(|object| {
											tg::graph::data::Edge::Object(
												object.id.clone().try_into().unwrap(),
											)
										})
									})
									.unwrap(),
							};
						let referent = tg::Referent {
							item,
							path: referent.path.clone(),
							tag: referent.tag.clone(),
						};
						Ok::<_, tg::Error>((reference.clone(), referent))
					})
					.try_collect()?;
				let executable = file.executable;
				let data = tg::graph::data::File {
					contents: Some(contents),
					dependencies,
					executable,
				};
				tg::graph::data::Node::File(data)
			},
			Variant::Symlink(symlink) => {
				let artifact = match &symlink.artifact {
					Some(Either::Left(artifact)) => {
						Some(tg::graph::data::Edge::Object(artifact.clone()))
					},
					Some(Either::Right(index)) => Some(
						graph_indices
							.get(index)
							.copied()
							.map(|node| {
								tg::graph::data::Edge::Graph(tg::graph::data::Ref {
									graph: None,
									node,
								})
							})
							.or_else(|| {
								state.graph.nodes[*index].object.as_ref().map(|object| {
									tg::graph::data::Edge::Object(
										object.id.clone().try_into().unwrap(),
									)
								})
							})
							.unwrap(),
					),
					None => None,
				};
				let path = symlink.path.clone();
				tg::graph::data::Node::Symlink(tg::graph::data::Symlink { artifact, path })
			},
			Variant::Object => return Ok(()),
		};
		graph.nodes.push(node);
		Ok(())
	}

	fn create_normal_object(state: &mut State, index: usize) -> tg::Result<()> {
		// Create an object for the node.
		let (kind, data) = match &state.graph.nodes[index].variant {
			Variant::Directory(directory) => {
				let kind = tg::object::Kind::Directory;
				let entries = directory
					.entries
					.iter()
					.map(|(name, index)| {
						let name = name.clone();
						let id = state.graph.nodes[*index]
							.object
							.as_ref()
							.unwrap()
							.id
							.clone()
							.try_into()
							.unwrap();
						let edge = tg::graph::data::Edge::Object(id);
						(name, edge)
					})
					.collect();
				let data = tg::directory::Data::Node(tg::directory::data::Node { entries });
				let data = tg::object::Data::from(data);
				(kind, data)
			},
			Variant::File(file) => {
				let kind = tg::object::Kind::File;
				let contents = match file.blob.as_ref().unwrap() {
					Blob::Create(blob) => blob.id.clone(),
					Blob::Id(id) => id.clone(),
				};
				let dependencies = file
					.dependencies
					.iter()
					.map(|(reference, referent)| {
						let referent = referent
							.as_ref()
							.ok_or_else(|| tg::error!(%reference, "unresolved reference"))?;
						let item = match &referent.item {
							Either::Left(id) => id.clone(),
							Either::Right(index) => state.graph.nodes[*index]
								.object
								.as_ref()
								.unwrap()
								.id
								.clone(),
						};
						let item = tg::graph::data::Edge::Object(item);
						let referent = tg::Referent {
							item,
							path: referent.path.clone(),
							tag: referent.tag.clone(),
						};
						Ok::<_, tg::Error>((reference.clone(), referent))
					})
					.try_collect()?;
				let executable = file.executable;
				let data = tg::file::Data::Node(tg::file::data::Node {
					contents: Some(contents),
					dependencies,
					executable,
				});
				let data = tg::object::Data::from(data);
				(kind, data)
			},
			Variant::Symlink(symlink) => {
				let kind = tg::object::Kind::Symlink;
				let artifact = match &symlink.artifact {
					Some(Either::Left(artifact)) => Some(artifact.clone()),
					Some(Either::Right(index)) => Some(
						state.graph.nodes[*index]
							.object
							.as_ref()
							.map(|object| object.id.clone())
							.unwrap()
							.try_into()
							.unwrap(),
					),
					None => None,
				};
				let artifact = artifact.map(tg::graph::data::Edge::Object);
				let path = symlink.path.clone();
				let data = tg::object::Data::from(tg::symlink::data::Symlink::Node(
					tg::symlink::data::Node { artifact, path },
				));
				(kind, data)
			},
			Variant::Object => return Ok(()),
		};

		// Create the object.
		let bytes = data
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		let id = tg::object::Id::new(kind, &bytes);

		// Validate if this object came from the artifacts directory.
		if let Some(expected) = &state.graph.nodes[index].id {
			if expected != &id {
				return Err(tg::error!(%expected, %found = id, "artifacts directory is corrupted"));
			}
		}

		let object = Object {
			bytes: Some(bytes),
			data: Some(data),
			id,
		};
		state.graph.nodes[index].object.replace(object);
		Ok(())
	}
}
