use super::{Blob, FileDependency, GraphObject, Object, State, Variant};
use crate::Server;
use futures::{FutureExt as _, TryStreamExt as _, stream::FuturesUnordered};
use itertools::Itertools as _;
use std::collections::BTreeMap;
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(super) async fn checkin_create_blobs(&self, state: &mut State) -> tg::Result<()> {
		let blobs = state
			.graph
			.nodes
			.iter()
			.enumerate()
			.filter_map(|(index, node)| {
				node.variant.try_unwrap_file_ref().ok().and_then(|_| {
					let path = node.path.as_ref()?.clone();
					let future = tokio::spawn({
						let server = self.clone();
						async move {
							let _permit = server.file_descriptor_semaphore.acquire().await;
							let mut file = tokio::fs::File::open(path.as_ref()).await.map_err(
								|source| tg::error!(!source, %path = path.display(), "failed to open the file"),
							)?;
							let blob = server.create_blob_inner(&mut file, None).await.map_err(
								|source| tg::error!(!source, %path = path.display(), "failed to create the blob"),
							)?;
							Ok::<_, tg::Error>((index, blob))
						}
					})
					.map(|result| result.unwrap());
					Some(future)
				})
			})
			.collect::<FuturesUnordered<_>>()
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
						let data = tg::directory::data::Directory::Graph {
							graph: object.id.clone(),
							node: local,
						};
						let data = tg::object::Data::from(data);
						(kind, data)
					},
					Variant::File(_) => {
						let kind = tg::object::Kind::File;
						let data = tg::file::data::File::Graph {
							graph: object.id.clone(),
							node: local,
						};
						let data = tg::object::Data::from(data);
						(kind, data)
					},
					Variant::Symlink(_) => {
						let kind = tg::object::Kind::Symlink;
						let data = tg::symlink::data::Symlink::Graph {
							graph: object.id.clone(),
							node: local,
						};
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
				let object = Object { bytes, data, id };
				state.graph.nodes[global].object = Some(object);
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
							.map(Either::Left)
							.or_else(|| {
								state.graph.nodes[*index].object.as_ref().map(|object| {
									Either::Right(object.id.clone().try_into().unwrap())
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
					.map(|dependency| match dependency {
						FileDependency::Import {
							import,
							node,
							path,
							subpath,
						} => {
							let index = node.ok_or_else(
								|| tg::error!(%import = import.reference, "unresolved import"),
							)?;
							let item = graph_indices
								.get(&index)
								.copied()
								.map(Either::Left)
								.or_else(|| {
									state.graph.nodes[index]
										.object
										.as_ref()
										.map(|object| Either::Right(object.id.clone()))
								})
								.unwrap();
							let path = path.clone();
							let subpath = subpath.clone();
							let tag = state.graph.nodes[index].tag.clone();
							let referent = tg::Referent {
								item,
								path,
								subpath,
								tag,
							};
							Ok::<_, tg::Error>((import.reference.clone(), referent))
						},
						FileDependency::Referent {
							reference,
							referent,
						} => {
							let item = match referent.item.as_ref().unwrap() {
								Either::Left(id) => Either::Right(id.clone()),
								Either::Right(index) => graph_indices
									.get(index)
									.copied()
									.map(Either::Left)
									.or_else(|| {
										state.graph.nodes[*index]
											.object
											.as_ref()
											.map(|object| Either::Right(object.id.clone()))
									})
									.unwrap(),
							};
							let referent = tg::Referent {
								item,
								path: referent.path.clone(),
								subpath: referent.subpath.clone(),
								tag: referent.tag.clone(),
							};
							Ok::<_, tg::Error>((reference.clone(), referent))
						},
					})
					.try_collect()?;
				let executable = file.executable;
				let data = tg::graph::data::File {
					contents,
					dependencies,
					executable,
				};
				tg::graph::data::Node::File(data)
			},
			Variant::Symlink(symlink) => {
				let target = symlink.target.clone();
				let data = tg::graph::data::Symlink::Target { target };
				tg::graph::data::Node::Symlink(data)
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
						(name, id)
					})
					.collect();
				let data = tg::directory::Data::Normal { entries };
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
					.map(|dependency| match dependency {
						FileDependency::Import {
							import,
							node,
							path,
							subpath,
						} => {
							let index = node.ok_or_else(
								|| tg::error!(%import = import.reference, "unresolved import"),
							)?;
							let item = state.graph.nodes[index].object.as_ref().unwrap().id.clone();
							let path = path.clone();
							let subpath = subpath.clone();
							let tag = state.graph.nodes[index].tag.clone();
							let referent = tg::Referent {
								item,
								path,
								subpath,
								tag,
							};
							Ok::<_, tg::Error>((import.reference.clone(), referent))
						},
						FileDependency::Referent {
							reference,
							referent,
						} => {
							let item = match referent
								.item
								.as_ref()
								.ok_or_else(|| tg::error!("unresolved reference"))?
							{
								Either::Left(id) => id.clone(),
								Either::Right(index) => state.graph.nodes[*index]
									.object
									.as_ref()
									.unwrap()
									.id
									.clone(),
							};
							let referent = tg::Referent {
								item,
								path: referent.path.clone(),
								subpath: referent.subpath.clone(),
								tag: referent.tag.clone(),
							};
							Ok::<_, tg::Error>((reference.clone(), referent))
						},
					})
					.try_collect()?;
				let executable = file.executable;
				let data = tg::file::Data::Normal {
					contents,
					dependencies,
					executable,
				};
				let data = tg::object::Data::from(data);
				(kind, data)
			},
			Variant::Symlink(symlink) => {
				let kind = tg::object::Kind::Symlink;
				let target = symlink.target.clone();
				let data = tg::symlink::Data::Target { target };
				let data = tg::object::Data::from(data);
				(kind, data)
			},
			Variant::Object => return Ok(()),
		};

		// Create the object.
		let bytes = data
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		let id = tg::object::Id::new(kind, &bytes);
		let object = Object { bytes, data, id };
		state.graph.nodes[index].object = Some(object);
		Ok(())
	}
}
