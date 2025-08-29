use {
	super::state::{Object, State, Variant},
	crate::Server,
	indexmap::IndexMap,
	num::ToPrimitive,
	std::{collections::BTreeSet, path::Path},
	tangram_client as tg,
	tangram_either::Either,
};

impl Server {
	pub(super) fn checkin_create_objects(state: &mut State) -> tg::Result<()> {
		// Create the objects.
		state.objects = Some(IndexMap::new());

		// Run Tarjan's algorithm and reverse the order of each strongly connected component.
		let mut sccs = petgraph::algo::tarjan_scc(&state.graph);
		for scc in &mut sccs {
			if scc.len() > 1 {
				scc.reverse();
			}
		}

		// Create objects for each strongly connected component. If the strongly connected component has only one node, then create a node artifact. Otherwise, create a graph and one reference artifact for each node.
		for scc in &sccs {
			if scc.len() == 1 {
				let index = scc[0];
				Self::checkin_create_node_artifact(state, index)?;
			} else {
				Self::checkin_create_graph(state, scc)?;
			}
		}

		// Create objects for the blobs.
		let mut blobs = Self::checkin_create_blob_objects(state, &sccs);
		blobs.extend(state.objects.take().unwrap());
		state.objects.replace(blobs);

		Ok(())
	}

	fn checkin_create_node_artifact(state: &mut State, index: usize) -> tg::Result<()> {
		// Get the node.
		let node = &state.graph.nodes[index];

		// Create the object.
		let data = match &node.variant {
			Variant::Directory(directory) => {
				let entries = directory
					.entries
					.iter()
					.map(|(name, edge)| {
						let name = name.clone();
						let edge = match edge {
							tg::graph::data::Edge::Reference(reference) => {
								if reference.graph.is_none() {
									let node = &state.graph.nodes[reference.node];
									let id = node.object_id.as_ref().unwrap().clone();
									let id = id
										.try_into()
										.map_err(|_| tg::error!("expected an artifact"))?;
									tg::graph::data::Edge::Object(id)
								} else {
									let reference = reference.clone();
									tg::graph::data::Edge::Reference(reference)
								}
							},
							tg::graph::data::Edge::Object(id) => {
								let id = id.clone();
								tg::graph::data::Edge::Object(id)
							},
						};
						Ok::<_, tg::Error>((name, edge))
					})
					.collect::<tg::Result<_>>()?;
				let data = tg::directory::data::Node { entries };
				tg::directory::Data::Node(data).into()
			},
			Variant::File(file) => {
				let contents = file
					.contents
					.as_ref()
					.ok_or_else(|| tg::error!("expected the blob to be set"))?;
				let contents = match contents {
					Either::Left(blob) => blob.id.clone(),
					Either::Right(id) => id.clone(),
				};
				let dependencies = file
					.dependencies
					.iter()
					.map(|(reference, referent)| {
						let reference = reference.clone();
						let referent = referent.as_ref().ok_or_else(
							|| tg::error!(%reference, "expected the referent to be set"),
						)?;
						let edge = referent.item();
						let edge = match edge {
							tg::graph::data::Edge::Reference(reference) => {
								if reference.graph.is_none() {
									let node = &state.graph.nodes[reference.node];
									let id = node.object_id.as_ref().unwrap().clone();
									tg::graph::data::Edge::Object(id)
								} else {
									let reference = reference.clone();
									tg::graph::data::Edge::Reference(reference)
								}
							},
							tg::graph::data::Edge::Object(id) => {
								let id = id.clone();
								tg::graph::data::Edge::Object(id)
							},
						};
						let referent = referent.clone().map(|_| edge);
						Ok::<_, tg::Error>((reference, referent))
					})
					.collect::<tg::Result<_>>()?;
				let executable = file.executable;
				let data = tg::file::data::Node {
					contents: Some(contents),
					dependencies,
					executable,
				};
				tg::file::Data::Node(data).into()
			},
			Variant::Symlink(symlink) => {
				let artifact = match &symlink.artifact {
					Some(edge) => {
						let edge = match edge {
							tg::graph::data::Edge::Reference(reference) => {
								if reference.graph.is_none() {
									let node = &state.graph.nodes[reference.node];
									let id = node.object_id.as_ref().unwrap().clone();
									let id = id
										.try_into()
										.map_err(|_| tg::error!("expected an artifact"))?;
									tg::graph::data::Edge::Object(id)
								} else {
									let reference = reference.clone();
									tg::graph::data::Edge::Reference(reference)
								}
							},
							tg::graph::data::Edge::Object(id) => {
								let id = id.clone();
								tg::graph::data::Edge::Object(id)
							},
						};
						Some(edge)
					},
					None => None,
				};
				let path = symlink.path.clone();
				let data = tg::symlink::data::Node { artifact, path };
				tg::symlink::Data::Node(data).into()
			},
		};
		let (id, object) = Self::checkin_create_object(state, data)?;

		// Add the object.
		state.objects.as_mut().unwrap().insert(id.clone(), object);

		// Update the node.
		state.graph.nodes[index].object_id.replace(id.clone());

		Ok(())
	}

	fn checkin_create_graph(state: &mut State, scc: &[usize]) -> tg::Result<()> {
		// Create the nodes.
		let mut nodes = Vec::with_capacity(scc.len());
		for index in scc {
			Self::checkin_create_graph_node(state, scc, &mut nodes, *index)?;
		}

		// Create the graph object.
		let data = tg::graph::Data { nodes }.into();
		let (id, object) = Self::checkin_create_object(state, data)?;
		state.objects.as_mut().unwrap().insert(id.clone(), object);

		// Create reference artifacts.
		let id = tg::graph::Id::try_from(id).unwrap();
		for (local, global) in scc.iter().copied().enumerate() {
			Self::checkin_create_reference_artifact(state, &id, local, global)?;
		}

		Ok(())
	}

	fn checkin_create_graph_node(
		state: &mut State,
		scc: &[usize],
		nodes: &mut Vec<tg::graph::data::Node>,
		index: usize,
	) -> tg::Result<()> {
		let node = &state.graph.nodes[index];
		let node = match &node.variant {
			Variant::Directory(directory) => {
				let entries = directory
					.entries
					.iter()
					.map(|(name, edge)| {
						let name = name.clone();
						let edge = match edge {
							tg::graph::data::Edge::Reference(reference) => {
								if reference.graph.is_none() {
									if let Some(node) =
										scc.iter().position(|node| node == &reference.node)
									{
										tg::graph::data::Edge::Reference(
											tg::graph::data::Reference { graph: None, node },
										)
									} else {
										let node = &state.graph.nodes[reference.node];
										let id = node.object_id.as_ref().unwrap().clone();
										let id = id
											.try_into()
											.map_err(|_| tg::error!("expected an artifact"))?;
										tg::graph::data::Edge::Object(id)
									}
								} else {
									let reference = reference.clone();
									tg::graph::data::Edge::Reference(reference)
								}
							},
							tg::graph::data::Edge::Object(id) => {
								let id = id.clone();
								tg::graph::data::Edge::Object(id)
							},
						};
						Ok::<_, tg::Error>((name, edge))
					})
					.collect::<tg::Result<_>>()?;
				let data = tg::graph::data::Directory { entries };
				tg::graph::data::Node::Directory(data)
			},
			Variant::File(file) => {
				let contents = file
					.contents
					.as_ref()
					.ok_or_else(|| tg::error!("expected the blob to be set"))?;
				let contents = match contents {
					Either::Left(blob) => blob.id.clone(),
					Either::Right(id) => id.clone(),
				};
				let dependencies = file
					.dependencies
					.iter()
					.map(|(reference, referent)| {
						let referent = referent.as_ref().ok_or_else(
							|| tg::error!(%reference, "expected the referent to be set"),
						)?;
						let edge = referent.item();
						let edge = match edge {
							tg::graph::data::Edge::Reference(reference) => {
								if reference.graph.is_none() {
									if let Some(node) =
										scc.iter().position(|node| node == &reference.node)
									{
										tg::graph::data::Edge::Reference(
											tg::graph::data::Reference { graph: None, node },
										)
									} else {
										let node = &state.graph.nodes[reference.node];
										let id = node.object_id.as_ref().unwrap().clone();
										tg::graph::data::Edge::Object(id)
									}
								} else {
									let reference = reference.clone();
									tg::graph::data::Edge::Reference(reference)
								}
							},
							tg::graph::data::Edge::Object(id) => {
								let id = id.clone();
								tg::graph::data::Edge::Object(id)
							},
						};
						let referent = tg::Referent {
							item: edge,
							options: referent.options.clone(),
						};
						Ok::<_, tg::Error>((reference.clone(), referent))
					})
					.collect::<tg::Result<_>>()?;
				let executable = file.executable;
				let data = tg::graph::data::File {
					contents: Some(contents),
					dependencies,
					executable,
				};
				tg::graph::data::Node::File(data)
			},
			Variant::Symlink(symlink) => {
				let artifact = if let Some(edge) = &symlink.artifact {
					let edge = match edge {
						tg::graph::data::Edge::Reference(reference) => {
							if reference.graph.is_none() {
								if let Some(node) =
									scc.iter().position(|node| node == &reference.node)
								{
									tg::graph::data::Edge::Reference(tg::graph::data::Reference {
										graph: None,
										node,
									})
								} else {
									let node = &state.graph.nodes[reference.node];
									let id = node.object_id.as_ref().unwrap().clone();
									let id = id
										.try_into()
										.map_err(|_| tg::error!("expected an artifact"))?;
									tg::graph::data::Edge::Object(id)
								}
							} else {
								let reference = reference.clone();
								tg::graph::data::Edge::Reference(reference)
							}
						},
						tg::graph::data::Edge::Object(id) => {
							let id = id.clone();
							tg::graph::data::Edge::Object(id)
						},
					};
					Some(edge)
				} else {
					None
				};
				let path = symlink.path.clone();
				let data = tg::graph::data::Symlink { artifact, path };
				tg::graph::data::Node::Symlink(data)
			},
		};
		nodes.push(node);
		Ok(())
	}

	fn checkin_create_reference_artifact(
		state: &mut State,
		graph: &tg::graph::Id,
		local: usize,
		global: usize,
	) -> tg::Result<()> {
		let node = &state.graph.nodes[global];
		let data = match &node.variant {
			Variant::Directory(_) => {
				let reference = tg::graph::data::Reference {
					graph: Some(graph.clone()),
					node: local,
				};
				tg::directory::Data::Reference(reference).into()
			},
			Variant::File(_) => {
				let reference = tg::graph::data::Reference {
					graph: Some(graph.clone()),
					node: local,
				};
				tg::file::Data::Reference(reference).into()
			},
			Variant::Symlink(_) => {
				let reference = tg::graph::data::Reference {
					graph: Some(graph.clone()),
					node: local,
				};
				tg::symlink::Data::Reference(reference).into()
			},
		};
		let (id, object) = Self::checkin_create_object(state, data)?;
		state.objects.as_mut().unwrap().insert(id.clone(), object);
		state.graph.nodes[global].object_id.replace(id);
		Ok(())
	}

	fn checkin_create_object(
		state: &State,
		data: tg::object::Data,
	) -> tg::Result<(tg::object::Id, Object)> {
		let kind = data.kind();
		let bytes = data
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		let id = tg::object::Id::new(kind, &bytes);
		let children = data.children().collect::<BTreeSet<_>>();
		let children = children.into_iter().map(|id| {
			if let Ok(id) = id.try_unwrap_blob_ref()
				&& let Some(blob) = state.blobs.get(id)
				&& id == &blob.id
			{
				let complete = true;
				let metadata = Some(tg::object::Metadata {
					count: Some(blob.count),
					depth: Some(blob.depth),
					weight: Some(blob.weight),
				});
				(complete, metadata)
			} else if let Some(object) = state.objects.as_ref().unwrap().get(&id) {
				let complete = object.complete;
				let metadata = object.metadata.clone();
				(complete, metadata)
			} else {
				(false, None)
			}
		});
		let mut complete = true;
		let mut metadata = tg::object::Metadata {
			count: Some(1),
			depth: Some(1),
			weight: Some(bytes.len().to_u64().unwrap()),
		};
		for (child_complete, child_metadata) in children {
			complete = complete && child_complete;
			metadata.count = metadata
				.count
				.zip(child_metadata.as_ref().and_then(|metadata| metadata.count))
				.map(|(a, b)| a + b);
			metadata.depth = metadata
				.depth
				.zip(child_metadata.as_ref().and_then(|metadata| metadata.depth))
				.map(|(a, b)| a.max(1 + b));
			metadata.weight = metadata
				.weight
				.zip(child_metadata.as_ref().and_then(|metadata| metadata.weight))
				.map(|(a, b)| a + b);
		}
		let object = Object {
			bytes: Some(bytes.clone()),
			cache_reference: None,
			complete,
			data: Some(data),
			id: id.clone(),
			metadata: Some(metadata),
			size: bytes.len().to_u64().unwrap(),
		};
		Ok((id, object))
	}

	fn checkin_create_blob_objects(
		state: &mut State,
		sccs: &Vec<Vec<usize>>,
	) -> IndexMap<tg::object::Id, Object> {
		let root = state.graph.nodes[0].object_id.as_ref().unwrap().clone();
		let mut objects = IndexMap::default();
		for scc in sccs {
			for index in scc {
				let node = &state.graph.nodes[*index];
				let Variant::File(file) = &node.variant else {
					continue;
				};
				let Some(Either::Left(blob)) = &file.contents else {
					continue;
				};
				let mut stack = vec![blob];
				while let Some(blob) = stack.pop() {
					let (artifact, path) = if state.arg.destructive {
						let path = node
							.path
							.as_ref()
							.unwrap()
							.strip_prefix(&state.root_path)
							.unwrap()
							.to_owned();
						let path = if path == Path::new("") {
							None
						} else {
							Some(path)
						};
						(root.clone().try_into().unwrap(), path)
					} else {
						let id = node.object_id.as_ref().unwrap().clone().try_into().unwrap();
						(id, None)
					};
					let cache_reference = crate::store::CacheReference {
						artifact,
						path,
						position: blob.position,
						length: blob.length,
					};
					let metadata = tg::object::Metadata {
						count: Some(blob.count),
						depth: Some(blob.depth),
						weight: Some(blob.weight),
					};
					let object = Object {
						bytes: blob.bytes.clone(),
						cache_reference: Some(cache_reference),
						complete: true,
						data: blob.data.clone().map(Into::into),
						id: blob.id.clone().into(),
						metadata: Some(metadata),
						size: blob.size,
					};
					objects.insert(object.id.clone(), object);
					stack.extend(&blob.children);
				}
			}
		}
		objects
	}
}
