use {
	super::graph::{Object, Variant},
	crate::{
		Server,
		checkin::{Graph, graph::Objects},
	},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, path::Path},
	tangram_client as tg,
};

impl Server {
	pub(super) fn checkin_create_artifacts(
		arg: &tg::checkin::Arg,
		graph: &mut Graph,
		objects: &mut Objects,
		root: &Path,
	) -> tg::Result<()> {
		// Run Tarjan's algorithm and reverse the order of each strongly connected component.
		let mut sccs = petgraph::algo::tarjan_scc(&*graph);
		for scc in &mut sccs {
			if scc.len() > 1 {
				scc.reverse();
			}
		}

		// Create objects for each strongly connected component. If the strongly connected component has only one node, then create a node artifact. Otherwise, create a graph and one reference artifact for each node.
		for scc in &sccs {
			if scc.len() == 1 {
				let index = scc[0];
				Self::checkin_create_node_artifact(graph, objects, index)?;
			} else {
				Self::checkin_create_graph(graph, objects, scc)?;
			}
		}

		// Update blob cache references now that artifact IDs are known.
		Self::checkin_update_blob_cache_references(arg, graph, objects, root, &sccs);

		Ok(())
	}

	fn checkin_create_node_artifact(
		graph: &mut Graph,
		objects: &mut Objects,
		index: usize,
	) -> tg::Result<()> {
		// Get the node.
		let node = graph.nodes.get(&index).unwrap();

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
									let node = graph.nodes.get(&reference.node).unwrap();
									let id = node.id.as_ref().unwrap().clone();
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
					.clone()
					.ok_or_else(|| tg::error!("expected the blob to be set"))?;
				let dependencies = file
					.dependencies
					.iter()
					.map(|(reference, referent)| {
						let reference = reference.clone();
						let Some(referent) = referent else {
							return Ok::<_, tg::Error>((reference, None));
						};
						let edge = referent.item();
						let edge = match edge {
							tg::graph::data::Edge::Reference(reference) => {
								if reference.graph.is_none() {
									let node = graph.nodes.get(&reference.node).unwrap();
									let id = node.id.as_ref().unwrap().clone();
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
						Ok::<_, tg::Error>((reference, Some(referent)))
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
									let node = graph.nodes.get(&reference.node).unwrap();
									let id = node.id.as_ref().unwrap().clone();
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
		let (id, object) = Self::checkin_create_artifact(graph, data, &[index])?;

		// Update the node.
		let node = graph.nodes.get_mut(&index).unwrap();
		node.complete = object.complete;
		node.metadata = object.metadata.clone();
		node.id.replace(id.clone());
		graph.ids.insert(id.clone(), index);

		// Add the object.
		objects.insert(id.clone(), object);

		Ok(())
	}

	fn checkin_create_graph(
		graph: &mut Graph,
		objects: &mut Objects,
		scc: &[usize],
	) -> tg::Result<()> {
		// Create the nodes.
		let mut nodes = Vec::with_capacity(scc.len());
		for index in scc {
			Self::checkin_create_graph_node(graph, scc, &mut nodes, *index)?;
		}

		// Create the graph object.
		let data = tg::graph::Data { nodes }.into();
		let (id, object) = Self::checkin_create_artifact(graph, data, scc)?;
		objects.insert(id.clone(), object);

		// Create reference artifacts.
		let id = tg::graph::Id::try_from(id).unwrap();
		for (local, global) in scc.iter().copied().enumerate() {
			Self::checkin_create_reference_artifact(graph, objects, &id, local, global)?;
		}

		Ok(())
	}

	fn checkin_create_graph_node(
		graph: &Graph,
		scc: &[usize],
		nodes: &mut Vec<tg::graph::data::Node>,
		index: usize,
	) -> tg::Result<()> {
		let node = graph.nodes.get(&index).unwrap();
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
										let node = graph.nodes.get(&reference.node).unwrap();
										let id = node.id.as_ref().unwrap().clone();
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
					.clone()
					.ok_or_else(|| tg::error!("expected the blob to be set"))?;
				let dependencies = file
					.dependencies
					.iter()
					.map(|(reference, referent)| {
						let Some(referent) = referent else {
							return Ok::<_, tg::Error>((reference.clone(), None));
						};
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
										let node = graph.nodes.get(&reference.node).unwrap();
										let id = node.id.as_ref().unwrap().clone();
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
						Ok::<_, tg::Error>((reference.clone(), Some(referent)))
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
									let node = graph.nodes.get(&reference.node).unwrap();
									let id = node.id.as_ref().unwrap().clone();
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
		graph: &mut Graph,
		objects: &mut Objects,
		graph_id: &tg::graph::Id,
		local: usize,
		global: usize,
	) -> tg::Result<()> {
		let node = graph.nodes.get(&global).unwrap();
		let data = match &node.variant {
			Variant::Directory(_) => {
				let reference = tg::graph::data::Reference {
					graph: Some(graph_id.clone()),
					node: local,
				};
				tg::directory::Data::Reference(reference).into()
			},
			Variant::File(_) => {
				let reference = tg::graph::data::Reference {
					graph: Some(graph_id.clone()),
					node: local,
				};
				tg::file::Data::Reference(reference).into()
			},
			Variant::Symlink(_) => {
				let reference = tg::graph::data::Reference {
					graph: Some(graph_id.clone()),
					node: local,
				};
				tg::symlink::Data::Reference(reference).into()
			},
		};
		let (id, object) = Self::checkin_create_artifact(graph, data, &[global])?;

		// Update the node.
		let node = graph.nodes.get_mut(&global).unwrap();
		node.complete = object.complete;
		node.metadata = object.metadata.clone();
		node.id.replace(id.clone());
		graph.ids.insert(id.clone(), global);

		// Add the object.
		objects.insert(id, object);

		Ok(())
	}

	fn checkin_create_artifact(
		graph: &Graph,
		data: tg::object::Data,
		scc: &[usize],
	) -> tg::Result<(tg::object::Id, Object)> {
		let kind = data.kind();
		let bytes = data
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		let id = tg::object::Id::new(kind, &bytes);
		let mut children = BTreeSet::new();
		data.children(&mut children);
		let children = children.into_iter().map(|id| {
			if let Some(&index) = graph.ids.get(&id) {
				let node = graph.nodes.get(&index).unwrap();
				(node.complete, node.metadata.clone())
			} else if let Ok(blob_id) = id.try_unwrap_blob_ref() {
				let metadata = scc.iter().find_map(|&index| {
					let node = graph.nodes.get(&index)?;
					let file = node.variant.try_unwrap_file_ref().ok()?;
					if file
						.contents
						.as_ref()
						.is_some_and(|contents| contents == blob_id)
					{
						file.contents_metadata.clone()
					} else {
						None
					}
				});
				(true, metadata)
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
			cache_reference_range: None,
			complete,
			data: Some(data),
			id: id.clone(),
			metadata: Some(metadata),
			size: bytes.len().to_u64().unwrap(),
		};
		Ok((id, object))
	}

	fn checkin_update_blob_cache_references(
		arg: &tg::checkin::Arg,
		graph: &Graph,
		objects: &mut Objects,
		root: &Path,
		sccs: &[Vec<usize>],
	) {
		for scc in sccs {
			for index in scc {
				let node = graph.nodes.get(index).unwrap();
				let Variant::File(file) = &node.variant else {
					continue;
				};
				let Some(blob) = &file.contents else {
					continue;
				};
				let (artifact, path) = if arg.options.destructive {
					let id = graph.nodes.get(&0).unwrap().id.as_ref().unwrap().clone();
					let path = node
						.path
						.as_ref()
						.unwrap()
						.strip_prefix(root)
						.unwrap()
						.to_owned();
					let path = if path == Path::new("") {
						None
					} else {
						Some(path)
					};
					(id.clone().try_into().unwrap(), path)
				} else {
					let id: tg::artifact::Id =
						node.id.as_ref().unwrap().clone().try_into().unwrap();
					(id, None)
				};

				// Update cache references for the blob and its children.
				let mut stack = vec![blob.clone()];
				while let Some(blob) = stack.pop() {
					let id: tg::object::Id = blob.clone().into();

					let Some(object) = objects.get(&id) else {
						continue;
					};
					let Some(cache_reference_range) = &object.cache_reference_range else {
						continue;
					};

					let position = cache_reference_range.position;
					let length = cache_reference_range.length;
					let children =
						if let Some(tg::object::Data::Blob(tg::blob::Data::Branch(branch))) =
							&object.data
						{
							branch.children.iter().map(|c| c.blob.clone()).collect()
						} else {
							Vec::new()
						};

					// Create and set the cache reference.
					let cache_reference = crate::store::CacheReference {
						artifact: artifact.clone(),
						path: path.clone(),
						position,
						length,
					};
					objects.get_mut(&id).unwrap().cache_reference = Some(cache_reference);

					// Add children to the stack.
					stack.extend(children);
				}
			}
		}
	}
}
