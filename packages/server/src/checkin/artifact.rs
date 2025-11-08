use {
	super::graph::Variant,
	crate::{
		Server,
		checkin::{
			Graph, IndexCacheEntryMessages, IndexObjectMessages, StoreArgs,
			graph::{Contents, Petgraph},
		},
	},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, path::Path},
	tangram_client::prelude::*,
};

impl Server {
	#[expect(clippy::too_many_arguments)]
	pub(super) fn checkin_create_artifacts(
		arg: &tg::checkin::Arg,
		graph: &mut Graph,
		next: usize,
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		cache_entry_messages: &mut IndexCacheEntryMessages,
		root: &Path,
		touched_at: i64,
	) -> tg::Result<()> {
		// Run Tarjan's algorithm and reverse the order of each strongly connected component.
		let mut sccs = petgraph::algo::tarjan_scc(&Petgraph { graph, next });
		for scc in &mut sccs {
			if scc.len() > 1 {
				scc.reverse();
			}
		}

		// Create objects for each strongly connected component. If the strongly connected component has only one node with no cycle, then create a node artifact. Otherwise, create a graph and one reference artifact for each node.
		for scc in &sccs {
			if scc.len() == 1
				&& !graph
					.nodes
					.get(&scc[0])
					.unwrap()
					.children()
					.contains(&scc[0])
			{
				Self::checkin_create_node_artifact(
					graph,
					store_args,
					object_messages,
					scc[0],
					touched_at,
				)?;
			} else {
				Self::checkin_create_graph(graph, store_args, object_messages, scc, touched_at)?;
			}
		}

		// Update blob cache references now that artifact IDs are known.
		Self::checkin_update_blob_cache_references(
			arg,
			graph,
			store_args,
			object_messages,
			cache_entry_messages,
			root,
			&sccs,
			touched_at,
		);

		Ok(())
	}

	fn checkin_create_node_artifact(
		graph: &mut Graph,
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		index: usize,
		touched_at: i64,
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
				let contents = match &file.contents {
					Some(Contents::Write(output)) => output.id.clone(),
					Some(Contents::Id { id, .. }) => id.clone(),
					None => return Err(tg::error!("expected the contents to be set")),
				};
				let dependencies = file
					.dependencies
					.iter()
					.map(|(reference, referent)| {
						let reference = reference.clone();
						let Some(referent) = referent else {
							return Ok::<_, tg::Error>((reference, None));
						};

						let edge = match referent.item() {
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
		let (id, complete, metadata) = Self::checkin_create_artifact(
			graph,
			&data,
			&[index],
			store_args,
			object_messages,
			touched_at,
		)?;

		// Update the node.
		let node = graph.nodes.get_mut(&index).unwrap();
		node.complete = complete;
		node.metadata = Some(metadata);
		node.id.replace(id.clone());
		graph.ids.entry(id).or_default().push(index);

		Ok(())
	}

	fn checkin_create_graph(
		graph: &mut Graph,
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		scc: &[usize],
		touched_at: i64,
	) -> tg::Result<()> {
		// Create the nodes.
		let mut nodes = Vec::with_capacity(scc.len());
		for index in scc {
			Self::checkin_create_graph_node(graph, scc, &mut nodes, *index)?;
		}

		// Create the graph object.
		let data = tg::graph::Data { nodes }.into();
		let (id, _, _) = Self::checkin_create_artifact(
			graph,
			&data,
			scc,
			store_args,
			object_messages,
			touched_at,
		)?;

		// Create reference artifacts.
		let graph_id = tg::graph::Id::try_from(id).unwrap();
		for (local, global) in scc.iter().copied().enumerate() {
			Self::checkin_create_reference_artifact(
				graph,
				store_args,
				object_messages,
				&graph_id,
				local,
				global,
				touched_at,
			)?;
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
				let contents = match &file.contents {
					Some(Contents::Write(output)) => output.id.clone(),
					Some(Contents::Id { id, .. }) => id.clone(),
					None => return Err(tg::error!("expected the contents to be set")),
				};
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
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		graph_id: &tg::graph::Id,
		local: usize,
		global: usize,
		touched_at: i64,
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
		let (id, complete, metadata) = Self::checkin_create_artifact(
			graph,
			&data,
			&[global],
			store_args,
			object_messages,
			touched_at,
		)?;

		// Update the node.
		let node = graph.nodes.get_mut(&global).unwrap();
		node.complete = complete;
		node.metadata = Some(metadata);
		node.id.replace(id.clone());
		graph.ids.entry(id).or_default().push(global);

		Ok(())
	}

	fn checkin_create_artifact(
		graph: &Graph,
		data: &tg::object::Data,
		scc: &[usize],
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		touched_at: i64,
	) -> tg::Result<(tg::object::Id, bool, tg::object::Metadata)> {
		let kind = data.kind();
		let bytes = data
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		let id = tg::object::Id::new(kind, &bytes);
		let mut children_ids = BTreeSet::new();
		data.children(&mut children_ids);
		let children = children_ids.iter().map(|id| {
			if let Some(nodes) = graph.ids.get(id) {
				if let Some(index) = nodes.first() {
					let node = graph.nodes.get(index).unwrap();
					(node.complete, node.metadata.clone())
				} else {
					(false, None)
				}
			} else if let Ok(id) = id.try_unwrap_blob_ref() {
				scc.iter()
					.find_map(|&index| {
						let node = graph.nodes.get(&index)?;
						let file = node.variant.try_unwrap_file_ref().ok()?;
						file.contents.as_ref().and_then(|contents| match contents {
							Contents::Write(output) if &output.id == id => {
								let metadata = tg::object::Metadata {
									count: Some(output.count),
									depth: Some(output.depth),
									weight: Some(output.weight),
								};
								Some((true, Some(metadata)))
							},
							Contents::Id {
								id: id_,
								complete,
								metadata,
							} if id_ == id => Some((*complete, metadata.clone())),
							_ => None,
						})
					})
					.unwrap_or((false, None))
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
		let size = bytes.len().to_u64().unwrap();

		// Create the store arg.
		let store_arg = crate::store::PutArg {
			bytes: Some(bytes),
			cache_reference: None,
			id: id.clone(),
			touched_at,
		};

		// Create the index message.
		let index_message = crate::index::message::PutObject {
			cache_entry: None,
			children: children_ids,
			complete,
			id: id.clone(),
			metadata: metadata.clone(),
			size,
			touched_at,
		};

		// Insert into maps.
		store_args.insert(id.clone(), store_arg);
		object_messages.insert(id.clone(), index_message);

		Ok((id, complete, metadata))
	}

	#[expect(clippy::too_many_arguments)]
	fn checkin_update_blob_cache_references(
		arg: &tg::checkin::Arg,
		graph: &Graph,
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		cache_entry_messages: &mut IndexCacheEntryMessages,
		root: &Path,
		sccs: &[Vec<usize>],
		touched_at: i64,
	) {
		for scc in sccs {
			for index in scc {
				// Get the node.
				let node = graph.nodes.get(index).unwrap();

				// Visit only files with write output.
				let Variant::File(file) = &node.variant else {
					continue;
				};
				let Some(Contents::Write(output)) = file.contents.as_ref() else {
					continue;
				};

				// Get the artifact and path.
				let (artifact, path) = if arg.options.destructive {
					let index = graph.paths.get(root).unwrap();
					let id = graph.nodes.get(index).unwrap().id.as_ref().unwrap().clone();
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

				// Create a cache entry message for this artifact.
				if !arg.options.destructive {
					cache_entry_messages.push(crate::index::message::PutCacheEntry {
						id: artifact.clone(),
						touched_at,
					});
				}

				// Update cache references for the blob and its children.
				let mut stack = vec![output.as_ref()];
				while let Some(output) = stack.pop() {
					let id: tg::object::Id = output.id.clone().into();

					// Create and set the cache reference.
					let cache_reference = crate::store::CacheReference {
						artifact: artifact.clone(),
						path: path.clone(),
						position: output.position,
						length: output.length,
					};
					store_args.get_mut(&id).unwrap().cache_reference = Some(cache_reference);
					object_messages.get_mut(&id).unwrap().cache_entry = Some(artifact.clone());

					// Add children to the stack.
					stack.extend(output.children.iter());
				}
			}
		}
	}
}
