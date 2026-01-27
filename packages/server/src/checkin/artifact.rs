use {
	crate::{
		Server,
		checkin::{
			Graph, IndexCacheEntryMessages, IndexObjectMessages, StoreArgs,
			graph::{Contents, Node, Petgraph, Variant},
			path::Paths,
		},
		config::Checkin,
	},
	num::ToPrimitive as _,
	std::{
		collections::{BTreeMap, BTreeSet, hash_map::DefaultHasher},
		hash::{Hash, Hasher},
		path::Path,
	},
	tangram_client::prelude::*,
	tangram_messenger::prelude::*,
	tangram_store::prelude::*,
};

impl Server {
	#[expect(clippy::too_many_arguments)]
	pub(super) fn checkin_create_artifacts(
		config: &Checkin,
		arg: &tg::checkin::Arg,
		graph: &mut Graph,
		paths: &Paths,
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
					config,
					graph,
					paths,
					store_args,
					object_messages,
					scc[0],
					touched_at,
				)?;
			} else {
				Self::checkin_create_graph(
					graph,
					paths,
					store_args,
					object_messages,
					scc,
					touched_at,
				)?;
			}
		}

		// Update blob cache pointers now that artifact IDs are known.
		Self::checkin_update_blob_cache_pointers(
			arg,
			graph,
			store_args,
			object_messages,
			cache_entry_messages,
			root,
			&sccs,
			touched_at,
		);

		// Create a reference artifact for the path if necessary.
		let index = graph.paths.get(&arg.path).copied().unwrap();
		let node = graph.nodes.get(&index).unwrap();
		if let tg::graph::data::Edge::Pointer(pointer) = node.edge.as_ref().unwrap().clone() {
			Self::checkin_create_reference_artifact(
				graph,
				store_args,
				object_messages,
				pointer.graph.as_ref().unwrap(),
				pointer.index,
				index,
				touched_at,
			)?;
		}

		Ok(())
	}

	fn checkin_create_node_artifact(
		config: &Checkin,
		graph: &mut Graph,
		paths: &Paths,
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
				let entries: BTreeMap<String, tg::graph::data::Edge<tg::artifact::Id>> = directory
					.entries
					.iter()
					.map(|(name, edge)| {
						let name = name.clone();
						let edge = match edge {
							tg::graph::data::Edge::Pointer(pointer) => {
								if pointer.graph.is_none() {
									let node = graph.nodes.get(&pointer.index).unwrap();
									let edge = node.edge.as_ref().unwrap().clone();
									match edge {
										tg::graph::data::Edge::Pointer(pointer) => {
											tg::graph::data::Edge::Pointer(pointer)
										},
										tg::graph::data::Edge::Object(id) => {
											let id = id
												.try_into()
												.map_err(|_| tg::error!("expected an artifact"))?;
											tg::graph::data::Edge::Object(id)
										},
									}
								} else {
									tg::graph::data::Edge::Pointer(pointer.clone())
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

				// Build the directory structure, potentially as a tree for large directories.
				let node = Self::checkin_create_directory(
					config,
					entries,
					store_args,
					object_messages,
					touched_at,
				)?;
				tg::directory::Data::Node(node).into()
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
					.map(|(reference, option)| {
						let reference = reference.clone();
						let Some(dependency) = option else {
							return Ok((reference, None));
						};
						let edge = if let Some(edge) = paths.get(&(index, reference.clone())) {
							Some(edge.clone())
						} else {
							dependency.item().clone()
						};
						let edge = match edge {
							Some(tg::graph::data::Edge::Pointer(pointer))
								if pointer.graph.is_none() =>
							{
								let node = graph.nodes.get(&pointer.index).unwrap();
								Some(node.edge.as_ref().unwrap().clone())
							},
							Some(edge) => Some(edge),
							None => None,
						};
						let referent = dependency.0.clone().map(|_| edge);
						Ok((reference, Some(tg::graph::data::Dependency(referent))))
					})
					.collect::<tg::Result<_>>()?;
				let executable = file.executable;
				let module = file.module;
				let data = tg::file::data::Node {
					contents: Some(contents),
					dependencies,
					executable,
					module,
				};
				tg::file::Data::Node(data).into()
			},

			Variant::Symlink(symlink) => {
				let artifact = match &symlink.artifact {
					Some(edge) => {
						let edge = match edge {
							tg::graph::data::Edge::Pointer(pointer) => {
								if pointer.graph.is_none() {
									let node = graph.nodes.get(&pointer.index).unwrap();
									let edge = node.edge.as_ref().unwrap().clone();
									match edge {
										tg::graph::data::Edge::Pointer(pointer) => {
											tg::graph::data::Edge::Pointer(pointer)
										},
										tg::graph::data::Edge::Object(id) => {
											let id = id
												.try_into()
												.map_err(|_| tg::error!("expected an artifact"))?;
											tg::graph::data::Edge::Object(id)
										},
									}
								} else {
									tg::graph::data::Edge::Pointer(pointer.clone())
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
		let (id, stored, metadata) = Self::checkin_create_artifact(
			graph,
			&data,
			&[index],
			store_args,
			object_messages,
			touched_at,
		)?;

		// Update the node.
		let node = graph.nodes.get_mut(&index).unwrap();
		node.stored = crate::index::ObjectStored { subtree: stored };
		node.metadata = Some(metadata);
		node.edge.replace(tg::graph::data::Edge::Object(id.clone()));
		node.id.replace(id.clone());
		graph.ids.entry(id).or_default().push(index);

		Ok(())
	}

	fn checkin_create_graph(
		graph: &mut Graph,
		paths: &Paths,
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		scc: &[usize],
		touched_at: i64,
	) -> tg::Result<()> {
		// Compute canonical labels using the Weisfeiler-Leman algorithm.
		let canonical_labels = Self::checkin_graph_canonical_labels(graph, paths, scc)?;

		// Sort the SCC indices by canonical labels, using paths as a tiebreaker for locally
		// checked-in nodes with identical content (e.g., symmetric imports).
		let mut sorted_scc: Vec<usize> = scc.to_vec();
		sorted_scc.sort_by(|a, b| {
			canonical_labels[a]
				.cmp(&canonical_labels[b])
				.then_with(|| graph.nodes[a].path.cmp(&graph.nodes[b].path))
		});

		// Create a mapping from global node index to position in the sorted SCC.
		let scc_positions: BTreeMap<usize, usize> = sorted_scc
			.iter()
			.enumerate()
			.map(|(position, &global)| (global, position))
			.collect();

		// Create the nodes using the sorted order.
		let mut nodes = Vec::with_capacity(sorted_scc.len());
		for index in &sorted_scc {
			Self::checkin_create_graph_node(graph, paths, &scc_positions, &mut nodes, *index)?;
		}

		// Create the graph object.
		let data = tg::graph::Data { nodes }.into();
		let (id, _, _) = Self::checkin_create_artifact(
			graph,
			&data,
			&sorted_scc,
			store_args,
			object_messages,
			touched_at,
		)?;

		// Set edges and ids for the nodes in the graph.
		let graph_id = tg::graph::Id::try_from(id).unwrap();
		for (local, global) in sorted_scc.iter().copied().enumerate() {
			let node = graph.nodes.get_mut(&global).unwrap();
			let artifact_kind = node.variant.kind();
			let data = match &node.variant {
				Variant::Directory(_) => {
					let pointer = tg::graph::data::Pointer {
						graph: Some(graph_id.clone()),
						index: local,
						kind: artifact_kind,
					};
					tg::object::Data::Directory(tg::directory::Data::Pointer(pointer))
				},
				Variant::File(_) => {
					let pointer = tg::graph::data::Pointer {
						graph: Some(graph_id.clone()),
						index: local,
						kind: artifact_kind,
					};
					tg::object::Data::File(tg::file::Data::Pointer(pointer))
				},
				Variant::Symlink(_) => {
					let pointer = tg::graph::data::Pointer {
						graph: Some(graph_id.clone()),
						index: local,
						kind: artifact_kind,
					};
					tg::object::Data::Symlink(tg::symlink::Data::Pointer(pointer))
				},
			};
			let kind = data.kind();
			let bytes = data
				.serialize()
				.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
			let id = tg::object::Id::new(kind, &bytes);
			node.edge
				.replace(tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: Some(graph_id.clone()),
					index: local,
					kind: artifact_kind,
				}));
			node.id.replace(id);
		}

		Ok(())
	}

	fn checkin_create_graph_node(
		graph: &Graph,
		paths: &Paths,
		scc_positions: &BTreeMap<usize, usize>,
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
							tg::graph::data::Edge::Pointer(pointer) => {
								if pointer.graph.is_none() {
									if let Some(&scc_index) = scc_positions.get(&pointer.index) {
										tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
											graph: None,
											index: scc_index,
											kind: pointer.kind,
										})
									} else {
										let node = graph.nodes.get(&pointer.index).unwrap();
										let edge = node.edge.as_ref().unwrap().clone();
										match edge {
											tg::graph::data::Edge::Pointer(pointer) => {
												tg::graph::data::Edge::Pointer(pointer)
											},
											tg::graph::data::Edge::Object(id) => {
												let id = id.try_into().map_err(|_| {
													tg::error!("expected an artifact")
												})?;
												tg::graph::data::Edge::Object(id)
											},
										}
									}
								} else {
									tg::graph::data::Edge::Pointer(pointer.clone())
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
				let leaf = tg::graph::data::DirectoryLeaf { entries };
				let data = tg::graph::data::Directory::Leaf(leaf);
				tg::graph::data::Node::Directory(data)
			},
			Variant::File(file) => {
				let contents = match &file.contents {
					Some(Contents::Write(output)) => output.id.clone(),
					Some(Contents::Id { id, .. }) => id.clone(),
					None => return Err(tg::error!("expected the contents to be set")),
				};
				// Apply path options to dependencies using the paths map.
				let dependencies = file
					.dependencies
					.iter()
					.map(|(reference, option)| {
						let Some(dependency) = option else {
							return Ok((reference.clone(), None));
						};
						let edge = if let Some(edge) = paths.get(&(index, reference.clone())) {
							Some(edge.clone())
						} else {
							dependency.item().clone()
						};
						let edge = match edge {
							Some(tg::graph::data::Edge::Pointer(pointer))
								if pointer.graph.is_none() =>
							{
								if let Some(&scc_index) = scc_positions.get(&pointer.index) {
									let kind =
										graph.nodes.get(&pointer.index).unwrap().variant.kind();
									Some(tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
										graph: None,
										index: scc_index,
										kind,
									}))
								} else {
									let node = graph.nodes.get(&pointer.index).unwrap();
									Some(node.edge.as_ref().unwrap().clone())
								}
							},
							Some(edge) => Some(edge),
							None => None,
						};
						let referent = tg::Referent {
							item: edge,
							options: dependency.options.clone(),
						};
						Ok((
							reference.clone(),
							Some(tg::graph::data::Dependency(referent)),
						))
					})
					.collect::<tg::Result<_>>()?;
				let executable = file.executable;
				let module = file.module;
				let data = tg::graph::data::File {
					contents: Some(contents),
					dependencies,
					executable,
					module,
				};
				tg::graph::data::Node::File(data)
			},
			Variant::Symlink(symlink) => {
				let artifact = if let Some(edge) = &symlink.artifact {
					let edge = match edge {
						tg::graph::data::Edge::Pointer(pointer) => {
							if pointer.graph.is_none() {
								if let Some(&scc_index) = scc_positions.get(&pointer.index) {
									tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
										graph: None,
										index: scc_index,
										kind: pointer.kind,
									})
								} else {
									let node = graph.nodes.get(&pointer.index).unwrap();
									let edge = node.edge.as_ref().unwrap().clone();
									match edge {
										tg::graph::data::Edge::Pointer(pointer) => {
											tg::graph::data::Edge::Pointer(pointer)
										},
										tg::graph::data::Edge::Object(id) => {
											let id = id
												.try_into()
												.map_err(|_| tg::error!("expected an artifact"))?;
											tg::graph::data::Edge::Object(id)
										},
									}
								}
							} else {
								tg::graph::data::Edge::Pointer(pointer.clone())
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
		let artifact_kind = node.variant.kind();
		let data = match &node.variant {
			Variant::Directory(_) => {
				let pointer = tg::graph::data::Pointer {
					graph: Some(graph_id.clone()),
					index: local,
					kind: artifact_kind,
				};
				tg::directory::Data::Pointer(pointer).into()
			},
			Variant::File(_) => {
				let pointer = tg::graph::data::Pointer {
					graph: Some(graph_id.clone()),
					index: local,
					kind: artifact_kind,
				};
				tg::file::Data::Pointer(pointer).into()
			},
			Variant::Symlink(_) => {
				let pointer = tg::graph::data::Pointer {
					graph: Some(graph_id.clone()),
					index: local,
					kind: artifact_kind,
				};
				tg::symlink::Data::Pointer(pointer).into()
			},
		};
		let (_id, stored, metadata) = Self::checkin_create_artifact(
			graph,
			&data,
			&[global],
			store_args,
			object_messages,
			touched_at,
		)?;

		// Update the node.
		let node = graph.nodes.get_mut(&global).unwrap();
		node.stored = crate::index::ObjectStored { subtree: stored };
		node.metadata = Some(metadata);

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
					(node.stored.subtree, node.metadata.clone())
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
								Some((true, Some(output.metadata.clone())))
							},
							Contents::Id {
								id: id_,
								stored,
								metadata,
							} if id_ == id => Some((stored.subtree, metadata.clone())),
							_ => None,
						})
					})
					.unwrap_or((false, None))
			} else {
				(false, None)
			}
		});
		let mut stored = true;
		let (node_solvable, node_solved) = if matches!(
			data,
			tg::object::Data::Directory(tg::directory::Data::Pointer(_))
				| tg::object::Data::File(tg::file::Data::Pointer(_))
				| tg::object::Data::Symlink(tg::symlink::Data::Pointer(_))
		) {
			(false, true)
		} else {
			scc.iter()
				.fold((false, true), |(solvable, solved), &index| {
					let node = graph.nodes.get(&index).unwrap();
					if let Variant::File(file) = &node.variant {
						let file_solvable =
							file.dependencies.keys().any(tg::Reference::is_solvable);
						let file_solved = file
							.dependencies
							.iter()
							.filter(|(reference, _)| reference.is_solvable())
							.all(|(_, option)| option.is_some());
						(solvable || file_solvable, solved && file_solved)
					} else {
						(solvable, solved)
					}
				})
		};
		let mut metadata = tg::object::Metadata {
			node: tg::object::metadata::Node {
				size: bytes.len().to_u64().unwrap(),
				solvable: node_solvable,
				solved: node_solved,
			},
			subtree: tg::object::metadata::Subtree {
				count: Some(1),
				depth: Some(1),
				size: Some(bytes.len().to_u64().unwrap()),
				solvable: Some(node_solvable),
				solved: Some(node_solved),
			},
		};
		for (child_stored, child_metadata) in children {
			stored = stored && child_stored;
			metadata.subtree.solvable = metadata
				.subtree
				.solvable
				.zip(child_metadata.as_ref().and_then(|m| m.subtree.solvable))
				.map(|(a, b)| a || b);
			metadata.subtree.solved = metadata
				.subtree
				.solved
				.zip(child_metadata.as_ref().and_then(|m| m.subtree.solved))
				.map(|(a, b)| a && b);
			metadata.subtree.count = metadata
				.subtree
				.count
				.zip(child_metadata.as_ref().and_then(|m| m.subtree.count))
				.map(|(a, b)| a + b);
			metadata.subtree.depth = metadata
				.subtree
				.depth
				.zip(child_metadata.as_ref().and_then(|m| m.subtree.depth))
				.map(|(a, b)| a.max(1 + b));
			metadata.subtree.size = metadata
				.subtree
				.size
				.zip(child_metadata.as_ref().and_then(|m| m.subtree.size))
				.map(|(a, b)| a + b);
		}

		// Create the store arg.
		let store_arg = crate::store::PutObjectArg {
			bytes: Some(bytes),
			cache_pointer: None,
			id: id.clone(),
			touched_at,
		};

		// Create the index message.
		let index_message = crate::index::message::PutObject {
			cache_entry: None,
			children: children_ids,
			id: id.clone(),
			metadata: metadata.clone(),
			stored: crate::index::ObjectStored { subtree: stored },
			touched_at,
		};

		// Insert into maps.
		store_args.insert(id.clone(), store_arg);
		object_messages.insert(id.clone(), index_message);

		Ok((id, stored, metadata))
	}

	#[expect(clippy::too_many_arguments)]
	fn checkin_update_blob_cache_pointers(
		arg: &tg::checkin::Arg,
		graph: &Graph,
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		cache_entry_messages: &mut IndexCacheEntryMessages,
		root: &Path,
		sccs: &[Vec<usize>],
		touched_at: i64,
	) {
		// Skip if cache pointers are disabled.
		if !arg.options.cache_pointers {
			return;
		}

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
					let root_index = graph.paths.get(root).unwrap();
					let root_node = graph.nodes.get(root_index).unwrap();
					let id = root_node.id.as_ref().unwrap().clone();
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

				// Update cache pointers for the blob and its children.
				let mut stack = vec![output.as_ref()];
				while let Some(output) = stack.pop() {
					let id: tg::object::Id = output.id.clone().into();

					// Create and set the cache pointer.
					let cache_pointer = crate::store::CachePointer {
						artifact: artifact.clone(),
						path: path.clone(),
						position: output.position,
						length: output.length,
					};
					store_args.get_mut(&id).unwrap().cache_pointer = Some(cache_pointer);
					object_messages.get_mut(&id).unwrap().cache_entry = Some(artifact.clone());

					// Add children to the stack.
					stack.extend(output.children.iter());
				}
			}
		}
	}

	pub(super) async fn checkin_store_and_index_pointer_artifact(
		&self,
		node: &Node,
		pointer: &tg::graph::data::Pointer,
	) -> tg::Result<tg::artifact::Id> {
		// Create the pointer artifact data.
		let data: tg::object::Data = match &node.variant {
			Variant::Directory(_) => tg::directory::Data::Pointer(pointer.clone()).into(),
			Variant::File(_) => tg::file::Data::Pointer(pointer.clone()).into(),
			Variant::Symlink(_) => tg::symlink::Data::Pointer(pointer.clone()).into(),
		};

		// Serialize and compute the ID.
		let bytes = data
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the reference artifact"))?;
		let id = tg::object::Id::new(data.kind(), &bytes);
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Store the object.
		let store_arg = crate::store::PutObjectArg {
			bytes: Some(bytes),
			cache_pointer: None,
			id: id.clone(),
			touched_at,
		};
		self.store
			.put_object(store_arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the reference artifact"))?;

		// Index the object.
		let mut children = std::collections::BTreeSet::new();
		data.children(&mut children);
		let index_message = crate::index::message::PutObject {
			cache_entry: None,
			children,
			id: id.clone(),
			metadata: node.metadata.clone().unwrap_or_default(),
			stored: node.stored.clone(),
			touched_at,
		};
		let message = crate::index::Message::PutObject(index_message);
		let message = crate::index::message::Messages(vec![message]);
		self.messenger
			.stream_publish("index".to_owned(), message)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the index message"))?
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the index message"))?;

		let id = id.try_into().unwrap();

		Ok(id)
	}

	/// Compute initial labels for graph nodes by serializing node data with intra-SCC references
	/// normalized and external references resolved to object IDs.
	fn checkin_graph_node_initial_label(
		graph: &Graph,
		paths: &Paths,
		scc: &[usize],
		index: usize,
	) -> tg::Result<Vec<u8>> {
		// Create the node data with normalized references.
		let node = graph.nodes.get(&index).unwrap();
		let data = match &node.variant {
			Variant::File(file) => {
				let contents = match &file.contents {
					Some(Contents::Write(output)) => Some(output.id.clone()),
					Some(Contents::Id { id, .. }) => Some(id.clone()),
					None => None,
				};
				let dependencies = file
					.dependencies
					.iter()
					.map(|(reference, option)| {
						let dep = option.as_ref().map(|dependency| {
							let edge = paths
								.get(&(index, reference.clone()))
								.cloned()
								.or_else(|| dependency.item().clone());
							let edge = match edge {
								Some(tg::graph::data::Edge::Pointer(pointer))
									if pointer.graph.is_none() && scc.contains(&pointer.index) =>
								{
									None
								},
								Some(tg::graph::data::Edge::Pointer(pointer))
									if pointer.graph.is_none() =>
								{
									let node = graph.nodes.get(&pointer.index).unwrap();
									let id = node.id.as_ref().unwrap();
									Some(tg::graph::data::Edge::Object(id.clone()))
								},
								other => other,
							};
							tg::graph::data::Dependency(tg::Referent {
								item: edge,
								options: dependency.options.clone(),
							})
						});
						(reference.clone(), dep)
					})
					.collect();
				tg::graph::data::Node::File(tg::graph::data::File {
					contents,
					dependencies,
					executable: file.executable,
					module: file.module,
				})
			},
			Variant::Directory(directory) => {
				let entries = directory
					.entries
					.iter()
					.map(|(name, edge)| {
						let edge = match edge {
							tg::graph::data::Edge::Pointer(pointer)
								if pointer.graph.is_none() && scc.contains(&pointer.index) =>
							{
								tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
									graph: None,
									index: 0,
									kind: pointer.kind,
								})
							},
							tg::graph::data::Edge::Pointer(pointer) if pointer.graph.is_none() => {
								let node = graph.nodes.get(&pointer.index).unwrap();
								let id = node.id.as_ref().unwrap().clone().try_into().unwrap();
								tg::graph::data::Edge::Object(id)
							},
							other => other.clone(),
						};
						(name.clone(), edge)
					})
					.collect();
				tg::graph::data::Node::Directory(tg::graph::data::Directory::Leaf(
					tg::graph::data::DirectoryLeaf { entries },
				))
			},
			Variant::Symlink(symlink) => {
				let artifact = symlink.artifact.as_ref().map(|edge| {
					let edge: tg::graph::data::Edge<tg::artifact::Id> = match edge {
						tg::graph::data::Edge::Pointer(pointer)
							if pointer.graph.is_none() && scc.contains(&pointer.index) =>
						{
							tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
								graph: None,
								index: 0,
								kind: pointer.kind,
							})
						},
						tg::graph::data::Edge::Pointer(pointer) if pointer.graph.is_none() => {
							let node = graph.nodes.get(&pointer.index).unwrap();
							let id = node.id.as_ref().unwrap().clone().try_into().unwrap();
							tg::graph::data::Edge::Object(id)
						},
						other => other.clone(),
					};
					edge
				});
				tg::graph::data::Node::Symlink(tg::graph::data::Symlink {
					artifact,
					path: symlink.path.clone(),
				})
			},
		};

		// Serialize the node data.
		let bytes = serde_json::to_vec(&data)
			.map_err(|source| tg::error!(!source, "failed to serialize the node"))?;

		Ok(bytes)
	}

	/// Compute canonical labels using the Weisfeiler-Leman algorithm. This produces a deterministic ordering of nodes in a strongly connected component.
	fn checkin_graph_canonical_labels(
		graph: &Graph,
		paths: &Paths,
		scc: &[usize],
	) -> tg::Result<BTreeMap<usize, u64>> {
		// Compute initial labels from serialized node data.
		let mut labels: BTreeMap<usize, u64> = BTreeMap::new();
		for &index in scc {
			let data = Self::checkin_graph_node_initial_label(graph, paths, scc, index)?;
			let mut hasher = DefaultHasher::new();
			data.hash(&mut hasher);
			labels.insert(index, hasher.finish());
		}

		// Collect edges within the SCC (both outgoing and incoming for undirected-style refinement).
		let mut outgoing: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
		let mut incoming: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
		for &index in scc {
			outgoing.insert(index, Vec::new());
			incoming.insert(index, Vec::new());
		}
		for &index in scc {
			let node = graph.nodes.get(&index).unwrap();
			for child in node.children() {
				if scc.contains(&child) {
					outgoing.get_mut(&index).unwrap().push(child);
					incoming.get_mut(&child).unwrap().push(index);
				}
			}
		}

		// Sort edges for determinism.
		for edges in outgoing.values_mut() {
			edges.sort_unstable();
		}
		for edges in incoming.values_mut() {
			edges.sort_unstable();
		}

		// Iteratively refine labels until stable.
		let max_iterations = scc.len() + 1;
		for _ in 0..max_iterations {
			let mut new_labels: BTreeMap<usize, u64> = BTreeMap::new();

			for &index in scc {
				let mut hasher = DefaultHasher::new();

				// Hash the current label.
				labels[&index].hash(&mut hasher);

				// Hash sorted outgoing neighbor labels.
				let mut out_labels: Vec<u64> =
					outgoing[&index].iter().map(|&n| labels[&n]).collect();
				out_labels.sort_unstable();
				out_labels.hash(&mut hasher);

				// Hash sorted incoming neighbor labels.
				let mut in_labels: Vec<u64> =
					incoming[&index].iter().map(|&n| labels[&n]).collect();
				in_labels.sort_unstable();
				in_labels.hash(&mut hasher);

				new_labels.insert(index, hasher.finish());
			}

			// Check for stability.
			if new_labels == labels {
				break;
			}
			labels = new_labels;
		}

		Ok(labels)
	}

	fn checkin_create_directory(
		config: &Checkin,
		entries: BTreeMap<String, tg::graph::data::Edge<tg::artifact::Id>>,
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		touched_at: i64,
	) -> tg::Result<tg::graph::data::Directory> {
		// If the entries fit in a single leaf, then return a leaf.
		if entries.len() <= config.directory.max_leaf_entries {
			return Ok(tg::graph::data::Directory::Leaf(
				tg::graph::data::DirectoryLeaf { entries },
			));
		}

		// Split entries into chunks and create leaf children.
		let entries_vec: Vec<_> = entries.into_iter().collect();
		let mut children = Vec::new();

		for chunk in entries_vec.chunks(config.directory.max_leaf_entries) {
			let chunk_entries: BTreeMap<_, _> = chunk.iter().cloned().collect();
			let count = chunk_entries.len().to_u64().unwrap();
			let last = chunk_entries.keys().last().unwrap().clone();

			// Create the leaf directory.
			let leaf = tg::graph::data::DirectoryLeaf {
				entries: chunk_entries,
			};
			let leaf_data = tg::graph::data::Directory::Leaf(leaf);

			// Create the leaf directory node and get its ID.
			let id = Self::checkin_create_directory_node(
				&leaf_data,
				store_args,
				object_messages,
				touched_at,
			)?;

			children.push(tg::graph::data::DirectoryChild {
				directory: tg::graph::data::Edge::Object(id),
				count,
				last,
			});
		}

		// Create intermediate branches if needed.
		Self::checkin_create_directory_branch(
			config,
			children,
			store_args,
			object_messages,
			touched_at,
		)
	}

	/// Recursively create branch nodes when there are too many children.
	fn checkin_create_directory_branch(
		config: &Checkin,
		children: Vec<tg::graph::data::DirectoryChild>,
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		touched_at: i64,
	) -> tg::Result<tg::graph::data::Directory> {
		// If the children fit in a single branch, return a branch.
		if children.len() <= config.directory.max_branch_children {
			return Ok(tg::graph::data::Directory::Branch(
				tg::graph::data::DirectoryBranch { children },
			));
		}

		// Split children into chunks and create branch children.
		let mut branch_children = Vec::new();

		for chunk in children.chunks(config.directory.max_branch_children) {
			let chunk_vec: Vec<_> = chunk.to_vec();
			let count = chunk_vec.iter().map(|c| c.count).sum();
			let last = chunk_vec.last().unwrap().last.clone();

			// Create the branch.
			let branch_data =
				tg::graph::data::Directory::Branch(tg::graph::data::DirectoryBranch {
					children: chunk_vec,
				});

			// Store the branch and get its ID.
			let id = Self::checkin_create_directory_node(
				&branch_data,
				store_args,
				object_messages,
				touched_at,
			)?;

			branch_children.push(tg::graph::data::DirectoryChild {
				directory: tg::graph::data::Edge::Object(id),
				count,
				last,
			});
		}

		// Recursively build if still too many children.
		Self::checkin_create_directory_branch(
			config,
			branch_children,
			store_args,
			object_messages,
			touched_at,
		)
	}

	/// Create a directory node and return its ID.
	fn checkin_create_directory_node(
		directory: &tg::graph::data::Directory,
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		touched_at: i64,
	) -> tg::Result<tg::directory::Id> {
		// Create the directory data.
		let data: tg::object::Data = tg::directory::Data::Node(directory.clone()).into();
		let kind = data.kind();
		let bytes = data
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the directory"))?;
		let id = tg::object::Id::new(kind, &bytes);

		// Collect children IDs.
		let mut children_ids = BTreeSet::new();
		data.children(&mut children_ids);

		// Compute metadata by aggregating from children.
		let node_size = bytes.len().to_u64().unwrap();
		let mut metadata = tg::object::Metadata {
			node: tg::object::metadata::Node {
				size: node_size,
				solvable: false,
				solved: true,
			},
			subtree: tg::object::metadata::Subtree {
				count: Some(1),
				depth: Some(1),
				size: Some(node_size),
				solvable: Some(false),
				solved: Some(true),
			},
		};

		// Aggregate metadata from children.
		for child_id in &children_ids {
			let child_metadata = object_messages.get(child_id).map(|msg| &msg.metadata);

			metadata.subtree.count = metadata
				.subtree
				.count
				.zip(child_metadata.and_then(|m| m.subtree.count))
				.map(|(a, b)| a + b);
			metadata.subtree.depth = metadata
				.subtree
				.depth
				.zip(child_metadata.and_then(|m| m.subtree.depth))
				.map(|(a, b)| a.max(1 + b));
			metadata.subtree.size = metadata
				.subtree
				.size
				.zip(child_metadata.and_then(|m| m.subtree.size))
				.map(|(a, b)| a + b);
			metadata.subtree.solvable = metadata
				.subtree
				.solvable
				.zip(child_metadata.and_then(|m| m.subtree.solvable))
				.map(|(a, b)| a || b);
			metadata.subtree.solved = metadata
				.subtree
				.solved
				.zip(child_metadata.and_then(|m| m.subtree.solved))
				.map(|(a, b)| a && b);
		}

		// Create the store arg.
		let store_arg = crate::store::PutObjectArg {
			bytes: Some(bytes),
			cache_pointer: None,
			id: id.clone(),
			touched_at,
		};

		// Create the index message.
		let index_message = crate::index::message::PutObject {
			cache_entry: None,
			children: children_ids,
			id: id.clone(),
			metadata,
			stored: crate::index::ObjectStored { subtree: true },
			touched_at,
		};

		// Insert into maps.
		store_args.insert(id.clone(), store_arg);
		object_messages.insert(id.clone(), index_message);

		Ok(id.try_into().unwrap())
	}
}
