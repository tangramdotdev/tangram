use crate::Server;
use itertools::Itertools as _;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(crate) async fn create_lockfile_for_artifact(
		&self,
		artifact: &tg::Artifact,
		checkout_dependencies: bool,
	) -> tg::Result<tg::Lockfile> {
		// Create the state.
		let mut nodes = Vec::new();
		let mut visited = BTreeMap::new();
		let mut graphs = BTreeMap::new();

		// Create nodes in the lockfile for the graph.
		let root = self
			.get_or_create_lockfile_node_for_artifact(
				artifact,
				checkout_dependencies,
				&mut nodes,
				&mut visited,
				&mut graphs,
			)
			.await?;
		let nodes: Vec<_> = nodes
			.into_iter()
			.enumerate()
			.map(|(index, node)| {
				node.ok_or_else(
					|| tg::error!(%node = index, "invalid graph, failed to create lockfile node"),
				)
			})
			.try_collect()?;

		// // Strip nodes. TODO
		// let nodes = self.strip_lockfile_nodes(&nodes, root)?;

		// Create the lockfile.
		let lockfile = tg::Lockfile { nodes };

		Ok(lockfile)
	}

	async fn get_or_create_lockfile_node_for_artifact(
		&self,
		artifact: &tg::Artifact,
		checkout_dependencies: bool,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut BTreeMap<tg::artifact::Id, usize>,
		graphs: &mut BTreeMap<tg::graph::Id, Vec<usize>>,
	) -> tg::Result<usize> {
		let id = artifact.id(self).await?;
		if let Some(visited) = visited.get(&id) {
			return Ok(*visited);
		}
		let index = nodes.len();

		// Flatten graphs into the lockfile.
		'a: {
			let (graph, node) = match artifact
				.object(self)
				.await
				.map_err(|_source| tg::error!(%id, "missing object"))?
			{
				tg::artifact::Object::Directory(directory) => {
					let tg::directory::Object::Graph { graph, node } = directory.as_ref() else {
						break 'a;
					};
					(graph.clone(), *node)
				},
				tg::artifact::Object::File(file) => {
					let tg::file::Object::Graph { graph, node } = file.as_ref() else {
						break 'a;
					};
					(graph.clone(), *node)
				},
				tg::artifact::Object::Symlink(symlink) => {
					let tg::symlink::Object::Graph { graph, node } = symlink.as_ref() else {
						break 'a;
					};
					(graph.clone(), *node)
				},
			};
			let nodes = Box::pin(self.create_lockfile_node_with_graph(
				checkout_dependencies,
				&graph,
				nodes,
				visited,
				graphs,
			))
			.await?;
			return Ok(nodes[node]);
		}

		// Only create a distinct node for non-graph artifacts.
		nodes.push(None);
		visited.insert(id.clone(), index);

		// Create a new lockfile node for the artifact, recursing over dependencies.
		let node = match artifact.object(self).await? {
			tg::artifact::Object::Directory(directory) => {
				let tg::directory::Object::Normal { entries } = directory.as_ref() else {
					unreachable!()
				};
				let id = id.unwrap_directory();
				let mut entries_ = BTreeMap::new();
				for (name, artifact) in entries {
					let index = Box::pin(self.get_or_create_lockfile_node_for_artifact(
						artifact,
						checkout_dependencies,
						nodes,
						visited,
						graphs,
					))
					.await?;
					entries_.insert(name.clone(), Either::Left(index));
				}
				tg::lockfile::Node::Directory(tg::lockfile::Directory {
					entries: entries_,
					id: Some(id),
				})
			},

			tg::artifact::Object::File(file) => {
				let tg::file::Object::Normal {
					contents,
					dependencies,
					executable,
				} = file.as_ref()
				else {
					unreachable!()
				};
				let id = id.unwrap_file();
				let mut dependencies_ = BTreeMap::new();
				for (reference, referent) in dependencies {
					let item = match &referent.item {
						tg::Object::Directory(directory) if checkout_dependencies => {
							let artifact = directory.clone().into();
							let index = Box::pin(self.get_or_create_lockfile_node_for_artifact(
								&artifact,
								checkout_dependencies,
								nodes,
								visited,
								graphs,
							))
							.await?;
							Either::Left(index)
						},
						tg::Object::File(file) if checkout_dependencies => {
							let artifact = file.clone().into();
							let index = Box::pin(self.get_or_create_lockfile_node_for_artifact(
								&artifact,
								checkout_dependencies,
								nodes,
								visited,
								graphs,
							))
							.await?;
							Either::Left(index)
						},
						tg::Object::Symlink(symlink) if checkout_dependencies => {
							let artifact = symlink.clone().into();
							let index = Box::pin(self.get_or_create_lockfile_node_for_artifact(
								&artifact,
								checkout_dependencies,
								nodes,
								visited,
								graphs,
							))
							.await?;
							Either::Left(index)
						},
						object => Either::Right(object.id(self).await?),
					};
					let dependency = tg::Referent {
						item,
						path: referent.path.clone(),
						subpath: referent.subpath.clone(),
						tag: referent.tag.clone(),
					};
					dependencies_.insert(reference.clone(), dependency);
				}
				let contents = Some(contents.id(self).await?);
				tg::lockfile::Node::File(tg::lockfile::File {
					contents,
					dependencies: dependencies_,
					executable: *executable,
					id: Some(id),
				})
			},

			tg::artifact::Object::Symlink(symlink) => match symlink.as_ref() {
				tg::symlink::Object::Graph { .. } => unreachable!(),
				tg::symlink::Object::Target { target } => {
					let id = id.unwrap_symlink();
					tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target {
						id: Some(id),
						target: target.clone(),
					})
				},
				tg::symlink::Object::Artifact { artifact, subpath } => {
					let id = id.unwrap_symlink();
					let artifact = {
						let index = Box::pin(self.get_or_create_lockfile_node_for_artifact(
							artifact,
							checkout_dependencies,
							nodes,
							visited,
							graphs,
						))
						.await?;
						Either::Left(index)
					};
					let subpath = subpath.as_ref().map(PathBuf::from);
					tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
						artifact,
						id: Some(id),
						subpath,
					})
				},
			},
		};

		// Update the visited set.
		nodes[index].replace(node);

		Ok(index)
	}

	async fn create_lockfile_node_with_graph(
		&self,
		checkout_dependencies: bool,
		graph: &tg::Graph,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut BTreeMap<tg::artifact::Id, usize>,
		graphs: &mut BTreeMap<tg::graph::Id, Vec<usize>>,
	) -> tg::Result<Vec<usize>> {
		let id = graph.id(self).await?;
		if let Some(existing) = graphs.get(&id) {
			return Ok(existing.clone());
		}

		// Get the graph object.
		let object = graph.object(self).await?;

		// Assign indices.
		let mut indices = Vec::with_capacity(object.nodes.len());
		for node in 0..object.nodes.len() {
			let id = match object.nodes[node].kind() {
				tg::artifact::Kind::Directory => {
					tg::Directory::with_graph_and_node(graph.clone(), node)
						.id(self)
						.await?
						.into()
				},
				tg::artifact::Kind::File => tg::Directory::with_graph_and_node(graph.clone(), node)
					.id(self)
					.await?
					.into(),
				tg::artifact::Kind::Symlink => {
					tg::Directory::with_graph_and_node(graph.clone(), node)
						.id(self)
						.await?
						.into()
				},
			};

			let index = visited.get(&id).copied().unwrap_or_else(|| {
				let index = nodes.len();
				visited.insert(id, index);
				nodes.push(None);
				index
			});
			indices.push(index);
		}
		graphs.insert(id.clone(), indices.clone());

		// Create nodes
		for (old_index, node) in object.nodes.iter().enumerate() {
			let node = match node {
				tg::graph::Node::Directory(directory) => {
					let id = tg::directory::Id::new(
						&tg::directory::Data::Graph {
							graph: id.clone(),
							node: old_index,
						}
						.serialize()?,
					);

					let mut entries = BTreeMap::new();
					for (name, entry) in &directory.entries {
						let index = match entry {
							Either::Left(index) => indices[*index],
							Either::Right(artifact) => {
								Box::pin(self.get_or_create_lockfile_node_for_artifact(
									artifact,
									checkout_dependencies,
									nodes,
									visited,
									graphs,
								))
								.await?
							},
						};
						entries.insert(name.clone(), Either::Left(index));
					}
					tg::lockfile::Node::Directory(tg::lockfile::Directory {
						entries,
						id: Some(id),
					})
				},

				tg::graph::Node::File(file) => {
					let id = tg::file::Id::new(
						&tg::file::Data::Graph {
							graph: id.clone(),
							node: old_index,
						}
						.serialize()?,
					);

					let mut dependencies = BTreeMap::new();
					for (reference, referent) in &file.dependencies {
						let item = match &referent.item {
							Either::Left(index) => Either::Left(indices[*index]),
							Either::Right(object) => match object {
								tg::Object::Directory(artifact) => {
									let artifact = artifact.clone().into();
									let index =
										Box::pin(self.get_or_create_lockfile_node_for_artifact(
											&artifact,
											checkout_dependencies,
											nodes,
											visited,
											graphs,
										))
										.await?;
									Either::Left(index)
								},
								tg::Object::File(artifact) => {
									let artifact = artifact.clone().into();
									let index =
										Box::pin(self.get_or_create_lockfile_node_for_artifact(
											&artifact,
											checkout_dependencies,
											nodes,
											visited,
											graphs,
										))
										.await?;
									Either::Left(index)
								},
								tg::Object::Symlink(artifact) => {
									let artifact = artifact.clone().into();
									let index =
										Box::pin(self.get_or_create_lockfile_node_for_artifact(
											&artifact,
											checkout_dependencies,
											nodes,
											visited,
											graphs,
										))
										.await?;
									Either::Left(index)
								},
								object => Either::Right(object.id(self).await?),
							},
						};
						let path = referent.path.clone();
						let subpath = referent.subpath.clone();
						let tag = referent.tag.clone();
						let dependency = tg::Referent {
							item,
							path,
							subpath,
							tag,
						};
						dependencies.insert(reference.clone(), dependency);
					}
					let contents = file.contents.id(self).await?;
					let executable = file.executable;
					tg::lockfile::Node::File(tg::lockfile::File {
						contents: Some(contents),
						dependencies,
						executable,
						id: Some(id),
					})
				},

				tg::graph::Node::Symlink(symlink) => match symlink {
					tg::graph::object::Symlink::Target { target } => {
						let id = tg::symlink::Id::new(
							&tg::symlink::Data::Graph {
								graph: id.clone(),
								node: old_index,
							}
							.serialize()?,
						);

						tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target {
							target: target.clone(),
							id: Some(id),
						})
					},
					tg::graph::object::Symlink::Artifact { artifact, subpath } => {
						let id = tg::symlink::Id::new(
							&tg::symlink::Data::Graph {
								graph: id.clone(),
								node: old_index,
							}
							.serialize()?,
						);

						let artifact = match artifact {
							Either::Left(index) => indices[*index],
							Either::Right(artifact) => {
								Box::pin(self.get_or_create_lockfile_node_for_artifact(
									artifact,
									checkout_dependencies,
									nodes,
									visited,
									graphs,
								))
								.await?
							},
						};
						let artifact = Either::Left(artifact);
						tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
							artifact,
							id: Some(id),
							subpath: subpath.clone(),
						})
					},
				},
			};

			let index = indices[old_index];
			nodes[index].replace(node);
		}

		Ok(indices)
	}
}
