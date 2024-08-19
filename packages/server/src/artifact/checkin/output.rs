use std::{
	collections::{BTreeMap, BTreeSet},
	os::unix::fs::PermissionsExt,
	path::PathBuf,
	sync::{Arc, RwLock},
};

use super::{
	input::Input,
	unify::{Graph, Id},
};
use crate::Server;
use either::Either;
use futures::{stream::FuturesUnordered, TryStreamExt};
use indoc::formatdoc;
use itertools::Itertools;
use tangram_client as tg;
use tangram_database::{self as db, Connection as _, Database as _, Query as _, Transaction as _};
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug)]
pub struct Output {
	pub data: tg::artifact::Data,
	pub lock_index: usize,
	pub dependencies: BTreeMap<tg::Reference, Self>,
}

struct State {
	visited: RwLock<BTreeMap<tg::Path, Option<Output>>>,
	lock: RwLock<tg::graph::Data>,
}

impl Server {
	pub async fn write_output_to_database(&self, output: &Output) -> tg::Result<()> {
		let mut connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to create a transaction"))?;

		let mut stack = vec![output];
		while let Some(output) = stack.pop() {
			// TODO: count/weight.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, complete, touched_at)
					values ({p}1, {p}2, {p}3, {p}4)
					on conflict (id) do update set touched_at = {p}4;
				"
			);
			let id = output.data.id()?;
			let bytes = output.data.serialize()?;
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			let params = db::params![id, bytes, 1, now];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to put the artifact into the database")
				})?;

			stack.extend(output.dependencies.values());
		}

		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		Ok(())
	}
}

impl Server {
	pub async fn create_output_graph(
		&self,
		graph: &Graph,
		root: &Id,
	) -> tg::Result<tg::graph::Data> {
		let mut output_graph_nodes = Vec::with_capacity(graph.nodes.len());
		let mut visited = BTreeMap::new();
		self.create_output_graph_inner(graph, root, &mut output_graph_nodes, &mut visited)
			.await?
			.left()
			.ok_or_else(|| tg::error!("expected a the root to have an index"))?;
		let nodes = output_graph_nodes.into_iter().map(Option::unwrap).collect();
		let lock = tg::graph::Data { nodes };
		Ok(lock)
	}

	async fn create_output_graph_inner(
		&self,
		graph: &Graph,
		id: &Id,
		output_graph_nodes: &mut Vec<Option<tg::graph::data::Node>>,
		visited: &mut BTreeMap<Id, Either<usize, tg::object::Id>>,
	) -> tg::Result<Either<usize, tg::object::Id>> {
		// Check if we've visited this node already.
		if let Some(either) = visited.get(id) {
			return Ok(either.clone());
		}

		// Get the graph node.
		let graph_node = graph.nodes.get(id).unwrap();

		// Get the artifact kind.
		let kind = match &graph_node.object {
			// If this is an artifact on disk,
			Either::Left(object) => {
				let input = object.read().unwrap();
				if input.metadata.is_dir() {
					tg::artifact::Kind::Directory
				} else if input.metadata.is_file() {
					tg::artifact::Kind::File
				} else {
					tg::artifact::Kind::Symlink
				}
			},

			// If this is an object without any dependencies, inline it.
			Either::Right(object) if graph_node.outgoing.is_empty() => {
				let either = Either::Right(object.clone());
				visited.insert(id.clone(), either.clone());
				return Ok(either);
			},

			// Otherwise, create a node.
			Either::Right(tg::object::Id::Directory(_)) => tg::artifact::Kind::Directory,
			Either::Right(tg::object::Id::File(_)) => tg::artifact::Kind::File,
			_ => return Err(tg::error!("invalid input graph")),
		};

		// Get the index of this node.
		let index = output_graph_nodes.len();

		// Update the visited table.
		visited.insert(id.clone(), Either::Left(index));

		// Recurse over dependencies.
		let mut dependencies = BTreeMap::new();
		for (reference, input_graph_id) in &graph_node.outgoing {
			let output_graph_id = Box::pin(self.create_output_graph_inner(
				graph,
				input_graph_id,
				output_graph_nodes,
				visited,
			))
			.await?;
			dependencies.insert(reference.clone(), output_graph_id);
		}

		// Create the node.
		let output_node = match kind {
			tg::artifact::Kind::Directory => {
				let entries = dependencies
					.into_iter()
					.map(|(reference, id)| {
						let name = reference
							.path()
							.try_unwrap_path_ref()
							.map_err(|_| tg::error!("invalid input graph"))?
							.components()
							.last()
							.ok_or_else(|| tg::error!("invalid input graph"))?
							.try_unwrap_normal_ref()
							.map_err(|_| tg::error!("invalid input graph"))?
							.clone();
						let id = match id {
							Either::Left(id) => Either::Left(id),
							Either::Right(tg::object::Id::Directory(id)) => {
								Either::Right(id.into())
							},
							Either::Right(tg::object::Id::File(id)) => Either::Right(id.into()),
							Either::Right(tg::object::Id::Symlink(id)) => Either::Right(id.into()),
							_ => return Err(tg::error!("invalid input graph")),
						};
						Ok::<_, tg::Error>((name, id))
					})
					.try_collect()?;
				let directory = tg::graph::data::Directory { entries };
				tg::graph::data::Node::Directory(directory)
			},
			tg::artifact::Kind::File => {
				let mut file = match &graph_node.object {
					Either::Left(input) => {
						self.get_file_data_from_input(input.read().unwrap().clone())
							.await?
					},
					Either::Right(tg::object::Id::File(file)) => {
						self.get_file_data_from_object(tg::File::with_id(file.clone()))
							.await?
					},
					_ => return Err(tg::error!("invalid input graph")),
				};
				file.dependencies = (!dependencies.is_empty()).then_some(dependencies);
				tg::graph::data::Node::File(file)
			},
			tg::artifact::Kind::Symlink => {
				let mut symlink = match &graph_node.object {
					Either::Left(input) => {
						self.get_symlink_data_from_input(input.read().unwrap().clone())
							.await?
					},
					Either::Right(tg::object::Id::Symlink(symlink)) => {
						self.get_symlink_data_from_object(tg::Symlink::with_id(symlink.clone()))
							.await?
					},
					_ => return Err(tg::error!("invalid input graph")),
				};
				tg::graph::data::Node::Symlink(symlink)
			},
		};

		// Update the node and return the index.
		output_graph_nodes[index].replace(output_node);
		Ok(Either::Left(index))
	}

	async fn get_file_data_from_input(&self, input: Input) -> tg::Result<tg::graph::data::File> {
		let super::input::Input { arg, metadata, .. } = input;

		// Create the blob.
		let file = tokio::fs::File::open(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read file"))?;
		let output = self
			.create_blob(file)
			.await
			.map_err(|source| tg::error!(!source, "failed to create blob"))?;

		// Create the data.
		Ok(tg::graph::data::File {
			contents: Some(output.blob),
			dependencies: None,
			executable: metadata.permissions() | 0b111 != 0,
			module: None, /* todo: modules */
		})
	}

	async fn get_file_data_from_object(&self, file: tg::File) -> tg::Result<tg::graph::data::File> {
		match file.data(self).await? {
			tg::file::Data::Normal {
				contents,
				executable,
				module,
				..
			} => Ok(tg::graph::data::File {
				contents: Some(contents),
				dependencies: None,
				executable,
				module,
			}),
			tg::file::Data::Graph { graph, node } => {
				let graph = tg::Graph::with_id(graph).data(self).await?;
				let tg::graph::data::Node::File(file) = &graph.nodes[node] else {
					return Err(tg::error!("expected a file"));
				};
				Ok(file.clone())
			},
		}
	}

	async fn get_symlink_data_from_input(
		&self,
		input: Input,
	) -> tg::Result<tg::graph::data::Symlink> {
		let Input { arg, .. } = input;
		let mut path: tg::Path = tokio::fs::read_link(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, %path = arg.path, "failed to read link"))?
			.try_into::<tg::Path>()
			.or_else(|| tg::error!("invalid string"))?;

		let artifact = 'a: {
			let Some(tg::path::Component::Normal(string)) = path.components().first() else {
				break 'a None;
			};

			string.parse().ok().map(Either::Right)
		};

		let path = (!(artifact.is_some() && path.components().len() == 1)).then_some(path);
		Ok(tg::graph::data::Symlink { artifact, path })
	}

	async fn get_symlink_data_from_object(
		&self,
		symlink: tg::Symlink,
	) -> tg::Result<tg::graph::data::Symlink> {
		match symlink.data(self).await? {
			tg::symlink::Data::Normal { artifact, path } => Ok(tg::graph::data::Symlink {
				artifact: artifact.map(Either::Right),
				path,
			}),
			tg::symlink::Data::Graph { graph, node } => {
				let graph = tg::Graph::with_id(graph).data(self).await?;
				let tg::graph::data::Node::Symlink(symlink) = &graph.nodes[node] else {
					return Err(tg::error!("expected a symlink"));
				};
				Ok(symlink.clone())
			},
		}
	}
}

// Output
impl Server {
	pub async fn collect_output(
		&self,
		input: Arc<RwLock<Input>>,
		lock: &tg::graph::Data,
	) -> tg::Result<(Output, tg::graph::Data)> {
		// Create the initial state.
		let state = State {
			lock: RwLock::new(lock.clone()),
			visited: RwLock::new(BTreeMap::new()),
		};

		// Get the output.
		let output = self.collect_output_inner(input, 0, &state).await?;

		// Add the lock to the root object if it is a file with dependencies.
		let lock = state.lock.into_inner().unwrap();

		Ok((output, lock))
	}

	async fn collect_output_inner(
		&self,
		input: Arc<RwLock<Input>>,
		node: usize,
		state: &State,
	) -> tg::Result<Output> {
		// Check if this node has been visited or there is a cycle in object creation.
		{
			let visited = state.visited.read().unwrap();
			if let Some(output) = visited.get(&input.read().unwrap().arg.path) {
				return output
					.clone()
					.ok_or_else(|| tg::error!("cycle detected when collecting output"));
			}
		}
		state
			.visited
			.write()
			.unwrap()
			.insert(input.read().unwrap().arg.path.clone(), None);

		// Create the output.
		let metadata = input.read().unwrap().metadata.clone();
		let output = if metadata.is_dir() {
			self.create_directory_output(input.clone(), node, state)
				.await?
		} else if metadata.is_file() {
			self.create_file_output(input.clone(), node, state).await?
		} else if metadata.is_symlink() {
			self.create_symlink_output(input.clone(), node, state)
				.await?
		} else {
			return Err(tg::error!("invalid file type"));
		};

		// Get the artifact ID.
		let id = output.data.id()?;

		// Copy or move the file.
		if input.read().unwrap().is_root {
			let arg = input.read().unwrap().arg.clone();
			self.copy_or_move_file(&arg, &id).await.map_err(|source| {
				tg::error!(!source, "failed to copy or move file to checkouts")
			})?;
		}

		// todo: Update the lock.
		// state.lock.write().unwrap().nodes[node]
		// 	.object
		// 	.replace(id.into());

		// Update the output.
		state
			.visited
			.write()
			.unwrap()
			.get_mut(&input.read().unwrap().arg.path)
			.unwrap()
			.replace(output.clone());

		Ok(output)
	}

	async fn create_directory_output(
		&self,
		input: Arc<RwLock<Input>>,
		node: usize,
		state: &State,
	) -> tg::Result<Output> {
		let dependencies = input.read().unwrap().dependencies.clone();
		let children = dependencies
			.iter()
			.filter_map(|(reference, input)| {
				let lock = state.lock.read().unwrap();
				let tg::graph::data::Node::Directory(node) = &lock.nodes[node] else {
					return None;
				};
				let input = input.as_ref()?.clone();
				let name = reference
					.path()
					.try_unwrap_path_ref()
					.ok()?
					.components()
					.last()?
					.try_unwrap_normal_ref()
					.ok()?;
				let server = self.clone();
				let node = *node.entries.get(name)?.as_ref().left()?;
				let fut = async move {
					let output = Box::pin(server.collect_output_inner(input, node, state)).await?;
					Ok::<_, tg::Error>((name.clone(), output))
				};
				Some(fut)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		let entries = children
			.iter()
			.filter_map(|(name, output)| Some((name.clone(), output.data.id().ok()?)))
			.collect();
		let dependencies = children
			.into_iter()
			.map(|(name, output)| (tg::Reference::with_path(&name.into()), output))
			.collect();
		let data = tg::artifact::Data::Directory(tg::directory::Data::Normal { entries });

		let output = Output {
			data,
			dependencies,
			lock_index: node,
		};

		Ok(output)
	}

	async fn create_file_output(
		&self,
		input: Arc<RwLock<Input>>,
		node: usize,
		state: &State,
	) -> tg::Result<Output> {
		// Create a blob.
		let path = input.read().unwrap().arg.path.clone();
		let _permit = self.file_descriptor_semaphore.acquire().await.ok();
		let reader = tokio::fs::File::open(&path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to open file"))?;
		let blob = self
			.create_blob(reader)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to create blob"))?;

		// Create the file data.
		let contents = blob.blob;
		let executable = (input.read().unwrap().metadata.permissions().mode() & 0o111) != 0;
		let dependencies = if let Some(dependencies) =
			xattr::get(&path, tg::file::TANGRAM_FILE_XATTR_NAME)
				.map_err(|source| tg::error!(!source, %path, "failed to read xattrs"))?
		{
			let dependencies = serde_json::from_slice(&dependencies)
				.map_err(|source| tg::error!(!source, %path, "failed to deserialize xattr"))?;
			Some(dependencies)
		} else {
			None
		};
		let data = tg::artifact::Data::File(tg::file::Data::Normal {
			contents,
			dependencies,
			executable,
			module: None, // todo
		});

		// Get the children.
		let dependencies = input.read().unwrap().dependencies.clone();
		let dependencies = dependencies
			.iter()
			.filter_map(|(reference, input)| {
				let graph = state.lock.read().unwrap();
				let tg::graph::data::Node::File(file) = &graph.nodes[node] else {
					return None;
				};
				let node = *file
					.dependencies
					.as_ref()?
					.get(reference)?
					.as_ref()
					.left()?;
				let input = input.as_ref()?.clone();
				let fut = async move {
					let output = Box::pin(self.collect_output_inner(input, node, state)).await?;
					Ok::<_, tg::Error>((reference.clone(), output))
				};
				Some(fut)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<BTreeMap<_, _>>()
			.await?;

		// Create the output.
		Ok(Output {
			data,
			dependencies,
			lock_index: node,
		})
	}

	async fn create_symlink_output(
		&self,
		input: Arc<RwLock<Input>>,
		node: usize,
		_state: &State,
	) -> tg::Result<Output> {
		let path = input.read().unwrap().arg.path.clone();

		// Read the target from the symlink.
		let target = tokio::fs::read_link(&path).await.map_err(
			|source| tg::error!(!source, %path, r#"failed to read the symlink at path"#,),
		)?;

		// Unrender the target.
		let target = target
			.to_str()
			.ok_or_else(|| tg::error!("the symlink target must be valid UTF-8"))?;
		let artifacts_path = self.artifacts_path();
		let artifacts_path = artifacts_path
			.to_str()
			.ok_or_else(|| tg::error!("the artifacts path must be valid UTF-8"))?;
		let target = tg::template::Data::unrender(artifacts_path, target)?;

		// Get the artifact and path.
		let (artifact, path) = if target.components.len() == 1 {
			let path = target.components[0]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(None, Some(path))
		} else if target.components.len() == 2 {
			let artifact = target.components[0]
				.try_unwrap_artifact_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = target.components[1]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid sylink"))?
				.clone();
			let path = &path[1..];
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(Some(artifact), Some(path))
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		let data = tg::artifact::Data::Symlink(tg::symlink::Data::Normal {
			artifact: artifact.clone(),
			path,
		});

		let output = Output {
			data,
			dependencies: BTreeMap::new(),
			lock_index: node,
		};

		Ok(output)
	}

	async fn copy_or_move_file(
		&self,
		arg: &tg::artifact::checkin::Arg,
		id: &tg::artifact::Id,
	) -> tg::Result<()> {
		// Skip copying anything in the checkouts directory.
		let checkouts_directory: tg::Path = self.checkouts_path().try_into()?;
		if let Some(path) = arg.path.diff(&checkouts_directory) {
			if matches!(
				path.components().first(),
				Some(tg::path::Component::Current)
			) {
				return Ok(());
			}
		}

		let destination = self.checkouts_path().join(id.to_string());
		if arg.destructive {
			match tokio::fs::rename(&arg.path, &destination).await {
				Ok(()) => return Ok(()),
				Err(error) if error.raw_os_error() == Some(libc::EEXIST) => return Ok(()),
				Err(error) if error.raw_os_error() == Some(libc::ENODEV) => (),
				Err(source) => return Err(tg::error!(!source, "failed to rename file")),
			};
		}

		match copy_all(arg.path.as_ref(), &destination).await {
			Ok(()) => Ok(()),
			Err(error) if error.raw_os_error() == Some(libc::EEXIST) => Ok(()),
			Err(source) => Err(tg::error!(!source, "failed to copy file")),
		}
	}
}

impl Server {
	pub async fn split_graphs(
		&self,
		graph: &tg::graph::Data,
		_old_files: &BTreeMap<tg::file::Id, tg::file::Data>,
	) -> tg::Result<(BTreeMap<usize, (tg::graph::Id, usize)>)> {
		let mut graphs: Vec<tg::graph::Id> = Vec::new();
		let mut indices = BTreeMap::new();

		for (graph_index, scc) in petgraph::algo::tarjan_scc(GraphImpl(graph))
			.into_iter()
			.enumerate()
		{
			let mut nodes = Vec::with_capacity(scc.len());

			// Create new indices for each node.
			for (new_index, old_index) in scc.iter().copied().enumerate() {
				indices.insert(old_index, (graph_index, new_index));
			}

			// Remap nodes.
			for old_index in scc {
				let old_node = graph.nodes[old_index].clone();
				let mut new_node = old_node.clone();
				let graph_dependencies = match &mut new_node {
					tg::graph::data::Node::Directory(directory) => {
						Either::Left(directory.entries.values_mut())
					},
					tg::graph::data::Node::File(file) => {
						let Some(dependencies) = &mut file.dependencies else {
							nodes.push(new_node);
							continue;
						};
						Either::Right(dependencies.values_mut())
					},
					tg::graph::data::Node::Symlink(_) => {
						nodes.push(new_node);
						continue;
					},
				};

				match graph_dependencies {
					Either::Left(artifacts) => {
						for artifact in artifacts {
							let Either::Left(old_index) = artifact else {
								continue;
							};
							let (graph_index_, new_index) =
								indices.get(old_index).copied().unwrap();
							if graph_index_ == graph_index {
								*old_index = new_index;
								continue;
							}
							let graph_ = tg::Graph::with_id(graphs[graph_index].clone());
							let new_artifact: tg::Artifact = match graph_.object(self).await?.nodes
								[new_index]
								.kind()
							{
								tg::artifact::Kind::Directory => {
									tg::Directory::with_graph_and_node(graph_.clone(), new_index)
										.into()
								},
								tg::artifact::Kind::File => {
									tg::File::with_graph_and_node(graph_.clone(), new_index).into()
								},
								tg::artifact::Kind::Symlink => {
									tg::Symlink::with_graph_and_node(graph_.clone(), new_index)
										.into()
								},
							};
							*artifact = Either::Right(new_artifact.id(self).await?);
						}
					},
					Either::Right(objects) => {
						for object in objects {
							let Either::Left(old_index) = object else {
								continue;
							};
							let (graph_index_, new_index) =
								indices.get(old_index).copied().unwrap();
							if graph_index_ == graph_index {
								*old_index = new_index;
								continue;
							}
							let graph_ = tg::Graph::with_id(graphs[graph_index].clone());
							let new_artifact: tg::Object = match graph_.object(self).await?.nodes
								[new_index]
								.kind()
							{
								tg::artifact::Kind::Directory => {
									tg::Directory::with_graph_and_node(graph_.clone(), new_index)
										.into()
								},
								tg::artifact::Kind::File => {
									tg::File::with_graph_and_node(graph_.clone(), new_index).into()
								},
								tg::artifact::Kind::Symlink => {
									tg::Symlink::with_graph_and_node(graph_.clone(), new_index)
										.into()
								},
							};
							*object = Either::Right(new_artifact.id(self).await?);
						}
					},
				}

				nodes.push(new_node);
			}

			// Create the lock.
			let graph = tg::graph::data::Data { nodes };
			let graph = tg::graph::Object::try_from(graph)?;
			let graph = tg::Graph::with_object(graph).id(self).await?;
			graphs.push(graph);
		}

		let graphs = indices
			.into_iter()
			.map(|(index, (graph, node))| (index, (graphs[graph].clone(), node)))
			.collect();

		Ok(graphs)
	}
}

impl Server {
	pub async fn write_hardlinks_and_xattrs(
		&self,
		input: Arc<RwLock<Input>>,
		output: &Output,
	) -> tg::Result<()> {
		let path = self
			.checkouts_path()
			.join(output.data.id()?.to_string())
			.try_into()?;
		let mut visited = BTreeSet::new();
		self.write_hardlinks_and_xattrs_inner(&path, input, output, &mut visited)
			.await?;
		Ok(())
	}

	async fn write_hardlinks_and_xattrs_inner(
		&self,
		path: &tg::Path,
		input: Arc<RwLock<Input>>,
		output: &Output,
		visited: &mut BTreeSet<tg::Path>,
	) -> tg::Result<()> {
		if visited.contains(path) {
			return Ok(());
		};
		visited.insert(path.clone());

		if let tg::artifact::Data::File(tg::file::Data::Normal {
			contents,
			dependencies,
			..
		}) = &output.data
		{
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();

			let dst: tg::Path = self
				.checkouts_path()
				.join(output.data.id()?.to_string())
				.try_into()?;

			if let Some(_dependencies) = dependencies {
				todo!(); // write xattr
				 // let data = serde_json::to_vec(dependencies)
				 // 	.map_err(|source| tg::error!(!source, "failed to serialize dependencies"))?;
				 // xattr::set(&dst, tg::file::TANGRAM_FILE_DEPENDENCIES_XATTR_NAME, &data)
				 // 	.map_err(|source| tg::error!(!source, "failed to set xattrs"))?;
			}

			// Create hard link to the file.
			match tokio::fs::hard_link(path, &dst).await {
				Ok(()) => (),
				Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
				Err(source) => {
					return Err(tg::error!(!source, %src = path, %dst, "failed to create hardlink"))
				},
			}

			// Create a symlink to the file in the blobs directory.
			let symlink_target = PathBuf::from("../checkouts").join(output.data.id()?.to_string());
			let symlink_path = self.blobs_path().join(contents.to_string());
			match tokio::fs::symlink(&symlink_target, &symlink_path).await {
				Ok(()) => (),
				Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
				Err(source) => {
					return Err(
						tg::error!(!source, %src = symlink_target.display(), %dst = symlink_path.display(), "failed to create blob symlink"),
					)
				},
			}
		}

		let dependencies = input.read().unwrap().dependencies.clone();
		for (reference, child_input) in dependencies
			.iter()
			.filter_map(|(reference, input)| Some((reference, input.as_ref()?)))
		{
			let child_output = output.dependencies.get(reference).ok_or_else(
				|| tg::error!(%referrer = path, %reference, "missing output reference"),
			)?;

			let path = if child_input.read().unwrap().is_root {
				self.checkouts_path()
					.join(child_output.data.id()?.to_string())
					.try_into()?
			} else {
				let path_ = reference
					.path()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| reference.query()?.path.as_ref())
					.cloned()
					.ok_or_else(|| tg::error!("expected a path dependency"))?;

				if input.read().unwrap().metadata.is_dir() {
					path.clone().join(path_)
				} else {
					path.clone().parent().join(path_).normalize()
				}
			};

			Box::pin(self.write_hardlinks_and_xattrs_inner(
				&path,
				child_input.clone(),
				child_output,
				visited,
			))
			.await?;
		}

		Ok(())
	}
}

#[derive(Copy, Clone)]
struct GraphImpl<'a>(&'a tg::graph::Data);

impl<'a> petgraph::visit::GraphBase for GraphImpl<'a> {
	type EdgeId = (usize, usize);
	type NodeId = usize;
}

impl<'a> petgraph::visit::GraphRef for GraphImpl<'a> {}

#[allow(clippy::needless_arbitrary_self_type)]
impl<'a> petgraph::visit::NodeIndexable for GraphImpl<'a> {
	fn from_index(self: &Self, i: usize) -> Self::NodeId {
		i
	}

	fn node_bound(self: &Self) -> usize {
		self.0.nodes.len()
	}

	fn to_index(self: &Self, a: Self::NodeId) -> usize {
		a
	}
}

impl<'a> petgraph::visit::IntoNeighbors for GraphImpl<'a> {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;
	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		match &self.0.nodes[a] {
			tg::graph::data::Node::Directory(directory) => Box::new(
				directory
					.entries
					.values()
					.filter_map(|either| either.as_ref().left().copied()),
			),
			tg::graph::data::Node::File(file) => {
				Box::new(file.dependencies.iter().flat_map(|entries| {
					entries.values().filter_map(|e| e.as_ref().left().copied())
				}))
			},
			tg::graph::data::Node::Symlink(_) => Box::new(None::<usize>.into_iter()),
		}
	}
}

impl<'a> petgraph::visit::IntoNodeIdentifiers for GraphImpl<'a> {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.0.nodes.len()
	}
}

async fn copy_all(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
	let mut stack = vec![(from.to_owned(), to.to_owned())];
	while let Some((from, to)) = stack.pop() {
		let metadata = tokio::fs::symlink_metadata(&from).await?;
		let file_type = metadata.file_type();
		if file_type.is_dir() {
			tokio::fs::create_dir_all(&to).await?;
			let mut entries = tokio::fs::read_dir(&from).await?;
			while let Some(entry) = entries.next_entry().await? {
				let from = from.join(entry.file_name());
				let to = to.join(entry.file_name());
				stack.push((from, to));
			}
		} else if file_type.is_file() {
			tokio::fs::copy(&from, &to).await?;
		} else if file_type.is_symlink() {
			let target = tokio::fs::read_link(&from).await?;
			tokio::fs::symlink(&target, &to).await?;
		} else {
			return Err(std::io::Error::other("invalid file type"));
		}
	}
	Ok(())
}
