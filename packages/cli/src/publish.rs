use {
	crate::Cli,
	indoc::formatdoc,
	petgraph::{
		algo::tarjan_scc,
		visit::{GraphBase, IntoNeighbors, IntoNodeIdentifiers, NodeIndexable},
	},
	std::collections::{HashMap, VecDeque},
	tangram_client as tg,
	tangram_either::Either,
};

/// Publish a package with its transitive dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The reference to publish.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	/// The remote to publish to.
	#[arg(long, short)]
	pub remote: Option<String>,
}

struct DependencyGraph {
	nodes: Vec<PackageNode>,
}

struct PackageNode {
	artifact: tg::Artifact,
	metadata: Metadata,
	dependency_indices: Vec<usize>,
	path: Option<std::path::PathBuf>,
}

enum Scc {
	Cycle(Vec<usize>),
	Single(usize),
}

#[derive(Clone, Debug, serde::Deserialize)]
struct Metadata {
	name: String,
	version: String,
}

impl Cli {
	pub async fn command_publish(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());

		if reference.export().is_some() {
			return Err(tg::error!("cannot publish a reference with an export"));
		}

		// Obtain the artifact (can be a file or directory).
		let referent = self.get_reference(&reference).await?;
		let item = referent
			.item
			.clone()
			.right()
			.ok_or_else(|| tg::error!("expected an object"))?;
		let artifact =
			tg::Artifact::try_from(item).map_err(|_| tg::error!("expected an artifact"))?;
		if matches!(artifact, tg::Artifact::Symlink(_)) {
			return Err(tg::error!("cannot publish a symlink"));
		}

		// Compute graph and report number of packages.
		let root_path = reference.item().try_unwrap_path_ref().ok().map(|path| {
			if path.is_absolute() {
				path.clone()
			} else {
				std::env::current_dir()
					.ok()
					.map(|cwd| cwd.join(path))
					.unwrap_or_else(|| path.clone())
			}
		});
		let graph = self
			.build_dependency_graph(&handle, artifact, root_path)
			.await?;
		let order = graph.compute_publish_order();
		let cyclic_packages: usize = order
			.iter()
			.map(|scc| match scc {
				Scc::Cycle(nodes) => nodes.len(),
				Scc::Single(_) => 0,
			})
			.sum();
		let total_packages = graph.nodes.len();
		eprintln!("publishing {total_packages} packages");
		if cyclic_packages > 0 {
			eprintln!("  {cyclic_packages} packages in dependency cycles");
		}

		// Publish packages.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());
		self.publish_in_order(&handle, &graph, order, &remote)
			.await?;

		eprintln!("publish complete");
		Ok(())
	}

	async fn build_dependency_graph(
		&mut self,
		handle: &impl tg::Handle,
		root: tg::Artifact,
		root_path: Option<std::path::PathBuf>,
	) -> tg::Result<DependencyGraph> {
		let mut nodes = Vec::new();
		let mut id_to_index = HashMap::new();
		let mut queue = VecDeque::new();

		// Check if the root is a package.
		if !is_package(handle, &root).await? {
			return Err(tg::error!("root is not a package"));
		}

		// Start with the root artifact.
		let root_id = root.id();
		let root_index = 0;
		id_to_index.insert(root_id.clone(), root_index);
		let root_metadata = get_package_metadata(handle, &root).await.map_err(
			|source| tg::error!(!source, ?artifact = root_id, "failed to get metadata for root package"),
		)?;
		nodes.push(PackageNode {
			artifact: root.clone(),
			metadata: root_metadata,
			dependency_indices: Vec::new(),
			path: root_path,
		});
		queue.push_back(root_index);

		while let Some(current_index) = queue.pop_front() {
			let current_node = &nodes[current_index];
			let current_artifact = current_node.artifact.clone();
			let current_path = current_node.path.clone();

			// Get dependencies with their references to extract local paths.
			let dependencies_with_refs = match &current_artifact {
				tg::Artifact::File(file) => {
					let deps = file.dependencies(handle).await?;
					deps.into_iter()
						.filter_map(|(reference, referent)| {
							tg::Artifact::try_from(referent.item)
								.ok()
								.map(|a| (reference, a))
						})
						.collect::<Vec<_>>()
				},
				tg::Artifact::Directory(_) => current_artifact
					.dependencies(handle)
					.await?
					.into_iter()
					.map(|artifact| (tg::Reference::with_object(artifact.id().into()), artifact))
					.collect::<Vec<_>>(),
				tg::Artifact::Symlink(_) => Vec::new(),
			};

			let mut dependency_indices = Vec::new();

			for (reference, dependency) in dependencies_with_refs {
				// Check if this dependency is a package (either a file or directory with root module).
				if !is_package(handle, &dependency).await? {
					continue;
				}

				// Check if this dependency is a member of the current package.
				if let tg::Artifact::Directory(current_dir) = &current_artifact {
					if is_contained_in_directory(handle, current_dir, &dependency).await? {
						match dependency {
							tg::Artifact::File(_) | tg::Artifact::Symlink(_) => {
								continue;
							},
							tg::Artifact::Directory(_) => {
								if !is_package(handle, &dependency).await? {
									continue;
								}
							},
						}
					}
				}

				let dep_id = dependency.id();
				let dep_index = if let Some(&existing_index) = id_to_index.get(&dep_id) {
					existing_index
				} else {
					let new_index = nodes.len();
					id_to_index.insert(dep_id.clone(), new_index);
					let metadata = get_package_metadata(handle, &dependency).await.map_err(
						|source| tg::error!(!source, ?artifact = dep_id, "failed to get metadata for dependency"),
					)?;

					// Try to resolve the dependency's filesystem path from the reference.
					let dep_path = if let Some(local_path) = reference.options().local.as_ref() {
						current_path.as_ref().and_then(|parent_path| {
							parent_path
								.parent()
								.map(|parent_dir| parent_dir.join(local_path))
								.and_then(|path| path.canonicalize().ok())
						})
					} else {
						None
					};

					nodes.push(PackageNode {
						artifact: dependency.clone(),
						metadata,
						dependency_indices: Vec::new(),
						path: dep_path,
					});
					queue.push_back(new_index);
					new_index
				};
				dependency_indices.push(dep_index);
			}

			nodes[current_index].dependency_indices = dependency_indices;
		}

		Ok(DependencyGraph { nodes })
	}

	async fn publish_in_order(
		&mut self,
		handle: &impl tg::Handle,
		graph: &DependencyGraph,
		order: Vec<Scc>,
		remote: &str,
	) -> tg::Result<()> {
		// Create dummy packages and tag them locally for all cycles.
		let has_cycles = order.iter().any(|scc| matches!(scc, Scc::Cycle(_)));
		if has_cycles {
			eprintln!("detected dependency cycles, creating local dummy package tags");

			for scc in &order {
				if let Scc::Cycle(nodes) = scc {
					for node_index in nodes {
						let node = &graph.nodes[*node_index];
						let metadata = &node.metadata;

						let dummy_artifact = create_dummy_package(handle, metadata).await?;
						let dummy_id: tg::object::Id = dummy_artifact.id().into();

						let tag_string = format!("{}/{}", metadata.name, metadata.version);
						let tag = tag_string
							.parse::<tg::Tag>()
							.map_err(|source| tg::error!(!source, "failed to parse tag"))?;

						eprintln!("creating local dummy tag {tag}");

						let arg = tg::tag::put::Arg {
							force: true,
							item: Either::Right(dummy_id),
							remote: None,
						};
						handle.put_tag(&tag, arg).await?;
					}
				}
			}
		}

		// Re-checkin packages with local path dependencies and create local tags.
		let mut updated_artifacts: HashMap<usize, tg::artifact::Id> = HashMap::new();
		for scc in &order {
			match scc {
				Scc::Single(node_index) => {
					let node = &graph.nodes[*node_index];
					let artifact = &node.artifact;
					let metadata = &node.metadata;
					let path = &node.path;

					let tag_string = format!("{}/{}", metadata.name, metadata.version);
					let tag = tag_string
						.parse::<tg::Tag>()
						.map_err(|source| tg::error!(!source, "failed to parse tag"))?;

					let has_local_deps = has_local_path_dependencies(handle, artifact).await?;

					let final_artifact_id = if has_local_deps && path.is_some() {
						let path = path.as_ref().unwrap();
						eprintln!("re-checking in {tag} without local dependencies");

						let arg = tg::checkin::Arg {
							path: path.clone(),
							options: tg::checkin::Options {
								local_dependencies: false,
								..Default::default()
							},
							updates: Vec::new(),
						};
						let re_checked_in_artifact = tg::checkin::checkin(handle, arg).await?;
						let new_artifact_id = re_checked_in_artifact.id();

						let object_id: tg::object::Id = new_artifact_id.clone().into();
						let arg = tg::tag::put::Arg {
							force: true,
							item: Either::Right(object_id),
							remote: None,
						};
						handle.put_tag(&tag, arg).await?;

						new_artifact_id
					} else {
						let artifact_id = artifact.id();
						let object_id: tg::object::Id = artifact_id.clone().into();
						let arg = tg::tag::put::Arg {
							force: true,
							item: Either::Right(object_id),
							remote: None,
						};
						handle.put_tag(&tag, arg).await?;

						artifact_id
					};

					updated_artifacts.insert(*node_index, final_artifact_id);
				},
				Scc::Cycle(indices) => {
					// First, tag all packages in the cycle with their original artifacts.
					for node_index in indices {
						let node = &graph.nodes[*node_index];
						let artifact = &node.artifact;
						let metadata = &node.metadata;

						let tag_string = format!("{}/{}", metadata.name, metadata.version);
						let tag = tag_string
							.parse::<tg::Tag>()
							.map_err(|source| tg::error!(!source, "failed to parse tag"))?;

						let artifact_id = artifact.id();
						let object_id: tg::object::Id = artifact_id.clone().into();
						let arg = tg::tag::put::Arg {
							force: true,
							item: Either::Right(object_id),
							remote: None,
						};
						handle.put_tag(&tag, arg).await?;

						updated_artifacts.insert(*node_index, artifact_id);
					}

					// Now that all tags exist, re-checkin packages with local path dependencies.
					for node_index in indices {
						let node = &graph.nodes[*node_index];
						let artifact = &node.artifact;
						let metadata = &node.metadata;
						let path = &node.path;

						let tag_string = format!("{}/{}", metadata.name, metadata.version);
						let tag = tag_string
							.parse::<tg::Tag>()
							.map_err(|source| tg::error!(!source, "failed to parse tag"))?;

						let has_local_deps = has_local_path_dependencies(handle, artifact).await?;
						if has_local_deps && path.is_some() {
							let path = path.as_ref().unwrap();
							eprintln!("re-checking in {tag} (cyclic) without local dependencies");

							let arg = tg::checkin::Arg {
								path: path.clone(),
								options: tg::checkin::Options {
									local_dependencies: false,
									..Default::default()
								},
								updates: Vec::new(),
							};
							let re_checked_in_artifact = tg::checkin::checkin(handle, arg).await?;
							let new_artifact_id = re_checked_in_artifact.id();

							let object_id: tg::object::Id = new_artifact_id.clone().into();
							let arg = tg::tag::put::Arg {
								force: true,
								item: Either::Right(object_id),
								remote: None,
							};
							handle.put_tag(&tag, arg).await?;

							updated_artifacts.insert(*node_index, new_artifact_id);
						}
					}
				},
			}
		}

		// Collect all items to push and their tags.
		let mut items_to_push = Vec::new();
		let mut tags_to_put: Vec<(tg::Tag, tg::artifact::Id)> = Vec::new();

		for scc in &order {
			match scc {
				Scc::Single(node_index) => {
					let node = &graph.nodes[*node_index];
					let metadata = &node.metadata;

					let tag_string = format!("{}/{}", metadata.name, metadata.version);
					let tag = tag_string
						.parse::<tg::Tag>()
						.map_err(|source| tg::error!(!source, "failed to parse tag"))?;

					// Use the updated artifact ID if available.
					let artifact_id = updated_artifacts
						.get(node_index)
						.cloned()
						.unwrap_or_else(|| node.artifact.id());

					let should_publish =
						should_publish_package(handle, &tag, &artifact_id, remote).await?;

					if should_publish {
						tags_to_put.push((tag, artifact_id.clone()));
						items_to_push.push(Either::Right(artifact_id.into()));
					}
				},
				Scc::Cycle(nodes) => {
					eprintln!("publishing {} packages in cycle", nodes.len());
					for node_index in nodes {
						let node = &graph.nodes[*node_index];
						let metadata = &node.metadata;

						let tag_string = format!("{}/{}", metadata.name, metadata.version);
						let tag = tag_string
							.parse::<tg::Tag>()
							.map_err(|source| tg::error!(!source, "failed to parse tag"))?;

						// Use the updated artifact ID if available.
						let artifact_id = updated_artifacts
							.get(node_index)
							.cloned()
							.unwrap_or_else(|| node.artifact.id());

						let should_publish =
							should_publish_package(handle, &tag, &artifact_id, remote).await?;

						if should_publish {
							tags_to_put.push((tag, artifact_id.clone()));
							items_to_push.push(Either::Right(artifact_id.into()));
						}
					}
				},
			}
		}

		// Push all items.
		if !items_to_push.is_empty() {
			let arg = tg::push::Arg {
				commands: false,
				items: items_to_push,
				logs: false,
				outputs: true,
				recursive: false,
				remote: Some(remote.to_owned()),
			};
			let stream = handle.push(arg).await?;
			self.render_progress_stream(stream).await?;
		}

		// Put all tags.
		for (tag, artifact_id) in &tags_to_put {
			let object_id: tg::object::Id = artifact_id.clone().into();
			let arg = tg::tag::put::Arg {
				force: true,
				item: Either::Right(object_id.clone()),
				remote: None,
			};
			handle.put_tag(tag, arg).await?;

			let arg = tg::tag::put::Arg {
				force: true,
				item: Either::Right(object_id),
				remote: Some(remote.to_owned()),
			};
			handle.put_tag(tag, arg).await?;
		}

		Ok(())
	}
}

async fn has_local_path_dependencies(
	handle: &impl tg::Handle,
	artifact: &tg::Artifact,
) -> tg::Result<bool> {
	match artifact {
		tg::Artifact::File(file) => {
			let dependencies = file.dependencies(handle).await?;
			Ok(dependencies
				.values()
				.any(|referent| referent.options().path.is_some()))
		},
		tg::Artifact::Directory(directory) => {
			let entries = directory.entries(handle).await?;
			for entry in entries.values() {
				if Box::pin(has_local_path_dependencies(handle, entry)).await? {
					return Ok(true);
				}
			}
			Ok(false)
		},
		tg::Artifact::Symlink(_) => Ok(false),
	}
}

async fn is_package(handle: &impl tg::Handle, artifact: &tg::Artifact) -> tg::Result<bool> {
	match artifact {
		tg::Artifact::File(_file) => {
			// Files don't have names, only directory entries have names.
			// Without path context, we assume a standalone file is a package.
			Ok(true)
		},
		tg::Artifact::Directory(directory) => {
			// A directory is a package if it has a root module file.
			Ok(tg::package::try_get_root_module_file_name(
				handle,
				Either::Left(&directory.clone().into()),
			)
			.await?
			.is_some())
		},
		tg::Artifact::Symlink(_) => {
			// Symlinks are not packages.
			Ok(false)
		},
	}
}

async fn is_contained_in_directory(
	handle: &impl tg::Handle,
	directory: &tg::Directory,
	artifact: &tg::Artifact,
) -> tg::Result<bool> {
	let artifact_id = artifact.id();
	let entries = directory.entries(handle).await?;

	for entry in entries.values() {
		if entry.id() == artifact_id {
			return Ok(true);
		}
		if let tg::Artifact::Directory(subdir) = entry {
			if Box::pin(is_contained_in_directory(handle, subdir, artifact)).await? {
				return Ok(true);
			}
		}
	}

	Ok(false)
}

async fn get_package_metadata(
	handle: &impl tg::Handle,
	artifact: &tg::Artifact,
) -> tg::Result<Metadata> {
	let (kind, item) = match artifact {
		tg::Artifact::File(file) => {
			// For a single file, use the file itself as the module.
			// Files don't have names - assume TypeScript.
			let kind = tg::module::Kind::Ts;
			let item = tg::module::Item::Object(file.clone().into());
			(kind, item)
		},
		tg::Artifact::Directory(directory) => {
			// For a directory, find the root module file.
			let root_module_file_name = tg::package::try_get_root_module_file_name(
				handle,
				Either::Left(&directory.clone().into()),
			)
			.await?
			.ok_or_else(|| tg::error!("could not find a root module in the directory"))?;

			let kind = if std::path::Path::new(root_module_file_name)
				.extension()
				.is_some_and(|ext| ext == "js")
			{
				tg::module::Kind::Js
			} else if std::path::Path::new(root_module_file_name)
				.extension()
				.is_some_and(|ext| ext == "ts")
			{
				tg::module::Kind::Ts
			} else {
				return Err(tg::error!("unsupported module type"));
			};

			let file = directory.get(handle, root_module_file_name).await?;
			let item = tg::module::Item::Object(file.into());
			(kind, item)
		},
		tg::Artifact::Symlink(_) => {
			return Err(tg::error!("symlinks cannot be packages"));
		},
	};

	let referent = tg::Referent::with_item(item);
	let module = tg::Module { kind, referent };
	let executable = tg::command::Executable::Module(tg::command::ModuleExecutable {
		module,
		export: Some("metadata".to_owned()),
	});

	let arg = tg::run::Arg {
		host: Some("js".to_owned()),
		executable: Some(executable),
		..Default::default()
	};
	let output = tg::run::run(handle, arg).await?;

	let map = output
		.try_unwrap_map()
		.map_err(|_| tg::error!("expected metadata to be a map"))?;
	let name = map
		.get("name")
		.ok_or_else(|| tg::error!("metadata missing 'name' field"))?
		.try_unwrap_string_ref()
		.map_err(|_| tg::error!("expected 'name' to be a string"))?
		.clone();
	let version = map
		.get("version")
		.ok_or_else(|| tg::error!("metadata missing 'version' field"))?
		.try_unwrap_string_ref()
		.map_err(|_| tg::error!("expected 'version' to be a string"))?
		.clone();

	Ok(Metadata { name, version })
}

async fn create_dummy_package(
	handle: &impl tg::Handle,
	metadata: &Metadata,
) -> tg::Result<tg::Artifact> {
	let content = formatdoc!(
		r#"
		export default () => {{ throw new Error("This is a dummy package placeholder for cycle resolution. The real implementation should be published shortly."); }};

		export let metadata = {{
			name: "{}",
			version: "{}",
		}};
	"#,
		metadata.name,
		metadata.version
	);
	let file = tg::File::builder(content).build();
	file.store(handle).await?;
	Ok(file.into())
}

async fn should_publish_package(
	handle: &impl tg::Handle,
	tag: &tg::Tag,
	artifact_id: &tg::artifact::Id,
	remote: &str,
) -> tg::Result<bool> {
	// Check if the tag already exists on the remote.
	let pattern: tg::tag::Pattern = tag.clone().into();
	let arg = tg::tag::list::Arg {
		length: Some(1),
		pattern: pattern.clone(),
		remote: Some(remote.to_string()),
		reverse: true,
	};
	let tg::tag::list::Output { data } = handle.list_tags(arg).await?;
	let existing = data.into_iter().next();

	let object_id: tg::object::Id = artifact_id.clone().into();
	let should_publish = if let Some(output) = existing {
		if let Some(item) = output.item {
			// Check if it points to the same artifact.
			match item {
				Either::Right(existing_object_id) => {
					if existing_object_id == object_id {
						eprintln!("tag {tag} already published, skipping");
						false
					} else {
						eprintln!("tag {tag} exists but points to different item, overwriting");
						true
					}
				},
				Either::Left(_process_id) => {
					eprintln!("tag {tag} points to a process, overwriting");
					true
				},
			}
		} else {
			eprintln!("tag {tag} exists but has no item, overwriting");
			true
		}
	} else {
		eprintln!("publishing {tag} ({object_id})");
		true
	};

	Ok(should_publish)
}

impl GraphBase for DependencyGraph {
	type EdgeId = ();
	type NodeId = usize;
}

impl IntoNodeIdentifiers for &DependencyGraph {
	type NodeIdentifiers = std::ops::Range<usize>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.nodes.len()
	}
}

impl NodeIndexable for DependencyGraph {
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

impl<'a> IntoNeighbors for &'a DependencyGraph {
	type Neighbors = std::iter::Copied<std::slice::Iter<'a, usize>>;

	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		self.nodes[a].dependency_indices.iter().copied()
	}
}

impl DependencyGraph {
	fn compute_publish_order(&self) -> Vec<Scc> {
		let tarjan_sccs = tarjan_scc(self);

		tarjan_sccs
			.into_iter()
			.map(|scc| {
				if scc.len() > 1 {
					Scc::Cycle(scc)
				} else {
					Scc::Single(scc[0])
				}
			})
			.collect()
	}
}
