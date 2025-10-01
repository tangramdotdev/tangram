use {
	crate::Cli,
	futures::future::try_join_all,
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

	/// Override the tag for the root package.
	#[arg(long, short)]
	pub tag: Option<String>,
}

struct DependencyGraph {
	nodes: Vec<PackageNode>,
}

struct PackageNode {
	artifact: tg::Artifact,
	tag: String,
	dependency_indices: Vec<usize>,
	path: Option<std::path::PathBuf>,
}

enum Scc {
	Cycle(Vec<usize>),
	Single(usize),
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
		let root_path = reference
			.item()
			.try_unwrap_path_ref()
			.ok()
			.and_then(|path| std::path::absolute(path).ok());
		let graph = self
			.build_dependency_graph(&handle, artifact, root_path, args.tag)
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
		root_tag_override: Option<String>,
	) -> tg::Result<DependencyGraph> {
		let mut nodes = Vec::new();
		let mut id_to_index = HashMap::new();
		let mut queue = VecDeque::new();

		if !is_package(handle, &root).await? {
			return Err(tg::error!("root is not a package"));
		}

		let root_id = root.id();
		let root_index = 0;
		id_to_index.insert(root_id.clone(), root_index);
		let root_tag = if let Some(tag) = root_tag_override {
			tag
		} else {
			get_package_tag(handle, &root).await.map_err(
				|source| tg::error!(!source, ?artifact = root_id, "failed to get tag for root package"),
			)?
		};
		nodes.push(PackageNode {
			artifact: root.clone(),
			tag: root_tag,
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
							referent.and_then(|r| {
								tg::Artifact::try_from(r.item)
									.ok()
									.map(|a| (reference, a))
							})
						})
						.collect::<Vec<_>>()
				},
				tg::Artifact::Directory(directory) => {
					Self::collect_all_dependencies(handle, directory).await?
				},
				tg::Artifact::Symlink(_) => Vec::new(),
			};

			let mut dependency_indices = Vec::new();

			for (reference, dependency) in dependencies_with_refs {
				// Skip if this dependency is a package or contained within a package.
				if !is_package(handle, &dependency).await? {
					continue;
				}
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
					let tag = get_package_tag(handle, &dependency).await.map_err(
						|source| tg::error!(!source, ?artifact = dep_id, "failed to get tag for dependency"),
					)?;

					// Try to resolve the dependency's filesystem path from the reference.
					let dep_path = if let Some(local_path) = reference.options().local.as_ref() {
						current_path.as_ref().and_then(|parent_path| {
							parent_path.join(local_path).canonicalize().ok()
						})
					} else {
						None
					};

					nodes.push(PackageNode {
						artifact: dependency.clone(),
						tag,
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

	async fn collect_all_dependencies(
		handle: &impl tg::Handle,
		directory: &tg::Directory,
	) -> tg::Result<Vec<(tg::Reference, tg::Artifact)>> {
		let mut all_deps = Vec::new();
		let mut visited = std::collections::HashSet::new();

		async fn collect_from_directory(
			handle: &impl tg::Handle,
			directory: &tg::Directory,
			all_deps: &mut Vec<(tg::Reference, tg::Artifact)>,
			visited: &mut std::collections::HashSet<tg::object::Id>,
		) -> tg::Result<()> {
			let entries = directory.entries(handle).await?;

			for entry in entries.values() {
				match entry {
					tg::Artifact::File(file) => {
						let file_id = file.id();
						if visited.insert(file_id.clone().into()) {
							let deps = file.dependencies(handle).await?;
							for (reference, referent) in deps {
								if let Some(r) = referent {
									if let Ok(artifact) = tg::Artifact::try_from(r.item) {
										all_deps.push((reference, artifact));
									}
								}
							}
						}
					},
					tg::Artifact::Directory(subdir) => {
						Box::pin(collect_from_directory(handle, subdir, all_deps, visited)).await?;
					},
					tg::Artifact::Symlink(_) => {},
				}
			}

			Ok(())
		}

		collect_from_directory(handle, directory, &mut all_deps, &mut visited).await?;

		Ok(all_deps)
	}

	async fn publish_in_order(
		&mut self,
		handle: &impl tg::Handle,
		graph: &DependencyGraph,
		order: Vec<Scc>,
		remote: &str,
	) -> tg::Result<()> {
		// For cycles, create tags if they don't already exist (using dummies as placeholders).
		let has_cycles = order.iter().any(Scc::is_cycle);
		if has_cycles {
			eprintln!("detected dependency cycles, creating local tags for unresolved packages");

			for scc in &order {
				if scc.is_cycle() {
					for &node_index in scc.node_indices() {
						let node = &graph.nodes[node_index];
						let tag = node
							.tag
							.parse::<tg::Tag>()
							.map_err(|source| tg::error!(!source, "failed to parse tag"))?;

						let pattern: tg::tag::Pattern = tag.clone().into();
						let existing = handle.try_get_tag(&pattern).await;
						if existing.is_ok() && existing.as_ref().unwrap().is_some() {
							eprintln!("tag {tag} already exists, skipping dummy creation");
							continue;
						}

						let dummy_artifact = create_dummy_package(handle, &node.tag).await?;
						eprintln!("creating local placeholder tag {tag}");
						put_local_tag(handle, &tag, &dummy_artifact.id()).await?;
					}
				}
			}
		}

		// Process all packages - re-checkin and collect items to publish.
		let mut items_to_push = Vec::new();
		let mut tags_to_put: Vec<(tg::Tag, tg::artifact::Id)> = Vec::new();

		for scc in &order {
			// For cycles, first tag all packages with their original artifacts.
			if scc.is_cycle() {
				for &node_index in scc.node_indices() {
					let node = &graph.nodes[node_index];
					let artifact_id = node.artifact.id();
					let tag = node
						.tag
						.parse::<tg::Tag>()
						.map_err(|source| tg::error!(!source, "failed to parse tag"))?;
					put_local_tag(handle, &tag, &artifact_id).await?;
				}
			}

			// Process each node: re-checkin and collect for publishing.
			for &node_index in scc.node_indices() {
				let node = &graph.nodes[node_index];
				let artifact = &node.artifact;
				let path = &node.path;

				let tag = node
					.tag
					.parse::<tg::Tag>()
					.map_err(|source| tg::error!(!source, "failed to parse tag"))?;
				let final_artifact_id =
					// Re-check in all packages without local dependencies if they have a path,
					if path.is_some() && !scc.is_cycle() {
						eprintln!("re-checking in {} without local dependencies", node.tag);
						let new_artifact =
							re_checkin_without_local_deps(handle, path.as_ref().unwrap()).await?;
						let new_artifact_id = new_artifact.id();
						put_local_tag(handle, &tag, &new_artifact_id).await?;
						new_artifact_id
					} else {
						let artifact_id = artifact.id();
						// Packages in cycles are already tagged, but we need tags for non-cyclic packages.
						if !scc.is_cycle() {
							put_local_tag(handle, &tag, &artifact_id).await?;
						}
						artifact_id
					};

				eprintln!("publishing {} ({final_artifact_id})", node.tag);
				tags_to_put.push((tag, final_artifact_id.clone()));
				items_to_push.push(Either::Right(final_artifact_id.into()));
			}

			if scc.is_cycle() {
				eprintln!("publishing {} packages in cycle", scc.node_indices().len());
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
		try_join_all(
			tags_to_put
				.iter()
				.map(|(tag, artifact_id)| async {
					let object_id: tg::object::Id = artifact_id.clone().into();

					// Put local tag.
					let arg = tg::tag::put::Arg {
						force: true,
						item: Either::Right(object_id.clone()),
						remote: None,
					};
					handle.put_tag(tag, arg).await?;

					// Put remote tag.
					let arg = tg::tag::put::Arg {
						force: true,
						item: Either::Right(object_id),
						remote: Some(remote.to_owned()),
					};
					handle.put_tag(tag, arg).await?;

					Ok::<(), tg::Error>(())
				})
				.collect::<Vec<_>>(),
		)
		.await?;

		Ok(())
	}
}

async fn create_dummy_package(handle: &impl tg::Handle, tag: &str) -> tg::Result<tg::Artifact> {
	let content = formatdoc!(
		r#"
		export default () => {{ throw new Error("This is a dummy package placeholder for cycle resolution. The real implementation should be published shortly."); }};

		export let metadata = {{
			tag: "{}",
		}};
	"#,
		tag
	);
	let file = tg::File::builder(content).build();
	file.store(handle).await?;
	Ok(file.into())
}

async fn is_package(handle: &impl tg::Handle, artifact: &tg::Artifact) -> tg::Result<bool> {
	match artifact {
		tg::Artifact::File(_file) => Ok(true),
		tg::Artifact::Directory(directory) => Ok(tg::package::try_get_root_module_file_name(
			handle,
			Either::Left(&directory.clone().into()),
		)
		.await?
		.is_some()),
		tg::Artifact::Symlink(_) => Ok(false),
	}
}

async fn is_contained_in_directory(
	handle: &impl tg::Handle,
	directory: &tg::Directory,
	artifact: &tg::Artifact,
) -> tg::Result<bool> {
	let entries = directory.entries(handle).await?;

	for entry in entries.values() {
		if entry.id() == artifact.id() {
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

async fn get_package_tag(handle: &impl tg::Handle, artifact: &tg::Artifact) -> tg::Result<String> {
	let (kind, item) = match artifact {
		tg::Artifact::File(file) => {
			let kind = tg::module::Kind::Ts;
			let item = tg::module::Item::Object(file.clone().into());
			(kind, item)
		},
		tg::Artifact::Directory(directory) => {
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
	let tag = map
		.get("tag")
		.ok_or_else(|| tg::error!("metadata missing 'tag' field"))?
		.try_unwrap_string_ref()
		.map_err(|_| tg::error!("expected 'tag' to be a string"))?
		.clone();

	Ok(tag)
}

async fn put_local_tag(
	handle: &impl tg::Handle,
	tag: &tg::Tag,
	artifact_id: &tg::artifact::Id,
) -> tg::Result<()> {
	let object_id: tg::object::Id = artifact_id.clone().into();
	let arg = tg::tag::put::Arg {
		force: true,
		item: Either::Right(object_id),
		remote: None,
	};
	handle.put_tag(tag, arg).await
}

async fn re_checkin_without_local_deps(
	handle: &impl tg::Handle,
	path: &std::path::Path,
) -> tg::Result<tg::Artifact> {
	let arg = tg::checkin::Arg {
		path: path.to_owned(),
		options: tg::checkin::Options {
			local_dependencies: false,
			lock: false,
			..Default::default()
		},
		updates: Vec::new(),
	};
	tg::checkin::checkin(handle, arg).await
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

impl Scc {
	fn node_indices(&self) -> &[usize] {
		match self {
			Scc::Single(idx) => std::slice::from_ref(idx),
			Scc::Cycle(indices) => indices,
		}
	}

	fn is_cycle(&self) -> bool {
		matches!(self, Scc::Cycle(_))
	}
}
