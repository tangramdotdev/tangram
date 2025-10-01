use {
	crate::Cli,
	petgraph::{
		algo::tarjan_scc,
		visit::{GraphBase, IntoNeighbors, IntoNodeIdentifiers, NodeIndexable},
	},
	std::{
		collections::{HashMap, VecDeque},
		path::PathBuf,
	},
	tangram_client as tg,
	tangram_either::Either,
};

/// Publish a package with its transitive dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The path of the package
	#[arg(default_value = ".")]
	pub path: Option<PathBuf>,

	/// The remote to publish to
	#[arg(long, short)]
	pub remote: Option<String>,
}

struct DependencyGraph {
	nodes: Vec<PackageNode>,
}

struct PackageNode {
	directory: tg::Directory,
	metadata: Metadata,
	dependency_indices: Vec<usize>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct Metadata {
	name: String,
	version: String,
}

impl Cli {
	pub async fn command_publish(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		let path = std::path::absolute(args.path.unwrap_or_default())
			.map_err(|source| tg::error!(!source, "failed to get the path"))?;
		let reference = tg::Reference::with_path(path.clone());
		let referent = self.get_reference(&reference).await?;
		let item = referent
			.item
			.clone()
			.right()
			.ok_or_else(|| tg::error!("expected an object"))?;
		let directory = item
			.try_unwrap_directory()
			.map_err(|source| tg::error!(!source, "expected a directory"))?;

		eprintln!("building dependency graph for {}", path.display());

		let graph = self.build_dependency_graph(&handle, directory).await?;
		// FIXME remove this.
		eprintln!("computing publish order from graph");
		eprint!("[");
		for node in &graph.nodes {
			let id = node.directory.id();
			eprint!(" {id} ");
		}
		eprintln!("]");

		let order = graph.compute_publish_order()?;
		eprintln!("publishing {} packages: {:?}", order.len(), order);
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());
		self.publish_in_order(&handle, &graph, order, &remote)
			.await?;

		eprintln!("publish complete");

		Ok(())
	}

	async fn is_package(
		&self,
		handle: &impl tg::Handle,
		directory: &tg::Directory,
	) -> tg::Result<bool> {
		Ok(tg::package::try_get_root_module_file_name(
			handle,
			Either::Left(&directory.clone().into()),
		)
		.await?
		.is_some())
	}

	async fn get_package_metadata(
		&mut self,
		handle: &impl tg::Handle,
		directory: &tg::Directory,
	) -> tg::Result<Metadata> {
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

		let item = directory.get(handle, root_module_file_name).await?;
		let item = tg::module::Item::Object(item.into());
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

	async fn build_dependency_graph(
		&mut self,
		handle: &impl tg::Handle,
		root: tg::Directory,
	) -> tg::Result<DependencyGraph> {
		let mut nodes = Vec::new();
		let mut id_to_index = HashMap::new();
		let mut queue = VecDeque::new();

		// Start with the root directory.
		let root_id = root.id();
		let root_index = 0;
		id_to_index.insert(root_id.clone(), root_index);
		let root_metadata = self.get_package_metadata(handle, &root).await.map_err(
			|source| tg::error!(!source, ?directory = root_id, "failed to get metadata for root package"),
		)?;
		nodes.push(PackageNode {
			directory: root.clone(),
			metadata: root_metadata,
			dependency_indices: Vec::new(),
		});
		queue.push_back(root_index);

		// Perform BFS traversal.
		while let Some(current_index) = queue.pop_front() {
			let current_node = &nodes[current_index];
			let current_directory = current_node.directory.clone();

			let artifact = tg::Artifact::from(current_directory);
			let dependencies = artifact.dependencies(handle).await?;

			let mut dependency_indices = Vec::new();

			for dependency in dependencies {
				// We only care about directory dependencies with root modules (packages).
				if let Ok(directory) = tg::Directory::try_from(dependency) {
					// Check if this directory is a package.
					if !self.is_package(handle, &directory).await? {
						continue;
					}
					let dep_id = directory.id();
					let dep_index = if let Some(&existing_index) = id_to_index.get(&dep_id) {
						existing_index
					} else {
						let new_index = nodes.len();
						id_to_index.insert(dep_id.clone(), new_index);
						let metadata = self
							.get_package_metadata(handle, &directory)
							.await
							.map_err(
								|source| tg::error!(!source, ?directory = dep_id, "failed to get metadata for dependency"),
							)?;
						nodes.push(PackageNode {
							directory: directory.clone(),
							metadata,
							dependency_indices: Vec::new(),
						});
						queue.push_back(new_index);
						new_index
					};
					dependency_indices.push(dep_index);
				}
			}

			nodes[current_index].dependency_indices = dependency_indices;
		}

		Ok(DependencyGraph { nodes })
	}

	async fn publish_in_order(
		&mut self,
		handle: &impl tg::Handle,
		graph: &DependencyGraph,
		order: Vec<usize>,
		remote: &str,
	) -> tg::Result<()> {
		// Collect all items and tags to publish.
		let mut items_to_push = Vec::new();
		let mut tags_to_put = Vec::new();

		for node_index in order {
			let node = &graph.nodes[node_index];
			let directory = &node.directory;
			let metadata = &node.metadata;
			dbg!(&metadata);

			// Construct the tag.
			let tag_string = format!("{}/{}", metadata.name, metadata.version);
			let tag = tag_string
				.parse::<tg::Tag>()
				.map_err(|source| tg::error!(!source, "failed to parse tag"))?;
			dbg!(&tag);

			// Check if the tag already exists on the remote.
			let pattern: tg::tag::Pattern = tag.clone().into();
			let arg = tg::tag::list::Arg {
				length: Some(1),
				pattern: pattern.clone(),
				remote: Some("default".to_string()), // FIXME
				reverse: true,
			};
			let tg::tag::list::Output { data } = handle.list_tags(arg).await?;
			let existing = data.into_iter().next();

			let should_publish = if let Some(output) = existing {
				if let Some(item) = output.item {
					// Check if it points to the same directory.
					match item {
						Either::Right(object_id) => {
							let current_id = directory.id();
							if object_id == current_id.clone().into() {
								eprintln!("tag {tag} already published, skipping");
								false
							} else {
								eprintln!(
									"tag {tag} exists but points to different item, overwriting"
								);
								true
							}
						},
						Either::Left(_process_id) => {
							// Tags shouldn't point to processes, but if they do, overwrite.
							eprintln!("tag {tag} points to a process, overwriting");
							true
						},
					}
				} else {
					// Tag exists but has no item, overwrite.
					eprintln!("tag {tag} exists but has no item, overwriting");
					true
				}
			} else {
				eprintln!("publishing {tag}");
				true
			};

			if should_publish {
				items_to_push.push(Either::Right(directory.id().clone().into()));
				tags_to_put.push((tag, directory.id().clone().into()));
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
		for (tag, item) in tags_to_put {
			let arg = tg::tag::put::Arg {
				force: true,
				item: Either::Right(item),
				remote: Some(remote.to_owned()),
			};
			handle.put_tag(&tag, arg).await?;
		}

		Ok(())
	}
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
	/// Compute the publish order using Tarjan's SCC algorithm.
	/// Returns a Vec of node indices in the order they should be published
	/// (dependencies before dependents).
	fn compute_publish_order(&self) -> tg::Result<Vec<usize>> {
		let sccs = tarjan_scc(self);

		let mut order = Vec::new();
		for scc in &sccs {
			// If there's a cycle, the SCC will have more than one node.
			if scc.len() > 1 {
				let ids: Vec<_> = scc.iter().map(|&i| self.nodes[i].directory.id()).collect();
				return Err(tg::error!(?ids, "dependency cycle detected"));
			}
			order.extend(scc);
		}

		Ok(order)
	}
}
