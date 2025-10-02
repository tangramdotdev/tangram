use {
	crate::Cli,
	indoc::formatdoc,
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

struct PublishOrder {
	cycles: Vec<Vec<usize>>,
	non_cyclic: Vec<usize>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct Metadata {
	name: String,
	version: String,
}

impl Cli {
	pub async fn command_publish(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Obtain directory.
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

		// Compute graph and report size.
		let graph = self.build_dependency_graph(&handle, directory).await?;
		let order = graph.compute_publish_order();
		let cyclic_packages = order.cycles.iter().map(std::vec::Vec::len).sum::<usize>();
		let total_packages = cyclic_packages + order.non_cyclic.len();
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

	async fn create_dummy_package(
		&self,
		handle: &impl tg::Handle,
		metadata: &Metadata,
	) -> tg::Result<tg::Directory> {
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
		let mut entries = std::collections::BTreeMap::new();
		entries.insert("tangram.ts".to_owned(), file.into());
		let directory = tg::Directory::with_entries(entries);
		directory.store(handle).await?;
		Ok(directory)
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

		while let Some(current_index) = queue.pop_front() {
			let current_node = &nodes[current_index];
			let current_directory = current_node.directory.clone();

			let artifact = tg::Artifact::from(current_directory);
			let dependencies = artifact.dependencies(handle).await?;

			let mut dependency_indices = Vec::new();

			for dependency in dependencies {
				// We only care about directory dependencies with root modules (packages).
				if let Ok(directory) = tg::Directory::try_from(dependency) {
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
		order: PublishOrder,
		remote: &str,
	) -> tg::Result<()> {
		// Phase 1: Create dummy packages and tag them locally for cycle resolution.
		if !order.cycles.is_empty() {
			eprintln!("detected dependency cycles, creating local dummy package tags");

			for cycle in &order.cycles {
				for &node_index in cycle {
					let node = &graph.nodes[node_index];
					let metadata = &node.metadata;

					let dummy_directory = self.create_dummy_package(handle, metadata).await?;

					let tag_string = format!("{}/{}", metadata.name, metadata.version);
					let tag = tag_string
						.parse::<tg::Tag>()
						.map_err(|source| tg::error!(!source, "failed to parse tag"))?;

					eprintln!("creating local dummy tag {tag}");

					let arg = tg::tag::put::Arg {
						force: true,
						item: Either::Right(dummy_directory.id().clone().into()),
						remote: None,
					};
					handle.put_tag(&tag, arg).await?;
				}
			}
		}

		// Phase 2: Publish non-cyclic packages in topological order.
		let mut items_to_push = Vec::new();
		let mut tags_to_put: Vec<(tg::Tag, tg::object::Id)> = Vec::new();

		for node_index in &order.non_cyclic {
			let node = &graph.nodes[*node_index];
			let directory = &node.directory;
			let metadata = &node.metadata;

			let tag_string = format!("{}/{}", metadata.name, metadata.version);
			let tag = tag_string
				.parse::<tg::Tag>()
				.map_err(|source| tg::error!(!source, "failed to parse tag"))?;

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
							eprintln!("tag {tag} points to a process, overwriting");
							true
						},
					}
				} else {
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

		// Push all non-cyclic items.
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

		// Put all non-cyclic tags both locally and on remote.
		for (tag, item) in &tags_to_put {
			let arg = tg::tag::put::Arg {
				force: true,
				item: Either::Right(item.clone()),
				remote: None,
			};
			handle.put_tag(tag, arg).await?;

			let arg = tg::tag::put::Arg {
				force: true,
				item: Either::Right(item.clone()),
				remote: Some(remote.to_owned()),
			};
			handle.put_tag(tag, arg).await?;
		}

		// Phase 3: Overwrite dummy packages with real implementations.
		if !order.cycles.is_empty() {
			eprintln!("overwriting dummy packages with real implementations");
			let mut real_items = Vec::new();
			let mut real_tags: Vec<(tg::Tag, tg::object::Id)> = Vec::new();

			for cycle in &order.cycles {
				for &node_index in cycle {
					let node = &graph.nodes[node_index];
					let directory = &node.directory;
					let metadata = &node.metadata;

					let tag_string = format!("{}/{}", metadata.name, metadata.version);
					let tag = tag_string
						.parse::<tg::Tag>()
						.map_err(|source| tg::error!(!source, "failed to parse tag"))?;

					eprintln!("publishing {tag} (real package {})", directory.id());
					real_items.push(Either::Right(directory.id().clone().into()));
					real_tags.push((tag.clone(), directory.id().clone().into()));
				}
			}

			// Push all real items.
			if !real_items.is_empty() {
				eprintln!("pushing {} real package objects", real_items.len());
				let arg = tg::push::Arg {
					commands: false,
					items: real_items,
					logs: false,
					outputs: true,
					recursive: false,
					remote: Some(remote.to_owned()),
				};
				let stream = handle.push(arg).await?;
				self.render_progress_stream(stream).await?;
			}

			// Put all real tags, overwriting the dummies both locally and on remote.
			for (tag, item) in &real_tags {
				eprintln!("updating tag {tag} to point to {item}");

				let arg = tg::tag::put::Arg {
					force: true,
					item: Either::Right(item.clone()),
					remote: None,
				};
				handle.put_tag(tag, arg).await?;

				let arg = tg::tag::put::Arg {
					force: true,
					item: Either::Right(item.clone()),
					remote: Some(remote.to_owned()),
				};
				handle.put_tag(tag, arg).await?;
			}
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
	fn compute_publish_order(&self) -> PublishOrder {
		let sccs = tarjan_scc(self);
		let mut cycles = Vec::new();
		let mut non_cyclic = Vec::new();

		for scc in &sccs {
			// If there's a cycle, the SCC will have more than one node.
			if scc.len() > 1 {
				cycles.push(scc.clone());
			} else {
				non_cyclic.extend(scc);
			}
		}

		PublishOrder { cycles, non_cyclic }
	}
}
