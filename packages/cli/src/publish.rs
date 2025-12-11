use {
	crate::Cli,
	petgraph::algo::tarjan_scc,
	radix_trie::TrieCommon as _,
	std::{collections::HashMap, path::PathBuf},
	tangram_client::prelude::*,
	tangram_either::Either,
};

/// Publish a package with its transitive dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Display the items that will be published in their order, but don't actually publish them.
	#[arg(default_value = "false", long)]
	pub dry_run: bool,

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

#[derive(Default)]
struct Graph {
	indices: HashMap<tg::object::Id, usize, tg::id::BuildHasher>,
	nodes: Vec<Node>,
}

pub struct Node {
	package: tg::Referent<tg::Object>,
	incoming: Vec<usize>,
	outgoing: Vec<usize>,
}

#[derive(Default)]
struct State {
	file_tree: radix_trie::Trie<PathBuf, tg::Artifact>,
	all_packages: Vec<tg::Referent<tg::Object>>,
	local_packages: Vec<tg::Referent<tg::Object>>,
	tags: Vec<(tg::Tag, tg::object::Id)>,
	graph: Graph,
}

#[derive(Debug, Clone, serde::Serialize)]
struct Item {
	referent: tg::Referent<tg::object::Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	checkin: Option<tg::checkin::Options>,
	tag: tg::Tag,
	push: bool,
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
		let referent = self
			.get_reference(&reference)
			.await?
			.try_map(|item| item.left().ok_or_else(|| tg::error!("expected an object")))?;

		// Create the state.
		let mut state = State::default();

		// Scan objects.
		state
			.visit_objects(&handle, &referent)
			.await
			.map_err(|source| tg::error!(!source, "failed to analyze objects"))?;

		// Build the local dependency graph.
		state
			.create_graph(&handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to create package graph"))?;

		// Create the publishing plan.
		let plan = state
			.create_plan(&handle, args.tag.map(tg::Tag::new))
			.await
			.map_err(|source| tg::error!(!source, "failed to create publishing plan"))?;

		// Print the plan if this is a dry run.
		if args.dry_run {
			self.print_serde(plan, crate::print::Options::default())
				.await?;
			return Ok(());
		}

		// Collect all the local tags.
		let mut tags = state.tags;

		// Collect all the local items.
		let mut items = tags
			.iter()
			.map(|(_, id)| Either::Left(id.clone()))
			.collect::<Vec<_>>();

		// Execute the plan.
		for mut item in plan {
			// Check in the artifact if requested. This is only possible if the root is referred to by path and the item has a path.
			if let Some(options) = item.checkin {
				let path = reference.item().unwrap_path_ref().join(
					item.referent
						.path()
						.ok_or_else(|| tg::error!("expected a path!"))?,
				);
				let path = tangram_util::fs::canonicalize_parent(&path)
					.await
					.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
				let args = tg::checkin::Arg {
					path: path.clone(),
					options,
					updates: Vec::new(),
				};
				let artifact = tg::checkin(&handle, args).await.map_err(|source| tg::error!(!source, path = %path.display(), "failed to checkin local package during publishing"))?;
				item.referent.item = artifact.id().into();
			}
			if item.push {
				items.push(Either::Left(item.referent.item.clone()));
				tags.push((item.tag.clone(), item.referent.item.clone()));
			}
			handle
				.put_tag(
					&item.tag,
					tg::tag::put::Arg {
						force: true,
						item: Either::Left(item.referent.item().clone()),
						local: None,
						remotes: None,
					},
				)
				.await
				.map_err(
					|source| tg::error!(!source, tag = %item.tag, "failed to put local tag"),
				)?;
		}

		// Get the remote.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());

		// Push.
		let stream = handle
			.push(tg::push::Arg {
				commands: false,
				eager: true,
				items,
				logs: false,
				outputs: true,
				recursive: false,
				remote: Some(remote.clone()),
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to push items"))?;
		let output = self
			.render_progress_stream(stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to push items"))?;
		let message = format!("pushed {} objects, {} bytes", output.objects, output.bytes);
		Self::print_info_message(&message);

		// Put tags on the remote.
		let tags = tags
			.into_iter()
			.map(|(tag, item)| tg::tag::post::Item {
				tag,
				item: Either::Left(item),
				force: false,
			})
			.collect::<Vec<_>>();
		handle
			.post_tag_batch(tg::tag::post::Arg {
				remote: Some(remote.clone()),
				tags: tags.clone(),
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to publish tags to remote"))?;
		for item in &tags {
			let message = format!("tagged {} {}", item.tag, item.item);
			Self::print_info_message(&message);
		}

		Ok(())
	}
}

/// Given an object, get the tag from its internal metadata.
async fn try_get_package_tag(
	handle: &impl tg::Handle,
	object: &tg::Object,
) -> tg::Result<Option<tg::Tag>> {
	// Currently only files and directories may have package metadata.
	let (kind, object) = match object {
		// Use the file directly
		tg::Object::File(_) => {
			// Assume ts.
			let kind = tg::module::Kind::Ts;
			(kind, object.clone())
		},
		// For directories, look for a root module.
		tg::Object::Directory(directory) => {
			// Get the root file name.
			let Some(name) =
				tg::package::try_get_root_module_file_name(handle, Either::Left(directory)).await?
			else {
				return Ok(None);
			};

			// Detect the kind.
			let kind = if std::path::Path::new(name)
				.extension()
				.is_some_and(|ext| ext == "js")
			{
				tg::module::Kind::Js
			} else if std::path::Path::new(name)
				.extension()
				.is_some_and(|ext| ext == "ts")
			{
				tg::module::Kind::Ts
			} else {
				return Err(tg::error!(%name, "unknown root module name"));
			};

			// Extract the file.
			let file = directory.get(handle, name).await?;
			(kind, file.into())
		},
		// Nothing else has metadata.
		_ => return Ok(None),
	};

	// Create a module for the item.
	let item = tg::graph::Edge::Object(object);
	let item = tg::module::Item::Edge(item);
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
	let Ok(output) = tg::run::run(handle, arg).await else {
		return Ok(None);
	};
	let map = output
		.try_unwrap_map()
		.map_err(|_| tg::error!("expected metadata to be a map"))?;

	let tag = map
		.get("tag")
		.ok_or_else(|| tg::error!("metadata missing 'tag' field"))?
		.try_unwrap_string_ref()
		.map_err(|_| tg::error!("expected 'tag' to be a string"))?
		.clone();
	Ok(Some(tg::Tag::new(tag)))
}

impl State {
	/// First pass: visit all objects.
	async fn visit_objects(
		&mut self,
		handle: &impl tg::Handle,
		root: &tg::Referent<tg::Object>,
	) -> tg::Result<()> {
		// Edge case: make sure the root is added if it is on the local file system.
		if root.path().is_some() {
			self.local_packages.push(root.clone());
			self.add_package(root);
		}

		// Visit all the objects.
		tg::object::visit(handle, self, root).await
	}

	/// Second pass, build the graph.
	async fn create_graph(&mut self, handle: &impl tg::Handle) -> tg::Result<()> {
		for package in self.all_packages.clone() {
			let Self {
				file_tree,
				graph,
				all_packages,
				..
			} = self;
			let index = Self::get_package_index(graph, package.clone());

			// Update the entry for this package.
			let mut stack = file_tree
				.subtrie(package.path().unwrap())
				.into_iter()
				.collect::<Vec<_>>();

			while let Some(subtrie) = stack.pop() {
				if let Some(tg::Artifact::File(file)) = subtrie.value() {
					let dependencies = file
						.dependencies(handle)
						.await?
						.values()
						.filter_map(|referent| {
							let referent = referent.as_ref()?;
							let package = all_packages
								.iter()
								.find(|r| r.item().id() == referent.item.id())?;
							Some(Self::get_package_index(graph, package.clone()))
						})
						.collect::<Vec<_>>();
					for dependency in dependencies {
						graph.nodes[index].outgoing.push(dependency);
						graph.nodes[dependency].incoming.push(index);
					}
				}
				stack.extend(subtrie.children());
			}
		}
		Ok(())
	}

	// Third pass, construct the plan of tags and checkins.
	async fn create_plan(
		&mut self,
		handle: &impl tg::Handle,
		mut tag: Option<tg::Tag>,
	) -> tg::Result<Vec<Item>> {
		let sccs = tarjan_scc(&self.graph);
		let mut plan = Vec::new();
		for scc in sccs {
			let mut items = Vec::new();
			for index in scc.iter().copied() {
				// Get the node.
				let node = &self.graph.nodes[index];
				node.package.item.unload();

				// A candidate may be published if:
				// - It is the root package.
				// - It is referred to by another package
				// - It is a bare file that isn't contained by any directories.
				let publishable = index == 0
					|| self.local_packages.iter().any(|referent| {
						(*referent).clone().map(|r| r.id()) == node.package.clone().map(|r| r.id())
					}) || !node
					.incoming
					.iter()
					.copied()
					.any(|incoming| self.graph.nodes[incoming].package.item().is_directory());
				if !publishable {
					continue;
				}

				// Check if this package has a tag.
				let override_ = (index == 0).then_some(()).and(tag.take());
				let original = try_get_package_tag(handle, node.package.item())
					.await
					.map_err(
						|source| tg::error!(!source, package = %node.package, "failed to read the package metadata"),
					)?;
				let Some(tag) = override_.or(original) else {
					continue;
				};

				// If this node has no local dependencies then we don't have to check it in again
				let checkin = (!node.outgoing.is_empty()).then(|| tg::checkin::Options {
					local_dependencies: false,
					lock: false,
					solve: true,
					..tg::checkin::Options::default()
				});

				// If this is in a strongly connected component add a checkin step without dependencies
				items.push(Item {
					referent: node.package.clone().map(|object| object.id()),
					checkin,
					tag,
					push: true,
				});
			}

			// If this is an SCC then we _must_ have a cycle among local dependencies, so we have to check it every item without dependencies first, then solve again.
			if items.len() > 1 {
				for item in &items {
					// If the tag already exists skip checking it back in.
					if handle
						.try_get_tag(
							&tg::tag::Pattern::new(item.tag.to_string()),
							tg::tag::get::Arg::default(),
						)
						.await
						.map(|t| t.is_some())
						.unwrap_or(false)
					{
						continue;
					}

					// Otherwise, we need to check this local package in twice.
					plan.push(Item {
						checkin: Some(tg::checkin::Options {
							local_dependencies: false,
							lock: false,
							solve: false,
							..tg::checkin::Options::default()
						}),
						push: false,
						..item.clone()
					});
				}
			}
			plan.extend(items);
		}
		Ok(plan)
	}

	fn get_package_index(graph: &mut Graph, referent: tg::Referent<tg::Object>) -> usize {
		*graph.indices.entry(referent.item.id()).or_insert_with(|| {
			let index = graph.nodes.len();
			graph.nodes.push(Node {
				package: referent,
				incoming: Vec::new(),
				outgoing: Vec::new(),
			});
			index
		})
	}

	fn add_package(&mut self, referent: &tg::Referent<tg::Object>) {
		if self
			.all_packages
			.iter()
			.any(|package| package.item.id() == referent.item.id())
		{
			return;
		}
		self.all_packages.push(referent.clone());
	}
}

impl<H> tg::object::Visitor<H> for State
where
	H: tg::Handle,
{
	async fn visit_blob(
		&mut self,
		_handle: &H,
		blob: tangram_client::Referent<&tangram_client::Blob>,
	) -> tangram_client::Result<bool> {
		// Track local tags.
		if let Some(tag) = blob.tag() {
			self.tags.push((tag.clone(), blob.item().id().into()));
		}
		Ok(false)
	}

	async fn visit_directory(
		&mut self,
		handle: &H,
		directory: tg::Referent<&tg::Directory>,
	) -> tg::Result<bool> {
		// Track local tags.
		if let Some(tag) = directory.tag() {
			self.tags.push((tag.clone(), directory.item().id().into()));
		}
		let Some(path) = directory.path() else {
			return Ok(true);
		};
		self.file_tree
			.insert(path.to_owned(), directory.item.clone().into());

		// Keep track of files.
		if tg::package::try_get_root_module_file_name(handle, Either::Left(directory.item()))
			.await?
			.is_some()
		{
			self.add_package(&directory.clone().map(|d| d.clone().into()));
		}

		Ok(true)
	}

	async fn visit_file(&mut self, handle: &H, file: tg::Referent<&tg::File>) -> tg::Result<bool> {
		// Track local tags.
		if let Some(tag) = file.tag() {
			self.tags.push((tag.clone(), file.item().id().into()));
		}

		let Some(path) = file.path() else {
			return Ok(true);
		};
		self.file_tree
			.insert(path.to_owned(), file.item.clone().into());

		// Mark the packages that are locals.
		for (reference, dependency) in file.item.dependencies(handle).await? {
			let Some(mut dependency) = dependency else {
				continue;
			};

			// Make sure to inherit the dependency.
			dependency.inherit(&file);
			if reference.options().local.is_some() {
				self.local_packages.push(dependency.clone());
				self.add_package(&dependency);
			}
		}
		Ok(true)
	}

	async fn visit_symlink(
		&mut self,
		_handle: &H,
		symlink: tg::Referent<&tg::Symlink>,
	) -> tg::Result<bool> {
		// Track local tags.
		if let Some(tag) = symlink.tag() {
			self.tags.push((tag.clone(), symlink.item().id().into()));
		}
		Ok(true)
	}

	async fn visit_command(
		&mut self,
		_handle: &H,
		command: tangram_client::Referent<&tangram_client::Command>,
	) -> tangram_client::Result<bool> {
		// Track local tags.
		if let Some(tag) = command.tag() {
			self.tags.push((tag.clone(), command.item().id().into()));
		}
		Ok(false)
	}

	async fn visit_graph(
		&mut self,
		_handle: &H,
		graph: tangram_client::Referent<&tangram_client::Graph>,
	) -> tangram_client::Result<bool> {
		// Track local tags.
		if let Some(tag) = graph.tag() {
			self.tags.push((tag.clone(), graph.item().id().into()));
		}
		Ok(false)
	}
}

impl petgraph::visit::GraphBase for Graph {
	type EdgeId = ();
	type NodeId = usize;
}

impl petgraph::visit::IntoNodeIdentifiers for &Graph {
	type NodeIdentifiers = std::ops::Range<usize>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.nodes.len()
	}
}

impl petgraph::visit::NodeIndexable for Graph {
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

impl<'a> petgraph::visit::IntoNeighbors for &'a Graph {
	type Neighbors = std::iter::Copied<std::slice::Iter<'a, usize>>;

	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		self.nodes[a].outgoing.iter().copied()
	}
}
