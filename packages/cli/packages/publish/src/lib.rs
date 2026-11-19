use {
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _},
	petgraph::algo::tarjan_scc,
	radix_trie::TrieCommon as _,
	std::{
		collections::{BTreeMap, BTreeSet, HashMap},
		path::{Path, PathBuf},
	},
	tangram_cli_core::Cli,
	tangram_client::prelude::*,
};

/// Publish packages with their transitive source dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = "false", long)]
	pub dry_run: bool,

	/// Build only these exports of the published packages. Can be specified multiple times.
	#[arg(action = clap::ArgAction::Append, long = "export")]
	pub exports: Vec<String>,

	/// Replace conflicting named nodes and their descendants.
	#[arg(long, short)]
	pub force: bool,

	/// Build only on these hosts. Can be specified multiple times.
	#[arg(action = clap::ArgAction::Append, long = "host")]
	pub hosts: Vec<String>,

	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	/// Do not build the exports of the published packages.
	#[arg(long)]
	pub no_builds: bool,

	/// The paths of the packages to publish.
	#[arg(default_value = ".", index = 1)]
	pub paths: Vec<PathBuf>,

	/// Grant public read access.
	#[arg(long)]
	pub public: bool,

	/// Retry failed builds.
	#[arg(long)]
	pub retry: bool,

	#[arg(long, short)]
	pub tag: Option<String>,

	/// Write the interface beside each config whose interface differs from the interface that its runtime generates.
	#[arg(default_value = "false", long)]
	pub update_interfaces: bool,
}

/// The builds that a package's metadata requests.
#[derive(Clone, Debug, Default)]
struct Builds {
	exports: Vec<BuildExport>,
	hosts: Vec<String>,
}

#[derive(Clone, Debug)]
struct BuildExport {
	export: String,
	path: String,
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
	all_packages: Vec<tg::Referent<tg::Object>>,
	file_trees: BTreeMap<Option<tg::object::Id>, radix_trie::Trie<PathBuf, tg::Artifact>>,
	graph: Graph,
	roots: Vec<tg::object::Id>,
	source_packages: Vec<tg::Referent<tg::Object>>,
	tags: Vec<(tg::Specifier, tg::Referent<tg::object::Id>)>,
}

/// A build of an export of a published package on a host, whose process gets the tag.
#[derive(Clone, Debug, serde::Serialize)]
struct Build {
	export: String,
	host: String,
	#[serde(skip)]
	package: tg::Specifier,
	tag: tg::Specifier,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(untagged)]
enum Step {
	Item(Item),
	Cycle(Vec<Item>),
}

#[derive(Clone, Debug, serde::Serialize)]
struct Item {
	#[serde(skip_serializing_if = "Vec::is_empty")]
	builds: Vec<Build>,
	referent: tg::Referent<tg::object::Id>,
	path: Option<PathBuf>,
	tag: tg::Specifier,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;

	// Reject a tag override for more than one package.
	if args.tag.is_some() && args.paths.len() > 1 {
		return Err(tg::error!(
			"the tag can only be overridden when publishing a single path"
		));
	}

	// Check in the root packages and visit their objects.
	let mut state = State::default();
	let mut absolute_paths = Vec::new();
	for path in &args.paths {
		let absolute_path = tangram_util::fs::canonicalize_parent(path)
			.await
			.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
		let arg = tg::checkin::Arg {
			options: tg::checkin::Options::default(),
			path: absolute_path.clone(),
			updates: Vec::new(),
		};
		let output = tg::checkin::checkin_with_instance(&client, arg).await.map_err(
			|error| tg::error!(!error, path = %absolute_path.display(), "failed to check in the root package"),
		)?;
		let artifact = tg::Artifact::with_referent(output.artifact.clone());
		let mut options = output.artifact.options;
		if options.id.is_none() {
			options.path = Some(path.clone());
		}
		let referent = tg::Referent::new(artifact.into(), options);
		state
			.visit_objects(&client, &referent)
			.await
			.map_err(|error| tg::error!(!error, "failed to analyze objects"))?;
		absolute_paths.push(absolute_path);
	}
	if args.paths.len() > 1 {
		state.deduplicate_tags();
	}

	// Create the graph.
	state
		.create_graph(&client)
		.await
		.map_err(|error| tg::error!(!error, "failed to create package graph"))?;

	// Create the plan.
	let tag = args
		.tag
		.map(|tag| tag.parse())
		.transpose()
		.map_err(|error| tg::error!(!error, "failed to parse the tag"))?;
	let tag_concurrency = cli.publish_config().tag_concurrency;
	let mut plan = state
		.create_plan(&client, tag, tag_concurrency)
		.boxed()
		.await
		.map_err(|error| tg::error!(!error, "failed to create publishing plan"))?;

	// Restrict the builds to the requested exports and hosts.
	for step in &mut plan {
		let items = match step {
			Step::Item(item) => std::slice::from_mut(item),
			Step::Cycle(items) => items.as_mut_slice(),
		};
		for item in items {
			item.builds.retain(|build| {
				!args.no_builds
					&& (args.exports.is_empty() || args.exports.contains(&build.export))
					&& (args.hosts.is_empty() || args.hosts.contains(&build.host))
			});
		}
	}

	// Check the interfaces of the configs in the packages that publishing checks in from disk against the interfaces that their runtimes generate, before checking them in again.
	let mut paths = absolute_paths;
	for step in &plan {
		match step {
			Step::Item(item) => paths.extend(item.path.clone()),
			Step::Cycle(items) => paths.extend(items.iter().filter_map(|item| item.path.clone())),
		}
	}
	let update = args.update_interfaces && !args.dry_run;
	for path in config_paths(&paths).await? {
		check_interface(cli, &client, &path, update)
			.boxed_local()
			.await?;
	}

	// Print the plan if this is a dry run.
	if args.dry_run {
		cli.print_serde(&plan, tangram_cli_core::print::Options::default())
			.await?;
		return Ok(());
	}

	// Collect the builds of the published packages.
	let builds = plan
		.iter()
		.flat_map(|step| match step {
			Step::Item(item) => std::slice::from_ref(item),
			Step::Cycle(items) => items.as_slice(),
		})
		.flat_map(|item| item.builds.clone())
		.collect::<Vec<_>>();

	// Collect all the local items.
	let mut items = state
		.tags
		.iter()
		.map(|(_, node)| node.clone())
		.collect::<Vec<_>>();

	// Collect all the local tags.
	let mut tags = state
		.tags
		.into_iter()
		.map(|(tag, node)| (tag, node.node))
		.collect::<Vec<_>>();

	// Execute the plan.
	for step in plan {
		match step {
			Step::Item(item) => {
				let Item {
					path,
					referent,
					tag,
					..
				} = item;
				let node = if let Some(path) = path {
					publish_checkin(&client, path, true).await?
				} else {
					referent
				};
				let id = node.node.clone();
				let tokens = node.options.tokens.clone();
				items.push(node);
				tags.push((tag.clone(), id.clone()));
				let arg = tg::tag::put::Arg {
					tokens,
					ancestors: tg::node::Ancestors {
						create: true,
						..Default::default()
					},
					force: args.force,
					location: None,
					public: args.public,
					specifier: tag.clone(),
					target: id.into(),
				};
				client
					.put_tag(arg)
					.await
					.map_err(|error| tg::error!(!error, tag = %tag, "failed to put local tag"))?;
			},

			Step::Cycle(cycle_items) => {
				for item in &cycle_items {
					let path = item
						.path
						.clone()
						.ok_or_else(|| tg::error!("cycle items must have paths"))?;
					let checked_in = publish_checkin(&client, path, false).await?;
					let id = checked_in.node;
					let arg = tg::tag::put::Arg {
						tokens: checked_in.options.tokens,
						ancestors: tg::node::Ancestors {
							create: true,
							..Default::default()
						},
						force: args.force,
						location: None,
						public: false,
						specifier: item.tag.clone(),
						target: id.into(),
					};
					client.put_tag(arg).await.map_err(
						|error| tg::error!(!error, tag = %item.tag, "failed to put local tag"),
					)?;
				}

				for item in cycle_items {
					let Item { path, tag, .. } = item;
					let path = path.ok_or_else(|| tg::error!("cycle items must have paths"))?;
					let node = publish_checkin(&client, path, true).await?;
					let id = node.node.clone();
					let tokens = node.options.tokens.clone();
					items.push(node);
					tags.push((tag.clone(), id.clone()));
					let arg = tg::tag::put::Arg {
						tokens,
						ancestors: tg::node::Ancestors {
							create: true,
							..Default::default()
						},
						force: true,
						location: None,
						public: args.public,
						specifier: tag.clone(),
						target: id.into(),
					};
					client.put_tag(arg).await.map_err(
						|error| tg::error!(!error, tag = %tag, "failed to put local tag"),
					)?;
				}
			},
		}
	}

	// Get the location.
	let location = args.location.to_location()?.unwrap_or_else(|| {
		tg::Location::Remote(tg::location::Remote {
			name: "default".to_owned(),
			region: None,
		})
	});

	// Push the tag ancestors.
	let specifiers = tags.iter().map(|(tag, _)| tag.clone()).collect::<Vec<_>>();
	push_tag_ancestors(cli, &client, &location, &specifiers, args.force).await?;

	// Push.
	let arg = tg::push::Arg {
		destination: Some(location.clone()),
		eager: true,
		force: args.force,
		nodes: items.into_iter().map(|node| node.map(Into::into)).collect(),
		metadata: false,
		process_error_objects: true,
		process_output_objects: true,
		source: Some(tg::Location::Local(tg::location::Local::default())),
		..Default::default()
	};
	let (_, stream) = client
		.push(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to push items"))?;
	let output = cli
		.render_progress_stream(stream)
		.await
		.map_err(|error| tg::error!(!error, "failed to push items"))?;

	let processes = output.skipped.processes;
	let objects = output.skipped.objects;
	let bytes = byte_unit::Byte::from_u64(output.skipped.bytes)
		.get_appropriate_unit(byte_unit::UnitType::Decimal);
	let message = format!("skipped {processes} processes, {objects} objects, {bytes:#.1}");
	cli.print_info_message(&message);
	let processes = output.transferred.processes;
	let objects = output.transferred.objects;
	let bytes = byte_unit::Byte::from_u64(output.transferred.bytes)
		.get_appropriate_unit(byte_unit::UnitType::Decimal);
	let message = format!("transferred {processes} processes, {objects} objects, {bytes:#.1}");
	cli.print_info_message(&message);

	// Put tags on the remote.
	let tags = tags
		.into_iter()
		.map(|(tag, node)| tg::tag::batch::Item {
			tokens: tg::authorization::Tokens::default(),
			target: node.into(),
			specifier: tag,
		})
		.collect::<Vec<_>>();
	let arg = tg::tag::batch::Arg {
		force: args.force,
		location: Some(location.clone().into()),
		parents: true,
		public: args.public,
		tags: tags.clone(),
	};
	client
		.post_tag_batch(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to publish tags to remote"))?;
	for item in &tags {
		let message = format!("tagged {} {:?}", item.specifier, item.target);
		cli.print_info_message(&message);
	}

	// Build the exports of the published packages concurrently, spawning each build while fewer than the build concurrency are running.
	let build_concurrency = cli.publish_config().build_concurrency;
	let mut results = builds.iter().map(|_| None).collect::<Vec<_>>();
	let mut waits = futures::stream::FuturesUnordered::new();
	let mut pending = builds.iter().enumerate();
	loop {
		while waits.len() < build_concurrency
			&& let Some((index, build)) = pending.next()
		{
			let result = publish_build_spawn(cli, build, args.public, args.retry)
				.boxed_local()
				.await;
			match result {
				Ok(process) => {
					let future =
						publish_build_wait(&client, build, process, args.force, args.public)
							.map(move |result| (index, result));
					waits.push(future);
				},
				Err(error) => {
					results[index] = Some(Err(error));
				},
			}
		}
		let Some((index, result)) = waits.next().await else {
			break;
		};
		results[index] = Some(result);
	}

	// Collect the results in the order of the builds, warning about and skipping a build that fails.
	let mut processes = Vec::new();
	let mut build_tags = Vec::new();
	for (build, result) in builds.iter().zip(results) {
		let result = result.ok_or_else(|| tg::error!("expected a build result"))?;
		match result {
			Ok(process) => {
				build_tags.push((build.tag.clone(), process.node.clone()));
				processes.push(process.map(Into::into));
			},
			Err(error) => {
				let message = format!(
					"skipped the build of {}#{} on {}: {}",
					build.package,
					build.export,
					build.host,
					error.trace()
				);
				Cli::print_warning_message(&message);
			},
		}
	}
	if processes.is_empty() {
		return Ok(());
	}

	// Push the build tag ancestors and the processes.
	let specifiers = build_tags
		.iter()
		.map(|(tag, _)| tag.clone())
		.collect::<Vec<_>>();
	push_tag_ancestors(cli, &client, &location, &specifiers, args.force).await?;
	let arg = tg::push::Arg {
		destination: Some(location.clone()),
		eager: true,
		force: args.force,
		nodes: processes,
		metadata: false,
		process_error_objects: true,
		process_output_objects: true,
		source: Some(tg::Location::Local(tg::location::Local::default())),
		..Default::default()
	};
	let (_, stream) = client
		.push(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to push the builds"))?;
	cli.render_progress_stream(stream)
		.await
		.map_err(|error| tg::error!(!error, "failed to push the builds"))?;

	// Put the build tags on the remote.
	let tags = build_tags
		.into_iter()
		.map(|(tag, process)| tg::tag::batch::Item {
			specifier: tag,
			target: process.into(),
			tokens: tg::authorization::Tokens::default(),
		})
		.collect::<Vec<_>>();
	let arg = tg::tag::batch::Arg {
		force: args.force,
		location: Some(location.into()),
		parents: true,
		public: args.public,
		tags: tags.clone(),
	};
	client
		.post_tag_batch(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to publish the build tags to remote"))?;
	for item in &tags {
		let message = format!("tagged {} {:?}", item.specifier, item.target);
		cli.print_info_message(&message);
	}

	Ok(())
}

impl State {
	async fn visit_objects(
		&mut self,
		client: &impl tg::Instance,
		root: &tg::Referent<tg::Object>,
	) -> tg::Result<()> {
		// Record the root, and make sure that it is added if it is on the local file system.
		let id = root.node().id();
		if !self.roots.contains(&id) {
			self.roots.push(id);
		}
		if root.path().is_some() {
			self.source_packages.push(root.clone());
			self.add_package(root);
		}

		// Visit all the objects.
		tg::object::visit(client, self, root, false).await
	}

	async fn create_graph(&mut self, client: &impl tg::Instance) -> tg::Result<()> {
		for package in self.all_packages.clone() {
			let Self {
				all_packages,
				file_trees,
				graph,
				..
			} = self;
			let index = Self::get_package_index(graph, package.clone());

			// Update the entry for this package.
			let mut stack = file_trees
				.get(&package.options.id)
				.and_then(|tree| tree.subtrie(package.path().unwrap()))
				.into_iter()
				.collect::<Vec<_>>();

			while let Some(subtrie) = stack.pop() {
				if let Some(tg::Artifact::File(file)) = subtrie.value() {
					let dependencies = file
						.dependencies_with_instance(client)
						.await?
						.values()
						.filter_map(|option| {
							let dependency = option.as_ref()?;
							let node = dependency.0.node.as_ref()?;
							let package =
								all_packages.iter().find(|r| r.node().id() == node.id())?;
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

	async fn create_plan(
		&mut self,
		client: &impl tg::Instance,
		mut tag: Option<tg::Specifier>,
		tag_concurrency: usize,
	) -> tg::Result<Vec<Step>> {
		// Fetch all package tags in parallel with limited concurrency.
		let packages: Vec<_> = self
			.graph
			.nodes
			.iter()
			.map(|node| {
				(
					node.package.node().id(),
					node.package.node().clone(),
					node.package.path().map(ToOwned::to_owned),
				)
			})
			.collect();
		let tags: HashMap<tg::object::Id, Option<(tg::Specifier, Builds)>> =
			futures::stream::iter(packages)
				.map(|(id, object, path)| async move {
					let tag =
						try_get_package_tag_and_builds(client, &object, path.as_deref()).await?;
					Ok::<_, tg::Error>((id, tag))
				})
				.buffer_unordered(tag_concurrency)
				.try_collect()
				.await?;

		let sccs = tarjan_scc(&self.graph);
		let mut plan = Vec::new();
		for scc in sccs {
			let mut items = Vec::new();
			for index in scc.iter().copied() {
				// Get the node.
				let node = &self.graph.nodes[index];
				node.package.node.unload();

				let root = self.roots.contains(&node.package.node().id());
				let publishable =
					root || self.source_packages.iter().any(|referent| {
						referent.clone().map(|r| r.id()) == node.package.clone().map(|r| r.id())
					}) || !node
						.incoming
						.iter()
						.copied()
						.any(|incoming| self.graph.nodes[incoming].package.node().is_directory());
				if !publishable {
					continue;
				}

				// Check if this package has a tag.
				let override_ = root.then_some(()).and(tag.take());
				let metadata = tags.get(&node.package.node().id()).cloned().flatten();
				let (original, builds) = metadata.unzip();
				let Some(tag) = override_.or(original) else {
					continue;
				};
				let builds = create_builds(&tag, &builds.unwrap_or_default())?;

				// If this node has source dependencies then we need to check it in again.
				let path = if node.outgoing.is_empty() {
					None
				} else {
					let path = publish_path(client, &node.package.options).await?;
					Some(path)
				};

				items.push(Item {
					builds,
					path,
					referent: node.package.clone().map(|object| object.id()),
					tag,
				});
			}

			// If there is more than one item in the SCC, then it is a cycle.
			if items.len() > 1 {
				// Cycles must have paths for all items.
				for item in &mut items {
					if item.path.is_none() {
						let path = publish_path(client, &item.referent.options).await?;
						item.path = Some(path);
					}
				}

				// Sort by tag to ensure deterministic ordering regardless of entry point.
				items.sort_by(|a, b| a.tag.cmp(&b.tag));
				plan.push(Step::Cycle(items));
			} else {
				for item in items {
					plan.push(Step::Item(item));
				}
			}
		}
		Ok(plan)
	}

	/// Remove the duplicate tags that several roots visit.
	fn deduplicate_tags(&mut self) {
		let mut seen = BTreeSet::new();
		self.tags
			.retain(|(tag, node)| seen.insert((tag.to_string(), node.node.clone())));
	}

	fn get_package_index(graph: &mut Graph, referent: tg::Referent<tg::Object>) -> usize {
		*graph.indices.entry(referent.node.id()).or_insert_with(|| {
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
			.any(|package| package.node.id() == referent.node.id())
		{
			return;
		}
		self.all_packages.push(referent.clone());
	}
}

impl<I> tg::object::Visitor<I> for State
where
	I: tg::Instance,
{
	async fn visit_blob(
		&mut self,
		_instance: &I,
		blob: tg::Referent<&tg::Blob>,
	) -> tg::Result<bool> {
		if let Some(tag) = blob.tag() {
			let node = blob.clone().map(|blob| blob.id().into());
			self.tags.push((tag.clone(), node));
		}
		Ok(false)
	}

	async fn visit_directory(
		&mut self,
		client: &I,
		directory: tg::Referent<&tg::Directory>,
	) -> tg::Result<bool> {
		if directory
			.options
			.path
			.as_ref()
			.is_some_and(|p| p.as_os_str().is_empty())
		{
			return Err(tg::error!(id = ?directory.id(), "invalid path"));
		}

		if let Some(tag) = directory.tag() {
			let node = directory.clone().map(|directory| directory.id().into());
			self.tags.push((tag.clone(), node));
		}
		let Some(path) = directory.path() else {
			return Ok(true);
		};
		self.file_trees
			.entry(directory.options.id.clone())
			.or_default()
			.insert(path.to_owned(), directory.node.clone().into());

		// Keep track of files.
		if tg::module::try_get_root_module_file_name_with_instance(
			client,
			tg::Either::Left(directory.node()),
		)
		.await?
		.is_some()
		{
			self.add_package(&directory.clone().map(|d| d.clone().into()));
		}

		Ok(true)
	}

	async fn visit_file(&mut self, client: &I, file: tg::Referent<&tg::File>) -> tg::Result<bool> {
		if file
			.options
			.path
			.as_ref()
			.is_some_and(|p| p.as_os_str().is_empty())
		{
			return Err(tg::error!(id = ?file.id(), "invalid path"));
		}

		if let Some(tag) = file.tag() {
			let node = file.clone().map(|file| file.id().into());
			self.tags.push((tag.clone(), node));
		}

		let Some(path) = file.path() else {
			return Ok(true);
		};
		self.file_trees
			.entry(file.options.id.clone())
			.or_default()
			.insert(path.to_owned(), file.node.clone().into());

		// Mark the packages that come from source overrides.
		for (reference, option) in file.node.dependencies_with_instance(client).await? {
			let Some(mut dependency) = option else {
				continue;
			};

			// Make sure to inherit the dependency.
			dependency.0.inherit(&file);
			if reference.options().source.is_some() {
				let Some(node) = dependency.0.node.clone() else {
					continue;
				};
				let referent = tg::Referent {
					node,
					options: dependency.0.options.clone(),
				};
				self.source_packages.push(referent.clone());
				self.add_package(&referent);
			}
		}
		Ok(true)
	}

	async fn visit_symlink(
		&mut self,
		_instance: &I,
		symlink: tg::Referent<&tg::Symlink>,
	) -> tg::Result<bool> {
		if symlink
			.options
			.path
			.as_ref()
			.is_some_and(|p| p.as_os_str().is_empty())
		{
			return Err(tg::error!("invalid path"));
		}
		if let Some(tag) = symlink.tag() {
			let node = symlink.clone().map(|symlink| symlink.id().into());
			self.tags.push((tag.clone(), node));
		}
		Ok(true)
	}

	async fn visit_command(
		&mut self,
		_instance: &I,
		command: tg::Referent<&tg::Command>,
	) -> tg::Result<bool> {
		if let Some(tag) = command.tag() {
			let node = command.clone().map(|command| command.id().into());
			self.tags.push((tag.clone(), node));
		}
		Ok(false)
	}

	async fn visit_graph(
		&mut self,
		_instance: &I,
		graph: tg::Referent<&tg::Graph>,
	) -> tg::Result<bool> {
		if let Some(tag) = graph.tag() {
			let node = graph.clone().map(|graph| graph.id().into());
			self.tags.push((tag.clone(), node));
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

async fn push_tag_ancestors(
	cli: &mut Cli,
	client: &tg::Client,
	destination: &tg::Location,
	tags: &[tg::Specifier],
	force: bool,
) -> tg::Result<()> {
	if destination.is_local() {
		return Ok(());
	}

	// Collect the ancestors by depth so that parents are pushed before children.
	let mut ancestors = BTreeMap::<usize, BTreeSet<tg::Specifier>>::new();
	for specifier in tags {
		for ancestor in specifier.ancestors() {
			let depth = ancestor.components().count();
			ancestors.entry(depth).or_default().insert(ancestor);
		}
	}

	// Push each depth.
	for ancestors in ancestors.into_values() {
		let mut items = Vec::with_capacity(ancestors.len());
		for ancestor in ancestors {
			let options = tg::reference::Options {
				location: Some(tg::Location::Local(tg::location::Local::default()).into()),
				..Default::default()
			};
			let reference = tg::Reference::with_node_and_options(
				tg::reference::Node::Specifier(ancestor.clone().into()),
				options,
			);
			let referent = tangram_cli_reference::get(cli, &reference)
				.await
				.map_err(|error| tg::error!(!error, %ancestor, "failed to get a tag ancestor"))?
				.referent;
			let node = referent.try_map(|node| match node {
				tg::get::Node::Id(id) => Ok(id),
				tg::get::Node::Pointer(_) => {
					Err(tg::error!(%ancestor, "expected a tag ancestor id"))
				},
			})?;
			items.push(node);
		}
		let arg = tg::push::Arg {
			destination: Some(destination.clone()),
			force,
			nodes: items,
			source: Some(tg::Location::Local(tg::location::Local::default())),
			tag_targets: false,
			..Default::default()
		};
		let (_, stream) = client
			.push(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to push tag ancestors"))?;
		cli.render_progress_stream(stream)
			.await
			.map_err(|error| tg::error!(!error, "failed to push tag ancestors"))?;
	}

	Ok(())
}

/// Get the paths of the configs in packages on disk, which are package directories or single files.
async fn config_paths(paths: &[PathBuf]) -> tg::Result<BTreeSet<PathBuf>> {
	let mut configs = BTreeSet::new();
	let mut pending = paths.to_vec();
	while let Some(path) = pending.pop() {
		let metadata = tokio::fs::symlink_metadata(&path).await.map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to get the metadata"),
		)?;
		if metadata.is_dir() {
			let mut entries = tokio::fs::read_dir(&path).await.map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to read the directory"),
			)?;
			while let Some(entry) = entries.next_entry().await.map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to read the directory"),
			)? {
				pending.push(entry.path());
			}
		} else if metadata.is_file() && tg::module::config::Format::for_path(&path).is_some() {
			configs.insert(path);
		}
	}
	Ok(configs)
}

/// Check the interface of a config against the interface file that its runtime's `interface` export generates, and write the generated file beside the config if `update` is true.
async fn check_interface(
	cli: &mut Cli,
	client: &tg::Client,
	path: &Path,
	update: bool,
) -> tg::Result<()> {
	// Find the interfaces beside the config.
	let stem = tg::module::config::interface_file_stem(path)
		.ok_or_else(|| tg::error!("failed to get the file name of the interface"))?;
	let stem = path.with_file_name(stem);
	let mut existing = Vec::new();
	for language in tg::interface::Language::ALL {
		let interface_path = interface_file_path(&stem, language);
		let exists = tokio::fs::try_exists(&interface_path)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to determine whether the interface exists")
			})?;
		if exists {
			existing.push((language, interface_path));
		}
	}
	if existing.len() > 1 {
		return Err(
			tg::error!(path = %path.display(), "the config has more than one interface beside it"),
		);
	}

	// Build the interface export, and skip the check if the runtime does not generate an interface file.
	let reference = path
		.to_str()
		.ok_or_else(|| tg::error!("invalid path"))?
		.parse::<tg::Reference>()?;
	let module = tangram_cli_reference::get_module(cli, &reference).await?;
	let result = generate_interface(client, module.clone())
		.boxed_local()
		.await;
	let (language, text, generated) = match result {
		Ok(output) => output,
		Err(error) => {
			let path = path.display();
			let message = format!(
				"skipped checking the interface of {path} because its runtime did not generate one: {error}"
			);
			Cli::print_warning_message(&message);
			return Ok(());
		},
	};

	// Compare the generated interface with the interface beside the config, or else with the config's interface, whose imports resolve relative to where it came from.
	let matches = if let Some((language, interface_path)) = existing.first() {
		let text = tokio::fs::read_to_string(interface_path).await.map_err(
			|error| tg::error!(!error, path = %interface_path.display(), "failed to read the interface"),
		)?;
		let interface = tg::interface::parse::parse(&text, *language)?;
		without_ranges(interface) == without_ranges(generated.clone())
	} else {
		let arg = tg::module::interface::Arg {
			module: module.to_data(),
		};
		let interface = without_ranges(client.module_interface(arg).await?.interface);
		let generated = without_ranges(generated.clone());
		interface.definitions == generated.definitions
			&& interface.exports == generated.exports
			&& interface
				.imports
				.iter()
				.map(|(name, import)| (name, import.kind))
				.eq(generated
					.imports
					.iter()
					.map(|(name, import)| (name, import.kind)))
	};
	if matches {
		return Ok(());
	}

	// Write the generated interface file beside the config, and remove an interface in another language, or fail.
	if !update {
		return Err(tg::error!(
			path = %path.display(),
			"the config's interface differs from the interface that its runtime generates, so publish with --update-interfaces to write it"
		));
	}
	let interface_path = interface_file_path(&stem, language);
	tokio::fs::write(&interface_path, text).await.map_err(
		|error| tg::error!(!error, path = %interface_path.display(), "failed to write the interface"),
	)?;
	for (other, path) in existing {
		if other != language {
			tokio::fs::remove_file(&path).await.map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to remove the interface"),
			)?;
		}
	}

	Ok(())
}

/// Build a config's interface export, which is an interface file whose module kind names its language.
async fn generate_interface(
	client: &tg::Client,
	module: tg::Module,
) -> tg::Result<(tg::interface::Language, String, tg::Interface)> {
	let command =
		tg::command::Builder::with_module(module, Some("interface".to_owned()), []).build()?;
	let command = tg::Referent::with_node(tg::Either::Right(command));
	let arg = tg::process::Arg {
		command: Some(command),
		stderr: tg::process::Stdio::Log,
		stdin: tg::process::Stdio::Null,
		stdout: tg::process::Stdio::Log,
		..Default::default()
	};
	let value = tg::process::build::build_with_instance(client, arg)
		.boxed_local()
		.await?;
	let file = value
		.try_unwrap_object()
		.ok()
		.and_then(|object| object.try_unwrap_file().ok())
		.ok_or_else(|| tg::error!("expected the interface to be a file"))?;
	let language = file
		.module_with_instance(client)
		.await?
		.and_then(tg::interface::Language::for_kind)
		.ok_or_else(|| {
			tg::error!("expected the interface file to have an interface module kind")
		})?;
	let text = file.text_with_instance(client).await?;
	let interface = tg::interface::parse::parse(&text, language)?;
	Ok((language, text, interface))
}

/// Remove the ranges of an interface's exports, which depend on the text that the interface was parsed from.
fn without_ranges(mut interface: tg::Interface) -> tg::Interface {
	for export in interface.exports.values_mut() {
		export.range = None;
	}
	interface
}

/// Get the path of an interface file from its stem and its language.
fn interface_file_path(stem: &Path, language: tg::interface::Language) -> PathBuf {
	let mut path = stem.as_os_str().to_owned();
	path.push(format!(".{}", language.extension()));
	path.into()
}

/// Given an object, get the tag and the builds from its internal metadata by statically parsing the module.
async fn try_get_package_tag_and_builds(
	client: &impl tg::Instance,
	object: &tg::Object,
	package_path: Option<&Path>,
) -> tg::Result<Option<(tg::Specifier, Builds)>> {
	// Get the file text and module file name from the object.
	let (text, module_name) = match object {
		tg::Object::File(file) => {
			let text = file.text_with_instance(client).await?;
			(text, None)
		},
		tg::Object::Directory(directory) => {
			let Some(name) = tg::module::try_get_root_module_file_name_with_instance(
				client,
				tg::Either::Left(directory),
			)
			.await?
			else {
				return Ok(None);
			};

			// Get the metadata of a config package from its config.
			if let Some(format) = tg::module::config::Format::for_path(name) {
				let file = directory
					.get_with_instance(client, name)
					.await?
					.try_unwrap_file()
					.map_err(|_| tg::error!("expected a file"))?;
				let text = file.text_with_instance(client).await?;
				let config = tg::module::Config::parse(&text, format)?;
				let Some(metadata) = config.metadata else {
					return Ok(None);
				};
				let tag = metadata
					.get("tag")
					.ok_or_else(|| tg::error!(%name, "metadata is missing the 'tag' field"))?;
				let tg::Value::String(tag) = tag else {
					return Err(tg::error!(%name, "expected 'tag' to be a string"));
				};
				let tag = tag
					.parse()
					.map_err(|error| tg::error!(!error, "failed to parse the tag"))?;
				let builds = metadata
					.get("builds")
					.map(value_to_json)
					.transpose()?
					.map(|builds| parse_builds(&builds))
					.transpose()
					.map_err(|error| tg::error!(!error, %name, "invalid 'builds' metadata"))?
					.unwrap_or_default();
				return Ok(Some((tag, builds)));
			}

			let file = directory
				.get_with_instance(client, name)
				.await?
				.try_unwrap_file()
				.map_err(|_| tg::error!("expected a file"))?;
			let text = file.text_with_instance(client).await?;
			(text, Some(name.to_owned()))
		},
		_ => return Ok(None),
	};

	// Compute the full path to the module file for error messages.
	let module_path = match (&package_path, &module_name) {
		(Some(path), Some(name)) => Some(path.join(name)),
		(Some(path), None) => Some(path.to_path_buf()),
		(None, _) => None,
	};

	// Read the metadata statically, with the parser for the module's language.
	let python = match (&module_name, &package_path) {
		(Some(name), _) => tg::module::module_kind_for_path(name).ok(),
		(None, Some(path)) => tg::module::module_kind_for_path(path).ok(),
		(None, None) => None,
	} == Some(tg::module::Kind::Python);
	let result = if python {
		tangram_compiler::metadata::python(&text)
	} else {
		tangram_compiler::metadata::metadata(&text)
	};
	let map = match result {
		Ok(Some(map)) => map,
		Ok(None) => return Ok(None),
		Err(error) => {
			return if let Some(path) = module_path {
				Err(tg::error!(!error, path = %path.display(), "failed to extract metadata"))
			} else {
				let id = object.id();
				Err(tg::error!(!error, %id, "failed to extract metadata"))
			};
		},
	};

	// Get the tag from the metadata.
	let tag = map
		.get("tag")
		.ok_or_else(|| {
			if let Some(path) = &module_path {
				tg::error!(path = %path.display(), "metadata is missing the 'tag' field")
			} else {
				let id = object.id();
				tg::error!(%id, "metadata is missing the 'tag' field")
			}
		})?
		.as_str()
		.ok_or_else(|| {
			if let Some(path) = &module_path {
				tg::error!(path = %path.display(), "expected 'tag' to be a string")
			} else {
				let id = object.id();
				tg::error!(%id, "expected 'tag' to be a string")
			}
		})?;

	let tag = tag
		.parse()
		.map_err(|error| tg::error!(!error, "failed to parse the tag"))?;

	// Get the builds from the metadata.
	let builds = map
		.get("builds")
		.map(parse_builds)
		.transpose()
		.map_err(|error| tg::error!(!error, "invalid 'builds' metadata"))?
		.unwrap_or_default();

	Ok(Some((tag, builds)))
}

/// Parse the `builds` metadata, which is a bool or an object with optional `hosts` and `exports`.
fn parse_builds(value: &serde_json::Value) -> tg::Result<Builds> {
	let default_exports = || {
		vec![BuildExport {
			export: "default".to_owned(),
			path: "default".to_owned(),
		}]
	};
	let default_hosts = || {
		tg::host::SUPPORTED
			.iter()
			.map(|host| (*host).to_owned())
			.collect::<Vec<_>>()
	};
	let builds = match value {
		serde_json::Value::Bool(false) => Builds::default(),
		serde_json::Value::Bool(true) => Builds {
			exports: default_exports(),
			hosts: default_hosts(),
		},
		serde_json::Value::Object(map) => {
			let hosts = match map.get("hosts") {
				None => default_hosts(),
				Some(serde_json::Value::Array(hosts)) => hosts
					.iter()
					.map(|host| {
						host.as_str()
							.map(ToOwned::to_owned)
							.ok_or_else(|| tg::error!("expected 'hosts' to contain strings"))
					})
					.collect::<tg::Result<_>>()?,
				Some(_) => return Err(tg::error!("expected 'hosts' to be an array")),
			};
			let exports = match map.get("exports") {
				None => default_exports(),
				Some(serde_json::Value::Array(exports)) => exports
					.iter()
					.map(parse_build_export)
					.collect::<tg::Result<_>>()?,
				Some(_) => return Err(tg::error!("expected 'exports' to be an array")),
			};
			Builds { exports, hosts }
		},
		_ => return Err(tg::error!("expected 'builds' to be a bool or an object")),
	};

	Ok(builds)
}

/// Parse an export of the `builds` metadata, which is an export name that is also the tag path, or an object with an export and a path.
fn parse_build_export(value: &serde_json::Value) -> tg::Result<BuildExport> {
	match value {
		serde_json::Value::String(export) => {
			let export = BuildExport {
				export: export.clone(),
				path: export.clone(),
			};
			Ok(export)
		},
		serde_json::Value::Object(map) => {
			let export = map
				.get("export")
				.and_then(serde_json::Value::as_str)
				.ok_or_else(|| tg::error!("expected the export to have a string 'export'"))?;
			let path = map
				.get("path")
				.and_then(serde_json::Value::as_str)
				.ok_or_else(|| tg::error!("expected the export to have a string 'path'"))?;
			let export = BuildExport {
				export: export.to_owned(),
				path: path.to_owned(),
			};
			Ok(export)
		},
		_ => Err(tg::error!(
			"expected the export to be a string or an object"
		)),
	}
}

/// Convert a config's metadata value to JSON, which supports the values that the `builds` metadata uses.
fn value_to_json(value: &tg::Value) -> tg::Result<serde_json::Value> {
	let value = match value {
		tg::Value::Null => serde_json::Value::Null,
		tg::Value::Bool(value) => serde_json::Value::Bool(*value),
		tg::Value::String(value) => serde_json::Value::String(value.clone()),
		tg::Value::Array(values) => serde_json::Value::Array(
			values
				.iter()
				.map(value_to_json)
				.collect::<tg::Result<_>>()?,
		),
		tg::Value::Map(map) => serde_json::Value::Object(
			map.iter()
				.map(|(key, value)| Ok((key.clone(), value_to_json(value)?)))
				.collect::<tg::Result<_>>()?,
		),
		_ => return Err(tg::error!("unexpected value in the 'builds' metadata")),
	};

	Ok(value)
}

/// Create the builds of a package from its tag, which name their tags `name/builds/version/path/host`.
fn create_builds(package: &tg::Specifier, builds: &Builds) -> tg::Result<Vec<Build>> {
	let mut components = package.components();
	let name = components
		.next()
		.ok_or_else(|| tg::error!(%package, "expected the tag to have a name"))?;
	let version = components.collect::<Vec<_>>();
	let mut output = Vec::new();
	for export in &builds.exports {
		for host in &builds.hosts {
			let mut tag = vec![name, "builds"];
			tag.extend(version.iter().copied());
			tag.push(&export.path);
			tag.push(host);
			let tag = tag
				.join("/")
				.parse()
				.map_err(|error| tg::error!(!error, %package, "failed to create the build tag"))?;
			let build = Build {
				export: export.export.clone(),
				host: host.clone(),
				package: package.clone(),
				tag,
			};
			output.push(build);
		}
	}

	Ok(output)
}

/// Spawn the build of an export of a published package on a host without a location.
async fn publish_build_spawn(
	cli: &mut Cli,
	build: &Build,
	public: bool,
	retry: bool,
) -> tg::Result<tg::Referent<tg::process::Id>> {
	let reference = format!("{}#{}", build.package, build.export)
		.parse::<tg::Reference>()
		.map_err(|error| tg::error!(!error, "failed to parse the reference"))?;
	let mut options = tangram_cli_process::run::Options {
		detach: true,
		..Default::default()
	};
	options.spawn.host = Some(build.host.clone());
	options.spawn.public = public;
	options.spawn.retry = retry;
	let args = tangram_cli_process::run::Args {
		options,
		reference: Some(reference),
		trailing: Vec::new(),
	};
	let output = tangram_cli_process::build::build(cli, args)
		.boxed_local()
		.await?;
	let output = output
		.try_unwrap_string()
		.map_err(|_| tg::error!("expected a string"))?;
	let process = output
		.parse::<tg::Referent<tg::Either<u32, tg::process::Id>>>()
		.map_err(|error| tg::error!(!error, "failed to parse the process referent"))?
		.try_map(|node| {
			node.right()
				.ok_or_else(|| tg::error!("expected a process id"))
		})?;

	Ok(process)
}

/// Wait for the build of an export of a published package to succeed, and tag the process.
async fn publish_build_wait(
	client: &tg::Client,
	build: &Build,
	process: tg::Referent<tg::process::Id>,
	force: bool,
	public: bool,
) -> tg::Result<tg::Referent<tg::process::Id>> {
	// Wait for the build to succeed.
	let arg = tg::process::wait::Arg {
		location: process.options.location.clone().map(Into::into),
		tokens: process.options.tokens.clone(),
		..Default::default()
	};
	let outcome = client.wait_process(&process.node, arg).await.map_err(
		|error| tg::error!(!error, process = %process.node, "failed to wait for the build"),
	)?;
	if let Some(tg::Either::Left(error)) = outcome.error {
		let source = tg::Error::try_from(error)
			.map_err(|error| tg::error!(!error, "failed to deserialize the error"))?;
		return Err(tg::error!(!source, process = %process.node, "the build failed"));
	}
	if outcome.error.is_some() || outcome.exit != 0 {
		let exit = outcome.exit;
		return Err(tg::error!(process = %process.node, %exit, "the build failed"));
	}

	// Tag the process.
	let arg = tg::tag::put::Arg {
		ancestors: tg::node::Ancestors {
			create: true,
			..Default::default()
		},
		force,
		location: None,
		public,
		specifier: build.tag.clone(),
		target: process.node.clone().into(),
		tokens: process.options.tokens.clone(),
	};
	client
		.put_tag(arg)
		.await
		.map_err(|error| tg::error!(!error, tag = %build.tag, "failed to tag the build"))?;

	Ok(process)
}

async fn publish_path(
	client: &impl tg::Instance,
	options: &tg::referent::Options,
) -> tg::Result<PathBuf> {
	// Resolve the artifact-relative path through its root, retaining the authorization tokens.
	let path = if let Some(id) = &options.id {
		let root_options = tg::referent::Options {
			location: options.location.clone(),
			tokens: options.tokens.clone(),
			..Default::default()
		};
		let node = tg::Referent::new(id.clone().into(), root_options);
		let arg = tg::checkout::Arg {
			dependencies: true,
			extension: None,
			force: false,
			lock: None,
			nodes: vec![node],
			path: None,
		};
		let root = tg::checkout::checkout_one_with_instance(client, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to check out the package root"))?;
		root.join(options.path.as_deref().unwrap_or(Path::new("")))
	} else {
		options
			.path
			.clone()
			.ok_or_else(|| tg::error!("missing path"))?
	};

	// Canonicalize the filesystem path used to check in the package again.
	let path = tangram_util::fs::canonicalize_parent(&path).await.map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to canonicalize the path"),
	)?;

	Ok(path)
}

async fn publish_checkin(
	client: &impl tg::Instance,
	path: PathBuf,
	solve: bool,
) -> tg::Result<tg::Referent<tg::object::Id>> {
	let path_display = path.display().to_string();
	let options = tg::checkin::Options {
		lock: None,
		solve,
		source_dependencies: false,
		..tg::checkin::Options::default()
	};
	let args = tg::checkin::Arg {
		path,
		options,
		updates: Vec::new(),
	};
	let output = tg::checkin::checkin_with_instance(client, args)
		.await
		.map_err(|error| tg::error!(!error, path = %path_display, "failed to checkin"))?;
	let node = output.artifact.map(Into::into);
	Ok(node)
}
