use {
	crate::Cli,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _},
	petgraph::algo::tarjan_scc,
	radix_trie::TrieCommon as _,
	std::{collections::HashMap, path::Path, path::PathBuf},
	tangram_client::prelude::*,
};

/// Publish a tag with its transitive source dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Display the items that will be published in their order, but don't actually publish them.
	#[arg(default_value = "false", long)]
	pub dry_run: bool,

	#[command(flatten)]
	pub location: crate::location::Args,

	/// The path to publish.
	#[arg(default_value = ".", index = 1)]
	pub path: PathBuf,

	/// Override the tag for the root.
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
	source_packages: Vec<tg::Referent<tg::Object>>,
	tags: Vec<(tg::Tag, tg::object::Id)>,
	graph: Graph,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(untagged)]
enum Step {
	Item(Item),
	Cycle(Vec<Item>),
}

#[derive(Clone, Debug, serde::Serialize)]
struct Item {
	referent: tg::Referent<tg::object::Id>,
	path: Option<PathBuf>,
	tag: tg::Tag,
}

impl Cli {
	pub async fn command_publish(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Check in the root package.
		let absolute_path = tangram_util::fs::canonicalize_parent(&args.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
		let arg = tg::checkin::Arg {
			options: tg::checkin::Options::default(),
			path: absolute_path.clone(),
			updates: Vec::new(),
		};
		let artifact = tg::checkin::checkin_with_handle(&handle, arg).await.map_err(
			|source| tg::error!(!source, path = %absolute_path.display(), "failed to check in the root package"),
		)?;
		let referent = tg::Referent::new(
			artifact.into(),
			tg::referent::Options::with_path(args.path.clone()),
		);

		// Create the state.
		let mut state = State::default();

		// Visit the objects.
		state
			.visit_objects(&handle, &referent)
			.await
			.map_err(|source| tg::error!(!source, "failed to analyze objects"))?;

		// Create the graph.
		state
			.create_graph(&handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to create package graph"))?;

		// Create the plan.
		let plan = state
			.create_plan(&handle, args.tag.map(tg::Tag::new))
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, "failed to create publishing plan"))?;

		// Print the plan if this is a dry run.
		if args.dry_run {
			self.print_serde(&plan, crate::print::Options::default())
				.await?;
			return Ok(());
		}

		// Collect all the local tags.
		let mut tags = state.tags;

		// Collect all the local items.
		let mut items = tags
			.iter()
			.map(|(_, id)| tg::Either::Left(id.clone()))
			.collect::<Vec<_>>();

		// Execute the plan.
		for step in plan {
			match step {
				Step::Item(item) => {
					let Item {
						referent,
						path,
						tag,
					} = item;
					let id = if let Some(path) = path {
						publish_checkin(&handle, path, true).await?
					} else {
						referent.item
					};
					items.push(tg::Either::Left(id.clone()));
					tags.push((tag.clone(), id.clone()));
					let arg = tg::tag::put::Arg {
						force: true,
						item: tg::Either::Left(id),
						location: None,
						replicate: false,
					};
					handle.put_tag(&tag, arg).await.map_err(
						|source| tg::error!(!source, tag = %tag, "failed to put local tag"),
					)?;
				},

				Step::Cycle(cycle_items) => {
					for item in &cycle_items {
						let path = item
							.path
							.clone()
							.ok_or_else(|| tg::error!("cycle items must have paths"))?;
						let id = publish_checkin(&handle, path, false).await?;
						let arg = tg::tag::put::Arg {
							force: true,
							item: tg::Either::Left(id),
							location: None,
							replicate: false,
						};
						handle.put_tag(&item.tag, arg).await.map_err(
							|source| tg::error!(!source, tag = %item.tag, "failed to put local tag"),
						)?;
					}

					for item in cycle_items {
						let Item { path, tag, .. } = item;
						let path = path.ok_or_else(|| tg::error!("cycle items must have paths"))?;
						let id = publish_checkin(&handle, path, true).await?;
						items.push(tg::Either::Left(id.clone()));
						tags.push((tag.clone(), id.clone()));
						let arg = tg::tag::put::Arg {
							force: true,
							item: tg::Either::Left(id),
							location: None,
							replicate: false,
						};
						handle.put_tag(&tag, arg).await.map_err(
							|source| tg::error!(!source, tag = %tag, "failed to put local tag"),
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

		// Push.
		let stream = handle
			.push(tg::push::Arg {
				commands: false,
				destination: Some(location.clone()),
				eager: true,
				errors: true,
				force: false,
				items,
				logs: false,
				metadata: false,
				outputs: true,
				recursive: false,
				source: None,
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to push items"))?;
		let output = self
			.render_progress_stream(stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to push items"))?;

		let processes = output.skipped.processes;
		let objects = output.skipped.objects;
		let bytes = byte_unit::Byte::from_u64(output.skipped.bytes)
			.get_appropriate_unit(byte_unit::UnitType::Decimal);
		let message = format!("skipped {processes} processes, {objects} objects, {bytes:#.1}");
		Self::print_info_message(&message);
		let processes = output.transferred.processes;
		let objects = output.transferred.objects;
		let bytes = byte_unit::Byte::from_u64(output.transferred.bytes)
			.get_appropriate_unit(byte_unit::UnitType::Decimal);
		let message = format!("transferred {processes} processes, {objects} objects, {bytes:#.1}");
		Self::print_info_message(&message);

		// Put tags on the remote.
		let tags = tags
			.into_iter()
			.map(|(tag, item)| tg::tag::batch::Item {
				tag,
				item: tg::Either::Left(item),
				force: false,
			})
			.collect::<Vec<_>>();
		handle
			.post_tag_batch(tg::tag::batch::Arg {
				location: Some(location.into()),
				replicate: false,
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

/// Given an object, get the tag from its internal metadata by statically parsing the module.
async fn try_get_package_tag(
	handle: &impl tg::Handle,
	object: &tg::Object,
	package_path: Option<&Path>,
) -> tg::Result<Option<tg::Tag>> {
	// Get the file text and module file name from the object.
	let (text, module_name) = match object {
		tg::Object::File(file) => {
			let text = file.text_with_handle(handle).await?;
			(text, None)
		},
		tg::Object::Directory(directory) => {
			let Some(name) = tg::module::try_get_root_module_file_name_with_handle(
				handle,
				tg::Either::Left(directory),
			)
			.await?
			else {
				return Ok(None);
			};
			let file = directory
				.get_with_handle(handle, name)
				.await?
				.try_unwrap_file()
				.map_err(|_| tg::error!("expected a file"))?;
			let text = file.text_with_handle(handle).await?;
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

	// Extract metadata statically.
	let map = match tangram_compiler::metadata::metadata(&text) {
		Ok(Some(map)) => map,
		Ok(None) => return Ok(None),
		Err(source) => {
			return if let Some(path) = module_path {
				Err(tg::error!(!source, path = %path.display(), "failed to extract metadata"))
			} else {
				let id = object.id();
				Err(tg::error!(!source, %id, "failed to extract metadata"))
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

	Ok(Some(tg::Tag::new(tag)))
}

impl State {
	async fn visit_objects(
		&mut self,
		handle: &impl tg::Handle,
		root: &tg::Referent<tg::Object>,
	) -> tg::Result<()> {
		// Make sure the root is added if it is on the local file system.
		if root.path().is_some() {
			self.source_packages.push(root.clone());
			self.add_package(root);
		}

		// Visit all the objects.
		tg::object::visit(handle, self, root, false).await
	}

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
						.dependencies_with_handle(handle)
						.await?
						.values()
						.filter_map(|option| {
							let dependency = option.as_ref()?;
							let item = dependency.0.item.as_ref()?;
							let package =
								all_packages.iter().find(|r| r.item().id() == item.id())?;
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
		handle: &impl tg::Handle,
		mut tag: Option<tg::Tag>,
	) -> tg::Result<Vec<Step>> {
		// Fetch all package tags in parallel with limited concurrency.
		let packages: Vec<_> = self
			.graph
			.nodes
			.iter()
			.map(|node| {
				(
					node.package.item().id(),
					node.package.item().clone(),
					node.package.path().map(ToOwned::to_owned),
				)
			})
			.collect();
		let tags: HashMap<tg::object::Id, Option<tg::Tag>> = futures::stream::iter(packages)
			.map(|(id, object, path)| async move {
				let tag = try_get_package_tag(handle, &object, path.as_deref()).await?;
				Ok::<_, tg::Error>((id, tag))
			})
			.buffer_unordered(16)
			.try_collect()
			.await?;

		let sccs = tarjan_scc(&self.graph);
		let mut plan = Vec::new();
		for scc in sccs {
			let mut items = Vec::new();
			for index in scc.iter().copied() {
				// Get the node.
				let node = &self.graph.nodes[index];
				node.package.item.unload();

				let publishable = index == 0
					|| self.source_packages.iter().any(|referent| {
						referent.clone().map(|r| r.id()) == node.package.clone().map(|r| r.id())
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
				let original = tags.get(&node.package.item().id()).cloned().flatten();
				let Some(tag) = override_.or(original) else {
					continue;
				};

				// If this node has source dependencies then we need to check it in again.
				let path = if node.outgoing.is_empty() {
					None
				} else {
					let path = node
						.package
						.path()
						.ok_or_else(|| tg::error!("missing path"))?;
					let path =
						tangram_util::fs::canonicalize_parent(&path)
							.await
							.map_err(|source| {
								tg::error!(
									!source,
									path = %path.display(),
									"failed to canonicalize the path"
								)
							})?;
					Some(path)
				};

				items.push(Item {
					referent: node.package.clone().map(|object| object.id()),
					path,
					tag,
				});
			}

			// If there is more than one item in the SCC, then it is a cycle.
			if items.len() > 1 {
				// Cycles must have paths for all items.
				for item in &mut items {
					if item.path.is_none() {
						let path = item
							.referent
							.path()
							.ok_or_else(|| tg::error!("missing path"))?;
						let path = tangram_util::fs::canonicalize_parent(&path).await.map_err(
							|source| {
								tg::error!(
									!source,
									path = %path.display(),
									"failed to canonicalize the path"
								)
							},
						)?;
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
		if directory
			.options
			.path
			.as_ref()
			.is_some_and(|p| p.as_os_str().is_empty())
		{
			return Err(tg::error!(id = ?directory.id(), "invalid path"));
		}

		if let Some(tag) = directory.tag() {
			self.tags.push((tag.clone(), directory.item().id().into()));
		}
		let Some(path) = directory.path() else {
			return Ok(true);
		};
		self.file_tree
			.insert(path.to_owned(), directory.item.clone().into());

		// Keep track of files.
		if tg::module::try_get_root_module_file_name_with_handle(
			handle,
			tg::Either::Left(directory.item()),
		)
		.await?
		.is_some()
		{
			self.add_package(&directory.clone().map(|d| d.clone().into()));
		}

		Ok(true)
	}

	async fn visit_file(&mut self, handle: &H, file: tg::Referent<&tg::File>) -> tg::Result<bool> {
		if file
			.options
			.path
			.as_ref()
			.is_some_and(|p| p.as_os_str().is_empty())
		{
			return Err(tg::error!(id = ?file.id(), "invalid path"));
		}

		if let Some(tag) = file.tag() {
			self.tags.push((tag.clone(), file.item().id().into()));
		}

		let Some(path) = file.path() else {
			return Ok(true);
		};
		self.file_tree
			.insert(path.to_owned(), file.item.clone().into());

		// Mark the packages that come from source overrides.
		for (reference, option) in file.item.dependencies_with_handle(handle).await? {
			let Some(mut dependency) = option else {
				continue;
			};

			// Make sure to inherit the dependency.
			dependency.0.inherit(&file);
			if reference.options().source.is_some() {
				let Some(item) = dependency.0.item.clone() else {
					continue;
				};
				let referent = tg::Referent {
					item,
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
		_handle: &H,
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
			self.tags.push((tag.clone(), symlink.item().id().into()));
		}
		Ok(true)
	}

	async fn visit_command(
		&mut self,
		_handle: &H,
		command: tangram_client::Referent<&tangram_client::Command>,
	) -> tangram_client::Result<bool> {
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

async fn publish_checkin(
	handle: &impl tg::Handle,
	path: PathBuf,
	solve: bool,
) -> tg::Result<tg::object::Id> {
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
	let artifact = tg::checkin::checkin_with_handle(handle, args)
		.await
		.map_err(|source| tg::error!(!source, path = %path_display, "failed to checkin"))?;
	Ok(artifact.id().into())
}
