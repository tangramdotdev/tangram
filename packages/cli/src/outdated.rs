use {
	crate::Cli,
	std::{
		collections::HashSet,
		path::PathBuf,
	},
	tangram_client::prelude::*,
};

/// Get a package's outdated dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub checkin: crate::checkin::Options,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(default_value = ".", index = 1)]
	pub path: PathBuf,
}

impl Cli {
	pub async fn command_outdated(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Find the root.
		let root = Self::find_root(args.path.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to find the root"))?;

		// Deserialize the lock.
		let lock = Self::try_read_lock(root.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to read lockfile"))?
			.ok_or_else(|| tg::error!("missing lockfile"))?;

		// Edge case, do nothing.
		if lock.nodes.is_empty() {
			return Ok(());
		}

		// Collect dependencies.
		let graph = Graph::new(&lock, root);
		let mut output: HashSet<Entry> = HashSet::new();
		for node in &graph.nodes {
			for (pattern, edge) in &node.edges {
				let Some(pattern) = pattern else {
					continue;
				};
				let current = match edge{
					Edge::Node(node) => {
						let Some(tag) = graph.nodes[*node].options.tag.clone() else {
							continue;
						};
						tag
					},
					Edge::Tag(tag) => tag.clone()
				};
				let compatible = handle
					.list_tags(tg::tag::list::Arg {
						cached: false,
						length: Some(1),
						local: None,
						pattern: pattern.clone(),
						recursive: false,
						remotes: None,
						reverse: true,
						ttl: None,
					})
					.await?
					.data
					.into_iter()
					.map(|output| output.tag)
					.next();

				let mut components = pattern.components().collect::<Vec<_>>();
				components.pop();
				components.push("*");
				let pattern = tg::tag::Pattern::new(components.join("/"));
				let latest = handle
					.list_tags(tg::tag::list::Arg {
						cached: false,
						length: Some(1),
						local: None,
						pattern: pattern.clone(),
						recursive: false,
						remotes: None,
						reverse: true,
						ttl: None,
					})
					.await?
					.data
					.into_iter()
					.map(|output| output.tag)
					.next();

				let entry = Entry {
					current,
					compatible,
					latest,
					required_by: tg::Referent::new((), node.options.clone()),
				};

				output.insert(entry);
			}
			// let c
		}

		// Display output.
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}

#[derive(Clone, Debug)]
struct Node {
	edges: Vec<(Option<tg::tag::Pattern>, Edge)>,
	options: tg::referent::Options,
	path: Vec<usize>,
}

#[derive(Clone, Debug)]
enum Edge {
	Node(usize),
	Tag(tg::Tag),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize)]
struct Entry {
	current: tg::Tag,
	compatible: Option<tg::Tag>,
	latest: Option<tg::Tag>,
	required_by: tg::Referent<()>,
}

#[derive(Debug)]
pub struct Graph {
	nodes: Vec<Node>,
}

impl Graph {
	pub fn new(lock: &tg::graph::Data, path: PathBuf) -> Self {
		let mut nodes = vec![None; lock.nodes.len()];
		let mut stack = vec![(0, tg::referent::Options::with_path(path), Vec::new())];
		while let Some((index, options, path)) = stack.pop() {
			if nodes[index].is_some() {
				continue;
			}
			let mut node = Node {
				edges: Vec::new(),
				options,
				path,
			};
			match &lock.nodes[index] {
				tg::graph::data::Node::Directory(directory) => {
					let entries = crate::update::flatten_directory(directory, &lock.nodes);
					for (name, child) in entries {
						let mut child_options = tg::referent::Options::with_path(name);
						child_options.inherit(&node.options);
						let mut path = node.path.clone();
						path.push(index);
						node.edges.push((None, Edge::Node(child)));
						stack.push((child, child_options, path));
					}
				},
				tg::graph::data::Node::File(file) => {
					for (reference, dependency) in &file.dependencies {
						let pattern = reference.item().try_unwrap_tag_ref().ok().cloned();
						let Some(dependency) = dependency else {
							continue;
						};
						match (dependency.item(), dependency.tag()) {
							(Some(tg::graph::data::Edge::Pointer(pointer)), tag) => {
								if pointer.graph.is_none() {
									let reference =
										reference.item().try_unwrap_tag_ref().ok().cloned();
									node.edges.push((reference, Edge::Node(pointer.index)));
									let mut child_options = dependency.options.clone();
									let mut path = node.path.clone();
									path.push(index);
									child_options.inherit(&node.options);
									stack.push((pointer.index, child_options, path));
								} else if let Some(tag) = tag {
									node.edges.push((pattern, Edge::Tag(tag.clone())));
								}
							},
							(_, Some(tag)) => {
								node.edges.push((pattern, Edge::Tag(tag.clone())))
							},
							_ => (),
						}
					}
				},
				tg::graph::data::Node::Symlink(symlink) => {
					let Some(tg::graph::data::Edge::Pointer(pointer)) = &symlink.artifact else {
						continue;
					};
					if pointer.graph.is_some() {
						continue;
					}
					node.edges.push((None, Edge::Node(pointer.index)));
					let child_options = tg::referent::Options::default();
					let mut path = node.path.clone();
					path.push(index);
					stack.push((pointer.index, child_options, path));
				},
			}
			nodes[index].replace(node);
		}

		let nodes = nodes.into_iter().map(Option::unwrap).collect::<Vec<_>>();
		Self { nodes }
	}
}
