use {
	crate::Cli,
	std::{
		collections::{BTreeSet, HashSet},
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
		let lock = Self::try_read_lock(root)
			.await
			.map_err(|source| tg::error!(!source, "failed to read lockfile"))?
			.ok_or_else(|| tg::error!("missing lockfile"))?;

		// Edge case, do nothing.
		if lock.nodes.is_empty() {
			return Ok(());
		}

		// Collect dependencies.
		let mut visitor = Visitor::default();
		visitor.walk(0, &lock.nodes);

		// Find compatible and latest versions.
		let mut output: HashSet<Entry> = HashSet::default();
		for (pattern, current) in visitor.dependencies {
			let compatible = handle
				.list_tags(tg::tag::list::Arg {
					pattern: pattern.clone(),
					cached: false,
					length: None,
					local: None,
					recursive: false,
					remotes: None,
					reverse: true,
					ttl: None,
				})
				.await
				.map_err(|source| tg::error!(!source, "failed to list tags"))?
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
			output.insert(Entry {
				current,
				compatible,
				latest,
			});
		}

		// Display output.
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}

#[derive(Default)]
struct Visitor {
	dependencies: Vec<(tg::tag::Pattern, tg::Tag)>,
	visited: BTreeSet<usize>,
}

#[derive(serde::Serialize, Debug, PartialEq, Eq, Hash)]
struct Entry {
	current: tg::Tag,
	compatible: Option<tg::Tag>,
	latest: Option<tg::Tag>,
}

impl Visitor {
	fn walk(&mut self, node: usize, nodes: &[tg::graph::data::Node]) {
		if !self.visited.insert(node) {
			return;
		}
		let node = &nodes[node];
		match node {
			tg::graph::data::Node::Directory(directory) => {
				let entries = crate::update::flatten_directory(directory, nodes);
				for node in entries.into_values() {
					self.walk(node, nodes);
				}
			},
			tg::graph::data::Node::File(file) => {
				for (reference, dependency) in &file.dependencies {
					let Some(dependency) = dependency else {
						continue;
					};

					let pattern = reference.item().try_unwrap_tag_ref().ok().cloned();
					let tag = dependency.tag().cloned();
					if let (Some(pattern), Some(tag)) = (pattern, tag) {
						self.dependencies.push((pattern, tag));
					}
					if let Some(dependency) = dependency.0.item().as_ref().and_then(|edge| {
						let pointer = edge.try_unwrap_pointer_ref().ok()?;
						pointer.graph.is_none().then_some(pointer.index)
					}) {
						self.walk(dependency, nodes);
					}
				}
			},
			tg::graph::data::Node::Symlink(symlink) => {
				if let Some(artifact) = symlink.artifact.as_ref().and_then(|edge| {
					let pointer = edge.try_unwrap_pointer_ref().ok()?;
					pointer.graph.is_none().then_some(pointer.index)
				}) {
					self.walk(artifact, nodes);
				}
			},
		}
	}
}
