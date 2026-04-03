use {
	crate::Cli,
	anstream::println,
	crossterm::style::Stylize as _,
	std::{
		collections::{BTreeMap, BTreeSet, HashMap},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

/// Update a lock.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub checkin: crate::checkin::Options,

	#[arg(long)]
	pub json: bool,

	#[arg(default_value = ".", index = 1)]
	pub path: PathBuf,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(
		action = clap::ArgAction::Append,
		index = 2,
		num_args = 1..,
	)]
	pub updates: Option<Vec<tg::tag::Pattern>>,
}

#[derive(Clone, Debug)]
struct Graph {
	nodes: Vec<Node>,
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

#[derive(Clone, Debug, serde::Serialize)]
struct Update {
	pattern: tg::Referent<tg::tag::Pattern>,
	old: Option<tg::Tag>,
	new: Option<tg::Tag>,
}

impl Cli {
	pub async fn command_update(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Canonicalize the path's parent.
		let path = tangram_util::fs::canonicalize_parent(&args.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;

		// Get the old lockfile.
		let root = if args.checkin.root {
			path.clone()
		} else {
			find_root(path.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to find the root"))?
		};
		let old_lock = try_read_lock(root.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to read lockfile"))?;

		// Get the updates.
		let updates = args.updates.unwrap_or_else(|| vec!["*".parse().unwrap()]);

		// Check in.
		let arg = tg::checkin::Arg {
			options: args.checkin.to_options(),
			path: path.clone(),
			updates,
		};
		let stream = handle
			.checkin(arg)
			.await
			.map_err(|source| tg::error!(!source, path = %path.display(), "failed to check in"))?;
		self.render_progress_stream(stream).await?;

		// Get the new lockfile.
		let new_lock = try_read_lock(root.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to read lockfile"))?;

		// Print the updates.
		let updates = create_updates(old_lock.as_ref(), new_lock.as_ref(), &root);
		if args.json {
			self.print_serde(updates, args.print).await?;
		} else {
			for updated in updates {
				let referrer = format_referrer(&updated.pattern);
				match (updated.old, updated.new) {
					(Some(old), Some(new)) => {
						println!(
							"{} updated {old} to {new}, required by {referrer}",
							"↑".blue()
						);
					},
					(None, Some(new)) => {
						println!("{} added {new}, required by {referrer}", "+".green());
					},
					(Some(old), None) => {
						println!("{} removed {old}, required by {referrer}", "-".red());
					},
					(None, None) => (),
				}
			}
		}

		Ok(())
	}
}

pub(crate) async fn find_root(path: PathBuf) -> tg::Result<PathBuf> {
	let output = tokio::task::spawn_blocking(move || {
		let mut output = None;
		for ancestor in path.ancestors() {
			let metadata = std::fs::symlink_metadata(ancestor).map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to get the metadata"),
			)?;
			if metadata.is_dir()
				&& tg::module::try_get_root_module_file_name_sync(ancestor)?.is_some()
			{
				output.replace(ancestor.to_owned());
			}
		}
		let output = output.unwrap_or(path);
		Ok::<_, tg::Error>(output)
	})
	.await
	.map_err(|source| tg::error!(!source, "the checkin root task panicked"))??;
	Ok(output)
}

pub(crate) async fn try_read_lock(path: PathBuf) -> tg::Result<Option<tg::graph::Data>> {
	tokio::task::spawn_blocking(move || {
		let contents = if path.is_dir() {
			let lockfile_path = path.join(tg::module::LOCKFILE_FILE_NAME);
			try_read_lockfile(&lockfile_path)?
		} else if std::fs::metadata(&path).is_ok_and(|metadata| metadata.is_file()) {
			let lockfile_path = path.with_extension("lock");
			let contents = try_read_lockfile(&lockfile_path)?;
			if contents.is_some() {
				contents
			} else {
				// Fall back to xattr.
				xattr::get(&path, tg::file::LOCKATTR_XATTR_NAME)
					.ok()
					.flatten()
			}
		} else {
			None
		};

		// Return early if no contents found.
		let Some(contents) = contents else {
			return Ok(None);
		};

		// Deserialize the lock.
		let lock = serde_json::from_slice::<tg::graph::Data>(&contents).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to deserialize the lock"),
		)?;

		Ok(Some(lock))
	})
	.await
	.map_err(|source| tg::error!(!source, "the lockfile task panicked"))?
}

fn try_read_lockfile(path: &Path) -> tg::Result<Option<Vec<u8>>> {
	match std::fs::read(path) {
		Ok(contents) => Ok(Some(contents)),
		Err(error)
			if matches!(
				error.kind(),
				std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
			) =>
		{
			Ok(None)
		},
		Err(source) => {
			Err(tg::error!(!source, path = %path.display(), "failed to read the lockfile"))
		},
	}
}

fn create_updates(
	old: Option<&tg::graph::Data>,
	new: Option<&tg::graph::Data>,
	root: &Path,
) -> Vec<Update> {
	let old_deps = old
		.map(|lock| graph_dependencies(&create_graph(lock, root.to_owned())))
		.unwrap_or_default();
	let new_deps = new
		.map(|lock| graph_dependencies(&create_graph(lock, root.to_owned())))
		.unwrap_or_default();
	let all_patterns: BTreeSet<_> = old_deps
		.keys()
		.cloned()
		.chain(new_deps.keys().cloned())
		.collect();
	let mut updates = Vec::new();
	for pattern in all_patterns {
		let old = old_deps.get(&pattern).cloned();
		let new = new_deps.get(&pattern).cloned();
		if old != new {
			updates.push(Update { pattern, old, new });
		}
	}
	updates
}

fn create_graph(lock: &tg::graph::Data, path: PathBuf) -> Graph {
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
				let entries = flatten_directory(directory, &lock.nodes);
				for (name, child) in entries {
					let child_options = directory_entry_options(&node.options, &name);
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
								let reference = reference.item().try_unwrap_tag_ref().ok().cloned();
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
						(_, Some(tag)) => node.edges.push((pattern, Edge::Tag(tag.clone()))),
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
	Graph { nodes }
}

fn graph_dependencies(graph: &Graph) -> HashMap<tg::Referent<tg::tag::Pattern>, tg::Tag> {
	graph
		.nodes
		.iter()
		.flat_map(|node| {
			node.edges.iter().filter_map(|(pattern, edge)| {
				let options = node.options.clone();
				let pattern = pattern.clone()?;
				let tag = match edge {
					Edge::Node(index) => graph.nodes[*index].options.tag.clone()?,
					Edge::Tag(tag) => tag.clone(),
				};
				let key = tg::Referent::new(pattern, options);
				Some((key, tag))
			})
		})
		.collect()
}

pub(crate) fn directory_entry_options(
	parent: &tg::referent::Options,
	name: impl AsRef<Path>,
) -> tg::referent::Options {
	let name = name.as_ref();
	let path = parent.path.as_ref().map_or_else(
		|| name.to_owned(),
		|path| tangram_util::path::normalize(path.join(name)),
	);
	tg::referent::Options {
		path: Some(path),
		..parent.clone()
	}
}

pub(crate) fn flatten_directory(
	directory: &tg::graph::data::Directory,
	nodes: &[tg::graph::data::Node],
) -> BTreeMap<String, usize> {
	match directory {
		tg::graph::data::Directory::Leaf(leaf) => leaf
			.entries
			.iter()
			.filter_map(|(name, edge)| {
				let pointer = edge.try_unwrap_pointer_ref().ok()?;
				if pointer.graph.is_some() {
					return None;
				}
				Some((name.clone(), pointer.index))
			})
			.collect(),
		tg::graph::data::Directory::Branch(branch) => branch
			.children
			.iter()
			.filter_map(|child| {
				let pointer = child.directory.try_unwrap_pointer_ref().ok()?;
				if pointer.graph.is_some() {
					return None;
				}
				let node = nodes.get(pointer.index)?;
				let child_directory = node.try_unwrap_directory_ref().ok()?;
				Some(flatten_directory(child_directory, nodes))
			})
			.fold(BTreeMap::new(), |mut acc, entries| {
				acc.extend(entries);
				acc
			}),
	}
}

pub(crate) fn format_referrer<T>(referrer: &tg::Referent<T>) -> String {
	let tg::referent::Options {
		artifact,
		id,
		name,
		path,
		tag,
	} = referrer.options();
	let mut name = name
		.clone()
		.or_else(|| artifact.as_ref().map(tg::artifact::Id::to_string))
		.or_else(|| tag.as_ref().map(tg::Tag::to_string))
		.unwrap_or_default();
	if let Some(path) = path {
		name = PathBuf::from(name).join(path).to_str().unwrap().to_owned();
	} else if let Some(id) = id {
		name = id.to_string();
	}
	name
}
