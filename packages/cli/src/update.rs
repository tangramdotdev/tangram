use {
	crate::Cli,
	std::{
		collections::{BTreeMap, BTreeSet},
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

	#[arg(default_value = ".", index = 1)]
	pub path: PathBuf,

	#[arg(
		action = clap::ArgAction::Append,
		index = 2,
		num_args = 1..,
	)]
	pub updates: Option<Vec<tg::tag::Pattern>>,
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
			Self::find_root(path.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to find the root"))?
		};
		let old_lock = Self::try_read_lock(root.clone())
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
		let new_lock = Self::try_read_lock(root)
			.await
			.map_err(|source| tg::error!(!source, "failed to read lockfile"))?;

		// Print out any changes.
		for updated in Self::updates(old_lock.as_ref(), new_lock.as_ref()) {
			match (updated.old, updated.new) {
				(Some(old), Some(new)) => {
					println!("↑ updated {old} to {new}");
				},
				(None, Some(new)) => {
					println!("+ added {new}");
				},
				(Some(old), None) => {
					println!("- removed {old}");
				},
				(None, None) => (),
			}
		}
		Ok(())
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
				Self::try_read_lockfile(&lockfile_path)?
			} else if std::fs::metadata(&path).is_ok_and(|metadata| metadata.is_file()) {
				let lockfile_path = path.with_extension("lock");
				let contents = Self::try_read_lockfile(&lockfile_path)?;
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

	fn updates(old: Option<&tg::graph::Data>, new: Option<&tg::graph::Data>) -> Vec<Updated> {
		let mut visitor = Visitor::default();
		let old = old.map(|old| old.nodes.as_slice()).unwrap_or_default();
		let new = new.map(|new| new.nodes.as_slice()).unwrap_or_default();
		let old_node = (!old.is_empty()).then_some(0);
		let new_node = (!new.is_empty()).then_some(0);
		visitor.walk(old_node, new_node, old, new);
		visitor.updated
	}
}

#[allow(unused)]
struct Updated {
	pattern: tg::tag::Pattern,
	old: Option<tg::Tag>,
	new: Option<tg::Tag>,
}

#[derive(Default)]
struct Visitor {
	updated: Vec<Updated>,
	visited: BTreeSet<(Option<usize>, Option<usize>)>,
}

impl Visitor {
	fn walk(
		&mut self,
		old_node_index: Option<usize>,
		new_node_index: Option<usize>,
		old: &[tg::graph::data::Node],
		new: &[tg::graph::data::Node],
	) {
		if !self.visited.insert((old_node_index, new_node_index)) {
			return;
		}

		match (old_node_index, new_node_index) {
			(Some(old_node_index), Some(new_node_index)) => {
				match (&old[old_node_index], &new[new_node_index]) {
					(
						tg::graph::data::Node::Directory(old_node),
						tg::graph::data::Node::Directory(new_node),
					) => {
						let old_entries = flatten_directory(old_node, old);
						let new_entries = flatten_directory(new_node, new);
						let all_entries: BTreeSet<&String> =
							old_entries.keys().chain(new_entries.keys()).collect();
						for name in all_entries {
							let old_node_index = old_entries.get(name).cloned();
							let new_node_index = new_entries.get(name).cloned();
							self.walk(old_node_index, new_node_index, old, new);
						}
					},
					(
						tg::graph::data::Node::File(old_node),
						tg::graph::data::Node::File(new_node),
					) => {
						let old_dependencies: &std::collections::BTreeMap<
							tangram_client::Reference,
							Option<tangram_client::graph::data::Dependency>,
						> = &old_node.dependencies;
						let new_dependencies = &new_node.dependencies;
						let all_references = old_dependencies
							.keys()
							.chain(new_dependencies.keys())
							.collect::<BTreeSet<_>>();
						for reference in all_references {
							let old_dep = old_dependencies.get(reference).and_then(|d| d.as_ref());
							let new_dep = new_dependencies.get(reference).and_then(|d| d.as_ref());
							let old_tag = old_dep.and_then(|d| d.0.options.tag.clone());
							let new_tag = new_dep.and_then(|d| d.0.options.tag.clone());
							if (old_tag != new_tag) && (old_tag.is_some() || new_tag.is_some()) {
								if let Ok(pattern) = reference.item().try_unwrap_tag_ref() {
									self.updated.push(Updated {
										pattern: pattern.clone(),
										old: old_tag,
										new: new_tag,
									});
								}
							}
							let old_node_index = old_dep.and_then(|d| {
								let edge = d.0.item.as_ref()?;
								let pointer = edge.try_unwrap_pointer_ref().ok()?;
								if pointer.graph.is_some() {
									return None;
								}
								Some(pointer.index)
							});
							let new_node_index = new_dep.and_then(|d| {
								let edge = d.0.item.as_ref()?;
								let pointer = edge.try_unwrap_pointer_ref().ok()?;
								if pointer.graph.is_some() {
									return None;
								}
								Some(pointer.index)
							});
							self.walk(old_node_index, new_node_index, old, new);
						}
					},
					(
						tg::graph::data::Node::Symlink(old_node),
						tg::graph::data::Node::Symlink(new_node),
					) => {
						let Some(old_artifact) = &old_node.artifact else {
							return;
						};
						let Some(new_artifact) = &new_node.artifact else {
							return;
						};
						let old_node_index = old_artifact
							.try_unwrap_pointer_ref()
							.ok()
							.and_then(|pointer| pointer.graph.is_none().then_some(pointer.index));
						let new_node_index = new_artifact
							.try_unwrap_pointer_ref()
							.ok()
							.and_then(|pointer| pointer.graph.is_none().then_some(pointer.index));
						self.walk(old_node_index, new_node_index, old, new);
					},
					_ => (),
				}
			},
			(Some(old_node_index), None) => {
				if let tg::graph::data::Node::File(old_file) = &old[old_node_index] {
					for (reference, dep) in &old_file.dependencies {
						let old_tag = dep.as_ref().and_then(|d| d.0.options.tag.clone());
						if let Some(old_tag) = old_tag {
							if let Ok(pattern) = reference.item().try_unwrap_tag_ref() {
								self.updated.push(Updated {
									pattern: pattern.clone(),
									old: Some(old_tag),
									new: None,
								});
							}
						}
					}
				}
			},
			(None, Some(new_node_index)) => {
				if let tg::graph::data::Node::File(new_file) = &new[new_node_index] {
					for (reference, dep) in &new_file.dependencies {
						let new_tag = dep.as_ref().and_then(|d| d.0.options.tag.clone());
						if let Some(new_tag) = new_tag {
							if let Ok(pattern) = reference.item().try_unwrap_tag_ref() {
								self.updated.push(Updated {
									pattern: pattern.clone(),
									old: None,
									new: Some(new_tag),
								});
							}
						}
					}
				}
			},
			(None, None) => (),
		}
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
