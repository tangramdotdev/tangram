use {
	super::graph::{Directory, File, Node, Symlink, Variant},
	crate::{Server, checkin::Graph, context::Context},
	smallvec::SmallVec,
	std::{
		collections::BTreeMap,
		os::unix::fs::PermissionsExt as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
	tangram_ignore as ignore,
};

struct State<'a> {
	arg: &'a tg::checkin::Arg,
	artifacts_path: Option<&'a Path>,
	context: &'a Context,
	fixup_sender: Option<std::sync::mpsc::Sender<super::fixup::Message>>,
	graph: &'a mut Graph,
	ignorer: Option<ignore::Ignorer>,
	lock: Option<&'a tg::graph::Data>,
	progress: crate::progress::Handle<super::TaskOutput>,
	root: &'a Path,
}

struct Item {
	path: PathBuf,
	parent: Option<Parent>,
}

struct Parent {
	index: usize,
	variant: ParentVariant,
}

enum ParentVariant {
	DirectoryEntry(String),
	FileDependency(tg::Reference),
	SymlinkArtifact,
}

impl Server {
	#[expect(clippy::too_many_arguments)]
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) fn checkin_input(
		&self,
		context: &Context,
		arg: &tg::checkin::Arg,
		artifacts_path: Option<&Path>,
		fixup_sender: Option<std::sync::mpsc::Sender<super::fixup::Message>>,
		graph: &mut Graph,
		ignorer: Option<ignore::Ignorer>,
		lock: Option<&tg::graph::Data>,
		next: usize,
		progress: crate::progress::Handle<super::TaskOutput>,
		root: &Path,
	) -> tg::Result<()> {
		// Start the progress indicators.
		progress.spinner("traversing", "traversing");
		progress.start(
			"artifacts".to_owned(),
			"artifacts".to_owned(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		progress.start(
			"bytes".to_owned(),
			"bytes".to_owned(),
			tg::progress::IndicatorFormat::Bytes,
			Some(0),
			None,
		);

		// Create the state.
		let mut state = State {
			arg,
			artifacts_path,
			context,
			fixup_sender,
			graph,
			ignorer,
			lock,
			progress,
			root,
		};

		// Add the root path to the stack.
		let item = Item {
			path: root.to_owned(),
			parent: None,
		};
		let mut stack = vec![item];

		// Collect the artifacts path entries.
		let artifacts_entries = if let Some(artifacts_path) = artifacts_path {
			let read_dir = std::fs::read_dir(artifacts_path).map_err(
				|source| tg::error!(!source, path = %artifacts_path.display(), "failed to read the artifacts directory"),
			)?;
			let mut entries = Vec::new();
			for entry in read_dir {
				let entry = entry
					.map_err(|source| tg::error!(!source, "failed to get the directory entry"))?;
				entries.push((entry.path(), entry.file_name()));
			}
			entries
		} else {
			Vec::new()
		};

		// Add the artifacts path entries to the stack.
		for (path, _) in &artifacts_entries {
			stack.push(Item {
				path: path.clone(),
				parent: None,
			});
		}

		// Visit.
		while let Some(item) = stack.pop() {
			self.checkin_visit(&mut state, &mut stack, item)?;
		}

		// Set the artifacts for artifacts path entries.
		for (path, name) in artifacts_entries {
			if let Some(index) = state.graph.paths.get(&path)
				&& let Some(name) = name.to_str()
				&& let Ok(id) = name.parse::<tg::artifact::Id>()
			{
				let node = state.graph.nodes.get_mut(index).unwrap();
				node.artifact = Some(id.clone());
				state.graph.artifacts.insert(id, *index);
			}
		}

		// Set the solvable and solved fields for all new nodes.
		let petgraph = super::graph::Petgraph {
			graph: state.graph,
			next,
		};
		let sccs = petgraph::algo::tarjan_scc(&petgraph);
		for scc in &sccs {
			let solvable = scc.iter().any(|index| {
				// Get the node.
				let node = state.graph.nodes.get(index).unwrap();

				// Check if the node has solvable dependencies.
				if let Variant::File(file) = &node.variant
					&& file.dependencies.keys().any(tg::Reference::is_solvable)
				{
					return true;
				}

				// Check if the node has solvable children.
				node.children()
					.iter()
					.any(|&child| state.graph.nodes.get(&child).unwrap().solvable)
			});

			// Set solvable and solved if necessary.
			if solvable {
				for index in scc {
					let node = state.graph.nodes.get_mut(index).unwrap();
					node.solvable = true;
					node.solved = false;
				}
			}
		}

		// Finish the progress indicators.
		state.progress.finish("traversing");
		state.progress.finish("artifacts");
		state.progress.finish("bytes");

		Ok(())
	}

	fn checkin_visit(
		&self,
		state: &mut State,
		stack: &mut Vec<Item>,
		item: Item,
	) -> tg::Result<()> {
		// Check if the path has been visited.
		if let Some(existing_index) = state.graph.paths.get(&item.path) {
			// Update the parent's edge to point to the existing node.
			if let Some(parent) = item.parent {
				Self::checkin_input_update_parent_edge(state, parent, *existing_index)?;
			}
			return Ok(());
		}

		// Get the metadata.
		let metadata = std::fs::symlink_metadata(&item.path).map_err(
			|source| tg::error!(!source, path = %item.path.display(), "failed to get the metadata"),
		)?;

		// Skip ignored files, unless the path is in the artifacts path.
		if !state
			.artifacts_path
			.is_some_and(|artifacts_path| item.path.starts_with(artifacts_path))
			&& state
				.ignorer
				.as_mut()
				.map(|ignorer| {
					let root = if item.path.starts_with(state.root) {
						Some(state.root)
					} else {
						None
					};
					ignorer.matches(root, &item.path, Some(metadata.is_dir()))
				})
				.transpose()
				.map_err(|source| tg::error!(!source, "failed to ignore"))?
				.is_some_and(|ignore| ignore)
		{
			return Ok(());
		}

		// Create the variant.
		let variant = if metadata.is_dir() {
			Variant::Directory(Directory {
				entries: BTreeMap::new(),
			})
		} else if metadata.is_file() {
			Variant::File(File {
				contents: None,
				dependencies: BTreeMap::new(),
				executable: metadata.permissions().mode() & 0o111 != 0,
				module: None,
			})
		} else if metadata.is_symlink() {
			let path = std::fs::read_link(&item.path)
				.map_err(|source| tg::error!(!source, "failed to read the symlink"))?;
			Variant::Symlink(Symlink {
				artifact: None,
				path: Some(path),
			})
		} else {
			return Err(tg::error!(?metadata, "invalid file type"));
		};

		// Get the node index.
		let index = state.graph.next;
		state.graph.next += 1;

		// Update the path.
		state.graph.paths.insert(&item.path, index);

		// Get the lock node.
		let lock_node = Self::checkin_input_get_lock_node(state, item.parent.as_ref());

		// Create the node.
		let node = Node {
			artifact: None,
			edge: None,
			id: None,
			lock_node,
			metadata: None,
			path: Some(item.path),
			path_metadata: Some(metadata.clone()),
			referrers: SmallVec::new(),
			solvable: false,
			solved: true,
			stored: tangram_index::ObjectStored::default(),
			variant,
		};
		state.graph.nodes.insert(index, Box::new(node));

		// Update the parent's edge to point to this node.
		if let Some(parent) = item.parent {
			Self::checkin_input_update_parent_edge(state, parent, index)?;
		}

		match &state.graph.nodes.get(&index).unwrap().variant {
			Variant::Directory(_) => {
				Self::checkin_visit_directory(state, stack, index)?;
			},
			Variant::File(_) => {
				self.checkin_visit_file(state, stack, index)?;
			},
			Variant::Symlink(_) => {
				self.checkin_visit_symlink(state, stack, index)?;
			},
		}

		// Send the message to the fixup task.
		if let Some(sender) = &state.fixup_sender {
			let node = state.graph.nodes.get(&index).unwrap();
			let message = super::fixup::Message {
				path: node.path.as_ref().unwrap().clone(),
				metadata: node.path_metadata.as_ref().unwrap().clone(),
			};
			sender.send(message).ok();
		}

		// Update the progress.
		state.progress.increment("artifacts", 1);
		if metadata.is_file() {
			state.progress.increment("bytes", metadata.len());
		}

		Ok(())
	}

	fn checkin_visit_directory(
		state: &mut State,
		stack: &mut Vec<Item>,
		index: usize,
	) -> tg::Result<()> {
		// Read the entries.
		let path = state
			.graph
			.nodes
			.get(&index)
			.unwrap()
			.path
			.as_ref()
			.unwrap();
		let read_dir = std::fs::read_dir(path).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to read the directory"),
		)?;
		let mut names = Vec::new();
		for result in read_dir {
			let entry = result
				.map_err(|source| tg::error!(!source, "failed to get the directory entry"))?;
			let name = entry
				.file_name()
				.to_str()
				.ok_or_else(|| tg::error!("expected the entry name to be a string"))?
				.to_owned();
			if name == ".tangram" {
				continue;
			}
			names.push(name);
		}

		// Sort the entries.
		names.sort_unstable();

		// Push items for children onto the stack.
		for name in names {
			let parent = Parent {
				index,
				variant: ParentVariant::DirectoryEntry(name.clone()),
			};
			let path = state
				.graph
				.nodes
				.get(&index)
				.unwrap()
				.path
				.as_ref()
				.unwrap()
				.join(&name);
			stack.push(Item {
				path,
				parent: Some(parent),
			});
		}

		Ok(())
	}

	fn checkin_visit_file(
		&self,
		state: &mut State,
		stack: &mut Vec<Item>,
		index: usize,
	) -> tg::Result<()> {
		let path = state
			.graph
			.nodes
			.get_mut(&index)
			.unwrap()
			.path
			.as_ref()
			.unwrap()
			.to_owned();

		// Get the dependencies.
		let mut dependencies = BTreeMap::new();
		if let Ok(Some(contents)) = xattr::get(&path, tg::file::DEPENDENCIES_XATTR_NAME) {
			// Read the dependencies xattr.
			let references = serde_json::from_slice::<Vec<tg::Reference>>(&contents)
				.map_err(|source| tg::error!(!source, "failed to deserialize dependencies"))?;

			// Create the dependencies and push items for path dependencies.
			for reference in references {
				let reference_path = if state.arg.options.local_dependencies {
					reference
						.options()
						.local
						.as_ref()
						.or(reference.item().try_unwrap_path_ref().ok())
				} else {
					reference.item().try_unwrap_path_ref().ok()
				};
				if let Some(reference_path) = reference_path {
					let parent = Parent {
						index,
						variant: ParentVariant::FileDependency(reference.clone()),
					};
					let referent = path.parent().unwrap().join(reference_path);
					let referent = referent.canonicalize().map_err(
						|source| tg::error!(!source, path = %referent.display(), "failed to canonicalize the path"),
					)?;
					stack.push(Item {
						path: referent,
						parent: Some(parent),
					});
					dependencies.insert(reference, None);
				} else if let Ok(id) = reference.item().try_unwrap_object_ref() {
					let path = reference.options().path.clone();
					let options = if path.is_some() {
						tg::referent::Options {
							id: Some(id.clone()),
							path,
							..Default::default()
						}
					} else {
						tg::referent::Options::default()
					};
					let dependency = tg::graph::data::Dependency(tg::Referent {
						item: Some(tg::graph::data::Edge::Object(id.clone())),
						options,
					});
					dependencies.insert(reference, Some(dependency));
				} else {
					dependencies.insert(reference, None);
				}
			}
		} else if let Some(kind) = Self::checkin_detect_module_kind(&path)? {
			// Update the node.
			state
				.graph
				.nodes
				.get_mut(&index)
				.unwrap()
				.variant
				.unwrap_file_mut()
				.module
				.replace(kind);

			// Read the module.
			let contents = std::fs::read(&path).map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to read the module"),
			)?;
			let text = String::from_utf8(contents).map_err(
				|source| tg::error!(!source, path = %path.display(), "the module is not valid utf-8"),
			)?;

			// Analyze.
			let module = tg::module::Data {
				kind,
				referent: tg::Referent::with_item(tg::module::data::Item::Path(path.clone())),
			};
			let analysis = tangram_compiler::Compiler::analyze(&module, &text);
			for diagnostic in analysis.diagnostics {
				state.progress.diagnostic(diagnostic);
			}

			// Create the dependencies and push items for path dependencies.
			for import in analysis.imports {
				let reference = import.reference;
				let reference_path = if state.arg.options.local_dependencies {
					reference
						.options()
						.local
						.as_ref()
						.or(reference.item().try_unwrap_path_ref().ok())
				} else {
					reference.item().try_unwrap_path_ref().ok()
				};
				if let Some(reference_path) = reference_path {
					let parent = Parent {
						index,
						variant: ParentVariant::FileDependency(reference.clone()),
					};
					let referent = path.parent().unwrap().join(reference_path);
					let result = if matches!(import.kind, Some(tg::module::Kind::Symlink)) {
						tangram_util::fs::canonicalize_parent_sync(&referent).map_err(
							|source| tg::error!(!source, path = %referent.display(), "failed to canonicalize the path"),
						)
					} else {
						referent.canonicalize().map_err(
							|source| tg::error!(!source, path = %referent.display(), "failed to canonicalize the path"),
						)
					};
					dependencies.insert(reference, None);
					let referent = match result {
						Ok(referent) => referent,
						Err(error) => {
							if state.arg.options.unsolved_dependencies {
								continue;
							}
							return Err(error);
						},
					};
					stack.push(Item {
						path: referent,
						parent: Some(parent),
					});
				} else if let Ok(id) = reference.item().try_unwrap_object_ref() {
					let path = reference.options().path.clone();
					let options = if path.is_some() {
						tg::referent::Options {
							id: Some(id.clone()),
							path,
							..Default::default()
						}
					} else {
						tg::referent::Options::default()
					};
					let dependency = tg::graph::data::Dependency(tg::Referent {
						item: Some(tg::graph::data::Edge::Object(id.clone())),
						options,
					});
					dependencies.insert(reference, Some(dependency));
				} else {
					dependencies.insert(reference, None);
				}
			}
		}

		// Spawn tasks to pull the tag dependencies.
		for reference in dependencies.keys() {
			if let Ok(pattern) = reference.item().try_unwrap_tag_ref() {
				tokio::spawn({
					let server = self.clone();
					let pattern = pattern.clone();
					let remote = reference.options().remote.clone();
					async move {
						server.pull_tag(pattern.clone(), remote.clone()).await.ok();
					}
				});
			}
		}

		// Update the graph.
		state
			.graph
			.nodes
			.get_mut(&index)
			.unwrap()
			.variant
			.unwrap_file_mut()
			.dependencies = dependencies;

		Ok(())
	}

	fn checkin_visit_symlink(
		&self,
		state: &mut State,
		stack: &mut Vec<Item>,
		index: usize,
	) -> tg::Result<()> {
		// Read the symlink.
		let path = state
			.graph
			.nodes
			.get(&index)
			.unwrap()
			.path
			.as_ref()
			.unwrap()
			.clone();
		let mut target = std::fs::read_link(&path)
			.map_err(|source| tg::error!(!source, "failed to read the symlink"))?;

		// If the target is absolute, then get the host path if necessary.
		if target.is_absolute()
			&& let Some(process) = &state.context.process
		{
			target = process.host_path_for_guest_path(target);
		}

		// Canonicalize the target.
		let Ok(absolute_target) =
			tangram_util::fs::canonicalize_parent_sync(path.parent().unwrap().join(&target))
		else {
			return Ok(());
		};

		// If the target is in the cache directory, then treat it as an artifact symlink.
		if let Ok(path_in_cache_path) = absolute_target.strip_prefix(self.cache_path()) {
			// Get the artifact.
			let mut components = path_in_cache_path.components();
			let Some(artifact) = components.next().and_then(|component| {
				if let std::path::Component::Normal(component) = component {
					component.to_str()?.parse::<tg::artifact::Id>().ok()
				} else {
					None
				}
			}) else {
				return Ok(());
			};

			// Get the path in the entry.
			let path_in_entry = components.collect();
			let path_in_entry = if path_in_entry == Path::new("") {
				None
			} else {
				Some(path_in_entry)
			};

			// If this is a destructive checkin and the target is absolute, make it relative.
			if state.arg.options.destructive && target.is_absolute() {
				let mut source = self.cache_path().join("_");
				if let Ok(path) = path.strip_prefix(state.root) {
					source.push(path);
				}
				let src = source
					.parent()
					.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
				let dst = &absolute_target;
				let target = tangram_util::path::diff(src, dst)
					.map_err(|source| tg::error!(!source, "failed to diff the paths"))?;
				Self::checkin_replace_symlink(state, &path, &target)?;
			}

			// Update the node.
			let node = state.graph.nodes.get_mut(&index).unwrap();
			let symlink = node.variant.unwrap_symlink_mut();
			symlink.artifact = Some(tg::graph::data::Edge::Object(artifact));
			symlink.path = path_in_entry;

			return Ok(());
		}

		// If the target is in the artifacts directory, then treat it as an artifact symlink.
		if let Some(artifacts_path) = &state.artifacts_path
			&& let Ok(path_in_artifacts_path) = absolute_target.strip_prefix(artifacts_path)
		{
			// Get the artifact.
			let mut components = path_in_artifacts_path.components();
			let Some(artifact) = components.next().and_then(|component| {
				if let std::path::Component::Normal(component) = component {
					component.to_str()?.parse::<tg::artifact::Id>().ok()
				} else {
					None
				}
			}) else {
				return Ok(());
			};

			// Get the path in the entry.
			let path_in_entry = components.collect();
			let path_in_entry = if path_in_entry == Path::new("") {
				None
			} else {
				Some(path_in_entry)
			};

			// If this is a destructive checkin and the target is absolute, make it relative.
			if state.arg.options.destructive && target.is_absolute() {
				let mut source = artifacts_path.join("_");
				if let Ok(path) = path.strip_prefix(state.root) {
					source.push(path);
				}
				let src = source
					.parent()
					.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
				let dst = &absolute_target;
				let target = tangram_util::path::diff(src, dst)
					.map_err(|source| tg::error!(!source, "failed to diff the paths"))?;
				Self::checkin_replace_symlink(state, &path, &target)?;
			}

			// Update the path.
			let node = state.graph.nodes.get_mut(&index).unwrap();
			let symlink = node.variant.unwrap_symlink_mut();
			symlink.path = path_in_entry;

			// Add the artifact to the stack.
			let parent = Parent {
				index,
				variant: ParentVariant::SymlinkArtifact,
			};
			let entry_path = artifacts_path.join(artifact.to_string());
			stack.push(Item {
				path: entry_path,
				parent: Some(parent),
			});

			return Ok(());
		}

		// Update the symlink.
		state
			.graph
			.nodes
			.get_mut(&index)
			.unwrap()
			.variant
			.unwrap_symlink_mut()
			.path = Some(target);

		Ok(())
	}

	fn checkin_replace_symlink(state: &State, path: &Path, target: &Path) -> tg::Result<()> {
		match std::fs::remove_file(path) {
			Ok(()) => (),
			Err(source) if source.kind() == std::io::ErrorKind::PermissionDenied => {
				Self::checkin_make_parent_writable(state, path)?;
				std::fs::remove_file(path)
					.map_err(|source| tg::error!(!source, "failed to remove the symlink"))?;
			},
			Err(source) => {
				return Err(tg::error!(!source, "failed to remove the symlink"));
			},
		}
		match std::os::unix::fs::symlink(target, path) {
			Ok(()) => (),
			Err(source) if source.kind() == std::io::ErrorKind::PermissionDenied => {
				Self::checkin_make_parent_writable(state, path)?;
				std::os::unix::fs::symlink(target, path)
					.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			},
			Err(source) => {
				return Err(tg::error!(!source, "failed to create the symlink"));
			},
		}
		Ok(())
	}

	fn checkin_make_parent_writable(state: &State, path: &Path) -> tg::Result<()> {
		let parent = path.parent().unwrap();
		if let Some(index) = state.graph.paths.get(parent)
			&& let Some(metadata) = state
				.graph
				.nodes
				.get(index)
				.and_then(|node| node.path_metadata.as_ref())
		{
			let mode = metadata.permissions().mode();
			let permissions = std::fs::Permissions::from_mode(mode | 0o222);
			std::fs::set_permissions(parent, permissions)
				.map_err(|source| tg::error!(!source, "failed to set the parent permissions"))?;
		} else {
			let mut permissions = std::fs::symlink_metadata(parent)
				.map_err(|source| tg::error!(!source, "failed to get the parent permissions"))?
				.permissions();
			if permissions.mode() & 0o222 == 0 {
				permissions.set_mode(permissions.mode() | 0o222);
				std::fs::set_permissions(parent, permissions).map_err(|source| {
					tg::error!(!source, "failed to set the parent permissions")
				})?;
			}
		}
		Ok(())
	}

	fn checkin_input_update_parent_edge(
		state: &mut State,
		parent: Parent,
		child_index: usize,
	) -> tg::Result<()> {
		let child_node = state.graph.nodes.get_mut(&child_index).unwrap();
		child_node.referrers.push(parent.index);
		let kind = child_node.variant.kind();
		match parent.variant {
			ParentVariant::DirectoryEntry(name) => {
				let edge: tg::graph::data::Edge<tg::artifact::Id> =
					tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
						graph: None,
						index: child_index,
						kind,
					});
				state
					.graph
					.nodes
					.get_mut(&parent.index)
					.unwrap()
					.variant
					.unwrap_directory_mut()
					.entries
					.insert(name, edge);
			},

			ParentVariant::FileDependency(reference) => {
				let edge: tg::graph::data::Edge<tg::object::Id> =
					tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
						graph: None,
						index: child_index,
						kind,
					});
				let path = state
					.graph
					.nodes
					.get(&child_index)
					.unwrap()
					.path
					.as_ref()
					.unwrap();
				let parent_path = state
					.graph
					.nodes
					.get(&parent.index)
					.unwrap()
					.path
					.as_ref()
					.unwrap();
				let path = tangram_util::path::diff(parent_path.parent().unwrap(), path)
					.map_err(|source| tg::error!(!source, "failed to diff the paths"))?;
				let path = if path.as_os_str().is_empty() {
					".".into()
				} else {
					path
				};
				let path = if let Some(reference_path) = reference.options().path.as_ref() {
					path.join(reference_path)
				} else {
					path
				};
				let options = tg::referent::Options::with_path(path);
				let dependency = tg::graph::data::Dependency(tg::Referent {
					item: Some(edge),
					options,
				});
				state
					.graph
					.nodes
					.get_mut(&parent.index)
					.unwrap()
					.variant
					.unwrap_file_mut()
					.dependencies
					.get_mut(&reference)
					.unwrap()
					.replace(dependency);
			},

			ParentVariant::SymlinkArtifact => {
				let edge: tg::graph::data::Edge<tg::artifact::Id> =
					tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
						graph: None,
						index: child_index,
						kind,
					});
				state
					.graph
					.nodes
					.get_mut(&parent.index)
					.unwrap()
					.variant
					.unwrap_symlink_mut()
					.artifact
					.replace(edge);
			},
		}
		Ok(())
	}

	fn checkin_input_get_lock_node(state: &State, parent: Option<&Parent>) -> Option<usize> {
		let Some(lock) = &state.lock else {
			return None;
		};
		let Some(parent) = parent else {
			return if lock.nodes.is_empty() { None } else { Some(0) };
		};
		let parent_index = state.graph.nodes.get(&parent.index).unwrap().lock_node?;
		let parent_node = &lock.nodes[parent_index];
		match &parent.variant {
			ParentVariant::DirectoryEntry(name) => {
				let directory = parent_node.try_unwrap_directory_ref().ok()?;
				let leaf = directory
					.try_unwrap_leaf_ref()
					.expect("lock directories must be leaves");
				Some(leaf.entries.get(name)?.try_unwrap_pointer_ref().ok()?.index)
			},
			ParentVariant::FileDependency(reference) => Some(
				parent_node
					.try_unwrap_file_ref()
					.ok()?
					.dependencies
					.get(reference)?
					.as_ref()?
					.item()
					.as_ref()?
					.try_unwrap_pointer_ref()
					.ok()?
					.index,
			),
			ParentVariant::SymlinkArtifact => Some(
				parent_node
					.try_unwrap_symlink_ref()
					.ok()?
					.artifact
					.as_ref()?
					.try_unwrap_pointer_ref()
					.ok()?
					.index,
			),
		}
	}

	fn checkin_detect_module_kind(path: &Path) -> tg::Result<Option<tg::module::Kind>> {
		let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
			return Ok(None);
		};
		if name == "tangram.js" || name.ends_with(".tg.js") {
			return Ok(Some(tg::module::Kind::Js));
		}
		if name == "tangram.ts" || name.ends_with(".tg.ts") {
			return Ok(Some(tg::module::Kind::Ts));
		}
		let Some(xattr) = xattr::get(path, tg::file::MODULE_XATTR_NAME)
			.map_err(|source| tg::error!(!source, "failed to get the module xattr"))?
		else {
			return Ok(None);
		};
		let xattr = String::from_utf8(xattr)
			.map_err(|source| tg::error!(!source, "the module xattr is not valid utf-8"))?;
		let kind = xattr
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the module kind"))?;
		Ok(Some(kind))
	}
}
