use {
	super::graph::{Directory, File, Node, Symlink, Variant},
	crate::{Server, checkin::Graph},
	indoc::indoc,
	smallvec::SmallVec,
	std::{
		collections::BTreeMap,
		os::unix::fs::PermissionsExt as _,
		path::{Path, PathBuf},
	},
	tangram_client as tg, tangram_ignore as ignore,
};

struct State<'a> {
	arg: &'a tg::checkin::Arg,
	artifacts_path: Option<&'a Path>,
	fixup_sender: Option<std::sync::mpsc::Sender<super::fixup::Message>>,
	graph: &'a mut Graph,
	ignorer: Option<ignore::Ignorer>,
	lock: Option<&'a tg::graph::Data>,
	progress: crate::progress::Handle<tg::checkin::Output>,
}

struct Item {
	path: PathBuf,
	parent: Option<Parent>,
}

struct Parent {
	node: usize,
	variant: ParentVariant,
}

enum ParentVariant {
	DirectoryEntry(String),
	FileDependency(tg::Reference),
	SymlinkArtifact,
}

impl Server {
	#[allow(clippy::too_many_arguments)]
	pub(super) fn checkin_input(
		&self,
		arg: &tg::checkin::Arg,
		artifacts_path: Option<&Path>,
		fixup_sender: Option<std::sync::mpsc::Sender<super::fixup::Message>>,
		graph: &mut Graph,
		lock: Option<&tg::graph::Data>,
		next: usize,
		progress: crate::progress::Handle<tg::checkin::Output>,
		root: PathBuf,
	) -> tg::Result<()> {
		// Create the ignorer if necessary.
		let ignorer = if arg.options.ignore {
			Some(Self::checkin_create_ignorer()?)
		} else {
			None
		};

		// Create the state.
		let mut state = State {
			arg,
			artifacts_path,
			fixup_sender,
			graph,
			ignorer,
			lock,
			progress,
		};

		// Visit.
		let item = Item {
			path: root,
			parent: None,
		};
		let mut stack = vec![item];
		while let Some(item) = stack.pop() {
			self.checkin_visit(&mut state, &mut stack, item)?;
		}

		// Set the solved field for all new nodes.
		let petgraph = super::graph::Petgraph {
			graph: state.graph,
			next,
		};
		let sccs = petgraph::algo::tarjan_scc(&petgraph);
		for scc in &sccs {
			let solved =
				!scc.iter().any(|index| {
					// Get the node.
					let node = state.graph.nodes.get(index).unwrap();

					// Check if the node has solveable dependencies.
					if let Variant::File(file) = &node.variant
						&& file.dependencies.keys().any(|reference| {
							reference.item().is_tag() || reference.item().is_object()
						}) {
						return true;
					}

					// Check if the node has unsolved children.
					node.children()
						.iter()
						.any(|&child| !state.graph.nodes.get(&child).unwrap().solved)
				});

			// Set solved to false if necessary.
			if !solved {
				for &index in scc {
					state.graph.nodes.get_mut(&index).unwrap().solved = false;
				}
			}
		}

		Ok(())
	}

	fn checkin_visit(
		&self,
		state: &mut State,
		stack: &mut Vec<Item>,
		item: Item,
	) -> tg::Result<()> {
		// Check if the path has been visited.
		if let Some(&existing_index) = state.graph.paths.get(&item.path) {
			// Update the parent's edge to point to the existing node.
			if let Some(parent) = item.parent {
				Self::checkin_input_update_parent_edge(state, parent, existing_index)?;
			}
			return Ok(());
		}

		// Get the metadata.
		let metadata = std::fs::symlink_metadata(&item.path).map_err(
			|source| tg::error!(!source, %path = item.path.display(), "failed to get the metadata"),
		)?;

		// Skip ignored files.
		if state.ignorer.as_mut().is_some_and(|ignorer| {
			ignorer
				.matches(&item.path, Some(metadata.is_dir()))
				.unwrap_or_default()
		}) {
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

		// Send the message to the fixup task.
		if let Some(sender) = &state.fixup_sender {
			let message = super::fixup::Message {
				path: item.path.clone(),
				metadata: metadata.clone(),
			};
			sender.send(message).ok();
		}

		// Get the node index.
		let index = state.graph.next;
		state.graph.next += 1;

		// Update the path.
		state.graph.paths.insert(item.path.clone(), index);

		// Get the lock node.
		let lock_node = Self::checkin_input_get_lock_node(state, item.parent.as_ref());

		// Create the node.
		let node = Node {
			complete: false,
			lock_node,
			metadata: None,
			id: None,
			path: Some(item.path),
			path_metadata: Some(metadata),
			referrers: SmallVec::new(),
			solved: true,
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
				Self::checkin_visit_symlink(state, stack, index)?;
			},
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
			|source| tg::error!(!source, %path = path.display(), "failed to read the directory"),
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
				node: index,
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
						node: index,
						variant: ParentVariant::FileDependency(reference.clone()),
					};
					let referent = path.parent().unwrap().join(reference_path);
					let referent = referent.canonicalize().map_err(
						|source| tg::error!(!source, %path = referent.display(), "failed to canonicalize the path"),
					)?;
					stack.push(Item {
						path: referent,
						parent: Some(parent),
					});
					dependencies.insert(reference, None);
				} else {
					dependencies.insert(reference, None);
				}
			}
		} else if tg::package::is_module_path(&path) {
			// Read the module.
			let contents = std::fs::read(&path).map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to read the module"),
			)?;
			let text = String::from_utf8(contents).map_err(
				|source| tg::error!(!source, %path = path.display(), "the module is not valid utf-8"),
			)?;

			// Analyze.
			let kind = tg::package::module_kind_for_path(&path)?;
			let module = tg::module::Data {
				kind,
				referent: tg::Referent::with_item(tg::module::data::Item::Path(path.clone())),
			};
			let analysis = tangram_module::analyze(&module, &text);
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
						node: index,
						variant: ParentVariant::FileDependency(reference.clone()),
					};
					let referent = path.parent().unwrap().join(reference_path);
					let referent = if matches!(import.kind, Some(tg::module::Kind::Symlink)) {
						tangram_util::fs::canonicalize_parent_sync(&referent).map_err(
							|source| tg::error!(!source, %path = referent.display(), "failed to canonicalize path"),
						)?
					} else {
						referent.canonicalize().map_err(
							|source| tg::error!(!source, %path = referent.display(), "failed to canonicalize the path"),
						)?
					};
					stack.push(Item {
						path: referent,
						parent: Some(parent),
					});
					dependencies.insert(reference, None);
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
		state: &mut State,
		stack: &mut Vec<Item>,
		index: usize,
	) -> tg::Result<()> {
		// Read the symlink.
		let path = state
			.graph
			.nodes
			.get_mut(&index)
			.unwrap()
			.path
			.as_ref()
			.unwrap();
		let target = std::fs::read_link(path)
			.map_err(|source| tg::error!(!source, "failed to read the symlink"))?;

		// If the target is in the artifacts directory, then treat it as an artifact symlink.
		let Ok(absolute_target) =
			tangram_util::fs::canonicalize_parent_sync(path.parent().unwrap().join(&target))
		else {
			return Ok(());
		};
		if let Some(artifacts_path) = &state.artifacts_path
			&& let Ok(path) = absolute_target.strip_prefix(artifacts_path)
		{
			// Get the entry.
			let mut components = path.components();
			let Some(artifact) = components.next().and_then(|component| {
				if let std::path::Component::Normal(component) = component {
					component.to_str()?.parse::<tg::artifact::Id>().ok()
				} else {
					None
				}
			}) else {
				return Ok(());
			};

			// Create an item for the artifact.
			let parent = Parent {
				node: index,
				variant: ParentVariant::SymlinkArtifact,
			};
			let path = artifacts_path.join(artifact.to_string());

			// Get the path within the artifact.
			let artifact_path = components.collect();
			let artifact_path = if artifact_path == Path::new("") {
				None
			} else {
				Some(artifact_path)
			};

			// Update the symlink path.
			state
				.graph
				.nodes
				.get_mut(&index)
				.unwrap()
				.variant
				.unwrap_symlink_mut()
				.path = artifact_path;

			stack.push(Item {
				path,
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

	fn checkin_input_update_parent_edge(
		state: &mut State,
		parent: Parent,
		child_index: usize,
	) -> tg::Result<()> {
		state
			.graph
			.nodes
			.get_mut(&child_index)
			.unwrap()
			.referrers
			.push(parent.node);
		match parent.variant {
			ParentVariant::DirectoryEntry(name) => {
				let edge: tg::graph::data::Edge<tg::artifact::Id> =
					tg::graph::data::Edge::Reference(tg::graph::data::Reference {
						graph: None,
						node: child_index,
					});
				state
					.graph
					.nodes
					.get_mut(&parent.node)
					.unwrap()
					.variant
					.unwrap_directory_mut()
					.entries
					.insert(name, edge);
			},
			ParentVariant::FileDependency(reference) => {
				let edge: tg::graph::data::Edge<tg::object::Id> =
					tg::graph::data::Edge::Reference(tg::graph::data::Reference {
						graph: None,
						node: child_index,
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
					.get(&parent.node)
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
				let options = tg::referent::Options::with_path(path);
				let referent = tg::Referent {
					item: edge,
					options,
				};
				state
					.graph
					.nodes
					.get_mut(&parent.node)
					.unwrap()
					.variant
					.unwrap_file_mut()
					.dependencies
					.get_mut(&reference)
					.unwrap()
					.replace(referent);
			},
			ParentVariant::SymlinkArtifact => {
				let edge: tg::graph::data::Edge<tg::artifact::Id> =
					tg::graph::data::Edge::Reference(tg::graph::data::Reference {
						graph: None,
						node: child_index,
					});
				state
					.graph
					.nodes
					.get_mut(&parent.node)
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
		let parent_index = state.graph.nodes.get(&parent.node).unwrap().lock_node?;
		let parent_node = &lock.nodes[parent_index];
		match &parent.variant {
			ParentVariant::DirectoryEntry(name) => Some(
				parent_node
					.try_unwrap_directory_ref()
					.ok()?
					.entries
					.get(name)?
					.try_unwrap_reference_ref()
					.ok()?
					.node,
			),
			ParentVariant::FileDependency(reference) => Some(
				parent_node
					.try_unwrap_file_ref()
					.ok()?
					.dependencies
					.get(reference)?
					.as_ref()?
					.item()
					.try_unwrap_reference_ref()
					.ok()?
					.node,
			),
			ParentVariant::SymlinkArtifact => Some(
				parent_node
					.try_unwrap_symlink_ref()
					.ok()?
					.artifact
					.as_ref()?
					.try_unwrap_reference_ref()
					.ok()?
					.node,
			),
		}
	}

	pub(crate) fn checkin_create_ignorer() -> tg::Result<ignore::Ignorer> {
		let file_names = vec![".tangramignore".into(), ".gitignore".into()];
		let global = indoc!(
			"
				.DS_Store
				.git
				.tangram
				tangram.lock
			"
		);
		ignore::Ignorer::new(file_names, Some(global))
			.map_err(|source| tg::error!(!source, "failed to create the matcher"))
	}
}
