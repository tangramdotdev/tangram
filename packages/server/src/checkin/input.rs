use {
	super::state::{Directory, File, FixupMessage, Node, State, Symlink, Variant},
	crate::Server,
	smallvec::SmallVec,
	std::{
		collections::BTreeMap,
		os::unix::fs::PermissionsExt as _,
		path::{Path, PathBuf},
	},
	tangram_client as tg,
};

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
	pub(super) fn checkin_input(&self, state: &mut State, root: PathBuf) -> tg::Result<()> {
		self.checkin_visit(state, None, root)?;
		Ok(())
	}

	fn checkin_visit(
		&self,
		state: &mut State,
		parent: Option<Parent>,
		path: PathBuf,
	) -> tg::Result<Option<usize>> {
		// Check if the path has been visited.
		if let Some(index) = state.graph.paths.get(&path) {
			return Ok(Some(*index));
		}

		// Get the metadata.
		let metadata = std::fs::symlink_metadata(&path).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to get the metadata"),
		)?;

		// Skip ignored files.
		if state.ignorer.as_mut().is_some_and(|ignorer| {
			ignorer
				.matches(&path, Some(metadata.is_dir()))
				.unwrap_or_default()
		}) {
			return Ok(None);
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
			let path = std::fs::read_link(&path)
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
			let message = FixupMessage {
				path: path.clone(),
				metadata: metadata.clone(),
			};
			sender.send(message).ok();
		}

		// Get the node index.
		let index = state.graph.next;
		state.graph.next += 1;

		// Update the path.
		state.graph.paths.insert(path.clone(), index);

		// Get the lock node.
		let lock_node = Self::checkin_get_lock_node(state, parent);

		// Create the node.
		let node = Node {
			dirty: false,
			lock_node,
			object_id: None,
			path: Some(path),
			path_metadata: Some(metadata),
			referrers: SmallVec::new(),
			variant,
			visited: false,
		};
		state.graph.nodes.insert(index, node);

		match &state.graph.nodes.get(&index).unwrap().variant {
			Variant::Directory(_) => {
				self.checkin_visit_directory(state, index)?;
			},
			Variant::File(_) => {
				self.checkin_visit_file(state, index)?;
			},
			Variant::Symlink(_) => {
				self.checkin_visit_symlink(state, index)?;
			},
		}

		Ok(Some(index))
	}

	fn checkin_visit_directory(&self, state: &mut State, index: usize) -> tg::Result<()> {
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

		// Visit the children.
		for name in names {
			let parent = Parent {
				node: index,
				variant: ParentVariant::DirectoryEntry(name.clone()),
			};
			let path = state
				.graph
				.nodes
				.get_mut(&index)
				.unwrap()
				.path
				.as_ref()
				.unwrap()
				.join(&name);
			let Some(child_index) = self.checkin_visit(state, Some(parent), path)? else {
				continue;
			};
			state
				.graph
				.nodes
				.get_mut(&child_index)
				.unwrap()
				.referrers
				.push(index);
			let edge = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
				graph: None,
				node: child_index,
			});
			state
				.graph
				.nodes
				.get_mut(&index)
				.unwrap()
				.variant
				.unwrap_directory_mut()
				.entries
				.insert(name, edge);
		}

		Ok(())
	}

	fn checkin_visit_file(&self, state: &mut State, index: usize) -> tg::Result<()> {
		let path = state
			.graph
			.nodes
			.get_mut(&index)
			.unwrap()
			.path
			.as_ref()
			.unwrap()
			.to_owned();

		// Read and visit the dependencies.
		let dependencies = if let Ok(Some(contents)) =
			xattr::get(&path, tg::file::DEPENDENCIES_XATTR_NAME)
		{
			// Read the dependencies xattr.
			let references = serde_json::from_slice::<Vec<tg::Reference>>(&contents)
				.map_err(|source| tg::error!(!source, "failed to deserialize dependencies"))?;

			// Create the dependencies and visit the path dependencies.
			let mut dependencies = BTreeMap::new();
			let referrer = index;
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
				let referent = if let Some(reference_path) = reference_path {
					let parent = Parent {
						node: index,
						variant: ParentVariant::FileDependency(reference.clone()),
					};
					let referent = path.parent().unwrap().join(reference_path);
					let referent = referent.canonicalize().map_err(
						|source| tg::error!(!source, %path = referent.display(), "failed to canonicalize the path"),
					)?;
					let Some(index) = self.checkin_visit(state, Some(parent), referent.clone())?
					else {
						continue;
					};
					state
						.graph
						.nodes
						.get_mut(&index)
						.unwrap()
						.referrers
						.push(referrer);
					let path = tangram_util::path::diff(path.parent().unwrap(), &referent)
						.map_err(|source| tg::error!(!source, "failed to diff the paths"))?;
					let path = if path.as_os_str().is_empty() {
						".".into()
					} else {
						path
					};
					let edge = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
						graph: None,
						node: index,
					});
					let item = edge;
					let options = tg::referent::Options::with_path(path);
					let referent = tg::Referent { item, options };
					Some(referent)
				} else {
					None
				};
				dependencies.insert(reference, referent);
			}

			dependencies
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

			// Create the dependencies and visit the path dependencies.
			let mut dependencies = BTreeMap::new();
			let referrer = index;
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
				let referent = if let Some(reference_path) = reference_path {
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
					let Some(index) = self.checkin_visit(state, Some(parent), referent.clone())?
					else {
						continue;
					};
					state
						.graph
						.nodes
						.get_mut(&index)
						.unwrap()
						.referrers
						.push(referrer);
					let path = tangram_util::path::diff(path.parent().unwrap(), &referent)
						.map_err(|source| tg::error!(!source, "failed to diff the paths"))?;
					let path = if path.as_os_str().is_empty() {
						".".into()
					} else {
						path
					};
					let edge = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
						graph: None,
						node: index,
					});
					let item = edge;
					let options = tg::referent::Options::with_path(path);
					let referent = tg::Referent { item, options };
					Some(referent)
				} else {
					None
				};
				dependencies.insert(reference, referent);
			}

			dependencies
		} else {
			BTreeMap::new()
		};

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

	fn checkin_visit_symlink(&self, state: &mut State, index: usize) -> tg::Result<()> {
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

			// Visit the entry.
			let parent = Parent {
				node: index,
				variant: ParentVariant::SymlinkArtifact,
			};
			let path = artifacts_path.join(artifact.to_string());
			let artifact = self
				.checkin_visit(state, Some(parent), path)?
				.ok_or_else(|| tg::error!("failed to visit dependency"))?;
			state
				.graph
				.nodes
				.get_mut(&artifact)
				.unwrap()
				.referrers
				.push(index);

			// Get the path.
			let path = components.collect();
			let path = if path == Path::new("") {
				None
			} else {
				Some(path)
			};

			// Update the symlink.
			let edge = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
				graph: None,
				node: artifact,
			});
			let variant = state
				.graph
				.nodes
				.get_mut(&index)
				.unwrap()
				.variant
				.unwrap_symlink_mut();
			variant.artifact.replace(edge);
			variant.path = path;

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

	fn checkin_get_lock_node(state: &State, parent: Option<Parent>) -> Option<usize> {
		let Some(lock) = &state.lock else {
			return None;
		};
		let Some(parent) = parent else {
			return if lock.nodes.is_empty() { None } else { Some(0) };
		};
		let parent_index = state.graph.nodes.get(&parent.node).unwrap().lock_node?;
		let parent_node = &lock.nodes[parent_index];
		match parent.variant {
			ParentVariant::DirectoryEntry(name) => Some(
				parent_node
					.try_unwrap_directory_ref()
					.ok()?
					.entries
					.get(&name)?
					.try_unwrap_reference_ref()
					.ok()?
					.node,
			),
			ParentVariant::FileDependency(reference) => Some(
				parent_node
					.try_unwrap_file_ref()
					.ok()?
					.dependencies
					.get(&reference)?
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
}
