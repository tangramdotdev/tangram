use super::{Directory, File, FileDependency, Node, State, Symlink, Variant};
use crate::Server;
use std::{
	os::unix::fs::PermissionsExt as _,
	path::{Path, PathBuf},
	sync::Arc,
};
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(super) fn checkin_collect_input(
		&self,
		state: &mut State,
		send: &std::sync::mpsc::Sender<(PathBuf, std::fs::Metadata)>,
		root: PathBuf,
	) -> tg::Result<()> {
		self.checkin_visit(state, send, root)?;
		Self::checkin_find_roots(state);
		Self::checkin_find_subpaths(state);
		Ok(())
	}

	fn checkin_visit(
		&self,
		state: &mut State,
		send: &std::sync::mpsc::Sender<(PathBuf, std::fs::Metadata)>,
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
				entries: Vec::new(),
			})
		} else if metadata.is_file() {
			Variant::File(File {
				blob: None,
				dependencies: Vec::new(),
				executable: metadata.permissions().mode() & 0o111 != 0,
			})
		} else if metadata.is_symlink() {
			let target = std::fs::read_link(&path)
				.map_err(|source| tg::error!(!source, "failed to read the symlink"))?;
			Variant::Symlink(Symlink { target })
		} else {
			return Err(tg::error!(?metadata, "invalid file type"));
		};

		// If this is a destructive checkin, update permissions/times.
		if state.arg.destructive {
			send.send((path.clone(), metadata.clone())).ok();
		}

		// Get the node index.
		let index = state.graph.nodes.len();

		// Update the path.
		state.graph.paths.insert(path.clone(), index);

		// Create the node.
		let node = Node {
			variant,
			metadata: Some(metadata),
			object: None,
			path: Some(Arc::new(path)),
			root: None,
			tag: None,
		};
		state.graph.nodes.push_back(node);

		// Visit the edges.
		match &state.graph.nodes[index].variant {
			Variant::Directory(_) => self.checkin_visit_directory_edges(state, send, index)?,
			Variant::File(_) => self.checkin_visit_file_edges(state, send, index)?,
			Variant::Symlink(_) => Self::checkin_visit_symlink_edges(state, send, index)?,
			Variant::Object => return Err(tg::error!("unreachable")),
		}

		Ok(Some(index))
	}

	fn checkin_visit_directory_edges(
		&self,
		state: &mut State,
		send: &std::sync::mpsc::Sender<(PathBuf, std::fs::Metadata)>,
		index: usize,
	) -> tg::Result<()> {
		// Read the entries.
		let read_dir = std::fs::read_dir(state.graph.nodes[index].path())
			.map_err(|source| tg::error!(!source, "failed to read the directory"))?;
		let mut names = Vec::new();
		for result in read_dir {
			let entry = result
				.map_err(|source| tg::error!(!source, "failed to get the directory entry"))?;
			let name = entry
				.file_name()
				.to_str()
				.ok_or_else(|| tg::error!("expected the entry name to be a string"))?
				.to_owned();
			names.push(name);
		}

		// Visit the children.
		for name in names {
			let path = state.graph.nodes[index].path().join(&name);
			let Some(child_index) = self.checkin_visit(state, send, path)? else {
				continue;
			};
			state.graph.nodes[index]
				.variant
				.unwrap_directory_mut()
				.entries
				.push((name, child_index));
		}

		Ok(())
	}

	fn checkin_visit_file_edges(
		&self,
		state: &mut State,
		send: &std::sync::mpsc::Sender<(PathBuf, std::fs::Metadata)>,
		index: usize,
	) -> tg::Result<()> {
		// Get the list of all dependencies.
		let path = state.graph.nodes[index].path().to_owned();
		let mut dependencies = self.get_file_dependencies(state, &path)?;

		// Visit path dependencies.
		for dependency in &mut dependencies {
			match dependency {
				FileDependency::Import { import, node, .. }
					if import.reference.path().is_some() =>
				{
					let path = path
						.parent()
						.unwrap()
						.join(import.reference.path().unwrap());
					let path = crate::util::fs::canonicalize_parent_sync(&path).map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to canonicalize path"),
					)?;
					let Some(index) = self.checkin_visit(state, send, path)? else {
						continue;
					};
					*node = Some(index);
				},
				_ => (),
			}
		}

		// Update the graph.
		state.graph.nodes[index]
			.variant
			.unwrap_file_mut()
			.dependencies = dependencies;

		Ok(())
	}

	fn get_file_dependencies(
		&self,
		state: &mut State,
		path: &Path,
	) -> tg::Result<Vec<FileDependency>> {
		// Check if this file has dependencies set in the xattr.
		if let Ok(Some(contents)) = xattr::get(path, tg::file::XATTR_LOCK_NAME) {
			let lockfile = serde_json::from_slice::<tg::Lockfile>(&contents)
				.map_err(|source| tg::error!(!source, "failed to deserialize lockfile"))?;
			if lockfile.nodes.len() != 1 {
				return Err(tg::error!(%path = path.display(), "expected single node in lockfile"));
			}
			let Some(tg::lockfile::Node::File(file_node)) = lockfile.nodes.first() else {
				return Err(tg::error!(%path = path.display(), "expected a file node"));
			};
			return file_node.dependencies.iter().try_fold(
				Vec::new(),
				|mut acc, (reference, referent)| match &referent.item {
					Either::Left(_) => Err(tg::error!("found a graph node")),
					Either::Right(object) => {
						let referent = tg::Referent {
							item: Some(Either::Left(object.clone())),
							path: referent.path.clone(),
							subpath: referent.subpath.clone(),
							tag: referent.tag.clone(),
						};
						let dependency = FileDependency::Referent {
							reference: reference.clone(),
							referent,
						};
						acc.push(dependency);
						Ok(acc)
					},
				},
			);
		}

		// If this is not a module, it has no dependencies.
		if !tg::package::is_module_path(path) {
			return Ok(Vec::new());
		}

		// Parse imports.
		let contents = std::fs::read(path).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to read module file"),
		)?;
		let text = String::from_utf8(contents)
			.map_err(|source| tg::error!(!source, %path = path.display(), "invalid utf8"))?;
		let analysis = crate::Compiler::analyze_module(text)?;
		for error in analysis.errors {
			// Dump diagnostics.
			let diagnostic = tg::Diagnostic {
				location: None,
				message: error.to_string(),
				severity: tg::diagnostic::Severity::Error,
			};
			state.progress.diagnostic(diagnostic);
		}

		// Get the locked dependencies.
		let locked_dependencies = state
			.lockfile
			.as_ref()
			.and_then(|lockfile| lockfile.get_file_dependencies(path).ok())
			.unwrap_or_default();

		// Make sure that the lockfile dependencies matches the import set.
		if state.arg.locked
			&& (locked_dependencies.len() != analysis.imports.len()
				|| !locked_dependencies.iter().all(|(reference, _)| {
					analysis
						.imports
						.iter()
						.any(|import| &import.reference == reference)
				})) {
			return Err(tg::error!(
				"the lockfile needs to be updated but --locked was passed"
			));
		}

		let dependencies =
			analysis
				.imports
				.into_iter()
				.try_fold(Vec::new(), |mut acc, import| {
					// Use the locked dependency if the reference matches.
					if let Some((reference, referent)) =
						locked_dependencies
							.iter()
							.find_map(|(reference, referent)| {
								if &import.reference != reference {
									return None;
								}
								let item = referent.item.as_ref()?.as_ref().right()?.clone();
								let referent = tg::Referent {
									item: Some(Either::Left(item)),
									path: referent.path.clone(),
									subpath: referent.subpath.clone(),
									tag: referent.tag.clone(),
								};
								Some((reference.clone(), referent))
							}) {
						acc.push(FileDependency::Referent {
							reference,
							referent,
						});
						return Ok(acc);
					}

					// Return an error if this dependency can't be resolved.
					if state.locked
						&& import.reference.path().is_none()
						&& import.reference.item().try_unwrap_tag_ref().is_ok()
					{
						return Err(
							tg::error!(%import = &import.reference, "unresolved import when --locked"),
						);
					}

					// Pull tags.
					if let Ok(pattern) = import.reference.item().try_unwrap_tag_ref() {
						tokio::spawn({
							let server = self.clone();
							let pattern = pattern.clone();
							let remote = import
								.reference
								.options()
								.and_then(|options| options.remote.clone());
							async move {
								server.pull_tag(pattern.clone(), remote.clone()).await.ok();
							}
						});
					}

					// Get the subpath of this import if it exists. Since this may change later, it's kept separate to preserve the original import reference/options.
					let subpath = import
						.reference
						.options()
						.and_then(|opt| opt.subpath.clone());

					// Add the import.
					acc.push(FileDependency::Import {
						import,
						node: None,
						path: None,
						subpath,
					});

					Ok(acc)
				})?;

		Ok(dependencies)
	}

	#[allow(clippy::unnecessary_wraps)]
	fn checkin_visit_symlink_edges(
		_state: &mut State,
		_send: &std::sync::mpsc::Sender<(PathBuf, std::fs::Metadata)>,
		_index: usize,
	) -> tg::Result<()> {
		Ok(())
	}

	fn checkin_find_roots(state: &mut State) {
		let mut visited = vec![false; state.graph.nodes.len()];
		let mut stack = vec![(0, None::<usize>)];
		'outer: while let Some((index, hint)) = stack.pop() {
			// Skip nodes that have already been visited.
			if visited[index] {
				continue;
			}
			visited[index] = true;

			// Walk up the path hierarchy.
			let path = state.graph.nodes[index].path();

			// Check if the hint matches.
			if let (Some(hint), false) = (hint, state.graph.nodes[index].is_package()) {
				let hint_path = state.graph.nodes[hint].path();
				if path.strip_prefix(hint_path).is_ok() {
					// Mark this node's root.
					state.graph.nodes[index].root.replace(hint);
					state.graph.roots.entry(hint).or_default().push(index);

					// Recurse on children using the same hint.
					let children = state.graph.nodes[index]
						.edges()
						.into_iter()
						.map(|child| (child, Some(hint)));
					stack.extend(children);
					continue 'outer;
				}
			}

			// Search the ancestors of this node to find its root, unless it is a package.
			let mut root = None;
			if !state.graph.nodes[index].is_package() {
				for ancestor in path.ancestors().skip(1) {
					let Some(node) = state.graph.paths.get(ancestor) else {
						break;
					};
					root.replace(*node);
				}
			}

			// Add to the list of roots if necessary.
			if let Some(root) = root {
				state.graph.roots.entry(root).or_default().push(index);
				state.graph.nodes[index].root.replace(root);
			} else {
				state.graph.roots.entry(index).or_default();
			}

			// Recurse, using the root if it was discovered or this node if it is a directory.
			let hint = Some(root.unwrap_or(index));
			let children = state.graph.nodes[index]
				.edges()
				.into_iter()
				.map(|child| (child, hint));

			stack.extend(children);
		}
	}

	fn checkin_find_subpaths(state: &mut State) {
		for index in 0..state.graph.nodes.len() {
			if !(state.graph.nodes[index]
				.metadata
				.as_ref()
				.unwrap()
				.is_file() && tg::package::is_module_path(state.graph.nodes[index].path()))
			{
				continue;
			}

			let updates = state.graph.nodes[index]
				.variant
				.unwrap_file_ref()
				.dependencies
				.iter()
				.enumerate()
				.filter_map(|(dep_index, dependency)| {
					// Skip unresolved dependencies.
					let FileDependency::Import {
						import,
						node: Some(node),
						subpath: None,
						..
					} = dependency
					else {
						return None;
					};

					// Skip any imports that have subpaths.
					if import
						.reference
						.options()
						.is_some_and(|options| options.subpath.is_some())
					{
						return None;
					}

					// Get the package and subpath of the import.
					let (package, subpath) = state.graph.nodes[*node].root.map_or_else(
						|| {
							// If the import kind is _not_ a module, make sure to ignore the result.
							if !matches!(
								import.kind,
								None | Some(
									tg::module::Kind::Dts
										| tg::module::Kind::Js | tg::module::Kind::Ts
								)
							) {
								return (*node, None);
							}

							// If this is a root directory and contains a tangram.ts, use it.
							let subpath = state.graph.nodes[*node]
								.variant
								.try_unwrap_directory_ref()
								.ok()
								.and_then(|directory| {
									directory.entries.iter().find_map(|(name, _)| {
										tg::package::ROOT_MODULE_FILE_NAMES
											.contains(&name.as_str())
											.then_some(name.into())
									})
								});
							(*node, subpath)
						},
						|root| {
							// Otherwise look for this as a subpath of the module.
							let subpath = state.graph.nodes[*node]
								.path()
								.strip_prefix(state.graph.nodes[root].path())
								.unwrap()
								.to_owned();
							(root, Some(subpath))
						},
					);

					// Use the relative path of the package
					let path = state.graph.nodes[package]
						.path
						.as_deref()
						.and_then(|module_path| {
							let path =
								crate::util::path::diff(&state.arg.path, module_path).ok()?;
							(!path.as_os_str().is_empty()).then_some(path)
						});

					// Recreate the new dependency.
					Some((
						dep_index,
						FileDependency::Import {
							import: import.clone(),
							node: Some(package),
							path,
							subpath,
						},
					))
				})
				.collect::<Vec<_>>(); // iterator ivnalidation.

			// Replace the import with the changed one.
			for (dep_index, dependency) in updates {
				state.graph.nodes[index]
					.variant
					.unwrap_file_mut()
					.dependencies[dep_index] = dependency;
			}
		}
	}
}
