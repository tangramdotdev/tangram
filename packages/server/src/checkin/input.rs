use super::{Directory, File, Node, State, Symlink, Variant};
use crate::Server;
use std::{
	os::unix::fs::PermissionsExt as _,
	path::{Path, PathBuf},
	sync::Arc,
};
use itertools::Itertools;
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(super) fn checkin_collect_input(&self, state: &mut State, root: PathBuf) -> tg::Result<()> {
		self.checkin_visit(state, root)?;
		Self::checkin_find_roots(state);
		Ok(())
	}

	fn checkin_visit(&self, state: &mut State, path: PathBuf) -> tg::Result<Option<usize>> {
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

		// Send the path to the fixup task.
		if let Some(fixup_sender) = &state.fixup_sender {
			fixup_sender.send((path.clone(), metadata.clone())).ok();
		}

		// Get the node index.
		let index = state.graph.nodes.len();

		// Update the path.
		state.graph.paths.insert(path.clone(), index);

		// Lookup the lockfile node.
		let lockfile_node = state
			.lockfile
			.as_ref()
			.and_then(|lockfile| lockfile.get_node_for_path(&path).ok());

		// Create the node.
		let node = Node {
			lockfile_index: lockfile_node,
			variant,
			metadata: Some(metadata),
			object: None,
			path: Some(Arc::new(path)),
			parent: None,
			root: None,
			tag: None,
		};
		state.graph.nodes.push_back(node);

		// Visit the edges.
		match &state.graph.nodes[index].variant {
			Variant::Directory(_) => self.checkin_visit_directory_edges(state, index)?,
			Variant::File(_) => self.checkin_visit_file_edges(state, index)?,
			Variant::Symlink(_) => Self::checkin_visit_symlink_edges(state, index)?,
			Variant::Object => return Err(tg::error!("unreachable")),
		}

		Ok(Some(index))
	}

	fn checkin_visit_directory_edges(&self, state: &mut State, index: usize) -> tg::Result<()> {
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

		// Sort the entries.
		names.sort_unstable();

		// Visit the children.
		for name in names {
			let path = state.graph.nodes[index].path().join(&name);
			let Some(child_index) = self.checkin_visit(state, path)? else {
				continue;
			};
			state.graph.nodes[child_index].parent.replace(index);
			state.graph.nodes[index]
				.variant
				.unwrap_directory_mut()
				.entries
				.push((name, child_index));
		}

		Ok(())
	}

	fn checkin_visit_file_edges(&self, state: &mut State, index: usize) -> tg::Result<()> {
		// Get the list of all dependencies.
		let path = state.graph.nodes[index].path().to_owned();
		let mut dependencies = self.get_file_dependencies(state, &path)?;

		// Visit path dependencies.
		for (reference, referent) in &mut dependencies {
			if let Some(reference) = reference.path() {
				let path = path
					.parent()
					.unwrap()
					.join(reference);
				let path = crate::util::fs::canonicalize_parent_sync(&path).map_err(						|source| tg::error!(!source, %path = path.display(), "failed to canonicalize path"))?;
				let Some(index) = self.checkin_visit(state, path)? else {
					continue;
				};
				referent.replace(tg::Referent {
					item: Either::Right(index),
					path: Some(reference.to_owned()),
					tag: None
				});
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
	) -> tg::Result<Vec<(tg::Reference, Option<tg::Referent<Either<tg::object::Id, usize>>>)>> {
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
			return file_node.dependencies.iter().map(
				|(reference, referent)| match &referent.item {
					Either::Left(_) => Err(tg::error!("found a graph node")),
					Either::Right(object) => {
						let referent = tg::Referent {
							item: Either::Left(object.clone()),
							path: referent.path.clone(),
							tag: referent.tag.clone(),
						};
						Ok((reference.clone(), Some(referent)))
					},
				},
			).try_collect();
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
		let analysis = crate::Compiler::analyze_module(text).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to analyze the module"),
		)?;
		for error in analysis.errors {
			// Dump diagnostics.
			let diagnostic = tg::Diagnostic {
				location: None,
				message: error.to_string(),
				severity: tg::diagnostic::Severity::Error,
			};
			state.progress.diagnostic(diagnostic);
		}

		// Get the file's dependencies.
		let dependencies = analysis
			.imports
			.into_iter()
			.map(|import| {
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

				// Add the import.
				(import.reference.clone(), None)
			})
			.collect();

		Ok(dependencies)
	}

	#[allow(clippy::unnecessary_wraps)]
	fn checkin_visit_symlink_edges(_state: &mut State, _index: usize) -> tg::Result<()> {
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
			let Some(path) = state.graph.nodes[index].path.as_ref() else {
				continue;
			};

			// Check if the hint matches.
			if let Some(hint) = hint {
				let hint_path = state.graph.nodes[hint].path();
				if path.strip_prefix(hint_path).map(Path::to_owned).is_ok() {
					// Mark this as a child of the root.
					state.graph.roots.entry(hint).or_default().push(index);
					state.graph.nodes[index].root.replace(hint);

					// Recurse on children using the same hint.
					let children = state.graph.nodes[index]
						.edges()
						.into_iter()
						.map(|child| (child, Some(hint)));
					stack.extend(children);
					continue 'outer;
				}
			}

			// Try and find the package of this node.
			let mut root = None;
			for ancestor in path.ancestors().skip(1) {
				let Some(node) = state.graph.paths.get(ancestor) else {
					break;
				};
				root.replace(*node);
			}

			// Add to the list of packages if necessary.
			if let Some(root) = root {
				// Mark this as a child of the root.
				state.graph.roots.entry(root).or_default().push(index);
				state.graph.nodes[index].root.replace(root);
			}

			// If this is a directory and we didn't find a root, set the hint to this directory.
			if root.is_none() {
				state.graph.roots.entry(index).or_default();
				root.replace(index);
			}

			// Recurse.
			let children = state.graph.nodes[index]
				.edges()
				.into_iter()
				.map(|child| (child, root));
			stack.extend(children);
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::test::test;
	use tangram_client as tg;
	use tangram_temp::{self as temp, Temp};

	#[tokio::test]
	async fn package_with_path_dependencies() {
		let artifact = temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import bar from "../bar";"#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => r#"import foo from "../foo";"#,
			},
		};
		let assertions = |state: &crate::checkin::State| {
			assert_eq!(state.graph.roots.len(), 2);
			assert_eq!(state.graph.roots.get(&0).unwrap(), &vec![1]);
			assert_eq!(state.graph.roots.get(&2).unwrap(), &vec![3]);
			assert!(state.graph.nodes[0].path().ends_with("foo"));
			assert!(state.graph.nodes[1].path().ends_with("foo/tangram.ts"));
			assert!(state.graph.nodes[2].path().ends_with("bar"));
			assert!(state.graph.nodes[3].path().ends_with("bar/tangram.ts"));
		};
		test_input(artifact, "foo", assertions).await;
	}

	#[tokio::test]
	async fn nested_path_dependencies() {
		let artifact = temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import bar from "./bar";"#,
				"bar" => temp::directory! {
					"tangram.ts" => "",
				},
			},
		};
		let assertions = |state: &crate::checkin::State| {
			assert_eq!(state.graph.roots.len(), 1);
			assert_eq!(state.graph.roots.get(&0).unwrap(), &vec![3, 1, 2]);
			assert!(state.graph.nodes[0].path().ends_with("foo"));
			assert!(state.graph.nodes[1].path().ends_with("foo/bar"));
			assert!(state.graph.nodes[2].path().ends_with("foo/bar/tangram.ts"));
			assert!(state.graph.nodes[3].path().ends_with("foo/tangram.ts"));
		};
		test_input(artifact, "foo", assertions).await;
	}

	async fn test_input<F>(
		artifact: impl Into<temp::Artifact> + Send + 'static,
		path: &'static str,
		assertions: F,
	) where
		F: FnOnce(&crate::checkin::State) + Send + 'static,
	{
		test(async move |context| {
			// Start the server.
			let server = context.start_server().await;

			// Create the test artifact.
			let artifact: temp::Artifact = artifact.into();
			let temp = Temp::new();
			artifact.to_path(temp.path()).await.unwrap();

			let progress = crate::progress::Handle::new();
			let mut state = crate::checkin::State {
				arg: tg::checkin::Arg {
					path: temp.path().join(path),
					destructive: false,
					deterministic: false,
					ignore: true,
					locked: false,
					lockfile: true,
					updates: Vec::new(),
				},
				fixup_sender: None,
				graph: crate::checkin::Graph::default(),
				graph_objects: Vec::new(),
				lockfile: None,
				ignorer: None,
				progress,
			};

			let state = tokio::task::spawn_blocking(move || {
				server
					.checkin_collect_input(&mut state, temp.path().join(path))
					.expect("collect input failed");
				state
			})
			.await
			.unwrap();

			assertions(&state);
		})
		.await;
	}
}
