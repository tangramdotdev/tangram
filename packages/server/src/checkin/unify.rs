use super::{FileDependency, Graph};
use crate::Server;
use std::collections::BTreeMap;
use tangram_client as tg;
use tangram_either::Either;

#[derive(Clone, Debug)]
struct State {
	// A cache of already solved objects.
	packages: im::HashMap<String, (tg::Tag, usize)>,

	// Flag set when any error occurs. Intermediate errors are reported as diagnostics.
	errored: bool,

	// The current graph.
	graph: Graph,

	// A work queue of edges we will have to follow, in depth-first order.
	queue: im::Vector<Unresolved>,

	// A lazily-initialized set of packages to try.
	objects: Option<im::Vector<(tg::Tag, tg::Object)>>,

	// A list of visited edges.
	visited: im::HashSet<Unresolved>,
}

// An unresolved reference in the graph.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Unresolved {
	// The constraint the referrer places on the dependency.
	reference: tg::Reference,

	// The source node of the edge.
	src: usize,

	// The destination of the edge.
	dst: Option<usize>,
}

impl Server {
	pub(super) async fn unify_file_dependencies(&self, state: &mut super::State) -> tg::Result<()> {
		// Copy the current state of the graph.
		let graph = state.graph.clone();

		// Queue up a list of edges to solve, from the root.
		let queue = graph.unresolved(0);

		// Create the current state.
		let mut current = State {
			packages: im::HashMap::new(),
			errored: false,
			graph,
			queue,
			objects: None,
			visited: im::HashSet::new(),
		};

		// Create the checkpoint history.
		let mut checkpoints = Vec::new();

		// Solve.
		while let Some(unresolved) = current.queue.pop_front() {
			self.walk_edge(&mut checkpoints, &mut current, &state.progress, unresolved)
				.await;
			current.objects.take();
		}

		// Validate.
		if current.errored {
			return Err(tg::error!("could not unify dependencies"));
		}

		// Update state.
		state.graph = current.graph;

		// Fix subpaths.
		Self::fix_unification_subpaths(state);

		// Return.
		Ok(())
	}

	async fn walk_edge(
		&self,
		checkpoints: &mut Vec<State>,
		current: &mut State,
		progress: &crate::progress::Handle<tg::checkin::Output>,
		unresolved: Unresolved,
	) {
		// Check if this edge has been visited.
		if current.visited.contains(&unresolved) {
			return;
		}
		current.visited.insert(unresolved.clone());

		// Add child edges and continue.
		if let Some(referent) = unresolved.dst {
			current.queue.append(current.graph.unresolved(referent));
			return;
		}

		// Attempt to resolve the reference.
		match self
			.resolve_reference(checkpoints, current, &unresolved.reference, progress)
			.await
		{
			Ok(resolved) => {
				// Update the file's dependency. Note: only files have unresolved dependencies.
				for dependency in &mut current.graph.nodes[unresolved.src]
					.variant
					.unwrap_file_mut()
					.dependencies
				{
					match dependency {
						FileDependency::Import { import, node, .. }
							if import.reference == unresolved.reference =>
						{
							node.replace(resolved);
						},
						FileDependency::Referent {
							reference,
							referent,
						} if reference == &unresolved.reference => {
							referent.item.replace(Either::Right(resolved));
						},
						_ => continue,
					}
					break;
				}

				// Add the new node's unresolved edges to the queue.
				current.queue.append(current.graph.unresolved(resolved));
			},
			Err(error) => {
				// If there was an error, attempt to backtrack.
				progress.diagnostic(tg::Diagnostic {
					location: None,
					severity: tg::diagnostic::Severity::Warning,
					message: format!(
						"(backtracking) could not resolve dependency {} {}: {error}",
						current.graph.fmt_node(unresolved.src),
						unresolved.reference,
					),
				});
				if let Some(previous) = try_backtrack(checkpoints, &unresolved) {
					*current = previous;
					return;
				}
				// If backtracking failed, issue an error.
				progress.diagnostic(tg::Diagnostic {
					location: None,
					severity: tg::diagnostic::Severity::Error,
					message: format!(
						"could not resolve dependency {} {}: {error}",
						current.graph.fmt_node(unresolved.src),
						unresolved.reference,
					),
				});
				current.errored = true;
			},
		}
	}

	async fn resolve_reference(
		&self,
		checkpoints: &mut Vec<State>,
		current: &mut State,
		reference: &tg::Reference,
		progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<usize> {
		let Ok(pattern) = reference.item().try_unwrap_tag_ref() else {
			if reference.item().try_unwrap_path_ref().is_ok() {
				return Err(tg::error!(%reference, "unexpected path reference"));
			}

			// If the reference does not name a tag or path, try to resolve it directly.
			let referent = reference.get(self).await?;
			let object = referent
				.item
				.as_ref()
				.right()
				.ok_or_else(|| tg::error!("expected an object"))?;
			let node = self
				.unify_visit_object(current, object, None, false)
				.await?;
			return Ok(node);
		};

		// Check if there is already a result.
		if let Some((tag, node)) = current.packages.get(pattern.name()) {
			if pattern.matches(tag) {
				return Ok(*node);
			}
			return Err(tg::error!(%tag, %pattern, "incompatible versions"));
		}

		// Initialize the set of objects to use.
		if current.objects.is_none() {
			let remote = reference
				.options()
				.as_ref()
				.and_then(|query| query.remote.clone());

			// List tags that match the pattern.
			let objects: im::Vector<_> = self
				.list_tags(tg::tag::list::Arg {
					length: None,
					pattern: pattern.clone(),
					remote,
					reverse: false,
				})
				.await
				.map_err(|source| tg::error!(!source, %pattern, "failed to get tags"))?
				.data
				.into_iter()
				.filter_map(|output| {
					let object = output.item.right()?;
					Some((output.tag, tg::Object::with_id(object)))
				})
				.collect();
			current.objects.replace(objects);
		}

		// Pick the next object.
		let (tag, object) = current
			.objects
			.as_mut()
			.unwrap()
			.pop_back()
			.ok_or_else(|| tg::error!(%reference, "tag does not exist"))?;

		progress.diagnostic(tg::Diagnostic {
			location: None,
			severity: tg::diagnostic::Severity::Info,
			message: format!("resolving {reference} with {tag}",),
		});

		// Create the node.
		let node = self
			.unify_visit_object(current, &object, Some(tag.clone()), true)
			.await?;

		// Update the list of solved packages.
		current.packages.insert(tag.name().to_owned(), (tag, node));

		// Checkpoint.
		checkpoints.push(current.clone());

		// Return.
		Ok(node)
	}
}

impl Server {
	async fn unify_visit_object(
		&self,
		state: &mut State,
		object: &tg::Object,
		tag: Option<tg::Tag>,
		unify: bool,
	) -> tg::Result<usize> {
		let mut visited = BTreeMap::new();
		self.unify_visit_object_inner(state, object, tag, unify, &mut visited)
			.await
	}

	async fn unify_visit_object_inner(
		&self,
		state: &mut State,
		object: &tg::Object,
		tag: Option<tg::Tag>,
		unify: bool,
		visited: &mut BTreeMap<tg::object::Id, usize>,
	) -> tg::Result<usize> {
		let object_id = object.id(self).await?;

		// Skip if already visited.
		if let Some(id) = visited.get(&object_id) {
			return Ok(*id);
		}

		// Get the object data.
		let object_ = if matches!(
			&object,
			tg::Object::Directory(_) | tg::Object::File(_) | tg::Object::Symlink(_)
		) {
			let data = object.data(self).await?;
			let bytes = data.serialize()?;
			Some(super::Object {
				id: object_id.clone(),
				bytes,
				data,
			})
		} else {
			None
		};

		// Create the variant.
		let variant = match &object {
			tg::Object::Directory(_) => super::Variant::Directory(super::Directory {
				entries: Vec::new(),
			}),
			tg::Object::File(file) => {
				let executable = file.executable(self).await?;
				let blob = file.contents(self).await?.id(self).await?;
				super::Variant::File(super::File {
					executable,
					blob: Some(super::Blob::Id(blob)),
					dependencies: Vec::new(),
				})
			},
			tg::Object::Symlink(symlink) => {
				let target = symlink
					.subpath(self)
					.await?
					.ok_or_else(|| tg::error!("unimplemented"))?;
				super::Variant::Symlink(super::Symlink { target })
			},
			_ => super::Variant::Object,
		};

		// Create the node.
		let node = super::Node {
			metadata: None,
			object: object_,
			tag: tag.clone(),
			path: None,
			root: None,
			variant,
		};

		// Add the node to the graph.
		let index = state.graph.nodes.len();
		state.graph.nodes.push_back(node);
		visited.insert(object_id.clone(), index);

		// Update the tag.
		if let Some(tag) = tag {
			let name = tag.name();
			if state.packages.contains_key(name) {
				return Err(tg::error!(%tag, "duplicate tag names"));
			}
			state.packages.insert(name.to_owned(), (tag, index));
		}

		// Recurse.
		match object {
			tg::Object::Directory(directory) if unify => {
				self.unify_visit_directory_edges(state, index, directory, visited)
					.await?;
			},
			tg::Object::File(file) if unify => {
				self.unify_visit_file_edges(state, index, file, visited)
					.await?;
			},
			tg::Object::Symlink(symlink) if unify => {
				self.unify_visit_symlink_edges(state, index, symlink, visited)
					.await?;
			},
			_ => (),
		}

		// Return the index.
		Ok(index)
	}

	async fn unify_visit_directory_edges(
		&self,
		state: &mut State,
		index: usize,
		directory: &tg::Directory,
		visited: &mut BTreeMap<tg::object::Id, usize>,
	) -> tg::Result<()> {
		let mut entries = Vec::new();
		for (name, object) in directory.entries(self).await? {
			let index =
				Box::pin(self.unify_visit_object_inner(state, &object.into(), None, true, visited))
					.await?;
			entries.push((name, index));
		}
		state.graph.nodes[index]
			.variant
			.unwrap_directory_mut()
			.entries = entries;
		Ok(())
	}

	async fn unify_visit_file_edges(
		&self,
		state: &mut State,
		index: usize,
		file: &tg::File,
		visited: &mut BTreeMap<tg::object::Id, usize>,
	) -> tg::Result<()> {
		let mut dependencies = Vec::new();
		for (reference, referent) in file.dependencies(self).await? {
			// Leave tag references as unresolved.
			if reference.item().try_unwrap_tag_ref().is_ok() {
				let dependency = FileDependency::Referent {
					reference,
					referent: tg::Referent {
						item: None,
						path: referent.path,
						subpath: referent.subpath,
						tag: referent.tag,
					},
				};
				dependencies.push(dependency);
			} else {
				let index = Box::pin(self.unify_visit_object_inner(
					state,
					&referent.item,
					referent.tag.clone(),
					true,
					visited,
				))
				.await?;
				let dependency = FileDependency::Referent {
					reference,
					referent: tg::Referent {
						item: Some(Either::Right(index)),
						path: referent.path,
						subpath: referent.subpath,
						tag: referent.tag,
					},
				};
				dependencies.push(dependency);
			}
		}
		state.graph.nodes[index]
			.variant
			.unwrap_file_mut()
			.dependencies = dependencies;
		Ok(())
	}

	async fn unify_visit_symlink_edges(
		&self,
		_state: &mut State,
		_index: usize,
		_symlink: &tg::Symlink,
		_visited: &mut BTreeMap<tg::object::Id, usize>,
	) -> tg::Result<()> {
		Ok(())
	}
}

impl Server {
	fn fix_unification_subpaths(state: &mut super::State) {
		for index in 0..state.graph.nodes.len() {
			// Skip nodes that are not files.
			if !state.graph.nodes[index].variant.is_file() {
				continue;
			}

			// Make sure any imports of packages contain the subpath to that package's root module.
			let mut dependencies = state.graph.nodes[index]
				.variant
				.unwrap_file_ref()
				.dependencies
				.clone();
			for dependency in &mut dependencies {
				match dependency {
					FileDependency::Import {
						subpath,
						node: Some(node),
						..
					} if subpath.is_none() => {
						if let Some(root_module) = state.graph.nodes[*node].root_module_name() {
							subpath.replace(root_module.into());
						}
					},
					FileDependency::Referent {
						referent:
							tg::Referent {
								item: Some(Either::Right(node)),
								subpath,
								..
							},
						..
					} if subpath.is_none() => {
						if let Some(root_module) = state.graph.nodes[*node].root_module_name() {
							subpath.replace(root_module.into());
						}
					},
					_ => (),
				}
			}
			state.graph.nodes[index]
				.variant
				.unwrap_file_mut()
				.dependencies = dependencies;
		}
	}
}

impl super::Graph {
	fn fmt_node(&self, index: usize) -> String {
		let node = &self.nodes[index];
		if let Some(path) = node.path.as_deref() {
			path.display().to_string()
		} else if let Some(tag) = node.tag.as_ref() {
			tag.to_string()
		} else {
			index.to_string()
		}
	}

	fn unresolved(&self, node: usize) -> im::Vector<Unresolved> {
		match &self.nodes[node].variant {
			super::Variant::Directory(directory) => directory
				.entries
				.iter()
				.map(|(name, dst)| Unresolved {
					src: node,
					dst: Some(*dst),
					reference: tg::Reference::with_path(name),
				})
				.collect(),
			super::Variant::File(file) => file
				.dependencies
				.iter()
				.map(|dep| {
					let (reference, dst) = match dep {
						FileDependency::Import { import, node, .. } => {
							(import.reference.clone(), *node)
						},
						FileDependency::Referent {
							reference,
							referent,
						} => {
							let dst = referent
								.item
								.as_ref()
								.and_then(|item| item.as_ref().right().copied());
							(reference.clone(), dst)
						},
					};
					Unresolved {
						src: node,
						dst,
						reference,
					}
				})
				.collect(),
			super::Variant::Symlink(_) | super::Variant::Object => im::Vector::new(),
		}
	}
}

fn try_backtrack(state: &mut Vec<State>, edge: &Unresolved) -> Option<State> {
	// Go back to where the reference was originally solved.
	let package = edge.reference.name()?;
	let position = state
		.iter()
		.position(|state| state.packages.contains_key(package))?;
	state.truncate(position);
	state.pop()
}
