use super::Graph;
use crate::{Server, lock::Lock};
use std::{
	collections::BTreeMap,
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_either::Either;

#[derive(Clone, Debug)]
struct State<'a> {
	arg: &'a tg::checkin::Arg,
	artifacts_path: &'a Path,
	lock: Option<&'a Lock>,
	packages: im::HashMap<String, tg::Referent<usize>>,
	errored: bool,
	graph: Graph,
	queue: im::Vector<Unresolved>,
	candidates: Option<im::Vector<Candidate>>,
	visited: im::HashSet<Unresolved>,
}

#[derive(Clone, Debug)]
pub struct Candidate {
	lock_node: Option<usize>,
	object: tg::Object,
	path: Option<PathBuf>,
	tag: tg::Tag,
	unify: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Unresolved {
	reference: tg::Reference,
	src: usize,
	dst: Option<usize>,
}

impl Server {
	pub(super) async fn checkin_solve(&self, state: &mut super::State) -> tg::Result<()> {
		// Copy the current state of the graph.
		let graph = state.graph.clone();

		// Queue up a list of edges to solve, from the root.
		let queue = graph.unresolved(0);

		// Create the current state.
		let mut current = State {
			arg: &state.arg,
			artifacts_path: state.artifacts_path.as_ref(),
			lock: state.lock.as_ref(),
			packages: im::HashMap::new(),
			errored: false,
			graph,
			queue,
			candidates: None,
			visited: im::HashSet::new(),
		};

		// Create the checkpoint history.
		let mut checkpoints = Vec::new();

		// Solve.
		while let Some(unresolved) = current.queue.pop_front() {
			self.walk_edge(&mut checkpoints, &mut current, &state.progress, unresolved)
				.await;
			current.candidates.take();
		}

		// Validate.
		if current.errored {
			return Err(tg::error!("failed to solve dependencies"));
		}

		// Update state.
		state.graph = current.graph;

		Ok(())
	}

	async fn walk_edge<'a>(
		&self,
		checkpoints: &mut Vec<State<'a>>,
		current: &mut State<'a>,
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
			.resolve_reference(
				checkpoints,
				current,
				unresolved.src,
				&unresolved.reference,
				progress,
			)
			.await
		{
			Ok(resolved) => {
				// Add the new node's unresolved edges to the queue.
				if let Either::Right(node) = &resolved.item {
					current.queue.append(current.graph.unresolved(*node));
				}
				// Update the file's dependencies.
				current.graph.nodes[unresolved.src]
					.variant
					.unwrap_file_mut()
					.dependencies
					.iter_mut()
					.find_map(|(reference, referent)| {
						(reference == &unresolved.reference).then_some(referent)
					})
					.unwrap()
					.replace(resolved);
			},
			Err(error) => {
				// If there was an error, attempt to backtrack.
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

	async fn resolve_reference<'a>(
		&self,
		checkpoints: &mut Vec<State<'a>>,
		current: &mut State<'a>,
		referrer: usize,
		reference: &tg::Reference,
		_progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<tg::Referent<Either<tg::object::Id, usize>>> {
		// Bail if the item is a path.
		if reference.item().try_unwrap_path_ref().is_ok() {
			return Err(tg::error!(%reference, "unexpected path reference"));
		}

		// Resolve objects directly.
		if reference.item().try_unwrap_object_ref().is_ok() {
			// If the reference does not name a tag or path, try to resolve it directly.
			let referent = reference.get(self).await?;
			let object = referent
				.item
				.as_ref()
				.right()
				.ok_or_else(|| tg::error!("expected an object"))?;
			let node = self
				.checkin_solve_visit_object(current, object, None, None, false)
				.await?;
			return Ok(tg::Referent::with_item(Either::Right(node)));
		}

		// Get the pattern.
		let pattern = reference
			.item()
			.try_unwrap_tag_ref()
			.map_err(|_| tg::error!("expected a tag pattern"))?;

		// Initialize the set of objects to use.
		if current.candidates.is_none() {
			// Check if there is already a result.
			if let Some(referent) = current.packages.get(pattern.name()) {
				let tag = referent.tag().unwrap();
				if pattern.matches(tag) {
					return Ok(referent.clone().map(Either::Right));
				}
				return Err(tg::error!(%tag, %pattern, "incompatible versions"));
			}

			let remote = reference.options().remote.clone();

			// List tags that match the pattern, if not locked.
			let mut candidates: im::Vector<_> = if current.arg.locked {
				im::Vector::new()
			} else {
				self.list_tags(tg::tag::list::Arg {
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
					Some(Candidate {
						object: tg::Object::with_id(object),
						lock_node: None,
						tag: output.tag,
						path: None,
						unify: true,
					})
				})
				.collect()
			};

			// If there is a solution in the lock already, but it doesn't match the list of updates, give it the highest precedence
			if let Some(candidate) = current.graph.nodes[referrer]
				.lock_index
				.and_then(|node| {
					current.lock.unwrap().nodes[node]
						.try_unwrap_file_ref()
						.ok()?
						.dependencies
						.get(reference)
				})
				.and_then(|referent| {
					let lock_node = referent
						.item
						.try_unwrap_reference_ref()
						.ok()
						.map(|reference| reference.node);
					let version = referent.tag().cloned()?;

					// Skip the lock version if any updates have been requested.
					if current
						.arg
						.updates
						.iter()
						.any(|pattern| pattern.matches(&version))
					{
						return None;
					}

					let (object, unify) = match referent.item() {
						tg::graph::data::Edge::Reference(reference) => {
							let object = current.lock.unwrap().objects[reference.node].clone()?;
							(object, true)
						},
						tg::graph::data::Edge::Object(object) => {
							// If the object is referred to by ID within the lock, do not attempt to unify it.
							let object = tg::Object::with_id(object.clone());
							(object, false)
						},
					};
					Some(Candidate {
						lock_node,
						object,
						tag: version,
						path: referent.path().cloned(),
						unify,
					})
				}) {
				candidates.push_back(candidate);
			}

			current.candidates.replace(candidates);
		}

		// Pick the next object.
		let candidate = current
			.candidates
			.as_mut()
			.unwrap()
			.pop_back()
			.ok_or_else(|| tg::error!(%reference, "tag does not exist"))?;

		// Create the node.
		let node = self
			.checkin_solve_visit_object(
				current,
				&candidate.object,
				candidate.lock_node,
				Some(candidate.tag.clone()),
				candidate.unify,
			)
			.await?;

		let referent = tg::Referent {
			item: node,
			options: tg::referent::Options {
				path: candidate.path.clone(),
				tag: Some(candidate.tag.clone()),
			},
		};

		// Update the list of solved packages.
		current
			.packages
			.insert(candidate.tag.name().to_owned(), referent.clone());

		// Checkpoint.
		checkpoints.push(current.clone());

		Ok(referent.map(Either::Right))
	}

	async fn checkin_solve_visit_object(
		&self,
		state: &mut State<'_>,
		object: &tg::Object,
		lock_node: Option<usize>,
		tag: Option<tg::Tag>,
		unify: bool,
	) -> tg::Result<usize> {
		let mut visited = BTreeMap::new();
		self.checkin_solve_visit_object_inner(
			state,
			object,
			lock_node,
			None,
			None,
			tag,
			unify,
			&mut visited,
		)
		.await
	}

	#[allow(clippy::too_many_arguments)]
	async fn checkin_solve_visit_object_inner(
		&self,
		state: &mut State<'_>,
		object: &tg::Object,
		lock_node: Option<usize>,
		root: Option<usize>,
		subpath: Option<PathBuf>,
		tag: Option<tg::Tag>,
		unify: bool,
		visited: &mut BTreeMap<tg::object::Id, usize>,
	) -> tg::Result<usize> {
		let id = object.id();

		// Skip if already visited.
		if let Some(id) = visited.get(&id) {
			return Ok(*id);
		}

		// Get the object data.
		let object_ = if unify
			&& matches!(
				&object,
				tg::Object::Directory(_) | tg::Object::File(_) | tg::Object::Symlink(_)
			) {
			let data = object.data(self).await?;
			let bytes = data.serialize()?;
			super::Object {
				id: id.clone(),
				bytes: Some(bytes),
				data: Some(data),
			}
		} else {
			super::Object {
				id: id.clone(),
				bytes: None,
				data: None,
			}
		};

		// Create the variant.
		let variant = match &object {
			tg::Object::Directory(_) if unify => super::Variant::Directory(super::Directory {
				entries: Vec::new(),
			}),
			tg::Object::File(file) if unify => {
				let executable = file.executable(self).await?;
				let blob = file.contents(self).await?.id();
				super::Variant::File(super::File {
					executable,
					blob: Some(super::Blob::Id(blob)),
					dependencies: Vec::new(),
				})
			},
			tg::Object::Symlink(symlink) if unify => {
				let artifact = symlink
					.artifact(self)
					.await?
					.map(|artifact| Either::Left(artifact.id()));
				let path = symlink.path(self).await?;
				super::Variant::Symlink(super::Symlink { artifact, path })
			},
			_ => super::Variant::Object,
		};

		// Check if we have this in the artifacts directory.
		let path = state.artifacts_path.join(id.to_string());
		let path = tokio::fs::try_exists(&path)
			.await
			.is_ok_and(|exists| exists)
			.then_some(path);

		// Use the ID if the path exists and --locked is true, which will be used to later verify that the object we're checking in is not corrupted.
		let id_ = (state.arg.locked && path.is_some()).then(|| id.clone());

		// Create the node.
		let node = super::Node {
			lock_index: lock_node,
			id: id_,
			metadata: None,
			object: Some(object_),
			tag: tag.clone(),
			path: None,
			parent: None,
			root,
			variant,
		};

		// Add the node to the graph.
		let index = state.graph.nodes.len();
		state.graph.nodes.push_back(node);
		visited.insert(id.clone(), index);

		if let Some(root) = root {
			state.graph.roots.entry(root).or_default().push(index);
		}
		let root = root.unwrap_or(index);

		// Recurse.
		if unify {
			match object {
				tg::Object::Directory(directory) => {
					self.checkin_solve_visit_directory(
						state, lock_node, root, subpath, index, directory, visited,
					)
					.await?;
				},
				tg::Object::File(file) => {
					self.checkin_solve_visit_file(state, lock_node, root, index, file, visited)
						.await?;
				},
				tg::Object::Symlink(symlink) => {
					self.checkin_solve_visit_symlink(
						state, lock_node, root, index, symlink, visited,
					)
					.await?;
				},
				_ => (),
			}
		}

		// Return the index.
		Ok(index)
	}

	#[allow(clippy::too_many_arguments)]
	async fn checkin_solve_visit_directory(
		&self,
		state: &mut State<'_>,
		lock_node: Option<usize>,
		root: usize,
		subpath: Option<PathBuf>,
		index: usize,
		directory: &tg::Directory,
		visited: &mut BTreeMap<tg::object::Id, usize>,
	) -> tg::Result<()> {
		let mut entries = Vec::new();
		for (name, object) in directory.entries(self).await? {
			let subpath = subpath
				.as_ref()
				.map_or_else(|| name.as_str().into(), |subpath| subpath.join(&name));
			let unify = lock_node.is_none_or(|node| {
				let Ok(directory) = state.lock.unwrap().nodes[node].try_unwrap_directory_ref()
				else {
					return true;
				};
				let Some(edge) = directory.entries.get(&name) else {
					return true;
				};
				edge.is_reference()
			});
			let lock_node = lock_node.and_then(|node| {
				state.lock.unwrap().nodes[node]
					.try_unwrap_directory_ref()
					.ok()?
					.entries
					.get(&name)?
					.try_unwrap_reference_ref()
					.ok()
					.map(|reference| reference.node)
			});
			let child_index = Box::pin(self.checkin_solve_visit_object_inner(
				state,
				&object.into(),
				lock_node,
				Some(root),
				Some(subpath),
				None,
				unify,
				visited,
			))
			.await?;
			state.graph.nodes[child_index].parent.replace(index);
			entries.push((name, child_index));
		}
		state.graph.nodes[index]
			.variant
			.unwrap_directory_mut()
			.entries = entries;
		Ok(())
	}

	async fn checkin_solve_visit_file(
		&self,
		state: &mut State<'_>,
		lock_node: Option<usize>,
		root: usize,
		index: usize,
		file: &tg::File,
		visited: &mut BTreeMap<tg::object::Id, usize>,
	) -> tg::Result<()> {
		let mut dependencies = Vec::new();
		for (reference, referent) in file.dependencies(self).await? {
			let unify = lock_node.is_none_or(|node| {
				let Ok(file) = state.lock.unwrap().nodes[node].try_unwrap_file_ref() else {
					return true;
				};
				let Some(referent) = file.dependencies.get(&reference) else {
					return true;
				};
				referent.item.is_reference()
			});
			let lock_node = lock_node.and_then(|node| {
				state.lock.unwrap().nodes[node]
					.try_unwrap_file_ref()
					.ok()?
					.dependencies
					.get(&reference)?
					.item
					.try_unwrap_reference_ref()
					.ok()
					.map(|reference| reference.node)
			});

			// Leave tag references as unresolved.
			if reference.item().try_unwrap_tag_ref().is_ok() {
				dependencies.push((reference, None));
			} else {
				let index = Box::pin(self.checkin_solve_visit_object_inner(
					state,
					referent.item(),
					lock_node,
					Some(root),
					referent.path().cloned(),
					None,
					unify,
					visited,
				))
				.await?;
				let referent = tg::Referent {
					item: Either::Right(index),
					options: tg::referent::Options {
						path: referent.path().cloned(),
						tag: None,
					},
				};
				dependencies.push((reference, Some(referent)));
			}
		}
		state.graph.nodes[index]
			.variant
			.unwrap_file_mut()
			.dependencies = dependencies;
		Ok(())
	}

	async fn checkin_solve_visit_symlink(
		&self,
		state: &mut State<'_>,
		lock_node: Option<usize>,
		root: usize,
		index: usize,
		_symlink: &tg::Symlink,
		visited: &mut BTreeMap<tg::object::Id, usize>,
	) -> tg::Result<()> {
		if let Some(artifact) = state.graph.nodes[index]
			.variant
			.try_unwrap_symlink_ref()
			.unwrap()
			.artifact
			.as_ref()
			.and_then(|artifact| artifact.as_ref().left())
		{
			let unify = lock_node.is_none_or(|node| {
				let Ok(symlink) = state.lock.unwrap().nodes[node].try_unwrap_symlink_ref() else {
					return true;
				};
				let tg::graph::data::Symlink {
					artifact: Some(artifact),
					..
				} = symlink
				else {
					return true;
				};
				artifact.is_reference()
			});
			let lock_node = lock_node.and_then(|node| {
				let symlink = state.lock.unwrap().nodes[node]
					.try_unwrap_symlink_ref()
					.ok()?;
				let tg::graph::data::Symlink {
					artifact:
						Some(tg::graph::data::Edge::Reference(tg::graph::data::Reference {
							node, ..
						})),
					..
				} = symlink
				else {
					return None;
				};
				Some(*node)
			});
			let path = state.graph.nodes[index]
				.variant
				.unwrap_symlink_ref()
				.path
				.clone();
			let node = Box::pin(self.checkin_solve_visit_object_inner(
				state,
				&tg::Object::with_id(artifact.clone().into()),
				lock_node,
				Some(root),
				path,
				None,
				unify,
				visited,
			))
			.await?;
			state.graph.nodes[index]
				.variant
				.unwrap_symlink_mut()
				.artifact
				.replace(Either::Right(node));
		}
		Ok(())
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
				.map(|(reference, referent)| {
					let dst = referent
						.as_ref()
						.and_then(|referent| referent.item.as_ref().right().copied());
					Unresolved {
						src: node,
						dst,
						reference: reference.clone(),
					}
				})
				.collect(),
			super::Variant::Symlink(_) | super::Variant::Object => im::Vector::new(),
		}
	}
}

fn try_backtrack<'a>(state: &mut Vec<State<'a>>, edge: &Unresolved) -> Option<State<'a>> {
	// Go back to where the reference was originally solved.
	let package = edge.reference.name()?;
	let position = state
		.iter()
		.position(|state| state.packages.contains_key(package))?;
	state.truncate(position);
	state.pop()
}
