use crate::Server;
use either::Either;
use futures::{future, stream::FuturesUnordered, TryStreamExt as _};
use std::{
	collections::{BTreeMap, BTreeSet},
	path::Path,
};
use tangram_client::{self as tg, Handle};

// Mutable state used during the version solving algorithm to cache package metadata and published packages.
struct Context {
	// A cache of get_package output.
	output: BTreeMap<tg::Dependency, tg::package::get::Output>,
}

// A stack frame, used for backtracking. Implicitly, a single stack frame corresponds to a single edge (the top of the edge stack).
#[derive(Clone, Debug)]
struct Frame {
	// The current solution space.
	solution: Solution,

	// The list of remaining edges to solve.
	edges: im::Vector<Edge>,

	// The list of remaining versions for a given edge.
	remaining_versions: Option<im::Vector<String>>,

	// The last error seen, used for error reporting.
	last_error: Option<Error>,
}

// The solution space of the algorithm.
#[derive(Clone, Debug, Default)]
struct Solution {
	// A table of package names to resolved package ids.
	global: im::HashMap<String, Result<tg::artifact::Id, Error>>,

	// A table of visited edges.
	visited: im::HashMap<Edge, Mark>,
}

// An edge in the dependency graph.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Edge {
	artifact: tg::artifact::Id,
	dependency: tg::Dependency,
}

// A Mark is used to detect cyclic dependencies.
#[derive(Clone, Debug)]
enum Mark {
	// A preliminary guess at what package may resolve an edge.
	Temporary(tg::artifact::Id),

	// A permanent solution for an edge, either success or an error.
	Permanent(Result<tg::artifact::Id, Error>),
}

/// An error type that can be pretty printed to describe why version solving failed.
struct Report {
	// List of errors and the corresponding Edge structures they correspond to.
	errors: Vec<(Edge, Error)>,

	// The context, used for formatting.
	context: Context,

	// The solution structure.
	solution: Solution,
}

/// Errors that may arise during version solving.
#[derive(Clone, Debug)]
enum Error {
	/// No version could be found that satisfies all constraints.
	PackageVersionConflict,

	/// A package cycle exists.
	PackageCycleExists {
		/// The terminal edge of a cycle in the dependency graph.
		edge: Edge,
	},

	/// A nested error that arises during backtracking.
	Backtrack {
		/// The package that we backtracked from.
		artifact: tg::artifact::Id,

		/// The version that was tried previously and failed.
		previous_version: String,

		/// A list of dependencies of `previous_version` that caused an error.
		erroneous_dependencies: Vec<(tg::Dependency, Error)>,
	},

	/// A tangram error.
	Other(tg::error::Error),
}

impl Server {
	pub(crate) async fn get_or_create_lock(
		&self,
		artifact: &tg::artifact::Id,
		path: Option<&tg::Path>,
		dependencies: &BTreeMap<tg::Dependency, Option<tg::package::get::Output>>,
		locked: bool,
	) -> tg::Result<tg::Lock> {
		// If this is a path dependency, then attempt to read the lock from the lockfile.
		let lock = if let Some(path) = path {
			self.try_read_lockfile(path).await?
		} else {
			None
		};

		// Add the path dependencies to the lock from the lockfile.
		let lock = 'a: {
			let Some(lock) = lock else {
				break 'a None;
			};
			let Some(lock) = self
				.try_add_path_dependencies_to_lock(dependencies, lock.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to add path dependencies to lock"))?
			else {
				break 'a None;
			};
			Some(lock)
		};

		// Verify that the dependencies match the lock.
		let lock = 'a: {
			let Some(lock) = lock else {
				break 'a None;
			};
			if !self.dependencies_match_lock(dependencies, &lock).await? {
				break 'a None;
			}
			Some(lock)
		};

		// If `locked` is true, then require the lock to exist and match.
		if locked {
			return lock.ok_or_else(|| tg::error!("missing lockfile"));
		}

		// Otherwise, create the lock.
		let mut created = false;
		let lock = if let Some(lock) = lock {
			lock
		} else {
			created = true;
			self.create_package_lock(artifact, dependencies).await?
		};

		// Write the lock without the resolved path dependencies to the lockfile.
		if let Some(path) = path {
			if created {
				let lock = self
					.remove_path_dependencies_from_lock(dependencies, &lock)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to remove path dependencies from lock")
					})?;
				self.write_lockfile(&lock, path.clone()).await?;
			}
		}

		// Normalize the lock.
		let lock = lock
			.normalize(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to normalize the lock"))?;

		Ok(lock)
	}

	pub async fn try_read_lockfile(&self, path: impl AsRef<Path>) -> tg::Result<Option<tg::Lock>> {
		let path = path.as_ref().join(tg::package::LOCKFILE_FILE_NAME);
		let bytes = match tokio::fs::read(&path).await {
			Ok(bytes) => bytes,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(error) => {
				let path = path.display();
				return Err(tg::error!(source = error, %path, "failed to read the lockfile"));
			},
		};
		let data: tg::lock::Data = serde_json::from_slice(&bytes).map_err(|error| {
			let path = path.display();
			tg::error!(source = error, %path, "failed to deserialize the lockfile")
		})?;
		let object: tg::lock::Object = data.try_into()?;
		let lock = tg::Lock::with_object(object);
		Ok(Some(lock))
	}

	async fn write_lockfile(&self, lock: &tg::Lock, path: impl AsRef<Path>) -> tg::Result<()> {
		let path = path.as_ref().join(tg::package::LOCKFILE_FILE_NAME);
		let data = lock.data(self).await?;
		let bytes = serde_json::to_vec_pretty(&data)
			.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
		tokio::fs::write(&path, &bytes).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to write the lock file"),
		)?;
		Ok(())
	}

	async fn dependencies_match_lock(
		&self,
		dependencies: &BTreeMap<tg::Dependency, Option<tg::package::get::Output>>,
		lock: &tg::Lock,
	) -> tg::Result<bool> {
		// Verify that the dependencies match.
		if !itertools::equal(dependencies.keys(), lock.dependencies(self).await?.iter()) {
			return Ok(false);
		}

		// Recurse.
		dependencies
			.iter()
			.filter_map(|(dependency, output)| {
				let dependencies = output
					.as_ref()
					.and_then(|output| output.dependencies.as_ref())?;
				Some((dependency, dependencies))
			})
			.map(|(dependency, dependencies)| async {
				let (_, lock) = lock.get(self, dependency).await?;
				self.dependencies_match_lock(dependencies, &lock).await
			})
			.collect::<FuturesUnordered<_>>()
			.try_all(future::ready)
			.await?;

		Ok(true)
	}

	pub(super) async fn create_package_lock(
		&self,
		artifact: &tg::artifact::Id,
		dependencies: &BTreeMap<tg::Dependency, Option<tg::package::get::Output>>,
	) -> tg::Result<tg::Lock> {
		// Construct the version solving context and initial set of edges.
		let (output, edges) = Self::flatten_output(artifact, dependencies);

		// Solve.
		let mut context = Context { output };
		let solution = self.solve(&mut context, edges.into()).await?;

		// Collect the errors.
		let errors = solution
			.visited
			.iter()
			.filter_map(|(edge, partial)| match partial {
				Mark::Permanent(Err(e)) => Some((edge.clone(), e.clone())),
				_ => None,
			})
			.collect::<Vec<_>>();

		// If the report is not empty, then return an error.
		if !errors.is_empty() {
			let report = Report {
				errors,
				context,
				solution,
			};
			return Err(tg::error!("{report}"));
		}

		// Create the lock.
		let mut nodes = Vec::new();
		let root = self
			.create_package_lock_inner(artifact, &context, &solution, &mut nodes)
			.await?;
		let nodes = nodes
			.into_iter()
			.map(|node| {
				let dependencies = node
					.dependencies
					.into_iter()
					.map(|(dependency, entry)| {
						let artifact = entry.artifact.map(tg::Artifact::with_id);
						let lock = entry.lock.map_right(tg::Lock::with_id);
						let entry = tg::lock::Entry { artifact, lock };
						(dependency, entry)
					})
					.collect();
				tg::lock::Node { dependencies }
			})
			.collect();
		let object = tg::lock::Object { root, nodes };
		let lock = tg::Lock::with_object(object);

		Ok(lock)
	}

	async fn create_package_lock_inner(
		&self,
		artifact: &tg::artifact::Id,
		context: &Context,
		solution: &Solution,
		nodes: &mut Vec<tg::lock::data::Node>,
	) -> tg::Result<usize> {
		// Get the cached analysis.
		let key = tg::Dependency::with_artifact(artifact.clone());
		let output = context
			.output
			.get(&key)
			.ok_or_else(|| tg::error!("missing package in solution"))?;

		// Recursively create the nodes.
		let mut dependencies = BTreeMap::new();
		for (dependency, output) in output.dependencies.as_ref().unwrap() {
			// Check if there is an existing solution in the output.
			let artifact = if let Some(output) = output {
				output.artifact.clone()
			} else {
				// Resolve by edge.
				let edge = Edge {
					artifact: artifact.clone(),
					dependency: dependency.clone(),
				};
				let Some(Mark::Permanent(Ok(artifact))) = solution.visited.get(&edge) else {
					return Err(tg::error!(?edge, "missing solution"));
				};
				artifact.clone()
			};
			let lock = Either::Left(
				Box::pin(self.create_package_lock_inner(&artifact, context, solution, nodes))
					.await?,
			);
			let entry = tg::lock::data::Entry {
				artifact: Some(artifact.clone()),
				lock,
			};
			dependencies.insert(dependency.clone(), entry);
		}

		// Insert the node if it doesn't exist.
		let node = tg::lock::data::Node { dependencies };
		let index = if let Some(index) = nodes.iter().position(|l| l == &node) {
			index
		} else {
			nodes.push(node);
			nodes.len() - 1
		};

		Ok(index)
	}

	fn flatten_output(
		root: &tg::artifact::Id,
		dependencies: &BTreeMap<tg::Dependency, Option<tg::package::get::Output>>,
	) -> (
		BTreeMap<tg::Dependency, tg::package::get::Output>,
		Vec<Edge>,
	) {
		let mut stack = vec![(root, dependencies)];
		let mut output = BTreeMap::new();
		let mut edges = Vec::new();

		// Walk the dependencies.
		while let Some((artifact, dependencies)) = stack.pop() {
			let key = tg::Dependency::with_artifact(artifact.clone());
			if output.contains_key(&key) {
				continue;
			}
			let output_ = tg::package::get::Output {
				artifact: artifact.clone(),
				dependencies: Some(dependencies.clone()),
				lock: None,
				metadata: None,
				path: None,
				yanked: None,
			};
			output.insert(key, output_);
			for (dependency, output_) in dependencies {
				edges.push(Edge {
					artifact: artifact.clone(),
					dependency: dependency.clone(),
				});
				let Some(output_) = output_.as_ref() else {
					continue;
				};
				let Some(dependencies_) = output_.dependencies.as_ref() else {
					continue;
				};
				stack.push((artifact, dependencies_));
			}
		}
		(output, edges)
	}

	pub(super) async fn try_add_path_dependencies_to_lock(
		&self,
		dependencies: &BTreeMap<tg::Dependency, Option<tg::package::get::Output>>,
		lock: tg::Lock,
	) -> tg::Result<Option<tg::Lock>> {
		let mut object = lock.object(self).await?.as_ref().clone();
		let mut stack = vec![(object.root, dependencies)];
		let mut visited = BTreeSet::new();

		while let Some((index, dependencies)) = stack.pop() {
			if visited.contains(&index) {
				continue;
			}
			visited.insert(index);
			let node = &mut object.nodes[index];
			for (dependency, output) in dependencies {
				let Some(entry) = node.dependencies.get_mut(dependency) else {
					return Ok(None);
				};
				let Some(artifact) = output.as_ref().map(|output| output.artifact.clone()) else {
					continue;
				};
				let artifact = tg::Artifact::with_id(artifact);
				entry.artifact.replace(artifact);
				let Some(dependencies) = output
					.as_ref()
					.and_then(|output| output.dependencies.as_ref())
				else {
					continue;
				};
				match &mut entry.lock {
					Either::Left(index) => stack.push((*index, dependencies)),
					Either::Right(lock) => {
						*lock = Box::pin(
							self.try_add_path_dependencies_to_lock(dependencies, lock.clone()),
						)
						.await?
						.unwrap();
					},
				}
			}
		}
		Ok(Some(tg::Lock::with_object(object)))
	}

	async fn remove_path_dependencies_from_lock(
		&self,
		dependencies: &BTreeMap<tg::Dependency, Option<tg::package::get::Output>>,
		lock: &tg::Lock,
	) -> tg::Result<tg::Lock> {
		let mut object = lock.object(self).await?.as_ref().clone();
		let mut stack = vec![(object.root, dependencies)];
		let mut visited = BTreeSet::new();
		while let Some((index, dependencies)) = stack.pop() {
			if visited.contains(&index) {
				continue;
			}
			visited.insert(index);
			let node = &mut object.nodes[index];
			for (dependency, output) in dependencies {
				if dependency.path.is_none() {
					continue;
				}
				let entry = node
					.dependencies
					.get_mut(dependency)
					.ok_or_else(|| tg::error!("invalid lock file"))?;
				entry.artifact.take();
				let Some(dependencies) = output
					.as_ref()
					.and_then(|output| output.dependencies.as_ref())
				else {
					continue;
				};
				match &mut entry.lock {
					Either::Left(index) => stack.push((*index, dependencies)),
					Either::Right(lock) => {
						*lock =
							Box::pin(self.remove_path_dependencies_from_lock(dependencies, lock))
								.await?;
					},
				}
			}
		}
		Ok(tg::Lock::with_object(object))
	}

	async fn solve(&self, context: &mut Context, edges: im::Vector<Edge>) -> tg::Result<Solution> {
		// Create the first stack frame.
		let solution = Solution::default();
		let last_error = None;
		let remaining_versions = None;
		let mut current_frame = Frame {
			solution,
			edges,
			remaining_versions,
			last_error,
		};
		let mut history = im::Vector::new();

		// The main driver loop operates on the current stack frame, and iteratively tries to build up the next stack frame.
		while let Some((new_edges, edge)) = current_frame.next_edge() {
			let mut next_frame = Frame {
				edges: new_edges,
				solution: current_frame.solution.clone(),
				remaining_versions: None,
				last_error: None,
			};

			// Get the global solution and mark of this edge.
			let global = current_frame.solution.get_global(context, &edge);
			let mark = current_frame.solution.visited.get(&edge);

			match (global, mark) {
				// Case 0: There is no solution for this package yet.
				(None, None) => {
					// Try and resolve the dependency.
					match context
						.resolve_dependency(self, &edge, &mut current_frame.remaining_versions)
						.await
					{
						// We successfully got a version.
						Ok(artifact) => {
							next_frame.solution = next_frame
								.solution
								.mark_temporarily(edge.clone(), artifact.clone());

							// Add this dependency to the top of the stack before adding all its dependencies.
							next_frame.edges.push_back(edge.clone());

							// Add all the dependencies to the stack.
							for child_dependency in context.get_dependencies(&artifact) {
								let edge = Edge {
									artifact: artifact.clone(),
									dependency: child_dependency.clone(),
								};
								next_frame.edges.push_back(edge);
							}

							// Update the stack. If we backtrack, we use the next version in the version stack.
							history.push_back(current_frame.clone());
						},
						// Something went wrong. Mark permanently as an error.
						Err(e) => {
							next_frame.solution =
								next_frame.solution.mark_permanently(context, &edge, Err(e));
						},
					}
				},

				// Case 1: There exists a global version for the package but we haven't solved this dependency constraint.
				(Some(global), None) => {
					match global {
						// Case 1.1: The happy path. Our version is solved and it matches this constraint.
						Ok(artifact) => {
							// Check if the version constraint matches the existing solution.
							let version = context.get_version(artifact);
							match edge.dependency.try_match_version(&version) {
								// Success: we can use this version.
								Ok(true) => {
									next_frame.solution = next_frame.solution.mark_permanently(
										context,
										&edge,
										Ok(artifact.clone()),
									);
								},
								// Failure: we need to attempt to backtrack.
								Ok(false) => {
									let error = Error::PackageVersionConflict;
									if let Some(frame_) = try_backtrack(
										&history,
										edge.dependency.name.as_ref().unwrap(),
										error.clone(),
									) {
										next_frame = frame_;
									} else {
										// There is no solution for this package. Add an error.
										next_frame.solution = next_frame.solution.mark_permanently(
											context,
											&edge,
											Err(error),
										);
									}
								},
								// This can only occur if the there is an error parsing the version constraint of this dependency. It cannot be solved, so mark permanently as an error.
								Err(e) => {
									next_frame.solution = next_frame.solution.mark_permanently(
										context,
										&edge,
										Err(Error::Other(e)),
									);
								},
							}
						},
						// Case 1.2: The less happy path. We know there's no solution to this package because we've already failed to satisfy some other set of constraints.
						Err(e) => {
							next_frame.solution = next_frame.solution.mark_permanently(
								context,
								&edge,
								Err(e.clone()),
							);
						},
					}
				},

				// Case 2: We only have a partial solution for this dependency and need to make sure we didn't create a cycle.
				(_, Some(Mark::Temporary(artifact))) => {
					let dependencies = context.get_dependencies(artifact);
					let mut erroneous_children = vec![];
					for child_dependency in dependencies {
						let child_edge = Edge {
							artifact: artifact.clone(),
							dependency: child_dependency.clone(),
						};

						let child = next_frame.solution.visited.get(&child_edge).unwrap();
						match child {
							// The child dependency has been solved.
							Mark::Permanent(Ok(_)) => (),

							// The child dependency has been solved, but it is an error.
							Mark::Permanent(Err(e)) => {
								let error = e.clone();
								erroneous_children.push((child_edge.dependency, error));
							},

							// The child dependency has not been solved.
							Mark::Temporary(_version) => {
								// Uh oh. We've detected a cycle. First try and backtrack. If backtracking fails, bail out.
								let error = Error::PackageCycleExists { edge: edge.clone() };
								erroneous_children.push((child_edge.dependency, error));
							},
						}
					}

					// If none of the children contain errors, we mark this edge permanently.
					if erroneous_children.is_empty() {
						next_frame.solution = next_frame.solution.mark_permanently(
							context,
							&edge,
							Ok(artifact.clone()),
						);
					} else {
						let previous_version = context.get_version(artifact);
						let error = Error::Backtrack {
							artifact: artifact.clone(),
							previous_version,
							erroneous_dependencies: erroneous_children,
						};

						if let Some(frame_) = try_backtrack(
							&history,
							edge.dependency.name.as_ref().unwrap(),
							error.clone(),
						) {
							next_frame = frame_;
						} else {
							// This means that backtracking totally failed and we need to fail with an error.
							next_frame.solution =
								next_frame
									.solution
									.mark_permanently(context, &edge, Err(error));
						}
					}
				},

				// Case 3: We've already solved this dependency. Continue.
				(_, Some(Mark::Permanent(_complete))) => (),
			}

			// Replace the solution and edges if needed.
			current_frame = next_frame;
		}

		Ok(current_frame.solution)
	}
}

impl Context {
	// Checks if the edge can be solved using the output of get_package.
	fn solution_in_output(&self, edge: &Edge) -> bool {
		let key = tg::Dependency::with_artifact(edge.artifact.clone());
		self.output
			.get(&key)
			.unwrap()
			.dependencies
			.as_ref()
			.unwrap()
			.get(&edge.dependency)
			.unwrap()
			.is_some()
	}

	// Get the dependencies of a package.
	fn get_dependencies(
		&self,
		artifact: &tg::artifact::Id,
	) -> impl Iterator<Item = &tg::Dependency> + '_ {
		let key = tg::Dependency::with_artifact(artifact.clone());
		let output = self.output.get(&key).unwrap();
		let dependencies = output.dependencies.as_ref().unwrap();
		dependencies.keys()
	}

	// Get the version of a package.
	fn get_version(&mut self, artifact: &tg::artifact::Id) -> String {
		let key = tg::Dependency::with_artifact(artifact.clone());
		self.output
			.get(&key)
			.unwrap()
			.metadata
			.as_ref()
			.unwrap()
			.version
			.clone()
			.unwrap()
	}
}

impl Context {
	async fn resolve_dependency(
		&mut self,
		server: &Server,
		edge: &Edge,
		remaining_versions: &mut Option<im::Vector<String>>,
	) -> Result<tg::artifact::Id, Error> {
		self.try_resolve_dependency(server, edge, remaining_versions)
			.await?
			.ok_or_else(|| Error::PackageVersionConflict)
	}

	async fn try_resolve_dependency(
		&mut self,
		server: &Server,
		edge: &Edge,
		remaining_versions: &mut Option<im::Vector<String>>,
	) -> Result<Option<tg::artifact::Id>, Error> {
		// Lookup this package in the local cache.
		let key = tg::Dependency::with_artifact(edge.artifact.clone());
		let output = self.output.get(&key).unwrap().clone();

		// Try and resolve the dependency using the existing output.
		if let Some(Some(resolved)) = output.dependencies.as_ref().unwrap().get(&edge.dependency) {
			// Update the cache with this dependency's information.
			let key = tg::Dependency::with_artifact(resolved.artifact.clone());
			self.output.insert(key, resolved.clone());
			let artifact = resolved.artifact.clone();
			return Ok(Some(artifact));
		}

		// Seed the list of versions if necessary.
		if remaining_versions.is_none() {
			let Some(versions) = server
				.try_get_package_versions(&edge.dependency, tg::package::versions::Arg::default())
				.await
				.map_err(|source| {
					Error::Other(
						tg::error!(!source, %dependency = &edge.dependency, "failed to get package versions"),
					)
				})?
			else {
				return Ok(None);
			};
			remaining_versions.replace(versions.into());
		}

		let remaining_versions = remaining_versions.as_mut().unwrap();

		// Get the name and version to try next.
		let name = edge.dependency.name.clone().ok_or_else(|| {
			Error::Other(tg::error!(%dependency = &edge.dependency, "missing dependency name"))
		})?;

		let Some(version) = remaining_versions.pop_back() else {
			return Ok(None);
		};
		let dependency_with_name_and_version =
			tg::Dependency::with_name_and_version(name.clone(), version.clone());

		// Check if there's an existing entry.
		if let Some(output) = self.output.get(&dependency_with_name_and_version) {
			return Ok(Some(output.artifact.clone()));
		}

		// Otherwise get the package.
		let arg = tg::package::get::Arg {
			dependencies: true,
			metadata: true,
			..Default::default()
		};
		let output = Box::pin(server.get_package(&dependency_with_name_and_version, arg))
			.await
			.map_err(Error::Other)?;

		// Update the cached output with the knowledge of this package.
		self.output
			.insert(dependency_with_name_and_version, output.clone());
		let key = tg::Dependency::with_artifact(output.artifact.clone());
		self.output.insert(key, output.clone());

		Ok(Some(output.artifact))
	}
}

fn try_backtrack(history: &im::Vector<Frame>, package_name: &str, error: Error) -> Option<Frame> {
	let index = history
		.iter()
		.take_while(|frame| !frame.solution.contains(package_name))
		.count();
	let mut frame = history.get(index).cloned()?;
	frame.last_error = Some(error);
	Some(frame)
}

impl Solution {
	// If there's an existing solution for this edge, return it. Path dependencies are ignored.
	fn get_global(
		&self,
		_context: &Context,
		edge: &Edge,
	) -> Option<&Result<tg::artifact::Id, Error>> {
		self.global.get(edge.dependency.name.as_ref()?)
	}

	/// Mark this edge with a temporary solution.
	fn mark_temporarily(&self, edge: Edge, artifact: tg::artifact::Id) -> Self {
		let mut solution = self.clone();
		solution.visited.insert(edge, Mark::Temporary(artifact));
		solution
	}

	/// Mark the edge permanently, adding it to the table of known solutions.
	fn mark_permanently(
		&self,
		context: &Context,
		edge: &Edge,
		complete: Result<tg::artifact::Id, Error>,
	) -> Self {
		let mut solution = self.clone();

		// Update the local solution.
		solution
			.visited
			.insert(edge.clone(), Mark::Permanent(complete.clone()));

		// If the solution doesn't appear in the output we add it to the global solution.
		if !context.solution_in_output(edge) {
			solution
				.global
				.insert(edge.dependency.name.clone().unwrap(), complete);
		}

		solution
	}

	fn contains(&self, package_name: &str) -> bool {
		self.global.contains_key(package_name)
	}
}

impl Frame {
	fn next_edge(&self) -> Option<(im::Vector<Edge>, Edge)> {
		let mut edges = self.edges.clone();
		let edge = edges.pop_back()?;
		Some((edges, edge))
	}
}

impl std::fmt::Display for Report {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (edge, error) in &self.errors {
			self.format(f, edge, error)?;
		}
		Ok(())
	}
}

impl Report {
	fn format(
		&self,
		f: &mut std::fmt::Formatter<'_>,
		edge: &Edge,
		error: &Error,
	) -> std::fmt::Result {
		let Edge {
			artifact,
			dependency,
		} = edge;
		let key = tg::Dependency::with_artifact(artifact.clone());
		let metadata = self.context.output.get(&key).unwrap().metadata.as_ref();
		let name = metadata
			.and_then(|metadata| metadata.name.as_deref())
			.unwrap_or("<unknown>");
		let version = metadata
			.and_then(|metadata| metadata.version.as_deref())
			.unwrap_or("<unknown>");
		write!(f, "{name}@{version} requires {dependency}, but ")?;
		match error {
			Error::PackageCycleExists { edge } => {
				let key = tg::Dependency::with_artifact(edge.artifact.clone());
				let metadata = self.context.output.get(&key).unwrap().metadata.as_ref();
				let name = metadata
					.and_then(|metadata| metadata.name.as_deref())
					.unwrap_or("<unknown>");
				let version = metadata
					.and_then(|metadata| metadata.version.as_deref())
					.unwrap_or("<unknown>");
				writeln!(f, "{name}@{version}, which creates a cycle")?;
			},
			Error::PackageVersionConflict => {
				writeln!(
					f,
					"no version could be found that satisfies the constraints"
				)?;
				let shared_edges = self
					.solution
					.visited
					.keys()
					.filter(|edge| edge.dependency.name == dependency.name);
				for shared in shared_edges {
					let Edge {
						artifact,
						dependency,
					} = shared;
					let key = tg::Dependency::with_artifact(artifact.clone());
					let metadata = self.context.output.get(&key).unwrap().metadata.as_ref();
					let name = metadata
						.and_then(|metadata| metadata.name.as_deref())
						.unwrap_or("<unknown>");
					let version = metadata
						.and_then(|metadata| metadata.version.as_deref())
						.unwrap_or("<unknown>");
					writeln!(f, "{name} @ {version} requires {dependency}")?;
				}
			},
			Error::Backtrack {
				artifact,
				previous_version,
				erroneous_dependencies,
			} => {
				let name = dependency.name.as_ref().unwrap();
				writeln!(f, "{name} {previous_version} has errors:")?;
				for (child, error) in erroneous_dependencies {
					let edge = Edge {
						artifact: artifact.clone(),
						dependency: child.clone(),
					};
					self.format(f, &edge, error)?;
				}
			},
			Error::Other(e) => {
				writeln!(f, "{e}")?;
			},
		}
		Ok(())
	}
}
