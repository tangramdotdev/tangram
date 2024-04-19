use super::analysis::Analysis;
use crate::Server;
use either::Either;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use std::collections::{BTreeMap, BTreeSet};
use tangram_client as tg;

// Mutable state used during the version solving algorithm to cache package metadata and published packages.
struct Context {
	// A cache of package analysis (metadata, direct dependencies).
	analysis: BTreeMap<tg::directory::Id, Analysis>,

	// A cache of published packages that we know about.
	published_packages: im::HashMap<tg::Dependency, tg::directory::Id>,
}

// A stack frame, used for backtracking. Implicitly, a single stack frame corresponds to a single dependant (the top of the dependant stack).
#[derive(Clone, Debug)]
struct Frame {
	// The current solution space.
	solution: Solution,

	// The list of remaining dependants to solve.
	dependants: im::Vector<Dependant>,

	// The list of remaining versions for a given dependant.
	remaining_versions: Option<im::Vector<String>>,

	// The last error seen, used for error reporting.
	last_error: Option<Error>,
}

// The solution space of the algorithm.
#[derive(Clone, Debug, Default)]
struct Solution {
	// A table of package names to resolved package ids.
	permanent: im::HashMap<String, Result<tg::directory::Id, Error>>,

	// The partial solution for each individual dependant.
	partial: im::HashMap<Dependant, Mark>,
}

// A dependant represents an edge in the dependency graph.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Dependant {
	package: tg::directory::Id,
	dependency: tg::Dependency,
}

// A Mark is used to detect cyclic dependencies.
#[derive(Clone, Debug)]
enum Mark {
	// A preliminary guess at what package may resolve a dependant.
	Temporary(tg::directory::Id),

	// A permanent solution for a dependant, either success or an error.
	Permanent(Result<tg::directory::Id, Error>),
}

/// An error type that can be pretty printed to describe why version solving failed.
struct Report {
	// List of errors and the corresponding Dependant structures they correspond to.
	errors: Vec<(Dependant, Error)>,

	// The context, used for formatting.
	context: Context,

	// The solution structure.
	solution: Solution,
}

/// Errors that may arise during version solving.
#[derive(Debug, Clone)]
enum Error {
	/// No version could be found that satisfies all constraints.
	PackageVersionConflict,

	/// A package cycle exists.
	PackageCycleExists {
		/// The terminal edge of a cycle in the dependency graph.
		dependant: Dependant,
	},

	/// A nested error that arises during backtracking.
	Backtrack {
		/// The package that we backtracked from.
		package: tg::directory::Id,

		/// The version that was tried previously and failed.
		previous_version: String,

		/// A list of dependencies of `previous_version` that caused an error.
		erroneous_dependencies: Vec<(tg::Dependency, Error)>,
	},

	/// A tangram error.
	Other(tg::error::Error),
}

impl Server {
	pub(super) async fn get_or_create_package_lock(
		&self,
		path: Option<&tg::Path>,
		analysis: &Analysis,
	) -> tg::Result<tg::Lock> {
		// If this is a path dependency, then attempt to read the lock from the lockfile.
		let lock = if let Some(path) = path {
			tg::Lock::try_read(path).await?
		} else {
			None
		};

		// Add the path dependencies to the lock from the lockfile.
		let lock = 'a: {
			let Some(lock) = lock else {
				break 'a None;
			};
			let Some(lock) = self
				.try_add_path_dependencies_to_lock(analysis, lock.clone())
				.await?
			else {
				break 'a None;
			};
			Some(lock)
		};

		// Verify that the lock's dependencies matches the analyzed path dependencies.
		let lock = 'a: {
			let Some(lock) = lock else {
				break 'a None;
			};
			if !self.analysis_matches_lock(analysis, &lock).await? {
				break 'a None;
			}
			Some(lock)
		};

		// Otherwise, create the lock.
		let mut created = false;
		let lock = if let Some(lock) = lock {
			lock
		} else {
			created = true;
			self.create_package_lock(analysis).await?
		};

		// Normalize the lock.
		let lock = lock
			.normalize(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to normalize the lock"))?;

		// Write the lock without the resolved path dependencies to the lockfile.
		if let Some(path) = path {
			if created {
				let lock = self
					.remove_path_dependencies_from_lock(analysis, &lock)
					.await?;
				lock.write(self, path.clone()).await?;
			}
		}
		Ok(lock)
	}

	async fn analysis_matches_lock(
		&self,
		analysis: &Analysis,
		lock: &tg::Lock,
	) -> tg::Result<bool> {
		// Get the dependencies from the package and its lock.
		let deps_in_package = self.get_package_dependencies(&analysis.package).await?;
		let deps_in_lock = lock.dependencies(self).await?;

		// Verify that the dependencies match.
		if !itertools::equal(deps_in_package, deps_in_lock) {
			return Ok(false);
		}

		// Recurse.
		analysis
			.path_dependencies
			.iter()
			.map(|(dependency, analysis)| async {
				let (_, lock) = lock.get(self, dependency).await?;

				self.analysis_matches_lock(analysis, &lock).await
			})
			.collect::<FuturesUnordered<_>>()
			.try_all(|matches| async move { matches })
			.await?;

		Ok(true)
	}

	pub(super) async fn create_package_lock(
		&self,
		root_analysis: &Analysis,
	) -> tg::Result<tg::Lock> {
		// Construct the version solving context and initial set of dependants.
		let (analysis, dependants) = self.collect_analysis(root_analysis).await?;
		let published_packages = im::HashMap::new();
		let mut context = Context {
			analysis,
			published_packages,
		};

		// Solve.
		let solution = self.solve(&mut context, dependants.into()).await?;

		// Collect the errors.
		let errors = solution
			.partial
			.iter()
			.filter_map(|(dependant, partial)| match partial {
				Mark::Permanent(Err(e)) => Some((dependant.clone(), e.clone())),
				_ => None,
			})
			.collect::<Vec<_>>();

		// If the report is not empty, return an error.
		if !errors.is_empty() {
			let report = Report {
				errors,
				context,
				solution,
			};
			return Err(tg::error!("{report}"));
		}

		// Create the lock.
		let root = root_analysis.package.id(self, None).await?;
		let mut nodes = Vec::new();
		let root = self
			.create_package_lock_inner(&root, &context, &solution, &mut nodes)
			.await?;
		let nodes = nodes
			.into_iter()
			.map(|node| {
				let dependencies = node
					.dependencies
					.into_iter()
					.map(|(dependency, entry)| {
						let package = entry.package.map(tg::Directory::with_id);
						let lock = entry.lock.map_right(tg::Lock::with_id);
						let entry = tg::lock::Entry { package, lock };
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
		package: &tg::directory::Id,
		context: &Context,
		solution: &Solution,
		nodes: &mut Vec<tg::lock::data::Node>,
	) -> tg::Result<usize> {
		// Get the cached analysis.
		let analysis = context
			.analysis
			.get(package)
			.ok_or_else(|| tg::error!("missing package in solution"))?;

		// Recursively create the nodes.
		let mut dependencies = BTreeMap::new();
		for dependency in &analysis.dependencies {
			// Check if this is resolved as a path dependency.
			let package = if let Some(package) = analysis
				.path_dependencies
				.get(dependency)
				.map(|p| &p.package)
			{
				package.id(self, None).await?
			} else {
				// Resolve by dependant.
				let dependant = Dependant {
					package: package.clone(),
					dependency: dependency.clone(),
				};
				let Some(Mark::Permanent(Ok(package))) = solution.partial.get(&dependant) else {
					return Err(tg::error!(?dependant, "missing solution"));
				};
				package.clone()
			};
			let lock = Either::Left(
				Box::pin(self.create_package_lock_inner(&package, context, solution, nodes))
					.await?,
			);
			let entry = tg::lock::data::Entry {
				package: Some(package.clone()),
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

	async fn collect_analysis(
		&self,
		analysis: &Analysis,
	) -> tg::Result<(BTreeMap<tg::directory::Id, Analysis>, Vec<Dependant>)> {
		let mut stack = vec![analysis];
		let mut analysis = BTreeMap::new();
		let mut dependants = Vec::new();

		while let Some(analysis_) = stack.pop() {
			let id = analysis_.package.id(self, None).await?;
			if analysis.contains_key(&id) {
				continue;
			}
			analysis.insert(id.clone(), analysis_.clone());
			dependants.extend(analysis_.dependencies.iter().map(|dependency| Dependant {
				package: id.clone(),
				dependency: dependency.clone(),
			}));
			stack.extend(analysis_.path_dependencies.values());
		}

		Ok((analysis, dependants))
	}

	pub(super) async fn try_add_path_dependencies_to_lock(
		&self,
		analysis: &Analysis,
		lock: tg::Lock,
	) -> tg::Result<Option<tg::Lock>> {
		let mut object = lock.object(self).await?.as_ref().clone();
		let mut stack = vec![(object.root, analysis)];
		let mut visited = BTreeSet::new();
		while let Some((index, analysis)) = stack.pop() {
			if visited.contains(&index) {
				continue;
			}
			visited.insert(index);
			let node = &mut object.nodes[index];
			for (dependency, analysis) in &analysis.path_dependencies {
				let Some(entry) = node.dependencies.get_mut(dependency) else {
					return Ok(None);
				};
				entry.package.replace(analysis.package.clone());
				match &mut entry.lock {
					Either::Left(index) => stack.push((*index, analysis)),
					Either::Right(lock) => {
						*lock = if let Some(lock) =
							Box::pin(self.try_add_path_dependencies_to_lock(analysis, lock.clone()))
								.await?
						{
							lock
						} else {
							return Ok(None);
						};
					},
				}
			}
		}
		Ok(Some(tg::Lock::with_object(object)))
	}

	async fn remove_path_dependencies_from_lock(
		&self,
		analysis: &Analysis,
		lock: &tg::Lock,
	) -> tg::Result<tg::Lock> {
		let mut object = lock.object(self).await?.as_ref().clone();
		let mut stack = vec![(object.root, analysis)];
		let mut visited = BTreeSet::new();
		while let Some((index, analysis)) = stack.pop() {
			if visited.contains(&index) {
				continue;
			}
			visited.insert(index);
			let node = &mut object.nodes[index];
			for (dependency, analysis) in &analysis.path_dependencies {
				let entry = node
					.dependencies
					.get_mut(dependency)
					.ok_or_else(|| tg::error!("invalid lock file"))?;
				entry.package.take();
				match &mut entry.lock {
					Either::Left(index) => stack.push((*index, analysis)),
					Either::Right(lock) => {
						*lock = Box::pin(self.remove_path_dependencies_from_lock(analysis, lock))
							.await?;
					},
				}
			}
		}
		Ok(tg::Lock::with_object(object))
	}

	async fn solve(
		&self,
		context: &mut Context,
		dependants: im::Vector<Dependant>,
	) -> tg::Result<Solution> {
		// Create the first stack frame.
		let solution = Solution::default();
		let last_error = None;
		let remaining_versions = None;
		let mut current_frame = Frame {
			solution,
			dependants,
			remaining_versions,
			last_error,
		};
		let mut history = im::Vector::new();

		// The main driver loop operates on the current stack frame, and iteratively tries to build up the next stack frame.
		while let Some((new_dependants, dependant)) = current_frame.next_dependant() {
			let mut next_frame = Frame {
				dependants: new_dependants,
				solution: current_frame.solution.clone(),
				remaining_versions: None,
				last_error: None,
			};

			// Get the permanent and partial solutions for this edge.
			let permanent = current_frame.solution.get_permanent(context, &dependant);
			let partial = current_frame.solution.partial.get(&dependant);

			match (permanent, partial) {
				// Case 0: There is no solution for this package yet.
				(None, None) => {
					// Try and resolve the dependency.
					match context
						.resolve_dependency(self, &dependant, &mut current_frame.remaining_versions)
						.await
					{
						// We successfully got a version.
						Ok(package) => {
							next_frame.solution = next_frame
								.solution
								.mark_temporarily(dependant.clone(), package.clone());

							// Add this dependency to the top of the stack before adding all its dependencies.
							next_frame.dependants.push_back(dependant.clone());

							// Add all the dependencies to the stack.
							for child_dependency in context.get_dependencies(&package) {
								let dependant = Dependant {
									package: package.clone(),
									dependency: child_dependency.clone(),
								};
								next_frame.dependants.push_back(dependant);
							}

							// Update the stack. If we backtrack, we use the next version in the version stack.
							history.push_back(current_frame.clone());
						},
						// Something went wrong. Mark permanently as an error.
						Err(e) => {
							next_frame.solution =
								next_frame
									.solution
									.mark_permanently(context, &dependant, Err(e));
						},
					}
				},

				// Case 1: There exists a global version for the package but we haven't solved this dependency constraint.
				(Some(permanent), None) => {
					match permanent {
						// Case 1.1: The happy path. Our version is solved and it matches this constraint.
						Ok(package) => {
							// Check if the version constraint matches the existing solution.
							let version = context.get_version(package);
							match dependant.dependency.try_match_version(&version) {
								// Success: we can use this version.
								Ok(true) => {
									next_frame.solution = next_frame.solution.mark_permanently(
										context,
										&dependant,
										Ok(package.clone()),
									);
								},
								// Failure: we need to attempt to backtrack.
								Ok(false) => {
									let error = Error::PackageVersionConflict;
									if let Some(frame_) = try_backtrack(
										&history,
										dependant.dependency.name.as_ref().unwrap(),
										error.clone(),
									) {
										next_frame = frame_;
									} else {
										// There is no solution for this package. Add an error.
										next_frame.solution = next_frame.solution.mark_permanently(
											context,
											&dependant,
											Err(error),
										);
									}
								},
								// This can only occur if the there is an error parsing the version constraint of this dependency. It cannot be solved, so mark permanently as an error.
								Err(e) => {
									next_frame.solution = next_frame.solution.mark_permanently(
										context,
										&dependant,
										Err(Error::Other(e)),
									);
								},
							}
						},
						// Case 1.2: The less happy path. We know there's no solution to this package because we've already failed to satisfy some other set of constraints.
						Err(e) => {
							next_frame.solution = next_frame.solution.mark_permanently(
								context,
								&dependant,
								Err(e.clone()),
							);
						},
					}
				},

				// Case 2: We only have a partial solution for this dependency and need to make sure we didn't create a cycle.
				(_, Some(Mark::Temporary(package))) => {
					let dependencies = context.get_dependencies(package);
					let mut erroneous_children = vec![];
					for child_dependency in dependencies {
						let child_dependant = Dependant {
							package: package.clone(),
							dependency: child_dependency.clone(),
						};

						let child = next_frame.solution.partial.get(&child_dependant).unwrap();
						match child {
							// The child dependency has been solved.
							Mark::Permanent(Ok(_)) => (),

							// The child dependency has been solved, but it is an error.
							Mark::Permanent(Err(e)) => {
								let error = e.clone();
								erroneous_children.push((child_dependant.dependency, error));
							},

							// The child dependency has not been solved.
							Mark::Temporary(_version) => {
								// Uh oh. We've detected a cycle. First try and backtrack. If backtracking fails, bail out.
								let error = Error::PackageCycleExists {
									dependant: dependant.clone(),
								};
								erroneous_children.push((child_dependant.dependency, error));
							},
						}
					}

					// If none of the children contain errors, we mark this edge permanently.
					if erroneous_children.is_empty() {
						next_frame.solution = next_frame.solution.mark_permanently(
							context,
							&dependant,
							Ok(package.clone()),
						);
					} else {
						let previous_version = context.get_version(package);
						let error = Error::Backtrack {
							package: package.clone(),
							previous_version,
							erroneous_dependencies: erroneous_children,
						};

						if let Some(frame_) = try_backtrack(
							&history,
							dependant.dependency.name.as_ref().unwrap(),
							error.clone(),
						) {
							next_frame = frame_;
						} else {
							// This means that backtracking totally failed and we need to fail with an error.
							next_frame.solution = next_frame.solution.mark_permanently(
								context,
								&dependant,
								Err(error),
							);
						}
					}
				},

				// Case 3: We've already solved this dependency. Continue.
				(_, Some(Mark::Permanent(_complete))) => (),
			}

			// Replace the solution and dependants if needed.
			current_frame = next_frame;
		}

		Ok(current_frame.solution)
	}
}

impl Context {
	// Check if the dependant can be resolved as a path dependency. Returns an error if we haven't analyzed the `dependant.package` yet.
	fn is_path_dependency(&self, dependant: &Dependant) -> bool {
		// We guarantee that the context already knows about the dependant package by the time this function is called.
		let analysis = self.analysis.get(&dependant.package).unwrap();
		analysis
			.path_dependencies
			.contains_key(&dependant.dependency)
	}

	// Get the dependencies of a package.
	fn get_dependencies(&mut self, package: &tg::directory::Id) -> &[tg::Dependency] {
		&self.analysis.get(package).unwrap().dependencies
	}

	// Get the version of a package.
	fn get_version(&mut self, package: &tg::directory::Id) -> String {
		self.analysis
			.get(package)
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
		dependant: &Dependant,
		remaining_versions: &mut Option<im::Vector<String>>,
	) -> Result<tg::directory::Id, Error> {
		self.try_resolve_dependency(server, dependant, remaining_versions)
			.await?
			.ok_or_else(|| Error::PackageVersionConflict)
	}

	async fn try_resolve_dependency(
		&mut self,
		server: &Server,
		dependant: &Dependant,
		remaining_versions: &mut Option<im::Vector<String>>,
	) -> Result<Option<tg::directory::Id>, Error> {
		// Lookup this package in the local cache.
		let analysis = self.analysis.get(&dependant.package).unwrap();

		// Try and resolve the dependency as a path dependency.
		if let Some(analysis) = analysis.path_dependencies.get(&dependant.dependency) {
			let package = analysis
				.package
				.id(server, None)
				.await
				.map_err(Error::Other)?;
			self.analysis.insert(package.clone(), analysis.clone());
			return Ok(Some(package));
		}

		// Seed the list of versions if necessary.
		if remaining_versions.is_none() {
			let Some(versions) = server
				.try_get_package_versions(&dependant.dependency)
				.await
				.map_err(|source| {
					Error::Other(
						tg::error!(!source, %dependency = &dependant.dependency, "failed to get package versions"),
					)
				})?
			else {
				return Ok(None);
			};
			remaining_versions.replace(versions.into());
		}

		let remaining_versions = remaining_versions.as_mut().unwrap();

		// Get the name and version to try next.
		let name = dependant.dependency.name.clone().ok_or_else(|| {
			Error::Other(tg::error!(%dependency = &dependant.dependency, "missing dependency name"))
		})?;

		let Some(version) = remaining_versions.pop_back() else {
			return Ok(None);
		};
		let dependency_with_name_and_version =
			tg::Dependency::with_name_and_version(name.clone(), version.clone());

		// Check if there's an existing solution for this package.
		if let Some(package) = self
			.published_packages
			.get(&dependency_with_name_and_version)
		{
			return Ok(Some(package.clone()));
		}

		// Otherwise go out to the server.
		let analysis = server
			.try_analyze_package(&dependency_with_name_and_version)
			.await
			.map_err(Error::Other)?
			.ok_or_else(|| {
				Error::Other(tg::error!(
					?dependant,
					"internal error: failed to analyze dependency"
				))
			})?;

		// Get the package.
		let package = analysis
			.package
			.id(server, None)
			.await
			.map_err(Error::Other)?;

		// Update the internal context with knowledge of this package.
		self.published_packages
			.insert(dependency_with_name_and_version, package.clone());
		self.analysis.insert(package.clone(), analysis);
		Ok(Some(package))
	}
}

fn try_backtrack(history: &im::Vector<Frame>, package: &str, error: Error) -> Option<Frame> {
	let index = history
		.iter()
		.take_while(|frame| !frame.solution.contains(package))
		.count();
	let mut frame = history.get(index).cloned()?;
	frame.last_error = Some(error);
	Some(frame)
}

impl Solution {
	// If there's an existing solution for this dependant, return it. Path dependencies are ignored.
	fn get_permanent(
		&self,
		_context: &Context,
		dependant: &Dependant,
	) -> Option<&Result<tg::directory::Id, Error>> {
		self.permanent.get(dependant.dependency.name.as_ref()?)
	}

	/// Mark this dependant with a temporary solution.
	fn mark_temporarily(&self, dependant: Dependant, package: tg::directory::Id) -> Self {
		let mut solution = self.clone();
		solution.partial.insert(dependant, Mark::Temporary(package));
		solution
	}

	/// Mark the dependant permanently, adding it to the list of known solutions and the partial solutions.
	fn mark_permanently(
		&self,
		context: &Context,
		dependant: &Dependant,
		complete: Result<tg::directory::Id, Error>,
	) -> Self {
		let mut solution = self.clone();

		// Update the local solution.
		solution
			.partial
			.insert(dependant.clone(), Mark::Permanent(complete.clone()));

		// If this is not a path dependency then we add it to the global solution.
		if !context.is_path_dependency(dependant) {
			solution
				.permanent
				.insert(dependant.dependency.name.clone().unwrap(), complete);
		}

		solution
	}

	fn contains(&self, package: &str) -> bool {
		self.permanent.contains_key(package)
	}
}

impl Frame {
	fn next_dependant(&self) -> Option<(im::Vector<Dependant>, Dependant)> {
		let mut dependants = self.dependants.clone();
		let dependant = dependants.pop_back()?;
		Some((dependants, dependant))
	}
}

impl std::fmt::Display for Report {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (dependant, error) in &self.errors {
			self.format(f, dependant, error)?;
		}
		Ok(())
	}
}

impl Report {
	fn format(
		&self,
		f: &mut std::fmt::Formatter<'_>,
		dependant: &Dependant,
		error: &Error,
	) -> std::fmt::Result {
		let Dependant {
			package,
			dependency,
		} = dependant;
		let metadata = self
			.context
			.analysis
			.get(package)
			.unwrap()
			.metadata
			.as_ref();
		let name = metadata
			.and_then(|metadata| metadata.name.as_deref())
			.unwrap_or("<unknown>");
		let version = metadata
			.and_then(|metadata| metadata.version.as_deref())
			.unwrap_or("<unknown>");
		write!(f, "{name}@{version} requires {dependency}, but ")?;
		match error {
			Error::PackageCycleExists { dependant } => {
				let metadata = self
					.context
					.analysis
					.get(&dependant.package)
					.unwrap()
					.metadata
					.as_ref();
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
				let shared_dependants = self
					.solution
					.partial
					.keys()
					.filter(|dependant| dependant.dependency.name == dependency.name);
				for shared in shared_dependants {
					let Dependant {
						package,
						dependency,
						..
					} = shared;
					let metadata = self
						.context
						.analysis
						.get(package)
						.unwrap()
						.metadata
						.as_ref();
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
				package,
				previous_version,
				erroneous_dependencies,
			} => {
				let name = dependency.name.as_ref().unwrap();
				writeln!(f, "{name} {previous_version} has errors:")?;
				for (child, error) in erroneous_dependencies {
					let dependant = Dependant {
						package: package.clone(),
						dependency: child.clone(),
					};
					self.format(f, &dependant, error)?;
				}
			},
			Error::Other(e) => {
				writeln!(f, "{e}")?;
			},
		}
		Ok(())
	}
}
