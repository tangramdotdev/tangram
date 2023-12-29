use async_recursion::async_recursion;
use futures::stream::{FuturesUnordered, TryStreamExt};
use itertools::Itertools;
use std::{
	collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_error::{error, return_error, Result, WrapErr};
use tg::Handle;

#[derive(Clone, Debug)]
pub struct PackageWithPathDependencies {
	pub package: tg::Directory,
	pub path_dependencies: BTreeMap<tg::Dependency, PackageWithPathDependencies>,
}

// Mutable state used during the version solving algorithm to cache package metadata and published packages.
struct Context {
	// A cache of package analysis (metadata, direct dependencies).
	analysis: BTreeMap<tg::directory::Id, Analysis>,

	// A cache of published packages that we know about.
	published_packages: im::HashMap<tg::package::Metadata, tg::directory::Id>,

	// A table of resolved path dependencies. A flattened representation of PackageWithPathDependencies.
	path_dependencies: BTreeMap<tg::directory::Id, BTreeMap<tg::Path, tg::directory::Id>>,
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

// A stack frame, used for backtracking. Implicitly, a single stack frame corresponds to a single dependant (the top of the working set).
#[derive(Clone, Debug)]
struct Frame {
	// The current solution space.
	solution: Solution,

	// The list of remaining dependants to solve.
	working_set: im::Vector<Dependant>,

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

// A cached representation of a package's metadata and dependencies.
#[derive(Debug, Clone)]
struct Analysis {
	metadata: tg::package::Metadata,
	dependencies: Vec<tg::Dependency>,
}

/// Errors that may arise during version solving.
#[derive(Debug, Clone)]
enum Error {
	/// No version could be found that satisfies all constraints.
	PackageVersionConflict,

	/// A package cycle exists.
	PackageCycleExists {
		/// Represents the terminal edge of cycle in the dependency graph.
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
	Other(tangram_error::Error),
}

pub async fn try_get_package(
	tg: &dyn tg::Handle,
	dependency: &tg::Dependency,
) -> Result<Option<tg::Directory>> {
	let package_with_path_dependencies = get_package_with_path_dependencies(tg, dependency).await?;
	let package = package_with_path_dependencies.package;
	Ok(Some(package))
}

pub async fn try_get_package_and_lock(
	tg: &dyn tg::Handle,
	dependency: &tg::Dependency,
) -> Result<Option<(tg::Directory, tg::Lock)>> {
	// Get the package with its path dependencies.
	let package_with_path_dependencies = get_package_with_path_dependencies(tg, dependency).await?;

	// If this is a path dependency, then attempt to read the lockfile from the path.
	let lockfile = if let Some(path) = dependency.path.as_ref() {
		try_read_lockfile_from_path(path).await?
	} else {
		None
	};

	// Verify that the lockfile's dependencies match the package with path dependencies.
	let lockfile = if let Some(lockfile) = lockfile {
		let matches = lockfile_matches(tg, &package_with_path_dependencies, &lockfile).await?;
		if matches {
			Some(lockfile)
		} else {
			None
		}
	} else {
		None
	};

	// Otherwise, create the lockfile.
	let lockfile_created = lockfile.is_none();
	let lockfile = if let Some(lockfile) = lockfile {
		lockfile
	} else {
		create_lockfile(tg, &package_with_path_dependencies).await?
	};

	// If this is a path dependency and the lockfile was created, then write the lockfile.
	if let Some(path) = dependency.path.as_ref() {
		if lockfile_created {
			write_lockfile(path, &lockfile).await?;
		}
	}

	// Get the package.
	let package = package_with_path_dependencies.package.clone();

	// Create the lock.
	let lock = create_lock(&package_with_path_dependencies, &lockfile)?;

	// Return.
	Ok(Some((package, lock)))
}

async fn get_package_with_path_dependencies(
	tg: &dyn tg::Handle,
	dependency: &tg::Dependency,
) -> Result<PackageWithPathDependencies> {
	if let Some(path) = dependency.path.as_ref().cloned() {
		// If the dependency is a path dependency, then get the package with its path dependencies from the path.
		let path = tokio::fs::canonicalize(PathBuf::from(path))
			.await
			.wrap_err("Failed to canonicalize the path.")?;
		Ok(get_package_with_path_dependencies_for_path(tg, &path).await?)
	} else {
		// If the dependency is a registry dependency, then get the package from the registry and make the path dependencies be empty.
		let id = tg
			.try_get_package(dependency)
			.await?
			.ok_or(tangram_error::error!(
				r#"Could not find package "{dependency}"."#
			))?;
		let package = tg::Directory::with_id(id.clone());
		Ok(PackageWithPathDependencies {
			package,
			path_dependencies: BTreeMap::default(),
		})
	}
}

async fn get_package_with_path_dependencies_for_path(
	tg: &dyn tg::Handle,
	path: &Path,
) -> tangram_error::Result<PackageWithPathDependencies> {
	let mut visited = BTreeMap::default();
	get_package_with_path_dependencies_for_path_inner(tg, path, &mut visited).await
}

#[async_recursion]
async fn get_package_with_path_dependencies_for_path_inner(
	tg: &dyn tg::Handle,
	path: &Path,
	visited: &mut BTreeMap<PathBuf, Option<PackageWithPathDependencies>>,
) -> tangram_error::Result<PackageWithPathDependencies> {
	// Check if the path has already been visited.
	match visited.get(path) {
		Some(Some(package_with_path_dependencies)) => {
			return Ok(package_with_path_dependencies.clone())
		},
		Some(None) => {
			return Err(tangram_error::error!(
				"The package has a circular path dependency."
			))
		},
		None => (),
	}

	// Add the path to the visited set with `None` to detect circular dependencies.
	visited.insert(path.to_owned(), None);

	// Create a builder for the package.
	let mut package = tg::directory::Builder::default();

	// Create a queue of module paths to visit and a visited set.
	let mut queue: VecDeque<tg::Path> =
		VecDeque::from(vec![tg::package::ROOT_MODULE_FILE_NAME.parse().unwrap()]);
	let mut visited_module_paths: HashSet<tg::Path, fnv::FnvBuildHasher> = HashSet::default();

	// Create the path dependencies.
	let mut path_dependencies = BTreeMap::default();

	// Visit each module.
	while let Some(module_path) = queue.pop_front() {
		// Get the module's absolute path.
		let module_absolute_path = path.join(module_path.to_string());
		let module_absolute_path = tokio::fs::canonicalize(&module_absolute_path)
			.await
			.wrap_err("Failed to canonicalize the module path.")?;

		// Add the module to the package directory.
		let artifact =
			tg::Artifact::check_in(tg, &module_absolute_path.clone().try_into()?).await?;
		package = package.add(tg, &module_path, artifact).await?;

		// Get the module's text.
		let text = tokio::fs::read_to_string(&module_absolute_path)
			.await
			.wrap_err("Failed to read the module.")?;

		// Analyze the module.
		let analysis =
			tangram_language::Module::analyze(text).wrap_err("Failed to analyze the module.")?;

		// Handle the includes.
		for include_path in analysis.includes {
			// Get the included artifact's path in the package.
			let included_artifact_path = module_path
				.clone()
				.parent()
				.join(include_path.clone())
				.normalize();

			// Get the included artifact's path.
			let included_artifact_absolute_path =
				path.join(included_artifact_path.to_string()).try_into()?;

			// Check in the artifact at the included path.
			let included_artifact =
				tg::Artifact::check_in(tg, &included_artifact_absolute_path).await?;

			// Add the included artifact to the directory.
			package = package
				.add(tg, &included_artifact_path, included_artifact)
				.await?;
		}

		// Recurse into the path dependencies.
		for import in &analysis.imports {
			if let tangram_language::Import::Dependency(
				dependency @ tg::Dependency { path: Some(_), .. },
			) = import
			{
				// Make the dependency path relative to the package.
				let mut dependency = dependency.clone();
				dependency.path.replace(
					module_path
						.clone()
						.parent()
						.join(dependency.path.as_ref().unwrap().clone())
						.normalize(),
				);

				// Get the dependency's absolute path.
				let dependency_path = path.join(dependency.path.as_ref().unwrap().to_string());
				let dependency_absolute_path = tokio::fs::canonicalize(&dependency_path)
					.await
					.wrap_err("Failed to canonicalize the dependency path.")?;

				// Recurse into the path dependency.
				let child = get_package_with_path_dependencies_for_path_inner(
					tg,
					&dependency_absolute_path,
					visited,
				)
				.await?;

				// Insert the path dependency.
				path_dependencies.insert(dependency, child);
			}
		}

		// Add the module path to the visited set.
		visited_module_paths.insert(module_path.clone());

		// Add the unvisited path imports to the queue.
		for import in &analysis.imports {
			if let tangram_language::Import::Module(import) = import {
				let imported_module_path = module_path
					.clone()
					.parent()
					.join(import.clone())
					.normalize();
				if !visited_module_paths.contains(&imported_module_path) {
					queue.push_back(imported_module_path);
				}
			}
		}
	}

	// Create the package.
	let package = package.build();

	// Create the package with path dependencies.
	let package_with_path_dependencies = PackageWithPathDependencies {
		package,
		path_dependencies,
	};

	// Mark the package with path dependencies as visited.
	visited.insert(
		path.to_owned(),
		Some(package_with_path_dependencies.clone()),
	);

	Ok(package_with_path_dependencies)
}

async fn try_read_lockfile_from_path(path: &tg::Path) -> Result<Option<tg::Lockfile>> {
	// Canonicalize the path.
	let path = tokio::fs::canonicalize(PathBuf::from(path.clone()))
		.await
		.wrap_err("Failed to canonicalize the path.")?;

	// Attempt to read the lockfile.
	let lockfile_path = path.join(tg::package::LOCKFILE_FILE_NAME);
	let exists = tokio::fs::try_exists(&lockfile_path)
		.await
		.wrap_err("Failed to determine if the lockfile exists.")?;
	if !exists {
		return Ok(None);
	}
	let lockfile = tokio::fs::read(&lockfile_path)
		.await
		.wrap_err("Failed to read the lockfile.")?;
	let lockfile: tg::Lockfile =
		serde_json::from_slice(&lockfile).wrap_err("Failed to deserialize the lockfile.")?;

	Ok(Some(lockfile))
}

async fn write_lockfile(path: &tg::Path, lockfile: &tg::Lockfile) -> Result<()> {
	let package_path = tokio::fs::canonicalize(PathBuf::from(path.clone()))
		.await
		.wrap_err("Failed to canonicalize the path.")?;
	let lockfile_path = package_path.join(tg::package::LOCKFILE_FILE_NAME);
	let lockfile =
		serde_json::to_vec_pretty(lockfile).wrap_err("Failed to serialize the lockfile.")?;
	tokio::fs::write(lockfile_path, lockfile)
		.await
		.wrap_err("Failed to write the lockfile.")?;
	Ok(())
}

async fn lockfile_matches(
	tg: &dyn Handle,
	package_with_path_dependencies: &PackageWithPathDependencies,
	lockfile: &tg::Lockfile,
) -> Result<bool> {
	lockfile_matches_inner(tg, package_with_path_dependencies, lockfile, 0).await
}

#[async_recursion]
async fn lockfile_matches_inner(
	tg: &dyn Handle,
	package_with_path_dependencies: &PackageWithPathDependencies,
	lockfile: &tg::Lockfile,
	index: usize,
) -> Result<bool> {
	// Get the package's dependencies.
	let dependencies = dependencies(tg, &package_with_path_dependencies.package).await?;

	// Get the package's lock from the lockfile.
	let lock = lockfile.locks.get(index).wrap_err("Invalid lockfile.")?;

	// Verify that the dependencies match.
	if !itertools::equal(lock.dependencies.keys(), dependencies.iter()) {
		return Ok(false);
	}

	// Recurse into the path dependencies.
	package_with_path_dependencies
		.path_dependencies
		.keys()
		.map(|dependency| {
			// let dependencies = &dependencies;
			async move {
				let index = lock.dependencies.get(dependency).unwrap().lock;
				lockfile_matches_inner(tg, package_with_path_dependencies, lockfile, index).await
			}
		})
		.collect::<FuturesUnordered<_>>()
		.try_all(|matches| async move { matches })
		.await?;

	Ok(true)
}

async fn create_lockfile(
	tg: &dyn tg::Handle,
	package_with_path_dependencies: &PackageWithPathDependencies,
) -> Result<tg::Lockfile> {
	// Construct the version solving context and working set.
	let mut analysis = BTreeMap::new();
	let mut path_dependencies = BTreeMap::new();
	let mut working_set = im::Vector::new();

	scan_package_with_path_dependencies(
		tg,
		package_with_path_dependencies,
		&mut analysis,
		&mut path_dependencies,
		&mut working_set,
	)
	.await?;
	let published_packages = im::HashMap::new();
	let mut context = Context {
		analysis,
		published_packages,
		path_dependencies,
	};

	// Solve.
	let solution = solve(tg, &mut context, working_set).await?;

	// Create the error report.
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
		return_error!("{report}");
	}

	// Create the set of locks for all dependencies.
	let mut locks = Vec::new();

	let root = create_lockfile_inner(
		tg,
		package_with_path_dependencies,
		&context,
		&solution,
		&mut locks,
	)
	.await?;

	Ok(tg::Lockfile { root, locks })
}

#[allow(clippy::only_used_in_recursion)]
#[async_recursion]
async fn create_lockfile_inner(
	tg: &dyn tg::Handle,
	package_with_path_dependencies: &PackageWithPathDependencies,
	context: &Context,
	solution: &Solution,
	locks: &mut Vec<tg::lockfile::Lock>,
) -> Result<usize> {
	// Get the cached analysis.
	let package = package_with_path_dependencies.package.id(tg).await?.clone();
	let analysis = context
		.analysis
		.get(&package)
		.wrap_err("Missing package in solution.")?;
	let path_dependencies = context.path_dependencies.get(&package);

	// Recursively create locks.
	let mut dependencies = BTreeMap::new();
	for dependency in &analysis.dependencies {
		let entry = match (dependency.path.as_ref(), path_dependencies) {
			// Use the path dependencies from the context to check if this is a resolved path dependency.
			(Some(path), Some(path_dependencies)) if path_dependencies.contains_key(path) => {
				// Resolve by path.
				let package_with_path_dependencies = package_with_path_dependencies
					.path_dependencies
					.iter()
					.find_map(|(dependency, pwpd)| {
						(dependency.path.as_ref().unwrap() == path).then_some(pwpd)
					})
					.unwrap();
				let lock = create_lockfile_inner(
					tg,
					package_with_path_dependencies,
					context,
					solution,
					locks,
				)
				.await?;
				tg::lockfile::Entry {
					package: None,
					lock,
				}
			},
			_ => {
				// Resolve by dependant.
				let dependant = Dependant {
					package: package.clone(),
					dependency: dependency.clone(),
				};
				let Some(Mark::Permanent(Ok(resolved))) = solution.partial.get(&dependant) else {
					return_error!("Missing solution for {dependant:?}.");
				};
				let pwpd = PackageWithPathDependencies {
					package: tg::Directory::with_id(resolved.clone()),
					path_dependencies: BTreeMap::new(),
				};
				let lock = create_lockfile_inner(tg, &pwpd, context, solution, locks).await?;
				tg::lockfile::Entry {
					package: Some(resolved.clone()),
					lock,
				}
			},
		};
		dependencies.insert(dependency.clone(), entry);
	}

	// Insert the lock if it doesn't exist.
	let lock = tg::lockfile::Lock { dependencies };
	let index = if let Some(index) = locks.iter().position(|l| l == &lock) {
		index
	} else {
		locks.push(lock);
		locks.len() - 1
	};
	Ok(index)
}

#[async_recursion]
async fn scan_package_with_path_dependencies(
	tg: &dyn tg::Handle,
	package_with_path_dependencies: &PackageWithPathDependencies,
	all_analysis: &mut BTreeMap<tg::directory::Id, Analysis>,
	all_path_dependencies: &mut BTreeMap<tg::directory::Id, BTreeMap<tg::Path, tg::directory::Id>>,
	working_set: &mut im::Vector<Dependant>,
) -> Result<()> {
	let PackageWithPathDependencies {
		package,
		path_dependencies,
	} = package_with_path_dependencies;
	let package_id = package.id(tg).await?.clone();

	// Check if we've already visited this dependency.
	if all_path_dependencies.contains_key(&package_id) {
		return Ok(());
	}
	// Update the path dependencies.
	all_path_dependencies.insert(package_id.clone(), BTreeMap::new());

	// Get the metadata and dependenencies of this package.
	let dependency = tg::Dependency::with_id(package_id.clone());
	let metadata = tg.get_package_metadata(&dependency).await?;
	let dependencies = tg.get_package_dependencies(&dependency).await?;

	// Convert dependencies to dependants and update the working set.
	let dependants = dependencies.iter().map(|dependency| Dependant {
		package: package_id.clone(),
		dependency: dependency.clone(),
	});
	working_set.extend(dependants);

	// Add the metadata and dependencies to the analysis cache.
	let analysis = Analysis {
		metadata,
		dependencies,
	};
	all_analysis.insert(package_id.clone(), analysis);

	// Recurse.
	for (dependency, package_with_path_dependencies) in path_dependencies {
		let path = dependency.path.as_ref().unwrap();
		let dependency_package_id = package_with_path_dependencies.package.id(tg).await?.clone();
		all_path_dependencies
			.get_mut(&package_id)
			.unwrap()
			.insert(path.clone(), dependency_package_id);

		scan_package_with_path_dependencies(
			tg,
			package_with_path_dependencies,
			all_analysis,
			all_path_dependencies,
			working_set,
		)
		.await?;
	}

	Ok(())
}

async fn solve(
	tg: &dyn tg::Handle,
	context: &mut Context,
	working_set: im::Vector<Dependant>,
) -> Result<Solution> {
	// Create the first stack frame.
	let solution = Solution::default();
	let last_error = None;
	let remaining_versions = None;
	let mut current_frame = Frame {
		solution,
		working_set,
		remaining_versions,
		last_error,
	};
	let mut history = im::Vector::new();

	// The main driver loop operates on the current stack frame, and iteratively tries to build up the next stack frame.
	while let Some((new_working_set, dependant)) = current_frame.next_dependant() {
		let mut next_frame = Frame {
			working_set: new_working_set,
			solution: current_frame.solution.clone(),
			remaining_versions: None,
			last_error: None,
		};

		let permanent = current_frame.solution.get_permanent(context, &dependant);

		let partial = current_frame.solution.partial.get(&dependant);
		match (permanent, partial) {
			// Case 0: There is no solution for this package yet.
			(None, None) => 'a: {
				// Note: this bit is tricky. The next frame will always have an empty set of remaining versions, because by construction it will never have been tried before. However we need to get a list of versions to attempt, which we will push onto the stack.
				if current_frame.remaining_versions.is_none() {
					let all_versions = match context.get_all_versions(tg, &dependant).await {
						Ok(all_versions) => all_versions,
						Err(e) => {
							tracing::error!(?dependant, ?e, "Failed to get versions of package.");

							// We cannot solve this dependency.
							next_frame.solution = current_frame.solution.mark_permanently(
								context,
								dependant,
								Err(Error::Other(e)),
							);

							// This is ugly, but writing out the full match statemenet is uglier and we already have a deeply nested tree of branches.
							break 'a;
						},
					};

					let remaining_versions = all_versions
						.into_iter()
						.filter_map(|version| {
							// TODO: handle the error here. If the published version cannot be parsed then we can continue the loop. If the dependency version cannot be parsed we need to issue a hard error and break out of the match statement.
							let version = version.version.as_deref()?;
							Context::matches(version, &dependant.dependency)
								.ok()?
								.then_some(version.to_owned())
						})
						.collect();
					current_frame.remaining_versions = Some(remaining_versions);
				}

				// Try and pick a version.
				let package = context
					.try_resolve(
						tg,
						&dependant,
						current_frame.remaining_versions.as_mut().unwrap(),
					)
					.await;

				match package {
					// We successfully popped a version.
					Ok(package) => {
						next_frame.solution = next_frame
							.solution
							.mark_temporarily(dependant.clone(), package.clone());

						// Add this dependency to the top of the stack before adding all its dependencies.
						next_frame.working_set.push_back(dependant.clone());

						// Add all the dependencies to the stack.
						for child_dependency in context.dependencies(tg, &package).await.unwrap() {
							let dependant = Dependant {
								package: package.clone(),
								dependency: child_dependency.clone(),
							};
							next_frame.working_set.push_back(dependant);
						}

						// Update the solution
						next_frame.solution =
							next_frame.solution.mark_temporarily(dependant, package);

						// Update the stack. If we backtrack, we use the next version in the version stack.
						history.push_back(current_frame.clone());
					},

					Err(e) => {
						tracing::error!(?dependant, ?e, "No solution exists.");
						next_frame.solution =
							next_frame
								.solution
								.mark_permanently(context, dependant, Err(e));
					},
				}
			},

			// Case 1: There exists a global version for the package but we haven't solved this dependency constraint.
			(Some(permanent), None) => {
				match permanent {
					// Case 1.1: The happy path. Our version is solved and it matches this constraint.
					Ok(package) => {
						// Successful caches of the version will be memoized, so it's safe to  unwrap here. Annoyingly, borrowck fails here because it doesn't know that the result holds a mutable reference to the context.
						let version = context.version(tg, package).await.unwrap().to_owned();

						// Case 1.1: The happy path. Our version is solved and it matches this constraint.
						match Context::matches(&version, &dependant.dependency) {
							Ok(true) => {
								next_frame.solution = next_frame.solution.mark_permanently(
									context,
									dependant,
									Ok(package.clone()),
								);
							},
							// Case 1.3: The unhappy path. We need to fail.
							Ok(false) => {
								let error = Error::PackageVersionConflict;
								if let Some(frame_) = try_backtrack(
									&history,
									dependant.dependency.name.as_ref().unwrap(),
									error.clone(),
								) {
									next_frame = frame_;
								} else {
									tracing::error!(?dependant, "No solution exists.");
									// There is no solution for this package. Add an error.
									next_frame.solution = next_frame.solution.mark_permanently(
										context,
										dependant,
										Err(error),
									);
								}
							},
							Err(e) => {
								tracing::error!(?dependant, ?e, "Existing solution is an error.");
								next_frame.solution.mark_permanently(
									context,
									dependant,
									Err(Error::Other(e)),
								);
							},
						}
					},
					// Case 1.2: The less happy path. We know there's no solution to this package because we've already failed to satisfy some other set of constraints.
					Err(e) => {
						next_frame.solution = next_frame.solution.mark_permanently(
							context,
							dependant,
							Err(e.clone()),
						);
					},
				}
			},

			// Case 2: We only have a partial solution for this dependency and need to make sure we didn't create a cycle.
			(_, Some(Mark::Temporary(package))) => {
				// Note: it is safe to unwrap here because a successful query to context.dependencies is memoized.
				let dependencies = context.dependencies(tg, package).await.unwrap();

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

				// If none of the children contain errors, we mark this edge permanently
				if erroneous_children.is_empty() {
					next_frame.solution = next_frame.solution.mark_permanently(
						context,
						dependant,
						Ok(package.clone()),
					);
				} else {
					// Successful lookups of the version are memoized, so it's safe to unwrap here.
					let previous_version = context.version(tg, package).await.unwrap().into();
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
						// This means that backtracking totally failed and we need to fail with an error
						next_frame.solution =
							next_frame
								.solution
								.mark_permanently(context, dependant, Err(error));
					}
				}
			},

			// Case 3: We've already solved this dependency. Continue.
			(_, Some(Mark::Permanent(_complete))) => (),
		}

		// Replace the solution and working set if needed.
		current_frame = next_frame;
	}

	Ok(current_frame.solution)
}

impl Context {
	// Check if the dependant can be resolved as a path dependency.
	#[must_use]
	fn is_path_dependency(&self, dependant: &Dependant) -> bool {
		self.resolve_path_dependency(dependant).is_some()
	}

	#[must_use]
	fn resolve_path_dependency(&self, dependant: &Dependant) -> Option<tg::directory::Id> {
		let Dependant {
			package,
			dependency,
		} = dependant;
		let path = dependency.path.as_ref()?;
		let path_dependencies = self.path_dependencies.get(package)?;
		path_dependencies.get(path).cloned()
	}

	// Check if a package satisfies a dependency.
	fn matches(version: &str, dependency: &tg::Dependency) -> Result<bool> {
		let Some(constraint) = dependency.version.as_ref() else {
			return Ok(true);
		};
		let version: semver::Version = version.parse().map_err(|e| {
			tracing::error!(?e, ?version, "Failed to parse metadata version.");
			error!("Failed to parse version: {version}.")
		})?;
		let constraint: semver::VersionReq = constraint.parse().map_err(|e| {
			tracing::error!(?e, ?dependency, "Failed to parse dependency version.");
			error!("Failed to parse version.")
		})?;
		Ok(constraint.matches(&version))
	}

	// Try and get the next version from a list of remaining ones. Returns an error if the list is empty.
	async fn try_resolve(
		&mut self,
		tg: &dyn tg::Handle,
		dependant: &Dependant,
		remaining_versions: &mut im::Vector<String>,
	) -> Result<tg::directory::Id, Error> {
		// First attempt to resolve this as a path dependency.
		if let Some(result) = self.resolve_path_dependency(dependant) {
			return Ok(result);
		}

		let name = dependant.dependency.name.as_ref().unwrap();

		// If the cache doesn't contain the package, we need to go out to the server to retrieve the ID. If this errors, we return immediately. If there is no package available for this version (which is extremely unlikely) we loop until we get the next version that's either in the cache, or available from the server.
		loop {
			let version = remaining_versions
				.pop_back()
				.ok_or(Error::PackageVersionConflict)?;
			let metadata = tg::package::Metadata {
				name: Some(name.into()),
				version: Some(version.clone()),
				description: None,
			};
			if let Some(package) = self.published_packages.get(&metadata) {
				return Ok(package.clone());
			}
			let dependency = tg::Dependency::with_name_and_version(name.into(), version);
			match tg.try_get_package(&dependency).await {
				Err(error) => {
					tracing::error!(?dependency, "Failed to get an artifact for the package.");
					return Err(Error::Other(error));
				},
				Ok(Some(package)) => {
					self.published_packages.insert(metadata, package.clone());
					return Ok(package);
				},
				Ok(None) => continue,
			}
		}
	}

	// Try to lookup the cached analysis for a package by its ID. If it is missing from the cache, we ask the server to analyze the package. If we cannot, we fail.
	async fn try_get_analysis(
		&mut self,
		tg: &dyn tg::Handle,
		package: &tg::directory::Id,
	) -> Result<&'_ Analysis> {
		if !self.analysis.contains_key(package) {
			let dependency = tg::Dependency::with_id(package.clone());
			let metadata = tg.get_package_metadata(&dependency).await?;
			let dependencies = tg
				.get_package_dependencies(&dependency)
				.await?
				.into_iter()
				.filter(|dependency| {
					!(self.path_dependencies.contains_key(package) && dependency.path.is_some())
				})
				.collect();
			let analysis = Analysis {
				metadata: metadata.clone(),
				dependencies,
			};
			self.published_packages
				.insert(metadata.clone(), package.clone());
			self.analysis.insert(package.clone(), analysis);
		}
		Ok(self.analysis.get(package).unwrap())
	}

	// Get a list of registry dependencies for a package given its metadata.
	async fn dependencies(
		&mut self,
		tg: &dyn tg::Handle,
		package: &tg::directory::Id,
	) -> Result<&'_ [tg::Dependency]> {
		Ok(&self.try_get_analysis(tg, package).await?.dependencies)
	}

	// Get the version of a package given its ID.
	async fn version(&mut self, tg: &dyn tg::Handle, package: &tg::directory::Id) -> Result<&str> {
		self.try_get_analysis(tg, package)
			.await?
			.metadata
			.version
			.as_deref()
			.wrap_err("Missing version for package.")
	}

	// Lookup all the versions we might use to solve this dependant.
	async fn get_all_versions(
		&mut self,
		tg: &dyn Handle,
		dependant: &Dependant,
	) -> Result<Vec<tg::package::Metadata>> {
		// If it is a path dependency, we don't care about the versions, which may not exist.
		if self.is_path_dependency(dependant) {
			return Ok(Vec::new());
		};
		let name = dependant
			.dependency
			.name
			.as_ref()
			.wrap_err("Missing name for dependency.")?
			.clone();
		let dependency = tg::Dependency::with_name(name.clone());
		let metadata = tg
			.get_package_versions(&dependency)
			.await?
			.into_iter()
			.map(|version| tg::package::Metadata {
				name: Some(name.clone()),
				version: Some(version),
				description: None,
			})
			.collect::<Vec<_>>();
		Ok(metadata)
	}
}

fn try_backtrack(history: &im::Vector<Frame>, package: &str, error: Error) -> Option<Frame> {
	let idx = history
		.iter()
		.take_while(|frame| !frame.solution.contains(package))
		.count();
	let mut frame = history.get(idx).cloned()?;
	frame.last_error = Some(error);
	Some(frame)
}

impl Solution {
	// If there's an existing solution for this dependant, return it. Path dependencies are ignored.
	fn get_permanent(
		&self,
		context: &Context,
		dependant: &Dependant,
	) -> Option<&Result<tg::directory::Id, Error>> {
		if context.is_path_dependency(dependant) {
			return None;
		}
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
		dependant: Dependant,
		complete: Result<tg::directory::Id, Error>,
	) -> Self {
		let mut solution = self.clone();

		// Update the global solution.
		if !context.is_path_dependency(&dependant) {
			solution
				.permanent
				.insert(dependant.dependency.name.clone().unwrap(), complete.clone());
		}

		// Update the local solution.
		solution
			.partial
			.insert(dependant, Mark::Permanent(complete));

		solution
	}

	fn contains(&self, package: &str) -> bool {
		self.permanent.contains_key(package)
	}
}

impl Frame {
	fn next_dependant(&self) -> Option<(im::Vector<Dependant>, Dependant)> {
		let mut working_set = self.working_set.clone();
		let dependant = working_set.pop_back()?;
		Some((working_set, dependant))
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

		let metadata = &self.context.analysis.get(package).unwrap().metadata;
		let name = metadata.name.as_ref().unwrap();
		let version = metadata.version.as_ref().unwrap();
		write!(f, "{name} @ {version} requires {dependency}, but ")?;

		match error {
			Error::PackageCycleExists { dependant } => {
				let metadata = &self
					.context
					.analysis
					.get(&dependant.package)
					.unwrap()
					.metadata;
				let name = metadata.name.as_ref().unwrap();
				let version = metadata.version.as_ref().unwrap();
				writeln!(f, "{name}@{version}, which creates a cycle.")
			},
			Error::PackageVersionConflict => {
				writeln!(
					f,
					"no version could be found that satisfies the constraints."
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
					let metadata = &self.context.analysis.get(package).unwrap().metadata;
					let name = metadata.name.as_ref().unwrap();
					let version = metadata.version.as_ref().unwrap();
					writeln!(f, "{name} @ {version} requires {dependency}")?;
				}
				Ok(())
			},
			Error::Backtrack {
				package,
				previous_version,
				erroneous_dependencies,
			} => {
				writeln!(
					f,
					"{} {previous_version} has errors:",
					dependency.name.as_ref().unwrap()
				)?;
				for (child, error) in erroneous_dependencies {
					let dependant = Dependant {
						package: package.clone(),
						dependency: child.clone(),
					};
					self.format(f, &dependant, error)?;
				}
				Ok(())
			},
			Error::Other(e) => writeln!(f, "{e}"),
		}
	}
}

impl std::fmt::Display for Mark {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Permanent(complete) => write!(f, "Complete({complete:?})"),
			Self::Temporary(version) => write!(f, "Incomplete({version})"),
		}
	}
}

fn create_lock(
	package_with_path_dependencies: &PackageWithPathDependencies,
	lockfile: &tg::Lockfile,
) -> Result<tg::Lock> {
	create_lock_inner(package_with_path_dependencies, lockfile, lockfile.root)
}

fn create_lock_inner(
	package_with_path_dependencies: &PackageWithPathDependencies,
	lockfile: &tg::Lockfile,
	index: usize,
) -> Result<tg::Lock> {
	let lock = lockfile.locks.get(index).wrap_err("Invalid lockfile.")?;
	let dependencies = lock
		.dependencies
		.iter()
		.map(|(dependency, entry)| -> Result<_> {
			let (package, lock) = if let Some(package) = entry.package.as_ref() {
				let package = tg::Directory::with_id(package.clone());
				let lock = create_lock_inner(package_with_path_dependencies, lockfile, entry.lock)?;
				(package, lock)
			} else {
				let package_with_path_dependencies = package_with_path_dependencies
					.path_dependencies
					.get(dependency)
					.wrap_err("Missing path dependency.")?;
				let package = package_with_path_dependencies.package.clone();
				let lock = create_lock_inner(package_with_path_dependencies, lockfile, entry.lock)?;
				(package, lock)
			};
			let entry = tg::lock::Entry { package, lock };
			Ok((dependency.clone(), entry))
		})
		.try_collect()?;
	Ok(tg::Lock::with_object(tg::lock::Object { dependencies }))
}

pub async fn dependencies(
	tg: &dyn tg::Handle,
	package: &tg::Directory,
) -> Result<Vec<tg::Dependency>> {
	// Create the dependencies set.
	let mut dependencies: BTreeSet<tg::Dependency> = BTreeSet::default();

	// Create a queue of module paths to visit and a visited set.
	let mut queue: VecDeque<tg::Path> =
		VecDeque::from(vec![tg::package::ROOT_MODULE_FILE_NAME.parse().unwrap()]);
	let mut visited: HashSet<tg::Path, fnv::FnvBuildHasher> = HashSet::default();

	// Visit each module.
	while let Some(module_path) = queue.pop_front() {
		// Get the file.
		let file = package
			.get(tg, &module_path.clone())
			.await?
			.try_unwrap_file()
			.ok()
			.wrap_err("Expected the module to be a file.")?;
		let text = file.text(tg).await?;

		// Analyze the module.
		let analysis =
			tangram_language::Module::analyze(text).wrap_err("Failed to analyze the module.")?;

		// Recurse into the dependencies.
		for import in &analysis.imports {
			if let tangram_language::Import::Dependency(dependency) = import {
				let mut dependency = dependency.clone();

				// Normalize the path dependency to be relative to the root.
				dependency.path = dependency
					.path
					.take()
					.map(|path| module_path.clone().parent().join(path).normalize());

				dependencies.insert(dependency.clone());
			}
		}

		// Add the module path to the visited set.
		visited.insert(module_path.clone());

		// Add the unvisited module imports to the queue.
		for import in &analysis.imports {
			if let tangram_language::Import::Module(import) = import {
				let imported_module_path = module_path
					.clone()
					.parent()
					.join(import.clone())
					.normalize();
				if !visited.contains(&imported_module_path) {
					queue.push_back(imported_module_path);
				}
			}
		}
	}

	Ok(dependencies.into_iter().collect())
}

pub async fn metadata(
	tg: &dyn tg::Handle,
	package: &tg::Directory,
) -> Result<tg::package::Metadata> {
	let path = tg::package::ROOT_MODULE_FILE_NAME.parse().unwrap();
	let file = package
		.get(tg, &path)
		.await?
		.try_unwrap_file()
		.ok()
		.wrap_err("Expected the module to be a file.")?;
	let text = file.text(tg).await?;
	let analysis = tangram_language::Module::analyze(text)?;
	if let Some(metadata) = analysis.metadata {
		Ok(metadata)
	} else {
		return_error!("Missing package metadata.")
	}
}

// #[cfg(test)]
// mod tests {
// 	use crate::ROOT_MODULE_FILE_NAME;
// 	use tangram_client as tg;
// 	use tangram_error::Result;

// 	#[tokio::test]
// 	async fn simple_diamond() {
// 		let client: Box<dyn tg::Client> = todo!();

// 		create_package(
// 			client.as_ref(),
// 			"simple_diamond_A",
// 			"1.0.0",
// 			&[
// 				"simple_diamond_B@^1.0".parse().unwrap(),
// 				"simple_diamond_C@^1.0".parse().unwrap(),
// 			],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"simple_diamond_B",
// 			"1.0.0",
// 			&["simple_diamond_D@^1.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"simple_diamond_C",
// 			"1.0.0",
// 			&["simple_diamond_D@^1.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(tg.as_ref(), "simple_diamond_D", "1.0.0", &[])
// 			.await
// 			.unwrap();
// 	}

// 	#[tokio::test]
// 	async fn simple_backtrack() {
// 		let client: Box<dyn tg::Client> = todo!();

// 		create_package(
// 			client.as_ref(),
// 			"simple_backtrack_A",
// 			"1.0.0",
// 			&[
// 				"simple_backtrack_B@^1.2.3".parse().unwrap(),
// 				"simple_backtrack_C@<1.2.3".parse().unwrap(),
// 			],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"simple_backtrack_B",
// 			"1.2.3",
// 			&["simple_backtrack_C@<1.2.3".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(tg.as_ref(), "simple_backtrack_C", "1.2.3", &[])
// 			.await
// 			.unwrap();

// 		create_package(tg.as_ref(), "simple_backtrack_C", "1.2.2", &[])
// 			.await
// 			.unwrap();
// 	}

// 	#[tokio::test]
// 	async fn diamond_backtrack() {
// 		let client: Box<dyn tg::Client> = todo!();

// 		create_package(
// 			client.as_ref(),
// 			"diamond_backtrack_A",
// 			"1.0.0",
// 			&[
// 				"diamond_backtrack_B@1.0.0".parse().unwrap(),
// 				"diamond_backtrack_C@1.0.0".parse().unwrap(),
// 			],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"diamond_backtrack_B",
// 			"1.0.0",
// 			&["diamond_backtrack_D@<1.5.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"diamond_backtrack_C",
// 			"1.0.0",
// 			&["diamond_backtrack_D@<1.3.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(tg.as_ref(), "diamond_backtrack_D", "1.1.0", &[])
// 			.await
// 			.unwrap();

// 		create_package(tg.as_ref(), "diamond_backtrack_D", "1.2.0", &[])
// 			.await
// 			.unwrap();

// 		create_package(tg.as_ref(), "diamond_backtrack_D", "1.3.0", &[])
// 			.await
// 			.unwrap();

// 		create_package(tg.as_ref(), "diamond_backtrack_D", "1.4.0", &[])
// 			.await
// 			.unwrap();

// 		create_package(tg.as_ref(), "diamond_backtrack_D", "1.5.0", &[])
// 			.await
// 			.unwrap();
// 	}

// 	#[tokio::test]
// 	async fn cycle_exists() {
// 		let client: Box<dyn tg::Client> = todo!();

// 		create_package(
// 			client.as_ref(),
// 			"cycle_exists_A",
// 			"1.0.0",
// 			&["cycle_exists_B@1.0.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"cycle_exists_B",
// 			"1.0.0",
// 			&["cycle_exists_C@1.0.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"cycle_exists_C",
// 			"1.0.0",
// 			&["cycle_exists_B@1.0.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();
// 	}

// 	#[tokio::test]
// 	async fn diamond_incompatible_versions() {
// 		let client: Box<dyn tg::Client> = todo!();
// 		create_package(
// 			client.as_ref(),
// 			"diamond_incompatible_versions_A",
// 			"1.0.0",
// 			&[
// 				"diamond_incompatible_versions_B@1.0.0".parse().unwrap(),
// 				"diamond_incompatible_versions_C@1.0.0".parse().unwrap(),
// 			],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"diamond_incompatible_versions_B",
// 			"1.0.0",
// 			&["diamond_incompatible_versions_D@<1.2.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"diamond_incompatible_versions_C",
// 			"1.0.0",
// 			&["diamond_incompatible_versions_D@>1.3.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"diamond_incompatible_versions_D",
// 			"1.0.0",
// 			&[],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"diamond_incompatible_versions_D",
// 			"1.1.0",
// 			&[],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"diamond_incompatible_versions_D",
// 			"1.2.0",
// 			&[],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"diamond_incompatible_versions_D",
// 			"1.3.0",
// 			&[],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"diamond_incompatible_versions_D",
// 			"1.4.0",
// 			&[],
// 		)
// 		.await
// 		.unwrap();
// 	}

// 	#[tokio::test]
// 	#[allow(clippy::similar_names)]
// 	async fn diamond_with_path_dependencies() {
// 		let client: Box<dyn tg::Client> = todo!();

// 		let foo = r#"
// 			export let metadata = {
// 				name: "foo",
// 				version: "1.0.0",
// 			};

// 			import bar from "tg:?path=./path/to/bar";
// 			import baz from "tg:baz@^1";
// 			export default tg.target(() => tg`foo ${bar} {baz}`);
// 		"#;

// 		let bar = r#"
// 			export let metadata = {
// 				name: "bar",
// 				version: "1.0.0",
// 			};

// 			import * as baz from "tg:baz@=1.2.3";
// 			export default tg.target(() => tg`bar ${baz}`);
// 		"#;

// 		let baz = r#"
// 			export let metadata = {
// 				name: "baz",
// 				version: "1.2.3",
// 			};

// 			export default tg.target(() => "baz");
// 		"#;
// 	}

// 	#[tokio::test]
// 	async fn complex_diamond() {
// 		let client: Box<dyn tg::Client> = todo!();

// 		create_package(
// 			client.as_ref(),
// 			"complex_diamond_A",
// 			"1.0.0",
// 			&[
// 				"complex_diamond_B@^1.0.0".parse().unwrap(),
// 				"complex_diamond_E@^1.1.0".parse().unwrap(),
// 				"complex_diamond_C@^1.0.0".parse().unwrap(),
// 				"complex_diamond_D@^1.0.0".parse().unwrap(),
// 			],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"complex_diamond_B",
// 			"1.0.0",
// 			&["complex_diamond_D@^1.0.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"complex_diamond_C",
// 			"1.0.0",
// 			&[
// 				"complex_diamond_D@^1.0.0".parse().unwrap(),
// 				"complex_diamond_E@>1.0.0".parse().unwrap(),
// 			],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"complex_diamond_D",
// 			"1.3.0",
// 			&["complex_diamond_E@=1.0.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(
// 			client.as_ref(),
// 			"complex_diamond_D",
// 			"1.2.0",
// 			&["complex_diamond_E@^1.0.0".parse().unwrap()],
// 		)
// 		.await
// 		.unwrap();

// 		create_package(tg.as_ref(), "complex_diamond_E", "1.0.0", &[])
// 			.await
// 			.unwrap();

// 		create_package(tg.as_ref(), "complex_diamond_E", "1.1.0", &[])
// 			.await
// 			.unwrap();
// 	}

// 	async fn create_package(
// 		client: &dyn tg::Client,
// 		name: &str,
// 		version: &str,
// 		dependencies: &[tg::Dependency],
// 	) -> Result<()> {
// 		let imports = dependencies
// 			.iter()
// 			.map(|dep| {
// 				let name = dep.name.as_ref().unwrap();
// 				let version = dep.version.as_ref().unwrap();
// 				format!(r#"import * as {name} from "tg:{name}@{version}";"#)
// 			})
// 			.collect::<Vec<_>>()
// 			.join("\n");
// 		let contents = format!(
// 			r#"
// 				{imports}
// 				export let metadata = {{
// 						name: "{name}",
// 						version: "{version}",
// 				}};
// 				export default tg.target(() => `Hello, from "{name}"!`);
// 			"#
// 		);
// 		let contents = tg::blob::Blob::with_reader(tg, contents.as_bytes())
// 			.await
// 			.unwrap();
// 		let file = tg::File::builder(contents).build();
// 		let package = tg::Directory::with_object(tg::directory::Object {
// 			entries: [(ROOT_MODULE_FILE_NAME.to_owned(), file.into())]
// 				.into_iter()
// 				.collect(),
// 		});
// 		let id = package.id(tg).await?;
// 		client.publish_package(None, &id.clone()).await?;
// 		Ok(())
// 	}
// }
