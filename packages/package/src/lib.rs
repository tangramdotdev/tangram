use async_recursion::async_recursion;
use either::Either;
use futures::stream::{FuturesUnordered, TryStreamExt};
use std::{
	collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};
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
}

// A cached representation of a package's metadata and dependencies.
#[derive(Debug, Clone)]
struct Analysis {
	metadata: tg::package::Metadata,
	path_dependencies: BTreeMap<tg::Dependency, tg::directory::Id>,
	dependencies: Vec<tg::Dependency>,
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
	Other(tangram_error::Error),
}

pub async fn get(
	tg: &dyn tg::Handle,
	dependency: &tg::Dependency,
) -> Result<PackageWithPathDependencies> {
	try_get(tg, dependency)
		.await?
		.wrap_err("Failed to get the package.")
}

pub async fn try_get(
	tg: &dyn tg::Handle,
	dependency: &tg::Dependency,
) -> Result<Option<PackageWithPathDependencies>> {
	if let Some(id) = dependency.id.as_ref().cloned() {
		// If the dependency is a path dependency, then get the package with its path dependencies from the path.
		Ok(Some(PackageWithPathDependencies {
			package: tg::Directory::with_id(id),
			path_dependencies: BTreeMap::default(),
		}))
	} else if let Some(path) = dependency.path.as_ref().cloned() {
		// If the dependency is a path dependency, then get the package with its path dependencies from the path.
		let path = tokio::fs::canonicalize(PathBuf::from(path))
			.await
			.wrap_err("Failed to canonicalize the path.")?;
		if !tokio::fs::try_exists(&path)
			.await
			.wrap_err("Failed to get the metadata for the path.")?
		{
			return Ok(None);
		}
		let package = get_package_with_path_dependencies_for_path(tg, &path).await?;
		Ok(Some(package))
	} else {
		// If the dependency is a registry dependency, then get the package from the registry and make the path dependencies be empty.
		let Some(package) = tg::package::try_get(tg, dependency).await? else {
			return Ok(None);
		};
		Ok(Some(PackageWithPathDependencies {
			package,
			path_dependencies: BTreeMap::default(),
		}))
	}
}

// Given a path, create the corresponding PackageWithPathDependencies structure.
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

	// Get the root module path.
	let root_module_path = tg::package::get_root_module_path_for_path(path).await?;

	// Create a queue of module paths to visit and a visited set.
	let mut queue = VecDeque::from(vec![root_module_path]);
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

				// Check if this is a child of the root and add it if necessary.
				if let Ok(subpath) = dependency_absolute_path.strip_prefix(path) {
					let subpath = subpath.try_into()?;
					package = package
						.add(tg, &subpath, child.package.clone().into())
						.await?;
				}

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
				if !visited_module_paths.contains(&imported_module_path)
					&& !queue.contains(&imported_module_path)
				{
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

pub async fn create_lock(
	tg: &dyn tg::Handle,
	path: Option<&tg::Path>,
	package_with_path_dependencies: &PackageWithPathDependencies,
) -> Result<tg::Lock> {
	// If this is a path dependency, then attempt to read the lockfile from the path.
	let lock = if let Some(path) = path {
		try_read_lockfile_from_path(path).await?
	} else {
		None
	};

	// Verify that the lockfile's dependencies match the package with path dependencies.
	let lock = if let Some(lock) = lock {
		let matches = lock_matches(tg, package_with_path_dependencies, &lock).await?;
		if matches {
			Some(lock)
		} else {
			None
		}
	} else {
		None
	};

	// Otherwise, create the lockfile.
	let lock_created = lock.is_none();
	let lock = if let Some(lockfile) = lock {
		lockfile
	} else {
		create_lockfile(tg, package_with_path_dependencies).await?
	};

	// If this is a path dependency and the lockfile was created, then write the lockfile.
	if let Some(path) = path {
		if lock_created {
			write_lock(tg, path, &lock).await?;
		}
	}

	// Fill in path dependencies.
	let lock = resolve_path_dependencies(tg, lock, package_with_path_dependencies)
		.await?
		.fold(tg)
		.await?;

	Ok(lock)
}

pub async fn get_dependencies(
	tg: &dyn tg::Handle,
	package: &tg::Directory,
) -> Result<Vec<tg::Dependency>> {
	// Create the dependencies set.
	let mut dependencies: BTreeSet<tg::Dependency> = BTreeSet::default();

	// Get the root module path.
	let root_module_path = tg::package::get_root_module_path(tg, package).await?;

	// Create a queue of module paths to visit and a visited set.
	let mut queue: VecDeque<tg::Path> = VecDeque::from(vec![root_module_path]);
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

pub async fn get_metadata(
	tg: &dyn tg::Handle,
	package: &tg::Directory,
) -> Result<tg::package::Metadata> {
	let path = tg::package::get_root_module_path(tg, package).await?;
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
		Err(error!("Missing package metadata."))
	}
}

async fn try_read_lockfile_from_path(path: &tg::Path) -> Result<Option<tg::Lock>> {
	// Canonicalize the path.
	let path = tokio::fs::canonicalize(PathBuf::from(path.clone()))
		.await
		.wrap_err("Failed to canonicalize the path.")?;

	// Attempt to read the lockfile.
	let path = path.join(tg::package::LOCKFILE_FILE_NAME);
	let exists = tokio::fs::try_exists(&path)
		.await
		.wrap_err("Failed to determine if the lockfile exists.")?;
	if !exists {
		return Ok(None);
	}
	let lock = tokio::fs::read(&path)
		.await
		.wrap_err("Failed to read the lockfile.")?;
	let lock: tg::lock::Data =
		serde_json::from_slice(&lock).wrap_err("Failed to deserialize the lockfile.")?;

	let root = lock.root;
	let nodes = lock
		.nodes
		.into_iter()
		.map(|node| {
			let dependencies = node
				.dependencies
				.into_iter()
				.map(|(dependency, entry)| {
					let package = entry.package.map(tg::Directory::with_id);
					let lock = entry.lock.map_right(tg::Lock::with_id);
					(dependency, tg::lock::Entry { package, lock })
				})
				.collect();
			tg::lock::Node { dependencies }
		})
		.collect::<Vec<_>>();
	let object = tg::lock::Object { root, nodes };
	let lock = tg::Lock::with_object(object);
	Ok(Some(lock))
}

async fn write_lock(tg: &dyn tg::Handle, path: &tg::Path, lock: &tg::Lock) -> Result<()> {
	let package_path = tokio::fs::canonicalize(PathBuf::from(path.clone()))
		.await
		.wrap_err("Failed to canonicalize the path.")?;
	let lock_path = package_path.join(tg::package::LOCKFILE_FILE_NAME);
	let lock = lock.data(tg).await?;
	let lock = serde_json::to_vec_pretty(&lock).wrap_err("Failed to serialize the lockfile.")?;
	tokio::fs::write(lock_path, lock)
		.await
		.wrap_err("Failed to write the lockfile.")?;
	Ok(())
}

async fn lock_matches(
	tg: &dyn Handle,
	package_with_path_dependencies: &PackageWithPathDependencies,
	lock: &tg::Lock,
) -> Result<bool> {
	let object = lock.object(tg).await?;
	lockfile_matches_inner(
		tg,
		package_with_path_dependencies,
		&object.nodes,
		object.root,
	)
	.await
}

#[async_recursion]
async fn lockfile_matches_inner(
	tg: &dyn Handle,
	package_with_path_dependencies: &PackageWithPathDependencies,
	nodes: &[tg::lock::Node],
	index: usize,
) -> Result<bool> {
	// Get the package's dependencies.
	let dependencies = get_dependencies(tg, &package_with_path_dependencies.package).await?;

	// Get the package's lock from the lockfile.
	let node = nodes.get(index).wrap_err("Invalid lockfile.")?;

	// Verify that the dependencies match.
	if !itertools::equal(node.dependencies.keys(), dependencies.iter()) {
		return Ok(false);
	}

	// Recurse into the path dependencies.
	package_with_path_dependencies
		.path_dependencies
		.keys()
		.map(|dependency| async move {
			let index = *node
				.dependencies
				.get(dependency)
				.unwrap()
				.lock
				.as_ref()
				.unwrap_left();
			lockfile_matches_inner(tg, package_with_path_dependencies, nodes, index).await
		})
		.collect::<FuturesUnordered<_>>()
		.try_all(|matches| async move { matches })
		.await?;

	Ok(true)
}

async fn create_lockfile(
	tg: &dyn tg::Handle,
	package_with_path_dependencies: &PackageWithPathDependencies,
) -> Result<tg::Lock> {
	// Construct the version solving context and working set.
	let mut analysis = BTreeMap::new();
	let mut working_set = im::Vector::new();

	scan_package_with_path_dependencies(
		tg,
		package_with_path_dependencies,
		&mut analysis,
		&mut working_set,
	)
	.await?;
	let published_packages = im::HashMap::new();
	let mut context = Context {
		analysis,
		published_packages,
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
		return Err(error!("{report}"));
	}

	// Create the nodes.
	let root = package_with_path_dependencies.package.id(tg).await?;
	let mut nodes = Vec::new();
	let root = create_lockfile_inner(tg, root, &context, &solution, &mut nodes).await?;

	// Convert from data to object and create the lock.
	let nodes = nodes
		.into_iter()
		.map(|node| {
			let dependencies = node
				.dependencies
				.into_iter()
				.map(|(dependency, lock)| {
					let package = lock.package.map(tg::Directory::with_id);
					let lock = lock.lock.map_right(tg::Lock::with_id);
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

#[allow(clippy::only_used_in_recursion)]
#[async_recursion]
async fn create_lockfile_inner(
	tg: &dyn tg::Handle,
	package: &tg::directory::Id,
	context: &Context,
	solution: &Solution,
	nodes: &mut Vec<tg::lock::data::Node>,
) -> Result<usize> {
	// Get the cached analysis.
	let analysis = context
		.analysis
		.get(package)
		.wrap_err("Missing package in solution.")?;

	// Recursively create the nodes.
	let mut dependencies = BTreeMap::new();
	for dependency in &analysis.dependencies {
		// Check if this is resolved as a path dependency.
		let (resolved, is_registry_dependency) =
			if let Some(resolved) = analysis.path_dependencies.get(dependency) {
				(resolved, false)
			} else {
				// Resolve by dependant.
				let dependant = Dependant {
					package: package.clone(),
					dependency: dependency.clone(),
				};
				let Some(Mark::Permanent(Ok(resolved))) = solution.partial.get(&dependant) else {
					return Err(error!("Missing solution for {dependant:?}."));
				};
				(resolved, true)
			};
		let package = is_registry_dependency.then_some(resolved.clone());
		let lock =
			Either::Left(create_lockfile_inner(tg, resolved, context, solution, nodes).await?);
		let entry = tg::lock::data::Entry { package, lock };
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

#[async_recursion]
async fn scan_package_with_path_dependencies(
	tg: &dyn tg::Handle,
	package_with_path_dependencies: &PackageWithPathDependencies,
	all_analysis: &mut BTreeMap<tg::directory::Id, Analysis>,
	working_set: &mut im::Vector<Dependant>,
) -> Result<()> {
	let PackageWithPathDependencies {
		package,
		path_dependencies,
	} = package_with_path_dependencies;
	let package_id = package.id(tg).await?.clone();

	// Check if we've already visited this dependency.
	if all_analysis.contains_key(&package_id) {
		return Ok(());
	}

	// Get the metadata and dependenencies of this package.
	let metadata = get_metadata(tg, package).await?;
	let dependencies = get_dependencies(tg, package).await?;

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
		path_dependencies: BTreeMap::new(),
	};
	all_analysis.insert(package_id.clone(), analysis);

	// Recurse.
	for (dependency, package_with_path_dependencies) in path_dependencies {
		let dependency_package_id = package_with_path_dependencies.package.id(tg).await?.clone();
		let analysis = all_analysis.get_mut(&package_id).unwrap();
		analysis
			.path_dependencies
			.insert(dependency.clone(), dependency_package_id.clone());
		scan_package_with_path_dependencies(
			tg,
			package_with_path_dependencies,
			all_analysis,
			working_set,
		)
		.await?;
	}

	Ok(())
}

async fn resolve_path_dependencies(
	tg: &dyn tg::Handle,
	lock: tg::lock::Lock,
	package_with_path_dependencies: &PackageWithPathDependencies,
) -> Result<tg::lock::Lock> {
	let mut object = lock.object(tg).await?.clone();
	resolve_path_dependencies_inner(
		package_with_path_dependencies,
		&mut object.nodes,
		object.root,
	);
	Ok(tg::Lock::with_object(object))
}

fn resolve_path_dependencies_inner(
	package_with_path_dependencies: &PackageWithPathDependencies,
	nodes: &mut [tg::lock::Node],
	index: usize,
) {
	for (dependency, package_with_path_dependencies) in
		&package_with_path_dependencies.path_dependencies
	{
		let Some(entry) = nodes[index].dependencies.get_mut(dependency) else {
			continue;
		};
		let index = *entry.lock.as_ref().left().unwrap();
		entry.package = Some(package_with_path_dependencies.package.clone());
		resolve_path_dependencies_inner(package_with_path_dependencies, nodes, index);
	}
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

		// Get the permanent and partial solutions for this edge.
		let permanent = current_frame.solution.get_permanent(context, &dependant);
		let partial = current_frame.solution.partial.get(&dependant);

		match (permanent, partial) {
			// Case 0: There is no solution for this package yet.
			(None, None) => {
				// Attempt to override with a path dependency.
				let package = match context.try_resolve_path_dependency(tg, &dependant).await {
					Ok(Some(resolved)) => Ok(resolved),

					// If we cannot resolve as a path dependency, attempt to resolve it as a registry dependency.
					Ok(None) => 'a: {
						// Make sure we have a list of versions we can try to resolve.
						if current_frame.remaining_versions.is_none() {
							match context.get_all_versions(tg, &dependant).await {
								Ok(versions) => {
									current_frame.remaining_versions = Some(versions);
								},
								Err(e) => break 'a Err(Error::Other(e)),
							}
						}
						context
							.try_resolve_registry_dependency(
								tg,
								&dependant,
								current_frame.remaining_versions.as_mut().unwrap(),
							)
							.await
					},
					Err(e) => Err(Error::Other(e)),
				};

				// Update the working set and solution.
				match package {
					// We successfully got a version.
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

						// Update the stack. If we backtrack, we use the next version in the version stack.
						history.push_back(current_frame.clone());
					},
					// Something went wrong. Mark permanently as an error.
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
						match dependant.dependency.try_match_version(&version) {
							// Success: we can use this version.
							Ok(true) => {
								next_frame.solution = next_frame.solution.mark_permanently(
									context,
									dependant,
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
									tracing::error!(?dependant, "No solution exists.");
									// There is no solution for this package. Add an error.
									next_frame.solution = next_frame.solution.mark_permanently(
										context,
										dependant,
										Err(error),
									);
								}
							},
							// This can only occur if the there is an error parsing the version constraint of this dependency. It cannot be solved, so mark permanently as an error.
							Err(e) => {
								next_frame.solution = next_frame.solution.mark_permanently(
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
	// Attempt to resolve `dependant` as a path dependency.
	async fn try_resolve_path_dependency(
		&mut self,
		tg: &dyn Handle,
		dependant: &Dependant,
	) -> Result<Option<tg::directory::Id>> {
		let Dependant {
			package,
			dependency,
		} = dependant;
		if dependency.path.is_none() {
			return Ok(None);
		};
		let analysis = self.try_get_analysis(tg, package).await?;
		Ok(analysis.path_dependencies.get(dependency).cloned())
	}

	// Attempt to resolve `dependant` as a registry dependency.
	async fn try_resolve_registry_dependency(
		&mut self,
		tg: &dyn tg::Handle,
		dependant: &Dependant,
		remaining_versions: &mut im::Vector<String>,
	) -> Result<tg::directory::Id, Error> {
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
			let Some(package) = tg::package::try_get(tg, &dependency)
				.await
				.map_err(Error::Other)?
			else {
				continue;
			};
			let id = package.id(tg).await.map_err(Error::Other)?.clone();
			self.published_packages.insert(metadata, id.clone());
			return Ok(id);
		}
	}

	// Try to lookup the cached analysis for a package by its ID. If it is missing from the cache, we ask the server to analyze the package. If we cannot, we fail.
	async fn try_get_analysis(
		&mut self,
		tg: &dyn tg::Handle,
		package_id: &tg::directory::Id,
	) -> Result<&'_ Analysis> {
		if !self.analysis.contains_key(package_id) {
			let package = tg::Directory::with_id(package_id.clone());
			let metadata = get_metadata(tg, &package).await?;
			let dependencies = get_dependencies(tg, &package).await?;

			let mut dependencies_ = Vec::new();
			let mut path_dependencies = BTreeMap::new();
			let package_source = tg::Directory::with_id(package_id.clone());

			// Attempt to resolve path dependencies.
			for dependency in dependencies {
				if let Some(path) = dependency.path.as_ref() {
					if path.is_absolute() {
						continue;
					}
					let path = path.clone().normalize();
					let Some(dependency_source) =
						package_source.try_get(tg, &path).await.wrap_err_with(|| {
							error!("Could not resolve {dependency} within {package_id}.")
						})?
					else {
						continue;
					};
					let dependency_package_id = dependency_source
						.try_unwrap_directory()
						.wrap_err_with(|| error!("Expected {path} to refer to a directory."))?
						.id(tg)
						.await?
						.clone();
					path_dependencies.insert(dependency.clone(), dependency_package_id);
				}
				dependencies_.push(dependency);
			}

			let analysis = Analysis {
				metadata: metadata.clone(),
				dependencies: dependencies_,
				path_dependencies,
			};

			self.published_packages
				.insert(metadata.clone(), package_id.clone());
			self.analysis.insert(package_id.clone(), analysis);
		}
		Ok(self.analysis.get(package_id).unwrap())
	}

	// Check if the dependant can be resolved as a path dependency. Returns an error if we haven't analyzed the `dependant.package` yet.
	fn is_path_dependency(&self, dependant: &Dependant) -> Result<bool> {
		// We guarantee that the context already knows about the dependant package by the time this function is called.
		let Some(analysis) = self.analysis.get(&dependant.package) else {
			tracing::error!(?dependant, "Missing analysis.");
			return Err(error!("Internal error: {dependant:?} missing analysis."));
		};
		Ok(analysis
			.path_dependencies
			.contains_key(&dependant.dependency))
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
	) -> Result<im::Vector<String>> {
		// If it is a path dependency, we don't care about the versions, which may not exist.
		if self.is_path_dependency(dependant).unwrap() {
			return Ok(im::Vector::new());
		};

		// Get a list of all the corresponding metadata for versions that satisfy the constraint.
		let Dependant { dependency, .. } = dependant;

		// Filter the versions to only those that satisfy the constraint.
		let metadata = tg
			.get_package_versions(dependency)
			.await?
			.into_iter()
			.filter_map(|version| {
				dependency
					.try_match_version(&version)
					.ok()?
					.then_some(version)
			})
			.collect();

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
		dependant: Dependant,
		complete: Result<tg::directory::Id, Error>,
	) -> Self {
		let mut solution = self.clone();

		// Update the local solution.
		solution
			.partial
			.insert(dependant.clone(), Mark::Permanent(complete.clone()));

		// If this is not a path dependency then we add it to the global solution.
		if !context.is_path_dependency(&dependant).unwrap() {
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
