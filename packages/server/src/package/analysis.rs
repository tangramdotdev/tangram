use crate::Server;
use std::collections::{BTreeMap, HashSet, VecDeque};
use tangram_client as tg;

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug)]
pub struct Analysis {
	pub dependencies: Vec<tg::Dependency>,
	pub metadata: Option<tg::package::Metadata>,
	pub package: tg::Directory,
	pub path_dependencies: BTreeMap<tg::Dependency, Analysis>,
}

impl Server {
	pub async fn try_analyze_package(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Option<Analysis>> {
		// If the dependency has an ID, then use it.
		let analysis = 'a: {
			let Some(id) = dependency.id.clone() else {
				break 'a None;
			};
			let Ok(id) = id.try_into() else {
				break 'a None;
			};
			let package = tg::Directory::with_id(id);
			Some(self.analyze_package(&package).await?)
		};

		// If the dependency has a path, then attempt to analyze the package using the path.
		let analysis = 'a: {
			if let Some(analysis) = analysis {
				break 'a Some(analysis);
			}
			let Some(path) = &dependency.path else {
				break 'a None;
			};
			Some(self.analyze_package_at_path(path).await?)
		};

		// Attempt to get the package from the database.
		let analysis = 'a: {
			if let Some(analysis) = analysis {
				break 'a Some(analysis);
			}
			let Some(mut versions) = self.try_get_package_versions_local(dependency).await? else {
				break 'a None;
			};
			let Some((_, latest)) = versions.pop() else {
				break 'a None;
			};
			let package = tg::Directory::with_id(latest);
			Some(self.analyze_package(&package).await?)
		};

		// Attempt to get the package from the remote.
		let analysis = 'a: {
			if let Some(analysis) = analysis {
				break 'a Some(analysis);
			}
			let Some(remote) = self.remotes.first() else {
				break 'a None;
			};
			let arg = tg::package::get::Arg::default();
			let Some(output) = Box::pin(remote.try_get_package(dependency, arg))
				.await
				.ok()
				.flatten()
			else {
				break 'a None;
			};

			let package = output
				.artifact
				.try_into()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			let package = tg::Directory::with_id(package);
			Some(self.analyze_package(&package).await?)
		};

		Ok(analysis)
	}

	async fn analyze_package(&self, package: &tg::Directory) -> tg::Result<Analysis> {
		let mut visited = BTreeMap::new();
		self.analyze_package_inner(package, tg::Path::default(), package, &mut visited)
			.await
	}

	async fn analyze_package_inner(
		&self,
		root: &tg::Directory,
		path: tg::Path,
		package: &tg::Directory,
		visited: &mut BTreeMap<tg::directory::Id, Option<Analysis>>,
	) -> tg::Result<Analysis> {
		// Check if this package has already been visited.
		let id = package.id(self, None).await?;
		match visited.get(&id) {
			Some(Some(analysis)) => return Ok(analysis.clone()),
			Some(None) => {
				return Err(tg::error!(
					"circular dependencies detected when resolving path dependencies"
				))?
			},
			None => (),
		};

		// Add the package to the visited set with `None` to detect circular dependencies.
		visited.insert(id.clone(), None);

		// Get the root module.
		let root_module_path = path
			.clone()
			.join(tg::package::get_root_module_path(self, &package.clone().into()).await?);

		// Create a queue of module paths to visit and a visited set.
		let mut queue = VecDeque::from(vec![root_module_path]);
		let mut visited_module_paths: HashSet<tg::Path, fnv::FnvBuildHasher> = HashSet::default();

		// Create the path dependencies.
		let mut path_dependencies = BTreeMap::default();

		// Visit each module.
		while let Some(module_path) = queue.pop_front() {
			// Add the module to the package directory.
			let artifact = root
				.get(self, &module_path)
				.await
				.map_err(
					|source| tg::error!(!source, %module_path, %package = id, "failed to get module"),
				)?
				.try_unwrap_file()
				.map_err(
					|source| tg::error!(!source, %module_path, %package = id, "expected a file"),
				)?;

			// Get the module's text.
			let text = artifact.text(self).await.map_err(
				|source| tg::error!(!source, %module_path, %package = id, "failed to read the file"),
			)?;

			// Analyze the module.
			let analysis = crate::compiler::Compiler::analyze_module(text)
				.map_err(|source| tg::error!(!source, "failed to analyze the module"))?;

			// Recurse into the path dependencies.
			let iter = analysis
				.imports
				.iter()
				.filter_map(|import| match &import.specifier {
					tg::import::Specifier::Dependency(
						dependency @ tg::Dependency {
							path: Some(path), ..
						},
					) => Some((dependency.clone(), path.clone())),
					_ => None,
				});
			for (dependency, dependency_path) in iter {
				// Get the dependency path relative to the root
				let path = path.clone().join(dependency_path.clone()).normalize();

				// Get the dependency.
				let package = root
					.get(self, &path)
					.await
					.map_err(
						|source| tg::error!(!source, %path, %package = id, "failed to resolve the dependency"),
					)?
					.try_unwrap_directory()
					.map_err(|source| tg::error!(!source, "expected a directory"))?;

				// Recurse into the path dependency.
				let child =
					Box::pin(self.analyze_package_inner(root, path, &package, visited)).await?;

				// Insert the path dependency.
				path_dependencies.insert(dependency, child);
			}

			// Add the module path to the visited set.
			visited_module_paths.insert(module_path.clone());

			// Add the unvisited path imports to the queue.
			let iter = analysis.imports.iter().filter_map(|import| {
				if let tg::import::Specifier::Path(path) = &import.specifier {
					Some(path.clone())
				} else {
					None
				}
			});
			for imported_module_path in iter {
				if !visited_module_paths.contains(&imported_module_path)
					&& !queue.contains(&imported_module_path)
				{
					queue.push_back(imported_module_path);
				}
			}
		}

		// Get the dependencies.
		let dependencies = self.get_package_dependencies(package).await?;

		// Get the package metadata.
		let metadata = self
			.try_get_package_metadata(&package.clone().into())
			.await?;

		// Create the analysis.
		let package = package.clone();
		let analysis = Analysis {
			dependencies,
			metadata,
			package,
			path_dependencies,
		};

		// Mark the analysis dependencies as visited.
		visited.insert(id.clone(), Some(analysis.clone()));

		Ok(analysis)
	}

	pub(super) async fn analyze_package_at_path(
		&self,
		path: &tg::Path,
	) -> tg::error::Result<Analysis> {
		// Canonicalize the path.
		let path = tokio::fs::canonicalize(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;

		let mut visited = BTreeMap::default();
		self.analyze_package_at_path_inner(&path, &mut visited)
			.await
	}

	async fn analyze_package_at_path_inner(
		&self,
		path: &std::path::Path,
		visited: &mut BTreeMap<std::path::PathBuf, Option<Analysis>>,
	) -> tg::error::Result<Analysis> {
		// Check if the path has already been visited.
		match visited.get(path) {
			Some(Some(analysis)) => return Ok(analysis.clone()),
			Some(None) => return Err(tg::error!("the package has a circular path dependency")),
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
			let module_absolute_path = path.join(module_path.clone());
			let module_absolute_path = tokio::fs::canonicalize(&module_absolute_path)
				.await
				.map_err(|error| {
					tg::error!(source = error, "failed to canonicalize the module path")
				})?;

			// Add the module to the package directory.
			let artifact =
				tg::Artifact::check_in(self, module_absolute_path.clone().try_into()?).await?;
			package = package.add(self, &module_path, artifact).await?;

			// Get the module's text.
			let text = tokio::fs::read_to_string(&module_absolute_path)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the module"))?;

			// Analyze the module.
			let analysis = crate::compiler::Compiler::analyze_module(text)
				.map_err(|source| tg::error!(!source, "failed to analyze the module"))?;

			// Recurse into the path dependencies.
			let iter = analysis
				.imports
				.iter()
				.filter_map(|import| match &import.specifier {
					tg::import::Specifier::Dependency(
						dependency @ tg::Dependency {
							path: Some(path), ..
						},
					) => Some((dependency.clone(), path.clone())),
					_ => None,
				});
			for (mut dependency, dependency_path) in iter {
				// Make the dependency path relative to the package.
				dependency.path.replace(
					module_path
						.clone()
						.parent()
						.join(dependency_path)
						.normalize(),
				);

				// Get the dependency's absolute path.
				let dependency_path = path.join(dependency.path.as_ref().unwrap());
				let dependency_absolute_path =
					tokio::fs::canonicalize(&dependency_path).await.map_err(
						|error| tg::error!(source = error, %dependency, "failed to canonicalize the dependency path"),
					)?;

				// Recurse into the path dependency.
				let child = Box::pin(
					self.analyze_package_at_path_inner(&dependency_absolute_path, visited),
				)
				.await?;

				// Check if this is a child of the root and add it if necessary.
				if let Ok(subpath) = dependency_absolute_path.strip_prefix(path) {
					let subpath = subpath.try_into()?;
					package = package
						.add(self, &subpath, child.package.clone().into())
						.await?;
				}

				// Insert the path dependency.
				path_dependencies.insert(dependency, child);
			}

			// Add the module path to the visited set.
			visited_module_paths.insert(module_path.clone());

			// Add the unvisited path imports to the queue.
			let iter = analysis.imports.iter().filter_map(|import| {
				if let tg::import::Specifier::Path(path) = &import.specifier {
					Some(path.clone())
				} else {
					None
				}
			});
			for imported_module_path in iter {
				if !visited_module_paths.contains(&imported_module_path)
					&& !queue.contains(&imported_module_path)
				{
					queue.push_back(imported_module_path);
				}
			}
		}

		// Create the package.
		let package = package.build();

		// Get the dependencies.
		let dependencies = self.get_package_dependencies(&package).await?;

		// Get the package metadata.
		let metadata = self
			.try_get_package_metadata(&package.clone().into())
			.await?;

		// Create the analysis.
		let analysis = Analysis {
			dependencies,
			metadata,
			package,
			path_dependencies,
		};

		// Mark the analyzed dependencies as visited.
		visited.insert(path.to_owned(), Some(analysis.clone()));

		Ok(analysis)
	}
}
