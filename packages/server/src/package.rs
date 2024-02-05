use crate::{database::Database, postgres_params, Http, Server};
use async_recursion::async_recursion;
use std::{
	collections::{BTreeMap, HashSet, VecDeque},
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};
use tangram_util::http::{full, not_found, Incoming, Outgoing};

mod dependencies;
mod lock;
mod metadata;
mod publish;
mod search;
mod versions;

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug)]
struct PackageWithPathDependencies {
	package: tg::Directory,
	path_dependencies: BTreeMap<tg::Dependency, PackageWithPathDependencies>,
}

impl Server {
	#[allow(clippy::too_many_lines)]
	pub async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> Result<Option<tg::package::GetOutput>> {
		// If the dependency has an ID, then use it.
		let package_with_path_dependencies = 'a: {
			let Some(id) = dependency.id.as_ref().cloned() else {
				break 'a None;
			};
			Some(PackageWithPathDependencies {
				package: tg::Directory::with_id(id),
				path_dependencies: BTreeMap::default(),
			})
		};

		// If the dependency has a path, then attempt to get the package with path dependency with the path.
		let package_with_path_dependencies = 'a: {
			if let Some(package_with_path_dependencies) = package_with_path_dependencies {
				break 'a Some(package_with_path_dependencies);
			}
			let Some(path) = dependency.path.as_ref().cloned() else {
				break 'a None;
			};
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
			let package_with_path_dependencies = self
				.get_package_with_path_dependencies_with_path(&path)
				.await?;
			Some(package_with_path_dependencies)
		};

		// Attempt to get the package from the database.
		let package_with_path_dependencies = 'a: {
			if let Some(package_with_path_dependencies) = package_with_path_dependencies {
				break 'a Some(package_with_path_dependencies);
			}

			// The dependency must have a name.
			let name = dependency
				.name
				.as_ref()
				.wrap_err("Expected the dependency to have a name.")?;

			// Get the versions.
			let Database::Postgres(database) = &self.inner.database else {
				break 'a None;
			};
			let connection = database.get().await?;
			let statement = "
				select version, id
				from package_versions
				where name = $1;
			";
			let params = postgres_params![name];
			let statement = connection
				.prepare_cached(statement)
				.await
				.wrap_err("Failed to prepare the statement.")?;
			let rows = connection
				.query(&statement, params)
				.await
				.wrap_err("Failed to execute the statement.")?;
			let mut versions = rows
				.into_iter()
				.map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
				.map(|(version, id)| {
					let version = version.parse().wrap_err("Invalid version.")?;
					let id = id.parse()?;
					Ok((version, id))
				})
				.collect::<Result<Vec<(semver::Version, tg::directory::Id)>>>()?;

			// Find the latest compatible version.
			versions.sort_unstable_by_key(|(version, _)| version.clone());
			versions.reverse();
			let package = if let Some(version) = dependency.version.as_ref() {
				let req: semver::VersionReq = version.parse().wrap_err("Invalid version.")?;
				versions
					.iter()
					.find(|(version, _)| req.matches(version))
					.map(|(_, id)| tg::Directory::with_id(id.clone()))
			} else {
				versions
					.last()
					.map(|(_, id)| tg::Directory::with_id(id.clone()))
			};

			package.map(|package| PackageWithPathDependencies {
				package,
				path_dependencies: BTreeMap::default(),
			})
		};

		// Attempt to get the package from the remote.
		let package_with_path_dependencies = 'a: {
			if let Some(package_with_path_dependencies) = package_with_path_dependencies {
				break 'a Some(package_with_path_dependencies);
			}

			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a None;
			};

			let arg = tg::package::GetArg::default();
			let Some(output) = remote.try_get_package(dependency, arg).await.ok().flatten() else {
				break 'a None;
			};

			let package = tg::Directory::with_id(output.id);

			let package_with_path_dependencies = PackageWithPathDependencies {
				package,
				path_dependencies: BTreeMap::default(),
			};

			Some(package_with_path_dependencies)
		};

		// If the package was not found, then return `None`.
		let Some(package_with_path_dependencies) = package_with_path_dependencies else {
			return Ok(None);
		};

		// Get the package.
		let package = package_with_path_dependencies.package.clone();

		// Get the dependencies if requested.
		let dependencies = if arg.dependencies {
			let dependencies = self.get_package_dependencies(&package).await?;
			Some(dependencies)
		} else {
			None
		};

		// Create the lock if requested.
		let lock = if arg.lock {
			let path = dependency.path.as_ref();
			let lock = self
				.get_or_create_package_lock(path, &package_with_path_dependencies)
				.await?;
			let id = lock.id(self).await?.clone();
			Some(id)
		} else {
			None
		};

		// Get the metadata if requested.
		let metadata = if arg.metadata {
			let metadata = self.get_package_metadata(&package).await?;
			Some(metadata)
		} else {
			None
		};

		// Get the package ID.
		let id = package.id(self).await?.clone();

		Ok(Some(tg::package::GetOutput {
			dependencies,
			id,
			lock,
			metadata,
		}))
	}

	async fn get_package_with_path_dependencies_with_path(
		&self,
		path: &Path,
	) -> tangram_error::Result<PackageWithPathDependencies> {
		let mut visited = BTreeMap::default();
		self.get_package_with_path_dependencies_with_path_inner(path, &mut visited)
			.await
	}

	#[allow(clippy::too_many_lines)]
	#[async_recursion]
	async fn get_package_with_path_dependencies_with_path_inner(
		&self,
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
				tg::Artifact::check_in(self, &module_absolute_path.clone().try_into()?).await?;
			package = package.add(self, &module_path, artifact).await?;

			// Get the module's text.
			let text = tokio::fs::read_to_string(&module_absolute_path)
				.await
				.wrap_err("Failed to read the module.")?;

			// Analyze the module.
			let analysis = tangram_language::Module::analyze(text)
				.wrap_err("Failed to analyze the module.")?;

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
					tg::Artifact::check_in(self, &included_artifact_absolute_path).await?;

				// Add the included artifact to the directory.
				package = package
					.add(self, &included_artifact_path, included_artifact)
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
					let child = self
						.get_package_with_path_dependencies_with_path_inner(
							&dependency_absolute_path,
							visited,
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
}

impl Http {
	pub async fn handle_get_package_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let dependency =
			urlencoding::decode(dependency).wrap_err("Failed to decode the dependency.")?;
		let dependency = dependency
			.parse()
			.wrap_err("Failed to parse the dependency.")?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.wrap_err("Failed to deserialize the search params.")?
			.unwrap_or_default();

		// Get the package.
		let Some(output) = self.inner.tg.try_get_package(&dependency, arg).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;

		// Create the response.
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}
}
