use crate::{Http, Server};
use async_recursion::async_recursion;
use std::{
	collections::{BTreeMap, HashSet, VecDeque},
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_error::{error, Result};
use tangram_util::http::{empty, full, not_found, Incoming, Outgoing};

mod dependencies;
mod format;
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
	pub async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> Result<Option<tg::package::GetOutput>> {
		// If the dependency has an ID, then use it.
		let package_with_path_dependencies = 'a: {
			let Some(id) = dependency.id.clone() else {
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
			let Some(path) = dependency.path.clone() else {
				break 'a None;
			};
			// If the dependency is a path dependency, then get the package with its path dependencies from the path.
			let path = tokio::fs::canonicalize(PathBuf::from(path))
				.await
				.map_err(|source| error!(!source, "failed to canonicalize the path"))?;
			if !tokio::fs::try_exists(&path).await.map_err(|error| {
				error!(source = error, "failed to get the metadata for the path")
			})? {
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

			// Get the versions.
			let Some(mut versions) = self.try_get_package_versions_local(dependency).await? else {
				break 'a None;
			};

			let Some((_, latest)) = versions.pop() else {
				break 'a None;
			};

			let package = tg::Directory::with_id(latest);
			let package_with_path_dependencies = PackageWithPathDependencies {
				package,
				path_dependencies: BTreeMap::default(),
			};
			Some(package_with_path_dependencies)
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

		// Get or create the lock if requested.
		let lock = if arg.lock {
			if arg.create_lock {
				let lock = self
					.create_package_lock(&package_with_path_dependencies)
					.await?;
				Some(lock.id(self).await?.clone())
			} else {
				let path = dependency.path.as_ref();
				let lock = self
					.get_or_create_package_lock(path, &package_with_path_dependencies)
					.await?;
				let id = lock.id(self).await?.clone();
				Some(id)
			}
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
					"the package has a circular path dependency"
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
				.map_err(|error| {
					error!(source = error, "failed to canonicalize the module path")
				})?;

			// Add the module to the package directory.
			let artifact =
				tg::Artifact::check_in(self, &module_absolute_path.clone().try_into()?).await?;
			package = package.add(self, &module_path, artifact).await?;

			// Get the module's text.
			let text = tokio::fs::read_to_string(&module_absolute_path)
				.await
				.map_err(|source| error!(!source, "failed to read the module"))?;

			// Analyze the module.
			let analysis = crate::language::Server::analyze_module(text)
				.map_err(|source| error!(!source, "failed to analyze the module"))?;

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
				if let tg::Import::Dependency(dependency @ tg::Dependency { path: Some(_), .. }) =
					import
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
						.map_err(|error| {
							error!(source = error, "failed to canonicalize the dependency path")
						})?;

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
				if let tg::Import::Module(import) = import {
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

	pub async fn check_package(&self, dependency: &tg::Dependency) -> Result<Vec<tg::Diagnostic>> {
		// Get the package.
		let (package, lock) = tg::package::get_with_lock(self, dependency).await?;

		// Write the lock.
		if dependency.path.is_some() {
			let path = dependency.path.as_ref().unwrap();
			lock.write(self, path, false)
				.await
				.map_err(|source| error!(!source, "failed to write the lock file"))?;
		}

		// Create the root module.
		let path = tg::package::get_root_module_path(self, &package).await?;
		let package = package.id(self).await?.clone();
		let lock = lock.id(self).await?.clone();
		let module = tg::Module::Normal(tg::module::Normal {
			lock,
			package,
			path,
		});

		// Create the language server.
		let language_server = crate::language::Server::new(self, tokio::runtime::Handle::current());

		// Get the diagnostics.
		let diagnostics = language_server.check(vec![module]).await?;

		Ok(diagnostics)
	}

	pub async fn get_runtime_doc(&self) -> Result<serde_json::Value> {
		// Create the module.
		let module = tg::Module::Library(tg::module::Library {
			path: "tangram.d.ts".parse().unwrap(),
		});

		// Create the language server.
		let language_server = crate::language::Server::new(self, tokio::runtime::Handle::current());

		// Get the doc.
		let doc = language_server.doc(&module).await?;

		Ok(doc)
	}

	pub async fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<serde_json::Value>> {
		// Get the package.
		let Some((package, lock)) = tg::package::try_get_with_lock(self, dependency).await? else {
			return Ok(None);
		};
		if dependency.path.is_some() {
			let path = dependency.path.as_ref().unwrap();
			lock.write(self, path, false)
				.await
				.map_err(|source| error!(!source, "failed to write the lock file"))?;
		}

		// Create the module.
		let path = tg::package::get_root_module_path(self, &package).await?;
		let package = package.id(self).await?.clone();
		let lock = lock.id(self).await?.clone();
		let module = tg::Module::Normal(tg::module::Normal {
			lock,
			package,
			path,
		});

		// Create the language server.
		let language_server = crate::language::Server::new(self, tokio::runtime::Handle::current());

		// Get the doc.
		let doc = language_server.doc(&module).await?;

		Ok(Some(doc))
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
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let dependency = urlencoding::decode(dependency)
			.map_err(|source| error!(!source, "failed to decode the dependency"))?;
		let dependency = dependency
			.parse()
			.map_err(|source| error!(!source, "failed to parse the dependency"))?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.map_err(|source| error!(!source, "failed to deserialize the search params"))?
			.unwrap_or_default();

		// Get the package.
		let Some(output) = self.inner.tg.try_get_package(&dependency, arg).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}

	pub async fn handle_check_package_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "check"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let dependency = urlencoding::decode(dependency)
			.map_err(|source| error!(!source, "failed to decode the dependency"))?;
		let dependency = dependency
			.parse()
			.map_err(|source| error!(!source, "failed to parse the dependency"))?;

		// Check the package.
		let output = self.inner.tg.check_package(&dependency).await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}

	pub async fn handle_format_package_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "format"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let dependency = urlencoding::decode(dependency)
			.map_err(|source| error!(!source, "failed to decode the dependency"))?;
		let dependency = dependency
			.parse()
			.map_err(|source| error!(!source, "failed to parse the dependency"))?;

		// Format the package.
		self.inner.tg.format_package(&dependency).await?;

		// Create the response.
		let response = http::Response::builder().body(empty()).unwrap();

		Ok(response)
	}

	pub async fn handle_get_runtime_doc_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the doc.
		let output = self.inner.tg.get_runtime_doc().await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}

	pub async fn handle_get_package_doc_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "doc"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let dependency = urlencoding::decode(dependency)
			.map_err(|source| error!(!source, "failed to decode the dependency"))?;
		let dependency = dependency
			.parse()
			.map_err(|source| error!(!source, "failed to parse the dependency"))?;

		// Get the doc.
		let Some(output) = self.inner.tg.try_get_package_doc(&dependency).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
