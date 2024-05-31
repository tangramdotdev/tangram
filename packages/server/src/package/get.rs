use crate::Server;
use indoc::formatdoc;
use std::collections::{BTreeMap, HashSet, VecDeque};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::get::Arg,
	) -> tg::Result<Option<tg::package::get::Output>> {
		if let Some(remote) = dependency.remote.as_ref() {
			// If the dependency has a remote, then attempt to get the package from the remote.
			let Some(remote) = self.remotes.get(remote) else {
				return Ok(None);
			};
			let arg = tg::package::get::Arg::default();
			let dependency = tg::Dependency {
				remote: None,
				..dependency.clone()
			};
			let Some(output) = remote.try_get_package(&dependency, arg).await? else {
				return Ok(None);
			};
			Ok(Some(output))
		} else if let Some(artifact) = dependency.artifact.clone() {
			// If the dependency has an artifact, then attempt to get the package with the artifact.
			let artifact = tg::Artifact::with_id(artifact);
			let Some(output) = self.try_get_package_with_artifact(&artifact, &arg).await? else {
				return Ok(None);
			};
			Ok(Some(output))
		} else if let Some(path) = &dependency.path {
			// If the dependency has a path, then attempt to get the package with the path.
			let Some(output) = self.try_get_package_with_path(path, &arg).await? else {
				return Ok(None);
			};
			Ok(Some(output))
		} else if let Some(remote) = self.options.registry.as_ref() {
			// If the server's registry is remote, then attempt to get the package from the remote.
			let Some(remote) = self.remotes.get(remote) else {
				return Ok(None);
			};
			let arg = tg::package::get::Arg::default();
			let Some(output) = remote.try_get_package(dependency, arg).await? else {
				return Ok(None);
			};
			Ok(Some(output))
		} else {
			// Otherwise, attempt to get the package locally.
			let arg_ = tg::package::versions::Arg {
				yanked: arg.yanked
			};
			let Some(mut versions) = self.try_get_package_versions_local(dependency, arg_).await? else {
				return Ok(None);
			};
			let Some((_, artifact)) = versions.pop() else {
				return Ok(None);
			};
			let artifact = tg::Artifact::with_id(artifact);
			let Some(output) = self.try_get_package_with_artifact(&artifact, &arg).await? else {
				return Ok(None);
			};
			Ok(Some(output))
		}
	}

	async fn try_get_package_with_artifact(
		&self,
		artifact: &tg::Artifact,
		arg: &tg::package::get::Arg,
	) -> tg::Result<Option<tg::package::get::Output>> {
		let path = tg::Path::default();
		let mut visited = BTreeMap::new();
		self.try_get_package_with_artifact_inner(artifact, path, artifact, arg, &mut visited)
			.await
	}

	async fn try_get_package_with_artifact_inner(
		&self,
		root: &tg::Artifact,
		path: tg::Path,
		artifact: &tg::Artifact,
		arg: &tg::package::get::Arg,
		visited: &mut BTreeMap<tg::artifact::Id, Option<tg::package::get::Output>>,
	) -> tg::Result<Option<tg::package::get::Output>> {
		// Check if this package has already been visited.
		let id = artifact.id(self, None).await?;
		match visited.get(&id) {
			Some(Some(output)) => return Ok(Some(output.clone())),
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
		let root_module_path = tg::package::get_root_module_path(self, artifact).await?;
		let root_module_path = path.clone().join(root_module_path);

		// Create a queue of module paths to visit and a visited set.
		let kind = tg::import::Kind::try_from_path(&root_module_path);
		let mut queue = VecDeque::from(vec![(root_module_path, kind)]);
		let mut visited_modules: HashSet<
			(tg::Path, Option<tg::import::Kind>),
			fnv::FnvBuildHasher,
		> = HashSet::default();

		// Create the path dependencies.
		let mut dependencies = BTreeMap::default();

		// Visit each module.
		while let Some((module_path, kind)) = queue.pop_front() {
			// Get the module.
			let module = root
				.try_unwrap_directory_ref()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?
				.get(self, &module_path)
				.await
				.map_err(
					|source| tg::error!(!source, %module_path, %package = id, "failed to get the module"),
				)?;

			// Add the module to the visited modules set.
			visited_modules.insert((module_path.clone(), kind));

			// If this is not a JavaScript or TypeScript module, then continue.
			if !matches!(kind, Some(tg::import::Kind::Js | tg::import::Kind::Ts)) {
				continue;
			}

			// Expect the module to be a file.
			let module = module.try_unwrap_file().map_err(
				|source| tg::error!(!source, %module_path, %package = id, "expected a file"),
			)?;

			// Get the module's text.
			let text = module.text(self).await.map_err(
				|source| tg::error!(!source, %module_path, %package = id, "failed to read the file"),
			)?;

			// Analyze the module.
			let analysis = crate::compiler::Compiler::analyze_module(text)
				.map_err(|source| tg::error!(!source, "failed to analyze the module"))?;

			// Add the dependencies.
			for import in &analysis.imports {
				if let tg::import::Specifier::Dependency(dependency) = &import.specifier {
					dependencies.entry(dependency.clone()).or_insert(None);
				}
			}

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
				let artifact = root
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?
					.get(self, &path)
					.await
					.map_err(
						|source| tg::error!(!source, %path, %package = id, "failed to resolve the dependency"),
					)?;

				// Recurse into the path dependency.
				let arg = tg::package::get::Arg {
					dependencies: true,
					..Default::default()
				};
				let child = Box::pin(
					self.try_get_package_with_artifact_inner(root, path, &artifact, &arg, visited),
				)
				.await?
				.ok_or_else(|| tg::error!("failed to get the package"))?;

				// Insert the path dependency.
				dependencies.insert(dependency, Some(child));
			}

			// Add the unvisited path imports to the queue.
			let iter = analysis.imports.iter().filter_map(|import| {
				if let tg::import::Specifier::Path(path) = &import.specifier {
					let path = module_path.clone().parent().join(path.clone()).normalize();
					Some((path, import.kind))
				} else {
					None
				}
			});
			for entry in iter {
				if !visited_modules.contains(&entry) && !queue.contains(&entry) {
					queue.push_back(entry);
				}
			}
		}

		// Create the lock if requested.
		let lock = if arg.lock {
			let lock = self
				.get_or_create_lock(&id, None, &dependencies, arg.locked)
				.await?;
			let lock = lock.id(self, None).await?;
			Some(lock)
		} else {
			None
		};

		// Get the dependencies if requested.
		let dependencies = if arg.dependencies {
			Some(dependencies)
		} else {
			None
		};

		// Get the metadata if requested.
		let metadata = if arg.metadata {
			self.try_get_package_metadata(artifact).await?
		} else {
			None
		};

		// Get the path if requested.
		let path = None;

		// Get the yanked flag if requested.
		let yanked = if arg.yanked {
			self.try_get_package_yanked(&id).await?
		} else {
			None
		};

		// Create the output.
		let output = tg::package::get::Output {
			artifact: id.clone(),
			dependencies,
			lock,
			metadata,
			path,
			yanked,
		};

		// Add to the visited set.
		visited.insert(id.clone(), Some(output.clone()));

		Ok(Some(output))
	}

	async fn try_get_package_with_path(
		&self,
		path: &tg::Path,
		arg: &tg::package::get::Arg,
	) -> tg::error::Result<Option<tg::package::get::Output>> {
		let mut visited = BTreeMap::default();
		self.try_get_package_with_path_inner(path, arg, &mut visited)
			.await
	}

	async fn try_get_package_with_path_inner(
		&self,
		path: &tg::Path,
		arg: &tg::package::get::Arg,
		visited: &mut BTreeMap<tg::Path, Option<tg::package::get::Output>>,
	) -> tg::error::Result<Option<tg::package::get::Output>> {
		// Check if the path has already been visited.
		match visited.get(path) {
			Some(Some(output)) => return Ok(Some(output.clone())),
			Some(None) => return Err(tg::error!("the package has a circular path dependency")),
			None => (),
		}

		// Add the path to the visited set with `None` to detect circular dependencies.
		visited.insert(path.to_owned(), None);

		// Create a builder for the artifact.
		let mut builder = tg::directory::Builder::default();

		// Get the root module path.
		let root_module_path = tg::package::get_root_module_path_for_path(path.as_ref()).await?;

		// Create a queue of module paths to visit and a visited set.
		let kind = tg::import::Kind::try_from_path(&root_module_path);
		let mut queue = VecDeque::from(vec![(root_module_path, kind)]);
		let mut visited_modules: HashSet<
			(tg::Path, Option<tg::import::Kind>),
			fnv::FnvBuildHasher,
		> = HashSet::default();

		// Create the dependencies.
		let mut dependencies = BTreeMap::default();

		// Visit each module.
		while let Some((module_path, kind)) = queue.pop_front() {
			// Get the module's absolute path.
			let module_absolute_path = path.clone().join(module_path.clone());

			// Add the module to the package directory.
			let artifact =
				tg::Artifact::check_in(self, module_absolute_path.clone())
				.await
				.map_err(|source| tg::error!(!source, %referrer = path, %module = module_absolute_path, "failed to check in the module"))?;
			builder = builder.add(self, &module_path, artifact).await?;

			// Add the module path to the visited set.
			visited_modules.insert((module_path.clone(), kind));

			// If this is not a JavaScript or TypeScript module, then continue.
			if !matches!(kind, Some(tg::import::Kind::Js | tg::import::Kind::Ts)) {
				continue;
			}

			// Get the module's text.
			let text = tokio::fs::read_to_string(&module_absolute_path)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the module"))?;

			// Analyze the module.
			let analysis = crate::compiler::Compiler::analyze_module(text)
				.map_err(|source| tg::error!(!source, "failed to analyze the module"))?;

			// Add the dependencies.
			for import in &analysis.imports {
				if let tg::import::Specifier::Dependency(dependency) = &import.specifier {
					dependencies.entry(dependency.clone()).or_insert(None);
				}
			}

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

				// Get the dependency's path.
				let dependency_path = dependency.path.as_ref().unwrap().clone();
				let dependency_path = path.clone().join(dependency_path);

				// Recurse into the path dependency, clearing the lock flag.
				let arg = tg::package::get::Arg {
					dependencies: true,
					..Default::default()
				};
				let child =
					Box::pin(self.try_get_package_with_path_inner(&dependency_path, &arg, visited))
						.await?
						.ok_or_else(|| tg::error!("failed to get the package"))?;

				// Check if this is a child of the root and add it if necessary.
				if let Some(subpath) = dependency_path.diff(path) {
					let artifact = tg::Artifact::with_id(child.artifact.clone());
					builder = builder.add(self, &subpath, artifact).await?;
				}

				// Insert the dependency.
				dependencies.insert(dependency, Some(child));
			}

			// Add the unvisited path imports to the queue.
			let iter = analysis.imports.iter().filter_map(|import| {
				if let tg::import::Specifier::Path(path) = &import.specifier {
					let path = module_path.clone().parent().join(path.clone()).normalize();
					Some((path, import.kind))
				} else {
					None
				}
			});
			for entry in iter {
				if !visited_modules.contains(&entry) && !queue.contains(&entry) {
					queue.push_back(entry);
				}
			}
		}

		// Create the artifact.
		let artifact: tg::Artifact = builder.build().into();
		let id = artifact.id(self, None).await?;

		// Create the lock if requested.
		let lock = if arg.lock {
			let lock = self
				.get_or_create_lock(&id, Some(path), &dependencies, arg.locked)
				.await?;
			let lock = lock.id(self, None).await?;
			Some(lock)
		} else {
			None
		};

		// Get the dependencies if requested.
		let dependencies = if arg.dependencies {
			Some(dependencies)
		} else {
			None
		};

		// Get the metadata if requested.
		let metadata = if arg.metadata {
			self.try_get_package_metadata(&artifact).await?
		} else {
			None
		};

		// Get the yanked flag if requested.
		let yanked = if arg.yanked {
			self.try_get_package_yanked(&id).await?
		} else {
			None
		};

		// Create the output.
		let output = tg::package::get::Output {
			artifact: id.clone(),
			dependencies,
			lock,
			metadata,
			path: Some(path.clone()),
			yanked,
		};

		// Add to the visited set.
		visited.insert(path.clone(), Some(output.clone()));

		Ok(Some(output))
	}

	async fn try_get_package_metadata(
		&self,
		artifact: &tg::Artifact,
	) -> tg::Result<Option<tg::package::Metadata>> {
		let Some(path) = tg::package::try_get_root_module_path(self, artifact).await? else {
			return Ok(None);
		};
		let package = artifact.unwrap_directory_ref();
		let file = package
			.get(self, &path)
			.await?
			.try_unwrap_file()
			.ok()
			.ok_or_else(|| tg::error!(%path, "expected the module to be a file"))?;
		let text = file.text(self).await?;
		let metadata = crate::compiler::Compiler::analyze_module(text)
			.map_err(|source| tg::error!(!source, %path, "failed to analyze module"))?
			.metadata;
		Ok(metadata)
	}

	async fn try_get_package_yanked(
		&self,
		artifact: &tg::artifact::Id,
	) -> tg::Result<Option<bool>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let p = connection.p();
		let statement = formatdoc!(
			"
				select yanked
				from package_versions
				where artifact = {p}1;
			"
		);
		let params = db::params![artifact];
		let yanked = connection
			.query_optional_value_into::<bool>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?;

		Ok(yanked)
	}
}

impl Server {
	pub(crate) async fn handle_get_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		dependency: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let Ok(dependency) = urlencoding::decode(dependency) else {
			return Ok(http::Response::builder().bad_request().empty().unwrap());
		};
		let Ok(dependency) = dependency.parse() else {
			return Ok(http::Response::builder().bad_request().empty().unwrap());
		};
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let Some(output) = handle.try_get_package(&dependency, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
