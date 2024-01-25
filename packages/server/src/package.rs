use crate::Server;
use http_body_util::BodyExt;
use std::collections::BTreeMap;
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};
use tangram_util::http::{bad_request, full, not_found, ok, Incoming, Outgoing};

impl Server {
	pub async fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> Result<tg::package::SearchOutput> {
		self.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?
			.search_packages(arg)
			.await
	}

	pub async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> Result<Option<tg::package::GetOutput>> {
		// Attempt to get the package with path dependencies locally.
		let package_with_path_dependencies = 'a: {
			let Some(package_with_path_dependencies) =
				tangram_package::try_get(self, dependency).await?
			else {
				break 'a None;
			};

			Some(package_with_path_dependencies)
		};

		// If the dependency has a name, then attempt to get it from the remote.
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

			let package_with_path_dependencies = tangram_package::PackageWithPathDependencies {
				package,
				path_dependencies: BTreeMap::default(),
			};

			Some(package_with_path_dependencies)
		};

		// If the package was not found, then return `None`.
		let Some(package_with_path_dependencies) = package_with_path_dependencies else {
			return Ok(None);
		};

		let package = package_with_path_dependencies.package.clone();

		// Get the dependencies if requested.
		let dependencies = if arg.dependencies {
			let dependencies = tangram_package::get_dependencies(self, &package).await?;
			Some(dependencies)
		} else {
			None
		};

		// Create the lock if requested.
		let lock = if arg.lock {
			let path = dependency.path.as_ref();
			let lock =
				tangram_package::create_lock(self, path, &package_with_path_dependencies).await?;
			let id = lock.id(self).await?.clone();
			Some(id)
		} else {
			None
		};

		// Get the metadata if requested.
		let metadata = if arg.metadata {
			let metadata = tangram_package::get_metadata(self, &package).await?;
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

	pub async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<String>>> {
		self.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?
			.try_get_package_versions(dependency)
			.await
	}

	pub async fn try_get_package_metadata(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<tg::package::Metadata>> {
		let Some(package_with_path_dependencies) =
			tangram_package::try_get(self, dependency).await?
		else {
			return Ok(None);
		};
		let package = package_with_path_dependencies.package;
		let metadata = tangram_package::get_metadata(self, &package).await?;
		Ok(Some(metadata))
	}

	pub async fn try_get_package_dependencies(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<tg::Dependency>>> {
		let Some(package_with_path_dependencies) =
			tangram_package::try_get(self, dependency).await?
		else {
			return Ok(None);
		};
		let package = package_with_path_dependencies.package;
		let dependencies = tangram_package::get_dependencies(self, &package).await?;
		Ok(Some(dependencies))
	}

	pub async fn publish_package(
		&self,
		user: Option<&tg::User>,
		id: &tg::directory::Id,
	) -> Result<()> {
		// Push the package.
		self.push_object(&id.clone().into()).await?;

		// Publish the package.
		let remote = self
			.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?;
		remote.publish_package(user, id).await?;

		Ok(())
	}
}

impl Server {
	pub async fn handle_search_packages_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Read the search params.
		let Some(query) = request.uri().query() else {
			return Ok(bad_request());
		};
		let arg = serde_urlencoded::from_str(query)
			.wrap_err("Failed to deserialize the search params.")?;

		// Perform the search.
		let output = self.search_packages(arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

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
		let Some(output) = self.try_get_package(&dependency, arg).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;

		// Create the response.
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	pub async fn handle_get_package_versions_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "versions"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let dependency =
			urlencoding::decode(dependency).wrap_err("Failed to decode the dependency.")?;
		let dependency = dependency
			.parse()
			.wrap_err("Failed to parse the dependency.")?;

		// Get the package.
		let Some(output) = self.try_get_package_versions(&dependency).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	pub async fn handle_get_package_metadata_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "metadata"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let dependency =
			urlencoding::decode(dependency).wrap_err("Failed to decode the dependency.")?;
		let dependency = dependency
			.parse()
			.wrap_err("Failed to parse the dependency.")?;

		// Get the package metadata.
		let Some(metadata) = self.try_get_package_metadata(&dependency).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = serde_json::to_vec(&metadata).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	pub async fn handle_get_package_dependencies_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "dependencies"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let dependency =
			urlencoding::decode(dependency).wrap_err("Failed to decode the dependency.")?;
		let dependency = dependency
			.parse()
			.wrap_err("Failed to parse the dependency.")?;

		// Get the package dependencies.
		let Some(dependencies) = self.try_get_package_dependencies(&dependency).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body =
			serde_json::to_vec(&dependencies).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	pub async fn handle_publish_package_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let package_id = serde_json::from_slice(&bytes).wrap_err("Invalid request.")?;

		// Publish the package.
		self.publish_package(user.as_ref(), &package_id).await?;

		Ok(ok())
	}
}
