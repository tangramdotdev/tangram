use crate::{
	util::http::{empty, full, not_found, Incoming, Outgoing},
	Server,
};
use tangram_client as tg;

mod analysis;
mod dependencies;
mod format;
mod lock;
mod metadata;
mod outdated;
mod publish;
mod search;
mod versions;
mod yank;

impl Server {
	pub async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::get::Arg,
	) -> tg::Result<Option<tg::package::get::Output>> {
		// Analyze the package.
		let analysis = self
			.try_analyze_package(dependency)
			.await
			.map_err(|source| tg::error!(!source, %dependency, "failed to analyze the package"))?;

		// If the package was not found, then return `None`.
		let Some(analysis) = analysis else {
			return Ok(None);
		};

		// Get the package.
		let package = analysis.package.clone();

		// Get the dependencies if requested.
		let dependencies = arg.dependencies.then(|| analysis.dependencies.clone());

		// Get or create the lock if requested.
		let lock = if arg.lock {
			let path = dependency.path.as_ref();
			let lock = self
				.get_or_create_package_lock(path, &analysis)
				.await
				.map_err(|error| {
					if let Some(path) = path {
						tg::error!(source = error, %path, "failed to get or create the lock")
					} else {
						tg::error!(source = error, "failed to get or create the lock")
					}
				})?;
			let lock = lock.id(self, None).await?;
			Some(lock)
		} else {
			None
		};

		// Get the metadata if requested.
		let metadata = if arg.metadata {
			analysis.metadata.clone()
		} else {
			None
		};

		// Get the package ID.
		let id = package.id(self, None).await?;

		// Get the package's path if requested.
		let path = None;

		// Check if the package is yanked.
		let yanked = if arg.yanked {
			let yanked = self.get_package_yanked(&package).await.map_err(|source| {
				tg::error!(!source, "failed to check if the package is yanked")
			})?;
			Some(yanked)
		} else {
			None
		};

		Ok(Some(tg::package::get::Output {
			dependencies,
			id,
			lock,
			metadata,
			path,
			yanked,
		}))
	}

	pub async fn check_package(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		// Get the package.
		let (package, lock) = tg::package::get_with_lock(self, dependency).await?;

		// Create the root module.
		let path = tg::package::get_root_module_path(self, &package).await?;
		let package = package.id(self, None).await?;
		let lock = lock.id(self, None).await?;
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

	pub async fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Option<serde_json::Value>> {
		// Get the package.
		let Some((package, lock)) = tg::package::try_get_with_lock(self, dependency).await? else {
			return Ok(None);
		};

		// Create the module.
		let path = tg::package::get_root_module_path(self, &package).await?;
		let package = package.id(self, None).await?;
		let lock = lock.id(self, None).await?;
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

impl Server {
	pub(crate) async fn handle_get_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let dependency = urlencoding::decode(dependency)
			.map_err(|source| tg::error!(!source, "failed to decode the dependency"))?;
		let dependency = dependency
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the dependency"))?;

		// Get the query.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize the query"))?
			.unwrap_or_default();

		// Get the package.
		let Some(output) = handle.try_get_package(&dependency, arg).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}

	pub(crate) async fn handle_check_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "check"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let dependency = urlencoding::decode(dependency)
			.map_err(|source| tg::error!(!source, "failed to decode the dependency"))?;
		let dependency = dependency
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the dependency"))?;

		// Check the package.
		let output = handle.check_package(&dependency).await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}

	pub(crate) async fn handle_format_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "format"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let dependency = urlencoding::decode(dependency)
			.map_err(|source| tg::error!(!source, "failed to decode the dependency"))?;
		let dependency = dependency
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the dependency"))?;

		// Format the package.
		handle.format_package(&dependency).await?;

		// Create the response.
		let response = http::Response::builder().body(empty()).unwrap();

		Ok(response)
	}

	pub(crate) async fn handle_outdated_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "outdated"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let dependency = urlencoding::decode(dependency)
			.map_err(|source| tg::error!(!source, "failed to decode the dependency"))?;
		let dependency = dependency
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the dependency"))?;

		// Get the outdated dependencies.
		let output = handle.get_package_outdated(&dependency).await?;
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}

	pub(crate) async fn handle_get_package_doc_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "doc"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let dependency = urlencoding::decode(dependency)
			.map_err(|source| tg::error!(!source, "failed to decode the dependency"))?;
		let dependency = dependency
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the dependency"))?;

		// Get the doc.
		let Some(output) = handle.try_get_package_doc(&dependency).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
