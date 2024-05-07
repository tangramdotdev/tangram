use crate::Server;
use tangram_client as tg;
use tangram_http::{outgoing::ResponseExt as _, Incoming, Outgoing};

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
			artifact: id,
			lock,
			metadata,
			path,
			yanked,
		}))
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
			return Ok(http::Response::bad_request());
		};
		let Ok(dependency) = dependency.parse() else {
			return Ok(http::Response::bad_request());
		};

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
			return Ok(http::Response::not_found());
		};

		// Create the response.
		let response = http::Response::builder()
			.body(Outgoing::json(output))
			.unwrap();

		Ok(response)
	}
}
