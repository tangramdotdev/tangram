use crate::{database::Database, postgres_params, Http, Server};
use tangram_client as tg;
use tangram_error::{error, Result};
use tangram_util::http::{full, not_found, Incoming, Outgoing};

impl Server {
	pub async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<String>>> {
		if let Some(remote) = self.inner.remote.as_ref() {
			return remote.try_get_package_versions(dependency).await;
		}

		// Get a database connection.
		let Database::Postgres(database) = &self.inner.database else {
			return Err(error!("unimplemented"));
		};
		let connection = database.get().await?;

		// The dependency must have a name.
		let name = dependency
			.name
			.as_ref()
			.ok_or_else(|| error!(%dependency, "Expected the dependency to have a name."))?;

		// Confirm the package exists.
		let statement = "
			select count(*) != 0
			from packages
			where name = $1;
		";
		let params = postgres_params![name];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "Failed to prepare the statement."))?;
		let row = connection
			.query_one(&statement, params)
			.await
			.map_err(|error| error!(source = error, "Failed to execute the statement."))?;
		let exists = row.get::<_, bool>(0);
		if !exists {
			return Ok(None);
		}

		// Get the versions.
		let statement = "
			select version
			from package_versions
			where name = $1;
		";
		let params = postgres_params![name];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "Failed to prepare the statement."))?;
		let rows = connection
			.query(&statement, params)
			.await
			.map_err(|error| error!(source = error, "Failed to execute the statement."))?;
		let versions = rows
			.into_iter()
			.map(|row| row.get::<_, String>(0))
			.map(|version| {
				version
					.parse()
					.map_err(|error| error!(source = error, "Invalid version."))
			})
			.collect::<Result<Vec<semver::Version>>>()?;

		// Get the req.
		let req = if let Some(version) = dependency.version.as_ref() {
			version
				.parse()
				.map_err(|error| error!(source = error, "Invalid version."))?
		} else {
			semver::VersionReq::STAR
		};

		// Filter for compatible versions.
		let mut versions = versions
			.into_iter()
			.filter(|version| req.matches(version))
			.collect::<Vec<_>>();

		// Sort the versions.
		versions.sort_unstable();

		// Convert the versions to strings.
		let versions = versions
			.into_iter()
			.map(|version| version.to_string())
			.collect();

		Ok(Some(versions))
	}
}

impl Http {
	pub async fn handle_get_package_versions_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "versions"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "Unexpected path."));
		};
		let dependency = urlencoding::decode(dependency)
			.map_err(|error| error!(source = error, "Failed to decode the dependency."))?;
		let dependency = dependency
			.parse()
			.map_err(|error| error!(source = error, "Failed to parse the dependency."))?;

		// Get the package.
		let Some(output) = self.inner.tg.try_get_package_versions(&dependency).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|error| error!(source = error, "Failed to serialize the body."))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
