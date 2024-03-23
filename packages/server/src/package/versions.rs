use crate::{database::Database, postgres_params, Http, Server};
use itertools::Itertools;
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

		let versions = self
			.try_get_package_versions_local(dependency)
			.await?
			.map(|versions| versions.into_iter().map(|(version, _)| version).collect());
		Ok(versions)
	}

	pub(super) async fn try_get_package_versions_local(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<(String, tg::directory::Id)>>> {
		// The dependency must have a name.
		let name = dependency
			.name
			.as_ref()
			.ok_or_else(|| error!(%dependency, "expected the dependency to have a name"))?;
		let version = dependency.version.as_ref();

		// Get a database connection.
		let Database::Postgres(database) = &self.inner.database else {
			return Err(error!("unimplemented"));
		};
		let connection = database.get().await?;

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
			.map_err(|source| error!(!source, "failed to prepare the statement"))?;
		let row = connection
			.query_one(&statement, params)
			.await
			.map_err(|source| error!(!source, "failed to execute the statement"))?;
		let exists = row.get::<_, bool>(0);
		if !exists {
			return Ok(None);
		}

		// Get the versions.
		let statement = "
			select version, id
			from package_versions
			where name = $1
			order by published_at asc
		";
		let params = postgres_params![name];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|source| error!(!source, "failed to prepare the statement"))?;
		let rows = connection
			.query(&statement, params)
			.await
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Get all the package versions.
		let all_versions: Vec<_> = rows
			.into_iter()
			.map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
			.map(|(version, id)| {
				let id = id.parse()?;
				Ok::<(String, tg::directory::Id), tangram_error::Error>((version, id))
			})
			.try_collect()?;

		let Some(version) = version else {
			return Ok(Some(all_versions));
		};

		// Try to match semver, but only against versions that parse as semver.
		'a: {
			if !"=<>^~*".chars().any(|ch| version.starts_with(ch)) {
				break 'a;
			}
			let req = semver::VersionReq::parse(version).map_err(
				|error| error!(source = error, %version, "failed to parse version constraint"),
			)?;
			let versions = all_versions
				.into_iter()
				.filter(|(version, _)| {
					let Ok(version) = version.parse() else {
						return false;
					};
					req.matches(&version)
				})
				.sorted_unstable_by_key(|(version, _)| semver::Version::parse(version).unwrap())
				.collect::<Vec<_>>();
			return Ok(Some(versions));
		}

		// Try to match with a regex against all versions.
		'a: {
			if !version.starts_with('/') {
				break 'a;
			}
			let (_, constraint) = version.split_at(1);
			let regex = regex::Regex::new(constraint)
				.map_err(|source| error!(!source, "failed to parse regex"))?;
			let versions = all_versions
				.into_iter()
				.filter(|(version, _)| regex.is_match(version))
				.collect::<Vec<_>>();
			return Ok(Some(versions));
		}

		// Fall back on string equality.
		let versions = all_versions
			.into_iter()
			.filter(|(version_, _)| version_ == version)
			.collect::<Vec<_>>();

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
			return Err(error!(%path, "unexpected path"));
		};
		let dependency = urlencoding::decode(dependency)
			.map_err(|source| error!(!source, "failed to decode the dependency"))?;
		let dependency = dependency
			.parse()
			.map_err(|source| error!(!source, "failed to parse the dependency"))?;

		// Get the package.
		let Some(output) = self.inner.tg.try_get_package_versions(&dependency).await? else {
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
