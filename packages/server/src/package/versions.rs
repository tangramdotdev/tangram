use crate::{Http, Server};
use indoc::formatdoc;
use itertools::Itertools;
use tangram_client as tg;
use tangram_database as db;
use tangram_error::{error, Result};
use tangram_http::{full, not_found, Incoming, Outgoing};

impl Server {
	pub async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<String>>> {
		if let Some(remote) = self.inner.remotes.first() {
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
		// Get the dependency name and version.
		let name = dependency
			.name
			.as_ref()
			.ok_or_else(|| error!(%dependency, "expected the dependency to have a name"))?;
		let version = dependency.version.as_ref();

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

		// Confirm the package exists.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count(*) != 0
				from packages
				where name = {p}1;
			"
		);
		let params = db::params![name];
		let exists = connection
			.query_one_scalar_into::<bool>(statement, params)
			.await
			.map_err(|source| error!(!source, "failed to execute the statement"))?;
		if !exists {
			return Ok(None);
		}

		// Get the package versions.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select version, id
				from package_versions
				where name = {p}1
				order by published_at asc
			"
		);
		let params = db::params![name];
		let versions = connection
			.query_all_into::<(String, tg::directory::Id)>(statement, params)
			.await
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// If there is no version constraint, then return all versions.
		let Some(version) = version else {
			return Ok(Some(versions));
		};

		// If the version constraint is semver, then match with it.
		if "=<>^~*".chars().any(|ch| version.starts_with(ch)) {
			let req = semver::VersionReq::parse(version).map_err(
				|source| error!(!source, %version, "failed to parse the version constraint"),
			)?;
			let versions = versions
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

		// If the version constraint is regex, then match with it.
		if version.starts_with('/') {
			let (_, constraint) = version.split_at(1);
			let regex = regex::Regex::new(constraint)
				.map_err(|source| error!(!source, "failed to parse regex"))?;
			let versions = versions
				.into_iter()
				.filter(|(version, _)| regex.is_match(version))
				.collect::<Vec<_>>();
			return Ok(Some(versions));
		}

		// Otherwise, use string equality.
		let versions = versions
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
