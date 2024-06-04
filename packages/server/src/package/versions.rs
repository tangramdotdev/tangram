use crate::Server;
use indoc::formatdoc;
use itertools::Itertools as _;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::versions::Arg,
	) -> tg::Result<Option<Vec<String>>> {
		match &self.options.registry {
			None => {
				let versions = self
					.try_get_package_versions_local(dependency, arg)
					.await?
					.map(|versions| versions.into_iter().map(|(version, _)| version).collect());
				Ok(versions)
			},
			Some(remote) => {
				let remote = self
					.remotes
					.get(remote)
					.ok_or_else(|| tg::error!("the remote does not exist"))?
					.clone();
				let versions = remote.try_get_package_versions(dependency, arg).await?;
				Ok(versions)
			},
		}
	}

	pub(super) async fn try_get_package_versions_local(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::versions::Arg,
	) -> tg::Result<Option<Vec<(String, tg::artifact::Id)>>> {
		// Get the dependency name and version.
		let name = dependency
			.name
			.as_ref()
			.ok_or_else(|| tg::error!(%dependency, "expected the dependency to have a name"))?;
		let version = dependency.version.as_ref();

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the package versions.
		#[derive(serde::Deserialize, Debug)]
		struct Row {
			artifact: tg::artifact::Id,
			version: String,
			yanked: bool,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select artifact, version, yanked
				from package_versions
				where name = {p}1
				order by created_at asc
			"
		);
		let params = db::params![name];
		let versions = connection
			.query_all_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// If there is no version constraint, then return all versions.
		let Some(version) = version else {
			let versions = versions
				.into_iter()
				.filter_map(|row| {
					(arg.yanked || !row.yanked).then_some((row.version, row.artifact))
				})
				.collect();
			return Ok(Some(versions));
		};

		// If the version constraint is semver, then match with it.
		if "=<>^~*".chars().any(|ch| version.starts_with(ch)) {
			let req = semver::VersionReq::parse(version).map_err(
				|source| tg::error!(!source, %version, "failed to parse the version constraint"),
			)?;
			let versions = versions
				.into_iter()
				.filter(|row| {
					let Ok(version) = row.version.parse() else {
						return false;
					};
					req.matches(&version) && !row.yanked
				})
				.sorted_unstable_by_key(|row| semver::Version::parse(&row.version).unwrap())
				.map(|row| (row.version, row.artifact))
				.collect();
			return Ok(Some(versions));
		}

		// If the version constraint is regex, then match with it.
		if version.starts_with('/') {
			let (_, constraint) = version.split_at(1);
			let regex = regex::Regex::new(constraint)
				.map_err(|source| tg::error!(!source, "failed to parse regex"))?;
			let versions = versions
				.into_iter()
				.filter_map(|row| {
					(regex.is_match(&row.version) && !row.yanked)
						.then_some((row.version, row.artifact))
				})
				.collect();
			return Ok(Some(versions));
		}

		// Otherwise, use string equality.
		let versions = versions
			.into_iter()
			.filter_map(|row| (&row.version == version).then_some((row.version, row.artifact)))
			.collect::<Vec<_>>();

		Ok(Some(versions))
	}
}

impl Server {
	pub(crate) async fn handle_get_package_versions_request<H>(
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
		let Some(output) = handle.try_get_package_versions(&dependency, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
