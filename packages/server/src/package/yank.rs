use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database, Query};
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn get_package_yanked(&self, package: &tg::Artifact) -> tg::Result<bool> {
		let id = package.id(self, None).await?.clone();
		if let Some(remote) = self.remotes.first() {
			let dependency = tg::Dependency::with_id(id);
			let arg = tg::package::get::Arg {
				yanked: true,
				..Default::default()
			};
			let output = remote
				.try_get_package(&dependency, arg)
				.await
				.map_err(|source| tg::error!(!source, %dependency, "failed to get the package"))?
				.ok_or_else(|| tg::error!(%dependency, "expected the package to exist"))?;
			let yanked = output
				.yanked
				.ok_or_else(|| tg::error!("expected the yanked field to be set"))?;
			return Ok(yanked);
		}

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
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let yanked = connection
			.query_one_value_into::<bool>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?;

		Ok(yanked)
	}

	pub async fn yank_package(&self, id: &tg::artifact::Id) -> tg::Result<()> {
		if let Some(remote) = self.remotes.first() {
			self.push_object(&id.clone().into()).await?;
			remote.yank_package(id).await?;
			return Ok(());
		}

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update the package versions table.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update package_versions
				set yanked = 1
				where id = {p}1;
			"
		);
		let params = db::params![id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_yank_package_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
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

		// Publish the package.
		handle.yank_package(&dependency).await?;

		// Create the response.
		let response = http::Response::builder().ok().empty().unwrap();

		Ok(response)
	}
}
