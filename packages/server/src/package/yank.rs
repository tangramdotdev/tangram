use crate::{
	util::http::{bad_request, ok, Incoming, Outgoing},
	Http, Server,
};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database, Query};

impl Server {
	pub async fn get_package_yanked(&self, package: &tg::Directory) -> tg::Result<bool> {
		let id = package.id(self, None).await?.clone();
		if let Some(remote) = self.remotes.first() {
			let dependency = tg::Dependency::with_id(id);
			let arg = tg::package::GetArg {
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

		#[derive(serde::Deserialize)]
		struct Row {
			yanked: bool,
		}

		let p = connection.p();
		let statement = formatdoc!(
			"
				select yanked from package_versions
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let row = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?;
		Ok(row.yanked)
	}

	pub async fn yank_package(&self, id: &tg::directory::Id) -> tg::Result<()> {
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

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_yank_package_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "yank"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let Ok(dependency) = dependency.parse() else {
			return Ok(bad_request());
		};

		// Publish the package.
		self.handle.yank_package(&dependency).await?;

		Ok(ok())
	}
}
