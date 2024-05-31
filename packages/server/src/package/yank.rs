use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database, Query};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn yank_package(
		&self,
		id: &tg::artifact::Id,
		arg: tg::package::yank::Arg,
	) -> tg::Result<()> {
		let remote = arg.remote.as_ref().or(self.options.registry.as_ref());
		match remote {
			None => {
				self.yank_package_local(id, arg).await?;
				Ok(())
			},
			Some(remote) => {
				let remote = self
					.remotes
					.get(remote)
					.ok_or_else(|| tg::error!("the remote does not exist"))?
					.clone();
				remote.yank_package(id, arg).await?;
				Ok(())
			},
		}
	}

	pub async fn yank_package_local(
		&self,
		id: &tg::artifact::Id,
		_arg: tg::package::yank::Arg,
	) -> tg::Result<()> {
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
		handle.yank_package(&dependency, arg).await?;
		let response = http::Response::builder().ok().empty().unwrap();
		Ok(response)
	}
}
