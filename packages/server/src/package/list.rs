use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn list_packages(
		&self,
		arg: tg::package::list::Arg,
	) -> tg::Result<tg::package::list::Output> {
		// Handle the remote.
		let remote = arg.remote.as_ref().or(self.options.registry.as_ref());
		if let Some(remote) = remote {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!("the remote does not exist"))?
				.clone();
			let arg = tg::package::list::Arg {
				remote: None,
				..arg
			};
			let packages = remote.list_packages(arg).await?;
			return Ok(packages);
		}

		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the search results.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select distinct name
				from package_versions
				where name like {p}1 || '%';
			"
		);
		let params = db::params![arg.query];
		let results = connection
			.query_all_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(results)
	}
}

impl Server {
	pub(crate) async fn handle_list_packages_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.list_packages(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
