use crate::{
	util::http::{bad_request, full, Incoming, Outgoing},
	Server,
};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub async fn list_packages(
		&self,
		arg: tg::package::list::Arg,
	) -> tg::Result<tg::package::list::Output> {
		if let Some(remote) = self.remotes.first() {
			return remote.list_packages(arg).await;
		}

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the search results.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select name
				from packages
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
		let Some(query) = request.uri().query() else {
			return Ok(bad_request());
		};
		let Ok(arg) = serde_urlencoded::from_str(query) else {
			return Ok(bad_request());
		};

		// Perform the search.
		let output = handle.list_packages(arg).await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
