use crate::{
	util::http::{bad_request, full, Incoming, Outgoing},
	Http, Server,
};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_error::{error, Result};

impl Server {
	pub async fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> Result<tg::package::SearchOutput> {
		if let Some(remote) = self.inner.remotes.first() {
			return remote.search_packages(arg).await;
		}

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

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
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(results)
	}
}

impl Http {
	pub async fn handle_search_packages_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Read the search params.
		let Some(query) = request.uri().query() else {
			return Ok(bad_request());
		};
		let arg = serde_urlencoded::from_str(query)
			.map_err(|source| error!(!source, "failed to deserialize the search params"))?;

		// Perform the search.
		let output = self.inner.tg.search_packages(arg).await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
