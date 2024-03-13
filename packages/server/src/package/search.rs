use crate::{database::Database, postgres_params, Http, Server};
use tangram_client as tg;
use tangram_error::{error, Result};
use tangram_util::http::{bad_request, full, Incoming, Outgoing};

impl Server {
	pub async fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> Result<tg::package::SearchOutput> {
		if let Some(remote) = self.inner.remote.as_ref() {
			return remote.search_packages(arg).await;
		}

		// Get a database connection.
		let Database::Postgres(database) = &self.inner.database else {
			return Err(error!("unimplemented"));
		};
		let connection = database.get().await?;

		// Get the search results.
		let statement = "
			select name
			from packages
			where name like $1 || '%';
		";
		let params = postgres_params![arg.query];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "Failed to prepare the statement."))?;
		let rows = connection
			.query(&statement, params)
			.await
			.map_err(|error| error!(source = error, "Failed to execute the statement."))?;
		let results = rows.into_iter().map(|row| row.get(0)).collect();

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
			.map_err(|error| error!(source = error, "Failed to deserialize the search params."))?;

		// Perform the search.
		let output = self.inner.tg.search_packages(arg).await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|error| error!(source = error, "Failed to serialize the body."))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
