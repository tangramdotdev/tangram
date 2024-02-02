use crate::{database::Database, postgres_params, Http, Server};
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};
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
		let db = database.get().await?;

		// Get the search results.
		let statement = "
			select name
			from packages
			where name like $1 || '%';
		";
		let params = postgres_params![arg.query];
		let statement = db
			.prepare_cached(statement)
			.await
			.wrap_err("Failed to prepare the statement.")?;
		let rows = db
			.query(&statement, params)
			.await
			.wrap_err("Failed to execute the statement.")?;
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
			.wrap_err("Failed to deserialize the search params.")?;

		// Perform the search.
		let output = self.inner.tg.search_packages(arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}
}
