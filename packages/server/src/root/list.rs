use crate::{
	util::http::{bad_request, full, Incoming, Outgoing},
	Server,
};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub async fn list_roots(&self, _arg: tg::root::list::Arg) -> tg::Result<tg::root::list::Output> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the builds.
		let statement = formatdoc!(
			"
				select name, id
				from roots;
			"
		);
		let params = db::params![];
		let items = connection
			.query_all_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let output = tg::root::list::Output { items };

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_list_roots_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let Some(query) = request.uri().query() else {
			return Ok(bad_request());
		};
		let arg = serde_urlencoded::from_str(query)
			.map_err(|source| tg::error!(!source, "failed to deserialize the query"))?;

		// List the roots.
		let output = handle.list_roots(arg).await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}
}
