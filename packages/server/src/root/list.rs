use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Incoming, Outgoing};

impl Server {
	pub async fn list_roots(
		&self,
		_arg: tg::root::list::Arg,
	) -> tg::Result<tg::root::list::Output> {
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
		// Get the query.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize the query"))?
			.unwrap_or_default();

		// List the roots.
		let output = handle.list_roots(arg).await?;

		// Create the response.
		let response = http::Response::builder()
			.body(Outgoing::json(output))
			.unwrap();

		Ok(response)
	}
}
