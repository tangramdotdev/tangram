use crate::{
	util::http::{full, not_found, Incoming, Outgoing},
	Server,
};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub async fn try_get_root(&self, name: &str) -> tg::Result<Option<tg::root::get::Output>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the builds.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select name, id
				from roots
				where name = {p}1;
			"
		);
		let params = db::params![name];
		let Some(output) = connection
			.query_optional_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		// Drop the database connection.
		drop(connection);

		Ok(Some(output))
	}
}

impl Server {
	pub(crate) async fn handle_get_root_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["roots", name] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};

		// Get the root.
		let Some(output) = handle.try_get_root(name).await? else {
			return Ok(not_found());
		};

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
