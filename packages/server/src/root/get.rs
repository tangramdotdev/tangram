use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{
	outgoing::response::Ext as _,
	Incoming, Outgoing,
};

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
				select name, build_or_object
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
		_request: http::Request<Incoming>,
		name: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Get the root.
		let Some(output) = handle.try_get_root(name).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.json(output)
			.unwrap();

		Ok(response)
	}
}
