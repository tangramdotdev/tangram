use crate::{
	util::http::{empty, Incoming, Outgoing},
	Http, Server,
};
use http_body_util::BodyExt as _;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub async fn add_root(&self, arg: tg::root::AddArg) -> tg::Result<()> {
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
				insert into roots (name, id)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![arg.name, arg.id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(())
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_add_root_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<hyper::Response<Outgoing>> {
		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;

		// Add the root.
		self.handle.add_root(arg).await?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}
}
