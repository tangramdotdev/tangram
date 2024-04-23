use crate::{
	util::http::{empty, Incoming, Outgoing},
	Http, Server,
};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub async fn remove_root(&self, name: &str) -> tg::Result<()> {
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
				delete from roots
				where name = {p}1;
			"
		);
		let params = db::params![name];
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
	pub async fn handle_remove_root_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["roots", name] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};

		// Remove the root.
		self.handle.remove_root(name).await?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}
}
