use crate::{
	util::http::{empty, Incoming, Outgoing},
	Server,
};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn touch_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update the status.
		let p = connection.p();
		let statement = format!(
			"
				update builds
				set touched_at = {p}1
				where id = {p}2;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![now, id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_touch_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "touch"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the ID"))?;

		// Touch the build.
		handle.touch_build(&id).await?;

		// Create the response.
		let body = empty();
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
