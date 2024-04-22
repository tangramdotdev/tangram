use crate::{
	util::http::{full, not_found, Incoming, Outgoing},
	Http, Server,
};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn try_start_build(&self, id: &tg::build::Id) -> tg::Result<Option<bool>> {
		if let Some(output) = self.try_start_build_local(id).await? {
			return Ok(Some(output));
		}
		if let Some(output) = self.try_start_build_remote(id).await? {
			return Ok(Some(output));
		}
		Ok(None)
	}

	async fn try_start_build_local(&self, id: &tg::build::Id) -> tg::Result<Option<bool>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Start the build.
		let p = connection.p();
		let statement = format!(
			"
				update builds
				set status = 'started', started_at = {p}1
				where id = {p}2 and (status = 'created' or status = 'dequeued')
				returning id;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![now, id];
		let output = connection
			.query_optional(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.is_some();

		// Drop the database connection.
		drop(connection);

		// Publish the message.
		if output {
			self.messenger.publish_to_build_status(id).await?;
		}

		Ok(Some(output))
	}

	async fn try_start_build_remote(&self, id: &tg::build::Id) -> tg::Result<Option<bool>> {
		let Some(remote) = self.remotes.first() else {
			return Ok(None);
		};
		let Some(output) = remote.try_start_build(id).await? else {
			return Ok(None);
		};
		Ok(Some(output))
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_start_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "start"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the ID"))?;

		// Attempt to start the build.
		let Some(output) = self.handle.try_start_build(&id).await? else {
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
