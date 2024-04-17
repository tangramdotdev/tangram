use crate::{
	util::http::{bad_request, full, not_found, Incoming, Outgoing},
	Http, Server,
};
use futures::TryFutureExt as _;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::GetOutput>> {
		if let Some(bytes) = self.try_get_object_local(id).await? {
			Ok(Some(bytes))
		} else if let Some(bytes) = self.try_get_object_remote(id).await? {
			Ok(Some(bytes))
		} else {
			Ok(None)
		}
	}

	async fn try_get_object_local(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::GetOutput>> {
		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the object.
		let p = self.inner.database.p();
		let statement = formatdoc!(
			"
				select bytes, count, weight
				from objects
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let output = connection
			.query_optional_into(statement, params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))
			.await?;

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	async fn try_get_object_remote(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::GetOutput>> {
		// Get the remote.
		let Some(remote) = self.inner.remotes.first() else {
			return Ok(None);
		};

		// Get the object from the remote server.
		let Some(output) = remote.try_get_object(id).await? else {
			return Ok(None);
		};

		// Put the object.
		let arg = tg::object::PutArg {
			bytes: output.bytes.clone(),
			count: output.count,
			weight: output.weight,
		};
		self.put_object(id, arg, None).await?;

		Ok(Some(output))
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_get_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get the object.
		let Some(output) = self.inner.tg.try_get_object(&id).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(output.bytes))
			.unwrap();

		Ok(response)
	}
}
