use crate::{
	database::{Database, Postgres, Sqlite},
	postgres_params, sqlite_params, Http, Server,
};
use bytes::Bytes;
use tangram_client as tg;
use tangram_error::{error, Result};
use tangram_util::http::{bad_request, full, not_found, Incoming, Outgoing};

impl Server {
	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<tg::object::GetOutput>> {
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
	) -> Result<Option<tg::object::GetOutput>> {
		match &self.inner.database {
			Database::Sqlite(database) => self.try_get_object_sqlite(id, database).await,
			Database::Postgres(database) => self.try_get_object_postgres(id, database).await,
		}
	}

	async fn try_get_object_sqlite(
		&self,
		id: &tg::object::Id,
		database: &Sqlite,
	) -> Result<Option<tg::object::GetOutput>> {
		let connection = database.get().await?;
		let statement = "
			select bytes
			from objects
			where id = ?1;
		";
		let params = sqlite_params![id.to_string()];
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|error| error!(source = error, "Failed to prepare the query."))?;
		let mut rows = statement
			.query(params)
			.map_err(|error| error!(source = error, "Failed to execute the statement."))?;
		let Some(row) = rows
			.next()
			.map_err(|error| error!(source = error, "Failed to retrieve the row."))?
		else {
			return Ok(None);
		};
		let bytes: Bytes = row
			.get::<_, Vec<u8>>(0)
			.map_err(|error| error!(source = error, "Failed to deserialize the column."))?
			.into();
		let output = tg::object::GetOutput {
			bytes,
			count: None,
			weight: None,
		};
		Ok(Some(output))
	}

	async fn try_get_object_postgres(
		&self,
		id: &tg::object::Id,
		database: &Postgres,
	) -> Result<Option<tg::object::GetOutput>> {
		let connection = database.get().await?;
		let statement = "
			select bytes
			from objects
			where id = $1;
		";
		let params = postgres_params![id.to_string()];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "Failed to prepare the statement."))?;
		let Some(row) = connection
			.query_opt(&statement, params)
			.await
			.map_err(|error| error!(source = error, "Failed to execute the statement."))?
		else {
			return Ok(None);
		};
		let bytes: Bytes = row
			.try_get::<_, Vec<u8>>(0)
			.map_err(|error| error!(source = error, "Failed to deserialize the column."))?
			.into();
		let output = tg::object::GetOutput {
			bytes,
			count: None,
			weight: None,
		};
		Ok(Some(output))
	}

	async fn try_get_object_remote(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<tg::object::GetOutput>> {
		// Get the remote.
		let Some(remote) = self.inner.remote.as_ref() else {
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
		self.put_object(id, &arg).await?;

		Ok(Some(output))
	}
}

impl Http {
	pub async fn handle_get_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "Unexpected path."));
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
