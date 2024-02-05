use super::Server;
use crate::{database::Database, postgres_params, sqlite_params, Http};
use bytes::Bytes;
use futures::{stream, TryStreamExt};
use http_body_util::BodyExt;
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};
use tangram_util::http::{bad_request, empty, full, not_found, ok, Incoming, Outgoing};
use tokio_stream::StreamExt;

impl Server {
	pub async fn get_object_exists(&self, id: &tg::object::Id) -> Result<bool> {
		if self.get_object_exists_local(id).await? {
			return Ok(true);
		}
		if self.get_object_exists_remote(id).await? {
			return Ok(true);
		}
		Ok(false)
	}

	async fn get_object_exists_local(&self, id: &tg::object::Id) -> Result<bool> {
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					select count(*) != 0
					from objects
					where id = ?1;
				";
				let params = sqlite_params![id.to_string()];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let mut rows = statement
					.query(params)
					.wrap_err("Failed to execute the statement.")?;
				let row = rows
					.next()
					.wrap_err("Failed to retrieve the row.")?
					.wrap_err("Expected a row.")?;
				let exists = row
					.get::<_, bool>(0)
					.wrap_err("Failed to deserialize the column.")?;
				Ok(exists)
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let statement = "
					select count(*) != 0
					from objects
					where id = $1;
				";
				let params = postgres_params![id.to_string()];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the statement.")?;
				let row = connection
					.query_one(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
				let exists = row.get(0);
				Ok(exists)
			},
		}
	}

	async fn get_object_exists_remote(&self, id: &tg::object::Id) -> Result<bool> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};
		let exists = remote.get_object_exists(id).await?;
		Ok(exists)
	}

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
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					select bytes
					from objects
					where id = ?1;
				";
				let params = sqlite_params![id.to_string()];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let mut rows = statement
					.query(params)
					.wrap_err("Failed to execute the statement.")?;
				let Some(row) = rows.next().wrap_err("Failed to retrieve the row.")? else {
					return Ok(None);
				};
				let bytes = row
					.get::<_, Vec<u8>>(0)
					.wrap_err("Failed to deserialize the column.")?
					.into();
				let output = tg::object::GetOutput { bytes };
				Ok(Some(output))
			},

			Database::Postgres(database) => {
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
					.wrap_err("Failed to prepare the statement.")?;
				let Some(row) = connection
					.query_opt(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?
				else {
					return Ok(None);
				};
				let bytes = row.get::<_, Vec<u8>>(0).into();
				let output = tg::object::GetOutput { bytes };
				Ok(Some(output))
			},
		}
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

		// Add the object to the database.
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					insert into objects (id, bytes)
					values (?1, ?2)
					on conflict (id) do update set bytes = ?2;
				";
				let params = sqlite_params![id.to_string(), output.bytes.to_vec()];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				statement
					.execute(params)
					.wrap_err("Failed to execute the statement.")?;
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let statement = "
					upsert into objects (id, bytes)
					values ($1, $2);
				";
				let params = postgres_params![id.to_string(), output.bytes.to_vec()];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the statement.")?;
				connection.execute(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
			},
		}

		Ok(Some(output))
	}

	pub async fn try_put_object(
		&self,
		id: &tg::object::Id,
		bytes: &Bytes,
	) -> Result<tg::object::PutOutput> {
		// Deserialize the data.
		let data = tg::object::Data::deserialize(id.kind(), bytes)
			.wrap_err("Failed to deserialize the data.")?;

		// Verify the ID.
		let verified_id: tg::object::Id = tg::Id::new_blake3(data.kind().into(), bytes)
			.try_into()
			.unwrap();
		if id != &verified_id {
			return Err(error!("The ID does not match the data."));
		}

		// Check if there are any missing children.
		let missing = stream::iter(data.children())
			.map(Ok)
			.try_filter_map(|id| async move {
				let exists = self.get_object_exists(&id).await?;
				Ok::<_, Error>(if exists { None } else { Some(id) })
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Add the object to the database if there are no missing objects.
		if missing.is_empty() {
			match &self.inner.database {
				Database::Sqlite(database) => {
					let connection = database.get().await?;
					let statement = "
						insert into objects (id, bytes)
						values (?1, ?2)
						on conflict (id) do update set bytes = ?2;
					";
					let params = sqlite_params![id.to_string(), bytes.to_vec()];
					let mut statement = connection
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the query.")?;
					statement
						.execute(params)
						.wrap_err("Failed to execute the statement.")?;
				},

				Database::Postgres(database) => {
					let connection = database.get().await?;
					let statement = "
						upsert into objects (id, bytes)
						values ($1, $2);
					";
					let params = postgres_params![id.to_string(), bytes.to_vec()];
					let statement = connection
						.prepare_cached(statement)
						.await
						.wrap_err("Failed to prepare the statement.")?;
					connection.execute(&statement, params)
						.await
						.wrap_err("Failed to execute the statement.")?;
				},
			}
		}

		let output = tg::object::PutOutput { missing };

		Ok(output)
	}

	pub async fn push_object(&self, id: &tg::object::Id) -> Result<()> {
		let remote = self
			.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?;
		tg::object::Handle::with_id(id.clone())
			.push(self, remote.as_ref())
			.await
			.wrap_err("Failed to push the object.")?;
		Ok(())
	}

	#[allow(clippy::unused_async)]
	pub async fn pull_object(&self, _id: &tg::object::Id) -> Result<()> {
		Err(error!("Not yet implemented."))
	}
}

impl Http {
	pub async fn handle_get_object_exists_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get whether the object exists.
		let exists = self.inner.tg.get_object_exists(&id).await?;

		// Create the response.
		let status = if exists {
			http::StatusCode::OK
		} else {
			http::StatusCode::NOT_FOUND
		};
		let response = http::Response::builder()
			.status(status)
			.body(empty())
			.unwrap();

		Ok(response)
	}

	pub async fn handle_get_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
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

	pub async fn handle_put_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();

		// Put the object.
		let output = self.inner.tg.try_put_object(&id, &bytes).await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();

		Ok(response)
	}

	pub async fn handle_push_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id, "push"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Push the object.
		self.inner.tg.push_object(&id).await?;

		Ok(ok())
	}

	pub async fn handle_pull_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id, "pull"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Pull the object.
		self.inner.tg.pull_object(&id).await?;

		Ok(ok())
	}
}
