use super::Server;
use crate::{
	database::{Database, Postgres, Sqlite},
	postgres_params, sqlite_params, Http,
};
use async_recursion::async_recursion;
use bytes::Bytes;
use http_body_util::BodyExt;
use itertools::Itertools;
use num::ToPrimitive;
use std::collections::VecDeque;
use tangram_client as tg;
use tangram_error::{error, Result, Wrap, WrapErr};
use tangram_util::{
	http::{bad_request, full, not_found, ok, Incoming, Outgoing},
	iter::IterExt,
};

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
			select bytes, complete, weight
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
		let bytes: Bytes = row
			.get::<_, Vec<u8>>(0)
			.wrap_err("Failed to deserialize the column.")?
			.into();
		let complete = row
			.get::<_, bool>(1)
			.wrap_err("Failed to deserialize the column.")?;
		let weight = row
			.get::<_, Option<i64>>(2)
			.wrap_err("Failed to deserialize the column.")?
			.map(|weight| weight.to_u64().unwrap());
		let output = tg::object::GetOutput {
			bytes,
			complete,
			weight,
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
			select bytes, complete, weight
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
		let bytes: Bytes = row
			.try_get::<_, Vec<u8>>(0)
			.wrap_err("Failed to deserialize the column.")?
			.into();
		let complete = row
			.try_get::<_, bool>(1)
			.wrap_err("Failed to deserialize the column.")?;
		let weight = row
			.try_get::<_, Option<i64>>(2)
			.wrap_err("Failed to deserialize the column.")?
			.map(|weight| weight.to_u64().unwrap());
		let output = tg::object::GetOutput {
			bytes,
			complete,
			weight,
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
		};
		self.put_object(id, &arg).await?;

		Ok(Some(output))
	}

	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
	) -> Result<tg::object::PutOutput> {
		match &self.inner.database {
			Database::Sqlite(database) => self.put_object_sqlite(id, arg, database).await,
			Database::Postgres(database) => self.put_object_postgres(id, arg, database).await,
		}
	}

	pub async fn put_object_sqlite(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
		database: &Sqlite,
	) -> Result<tg::object::PutOutput> {
		let mut connection = database.get().await?;

		// Deserialize the data.
		let data = tg::object::Data::deserialize(id.kind(), &arg.bytes)
			.wrap_err("Failed to deserialize the data.")?;

		// Verify the ID.
		let verified_id: tg::object::Id = tg::Id::new_blake3(data.kind().into(), &arg.bytes)
			.try_into()
			.unwrap();
		if id != &verified_id {
			return Err(error!("The ID does not match the data."));
		}

		'a: {
			// Begin a transaction.
			let txn = connection
				.transaction()
				.wrap_err("Failed to begin the transaction.")?;

			// Add the object.
			{
				let statement = "
					insert into objects (id, bytes, complete, weight)
					values (?1, ?2, false, null)
					on conflict (id) do nothing;
				";
				let id = id.to_string();
				let bytes = arg.bytes.to_vec();
				let params = sqlite_params![id, bytes];
				let mut statement = txn
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let n = statement
					.execute(params)
					.wrap_err("Failed to execute the statement.")?;
				if n == 0 {
					break 'a;
				}
			}

			// Add the children.
			for child in data.children() {
				let statement = "
					insert into object_children (object, child)
					values (?1, ?2)
					on conflict (object, child) do nothing;
				";
				let object = id.to_string();
				let child = child.to_string();
				let params = sqlite_params![object, child];
				let mut statement = txn
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				statement
					.execute(params)
					.wrap_err("Failed to execute the statement.")?;
			}

			// Commit the transaction.
			txn.commit().wrap_err("Failed to commit the transaction.")?;
		}

		// Find the incomplete children.
		let incomplete: Vec<tg::object::Id> = {
			let statement = "
				select child
				from object_children
				left join objects on objects.id = object_children.child
				where object = ?1 and (complete is null or complete = false);
			";
			let id = id.to_string();
			let params = sqlite_params![id];
			let mut statement = connection
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let rows = statement
				.query(params)
				.wrap_err("Failed to execute the statement.")?;
			rows.and_then(|row| row.get::<_, String>(0))
				.map_err(|error| error.wrap("Failed to deserialize the rows."))
				.and_then(|id| id.parse())
				.try_collect()?
		};

		// If there are no incomplete children, then update this object and its ancestors.
		if incomplete.is_empty() {
			let mut queue = VecDeque::from(vec![id.clone()]);
			while let Some(id) = queue.pop_front() {
				// Mark the object as complete and set its weight.
				let statement = "
					update objects
					set
						complete = true,
						weight = (select length(bytes) + (
							select coalesce(sum(weight), 0)
							from objects
							where id in (
								select child
								from object_children
								where object = ?1
							)
						))
					where id = ?1;
				";
				let id = id.to_string();
				let params = sqlite_params![id];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				statement
					.execute(params)
					.wrap_err("Failed to execute the statement.")?;

				// Add any parents whose children are all complete to the queue.
				let statement = "
					select object
					from object_children
					where child = ?1 and (
						select min(complete)
						from objects
						where id in (
							select child
							from object_children oc
							where object = object_children.object
						)
					);
				";
				let id = id.to_string();
				let params = sqlite_params![id];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let rows = statement
					.query(params)
					.wrap_err("Failed to execute the statement.")?;
				let parents = rows
					.and_then(|row| row.get::<_, String>(0))
					.map_err(|error| error.wrap("Failed to deserialize the rows."))
					.and_then(|id| id.parse());
				for parent in parents {
					queue.push_back(parent?);
				}
			}
		}

		let output = tg::object::PutOutput { incomplete };

		Ok(output)
	}

	pub async fn put_object_postgres(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
		database: &Postgres,
	) -> Result<tg::object::PutOutput> {
		let mut connection = database.get().await?;

		// Deserialize the data.
		let data = tg::object::Data::deserialize(id.kind(), &arg.bytes)
			.wrap_err("Failed to deserialize the data.")?;

		// Verify the ID.
		let verified_id: tg::object::Id = tg::Id::new_blake3(data.kind().into(), &arg.bytes)
			.try_into()
			.unwrap();
		if id != &verified_id {
			return Err(error!("The ID does not match the data."));
		}

		'a: {
			// Begin a transaction.
			let txn = connection
				.transaction()
				.await
				.wrap_err("Failed to begin the transaction.")?;

			// Add the object.
			{
				let statement = "
					insert into objects (id, bytes, complete, weight)
					values ($1, $2, false, null)
					on conflict (id) do nothing;
				";
				let id = id.to_string();
				let bytes = arg.bytes.to_vec();
				let params = postgres_params![id, bytes];
				let statement = txn
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the query.")?;
				let n = txn
					.execute(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
				if n == 0 {
					break 'a;
				}
			}

			// Add the children.
			for child in data.children() {
				let statement = "
					insert into object_children (object, child)
					values ($1, $2)
					on conflict (object, child) do nothing;
				";
				let object = id.to_string();
				let child = child.to_string();
				let params = postgres_params![object, child];
				let statement = txn
					.prepare(statement)
					.await
					.wrap_err("Failed to prepare the query.")?;
				txn.execute(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
			}

			// Commit the transaction.
			txn.commit()
				.await
				.wrap_err("Failed to commit the transaction.")?;
		}

		// Find the incomplete children.
		let incomplete: Vec<tg::object::Id> = {
			let statement = "
				select child
				from object_children
				left join objects on objects.id = object_children.child
				where object = $1 and (complete is null or complete = false);
			";
			let id = id.to_string();
			let params = postgres_params![id];
			let statement = connection
				.prepare_cached(statement)
				.await
				.wrap_err("Failed to prepare the query.")?;
			let rows = connection
				.query(&statement, params)
				.await
				.wrap_err("Failed to execute the statement.")?;
			rows.into_iter()
				.map(|row| row.try_get::<_, String>(0))
				.map_err(|error| error.wrap("Failed to deserialize the rows."))
				.and_then(|id| id.parse())
				.try_collect()?
		};

		// If there are no incomplete children, then update this object and its ancestors.
		if incomplete.is_empty() {
			let mut queue = VecDeque::from(vec![id.clone()]);
			while let Some(id) = queue.pop_front() {
				// Mark the object as complete and set its weight.
				let statement = "
					update objects
					set
						complete = true,
						weight = (select length(bytes) + (
							select coalesce(sum(weight), 0)
							from objects
							where id in (
								select child
								from object_children
								where object = $1
							)
						))
					where id = $1;
				";
				let id = id.to_string();
				let params = postgres_params![id];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the query.")?;
				connection
					.execute(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;

				// Add any parents whose children are all complete to the queue.
				let statement = "
					select object
					from object_children
					where child = $1 and (
						select min(complete)
						from objects
						where id in (
							select child
							from object_children oc
							where object = object_children.object
						)
					);
				";
				let id = id.to_string();
				let params = postgres_params![id];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the query.")?;
				let rows = connection
					.query(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
				let parents = rows
					.into_iter()
					.map(|row| row.try_get::<_, String>(0))
					.map_err(|error| error.wrap("Failed to deserialize the rows."))
					.and_then(|id| id.parse());
				for parent in parents {
					queue.push_back(parent?);
				}
			}
		}

		let output = tg::object::PutOutput { incomplete };

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

	#[async_recursion]
	pub async fn pull_object(&self, id: &tg::object::Id) -> Result<()> {
		let remote = self
			.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?;
		tg::object::Handle::with_id(id.clone())
			.pull(self, remote.as_ref())
			.await
			.wrap_err("Failed to pull the object.")?;
		Ok(())
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
		let complete = serde_json::to_string(&output.complete)
			.wrap_err("Failed to serialize the complete header.")?;
		let weight = serde_json::to_string(&output.weight)
			.wrap_err("Failed to serialize the weight header.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.header("x-tangram-object-complete", complete)
			.header("x-tangram-object-weight", weight)
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
		let arg = tg::object::PutArg { bytes };
		let output = self.inner.tg.put_object(&id, &arg).await?;

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
