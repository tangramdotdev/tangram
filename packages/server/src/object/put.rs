use crate::{
	database::{Database, Postgres, PostgresTransaction, Sqlite, Transaction},
	postgres_params, sqlite_params, Http, Server,
};
use http_body_util::BodyExt;
use itertools::Itertools;
use tangram_client as tg;
use tangram_error::{error, Result, Wrap, WrapErr};
use tangram_util::{
	http::{bad_request, full, Incoming, Outgoing},
	iter::IterExt,
};

impl Server {
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

	async fn put_object_sqlite(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
		database: &Sqlite,
	) -> Result<tg::object::PutOutput> {
		let connection = database.get().await?;

		// Add the object.
		let children = {
			let statement = "
				insert into objects (id, bytes, children, complete, count, weight)
				values (?1, ?2, false, false, null, null)
				on conflict (id) do update set id = excluded.id
				returning children;
			";
			let id = id.to_string();
			let bytes = arg.bytes.to_vec();
			let params = sqlite_params![id, bytes];
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
			row.get::<_, bool>(0)
				.wrap_err("Failed to deserialize the column.")?
		};

		// Find the incomplete children.
		let incomplete: Vec<tg::object::Id> = if children {
			let statement = "
				select child
				from object_children
				left join objects on objects.id = object_children.child
				where object = ?1 and complete = false;
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
		} else {
			let data = tg::object::Data::deserialize(id.kind(), &arg.bytes)
				.wrap_err("Failed to deserialize the data.")?;
			data.children()
		};

		let output = tg::object::PutOutput { incomplete };

		Ok(output)
	}

	async fn put_object_postgres(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
		database: &Postgres,
	) -> Result<tg::object::PutOutput> {
		let connection = database.get().await?;

		// Add the object.
		let children = {
			let statement = "
				upsert into objects (id, bytes, children, complete, count, weight)
				values ($1, $2, false, false, null, null)
				returning children;
			";
			let id = id.to_string();
			let bytes = arg.bytes.to_vec();
			let params = postgres_params![id, bytes];
			let statement = connection
				.prepare_cached(statement)
				.await
				.wrap_err("Failed to prepare the query.")?;
			let rows = connection
				.query(&statement, params)
				.await
				.wrap_err("Failed to execute the statement.")?;
			let row = rows.into_iter().next().wrap_err("Expected a row.")?;
			row.try_get::<_, bool>(0)
				.wrap_err("Failed to deserialize the column.")?
		};

		// Find the incomplete children.
		let incomplete: Vec<tg::object::Id> = if children {
			let statement = "
				select child
				from object_children
				left join objects on objects.id = object_children.child
				where object = $1 and complete = false;
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
		} else {
			let data = tg::object::Data::deserialize(id.kind(), &arg.bytes)
				.wrap_err("Failed to deserialize the data.")?;
			data.children()
		};

		let output = tg::object::PutOutput { incomplete };

		Ok(output)
	}

	pub(crate) async fn put_complete_object_with_transaction(
		&self,
		id: tg::object::Id,
		data: tg::object::Data,
		txn: &Transaction<'_>,
	) -> Result<()> {
		match txn {
			Transaction::Sqlite(txn) => {
				self.put_complete_object_with_transaction_sqlite(&id, &data, txn)
					.await
			},
			Transaction::Postgres(txn) => {
				self.put_complete_object_with_transaction_postgres(&id, &data, txn)
					.await
			},
		}
	}

	async fn put_complete_object_with_transaction_sqlite(
		&self,
		id: &tg::object::Id,
		data: &tg::object::Data,
		txn: &rusqlite::Transaction<'_>,
	) -> Result<()> {
		// Serialize the object.
		let bytes = data.serialize()?.to_vec();

		// Add the object.
		{
			let statement = "
				insert into objects (id, bytes, children, complete, count, weight)
				values (?1, ?2, false, true, null, null)
				on conflict (id) do update set complete = true;
			";
			let id = id.to_string();
			let params = sqlite_params![id, bytes];
			let mut statement = txn
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the statement.")?;
		}

		Ok(())
	}

	async fn put_complete_object_with_transaction_postgres(
		&self,
		id: &tg::object::Id,
		data: &tg::object::Data,
		txn: &PostgresTransaction<'_>,
	) -> Result<()> {
		// Serialize the object.
		let bytes = data.serialize()?.to_vec();

		// Add the object.
		{
			let statement = "
				insert into objects (id, bytes, children, complete, count, weight)
				values ($1, $2, false, true, null, null)
				on conflict (id) do update set complete = true;
			";
			let id = id.to_string();
			let params = postgres_params![id, bytes];
			let statement = txn
				.prepare_cached(statement)
				.await
				.wrap_err("Failed to prepare the query.")?;
			txn.execute(&statement, params)
				.await
				.wrap_err("Failed to execute the statement.")?;
		}

		Ok(())
	}
}

impl Http {
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
		let arg = tg::object::PutArg {
			bytes,
			count: None,
			weight: None,
		};
		let output = self.inner.tg.put_object(&id, &arg).await?;

		// Create the body.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the body.")?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}
}
