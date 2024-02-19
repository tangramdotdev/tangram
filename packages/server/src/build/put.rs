use crate::{
	database::{Database, Postgres, PostgresJson, Sqlite, SqliteJson},
	postgres_params, sqlite_params, Http, Server,
};
use futures::{stream::FuturesUnordered, TryStreamExt};
use http_body_util::BodyExt;
use itertools::Itertools;
use num::ToPrimitive;
use std::sync::Arc;
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};
use tangram_util::http::{empty, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn put_build(
		&self,
		_user: Option<&tg::User>,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> Result<()> {
		// Verify the build is finished.
		if arg.status != tg::build::Status::Finished {
			return Err(error!("The build is not finished."));
		}

		// Insert the build.
		self.insert_build(id, arg).await?;

		Ok(())
	}

	pub(crate) async fn insert_build(
		&self,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> Result<()> {
		match &self.inner.database {
			Database::Sqlite(database) => self.insert_build_sqlite(id, arg, database).await,
			Database::Postgres(database) => self.insert_build_postgres(id, arg, database).await,
		}
	}

	async fn insert_build_sqlite(
		&self,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
		database: &Sqlite,
	) -> Result<()> {
		let connection = database.get().await?;

		// Delete any existing children.
		let statement = "
			delete from build_children
			where build = ?1;
		";
		let params = sqlite_params![id.to_string()];
		let mut statement = connection
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		statement
			.execute(params)
			.wrap_err("Failed to execute the statement.")?;

		// Insert the children.
		let statement = "
			insert into build_children (build, position, child)
			values (?1, ?2, ?3);
		";
		let mut statement = connection
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		arg.children
			.iter()
			.flatten()
			.enumerate()
			.map(|(position, child)| {
				let build = id.to_string();
				let position = position.to_i64().unwrap();
				let child = child.to_string();
				let params = sqlite_params![build, position, child];
				statement
					.execute(params)
					.wrap_err("Failed to execute the statement.")?;
				Ok::<_, Error>(())
			})
			.try_collect()?;

		// Delete any existing objects.
		let statement = "
			delete from build_objects
			where build = ?1;
		";
		let params = sqlite_params![id.to_string()];
		let mut statement = connection
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		statement
			.execute(params)
			.wrap_err("Failed to execute the statement.")?;

		// Add the objects.
		let objects = arg
			.log
			.clone()
			.map(Into::into)
			.into_iter()
			.chain(
				arg.outcome
					.as_ref()
					.and_then(|outcome| outcome.try_unwrap_succeeded_ref().ok())
					.map(tg::value::Data::children)
					.into_iter()
					.flatten(),
			)
			.chain(std::iter::once(arg.target.clone().into()));
		for object in objects {
			let statement = "
				insert into build_objects (build, object)
				values (?1, ?2)
				on conflict (build, object) do nothing;
			";
			let build = id.to_string();
			let object = object.to_string();
			let params = sqlite_params![build, object];
			let mut statement = connection
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the statement.")?;
		}

		// Insert the build.
		let statement = "
			insert into builds (
				id,
				children,
				descendants,
				host,
				log,
				outcome,
				retry,
				status,
				target,
				weight,
				created_at,
				queued_at,
				started_at,
				finished_at
			)
			values (
				?1,
				?2,
				?3,
				?4,
				?5,
				?6,
				?7,
				?8,
				?9,
				?10,
				?11,
				?12,
				?13,
				?14
			)
			on conflict do update set 
				id = ?1,
				children = ?2,
				descendants = ?3,
				host = ?4,
				log = ?5,
				outcome = ?6,
				retry = ?7,
				status = ?8,
				target = ?9,
				weight = ?10,
				created_at = ?11,
				queued_at = ?12,
				started_at = ?13,
				finished_at = ?14;
		";
		let id = id.to_string();
		let children = arg.children.clone().map(SqliteJson);
		let descendants = arg
			.descendants
			.map(|descendants| descendants.to_i64().unwrap());
		let host = arg.host.to_string();
		let log = arg.log.as_ref().map(ToString::to_string);
		let outcome = arg.outcome.clone().map(SqliteJson);
		let retry = arg.retry.to_string();
		let status = arg.status.to_string();
		let target = arg.target.to_string();
		let weight = arg.weight.map(|weight| weight.to_i64().unwrap());
		let created_at = arg
			.created_at
			.format(&Rfc3339)
			.wrap_err("Failed to format the created_at timestamp.")?;
		let queued_at = arg
			.queued_at
			.map(|timestamp| {
				timestamp
					.format(&Rfc3339)
					.wrap_err("Failed to format the queued_at timestamp.")
			})
			.transpose()?;
		let started_at = arg
			.started_at
			.map(|timestamp| {
				timestamp
					.format(&Rfc3339)
					.wrap_err("Failed to format the started_at timestamp.")
			})
			.transpose()?;
		let finished_at = arg
			.finished_at
			.map(|timestamp| {
				timestamp
					.format(&Rfc3339)
					.wrap_err("Failed to format the finished_at timestamp.")
			})
			.transpose()?;
		let params = sqlite_params![
			id,
			children,
			descendants,
			host,
			log,
			outcome,
			retry,
			status,
			target,
			weight,
			created_at,
			queued_at,
			started_at,
			finished_at
		];
		let mut statement = connection
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		statement
			.execute(params)
			.wrap_err("Failed to execute the statement.")?;

		Ok(())
	}

	async fn insert_build_postgres(
		&self,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
		database: &Postgres,
	) -> Result<()> {
		let connection = Arc::new(database.get().await?);

		// Delete any existing children.
		let statement = "
			delete from build_children
			where build = $1;
		";
		let params = postgres_params![id.to_string()];
		let statement = connection
			.prepare_cached(statement)
			.await
			.wrap_err("Failed to prepare the query.")?;
		connection
			.execute(&statement, params)
			.await
			.wrap_err("Failed to execute the statement.")?;

		// Insert the children.
		let statement = "
			insert into build_children (build, position, child)
			values ($1, $2, $3);
		";
		let statement = connection
			.prepare_cached(statement)
			.await
			.wrap_err("Failed to prepare the query.")?;
		arg.children
			.iter()
			.flatten()
			.enumerate()
			.map(|(position, child)| {
				let connection = connection.clone();
				let statement = statement.clone();
				async move {
					let build = id.to_string();
					let position = position.to_i64().unwrap();
					let child = child.to_string();
					let params = postgres_params![build, position, child];
					connection
						.execute(&statement, params)
						.await
						.wrap_err("Failed to execute the statement.")?;
					Ok::<_, Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Delete any existing objects.
		let statement = "
			delete from build_objects
			where build = $1;
		";
		let params = postgres_params![id.to_string()];
		let statement = connection
			.prepare_cached(statement)
			.await
			.wrap_err("Failed to prepare the query.")?;
		connection
			.execute(&statement, params)
			.await
			.wrap_err("Failed to execute the statement.")?;

		// Add the objects.
		let objects = arg
			.log
			.clone()
			.map(Into::into)
			.into_iter()
			.chain(
				arg.outcome
					.as_ref()
					.and_then(|outcome| outcome.try_unwrap_succeeded_ref().ok())
					.map(tg::value::Data::children)
					.into_iter()
					.flatten(),
			)
			.chain(std::iter::once(arg.target.clone().into()));
		objects
			.map(|object| {
				let connection = connection.clone();
				async move {
					let statement = "
					insert into build_objects (build, object)
					values ($1, $2)
					on conflict (build, object) do nothing;
				";
					let build = id.to_string();
					let object = object.to_string();
					let params = postgres_params![build, object];
					let statement = connection
						.prepare_cached(statement)
						.await
						.wrap_err("Failed to prepare the query.")?;
					connection
						.execute(&statement, params)
						.await
						.wrap_err("Failed to execute the statement.")?;
					Ok::<_, Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Insert the build.
		let statement = "
			upsert into builds (
				id,
				children,
				descendants,
				host,
				log,
				outcome,
				retry,
				status,
				target,
				weight,
				created_at,
				queued_at,
				started_at,
				finished_at
			)
			values (
				$1,
				$2,
				$3,
				$4,
				$5,
				$6,
				$7,
				$8,
				$9,
				$10,
				$11,
				$12,
				$13,
				$14
			);
		";
		let id = id.to_string();
		let children = arg.children.clone().map(PostgresJson);
		let descendants = arg
			.descendants
			.map(|descendants| descendants.to_i64().unwrap());
		let host = arg.host.to_string();
		let log = arg.log.as_ref().map(ToString::to_string);
		let outcome = arg.outcome.clone().map(PostgresJson);
		let retry = arg.retry.to_string();
		let status = arg.status.to_string();
		let target = arg.target.to_string();
		let weight = arg.weight.map(|weight| weight.to_i64().unwrap());
		let created_at = arg
			.created_at
			.format(&Rfc3339)
			.wrap_err("Failed to format the created_at timestamp.")?;
		let queued_at = arg
			.queued_at
			.map(|timestamp| {
				timestamp
					.format(&Rfc3339)
					.wrap_err("Failed to format the queued_at timestamp.")
			})
			.transpose()?;
		let started_at = arg
			.started_at
			.map(|timestamp| {
				timestamp
					.format(&Rfc3339)
					.wrap_err("Failed to format the started_at timestamp.")
			})
			.transpose()?;
		let finished_at = arg
			.finished_at
			.map(|timestamp| {
				timestamp
					.format(&Rfc3339)
					.wrap_err("Failed to format the finished_at timestamp.")
			})
			.transpose()?;
		let params = postgres_params![
			id,
			children,
			descendants,
			host,
			log,
			outcome,
			retry,
			status,
			target,
			weight,
			created_at,
			queued_at,
			started_at,
			finished_at
		];
		let statement = connection
			.prepare_cached(statement)
			.await
			.wrap_err("Failed to prepare the query.")?;
		connection
			.execute(&statement, params)
			.await
			.wrap_err("Failed to execute the statement.")?;

		Ok(())
	}
}

impl Http {
	pub async fn handle_put_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", build_id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let build_id = build_id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		// Put the build.
		self.inner
			.tg
			.put_build(user.as_ref(), &build_id, &arg)
			.await?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}
}
