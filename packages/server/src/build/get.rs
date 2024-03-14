use crate::{
	database::{Database, Postgres, PostgresJson, Sqlite, SqliteJson},
	postgres_params, sqlite_params, Http, Server,
};
use futures::{stream, StreamExt, TryStreamExt};
use num::ToPrimitive;
use tangram_client as tg;
use tangram_error::{error, Error, Result};
use tangram_util::http::{full, not_found, Incoming, Outgoing};
use tg::Handle;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn try_get_build(
		&self,
		id: &tg::build::Id,
		_arg: tg::build::GetArg,
	) -> Result<Option<tg::build::GetOutput>> {
		if let Some(output) = self.try_get_build_local(id).await? {
			Ok(Some(output))
		} else if let Some(output) = self.try_get_build_remote(id).await? {
			Ok(Some(output))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn try_get_build_local(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::GetOutput>> {
		match &self.inner.database {
			Database::Sqlite(database) => self.try_get_build_sqlite(id, database).await,
			Database::Postgres(database) => self.try_get_build_postgres(id, database).await,
		}
	}

	async fn try_get_build_sqlite(
		&self,
		id: &tg::build::Id,
		database: &Sqlite,
	) -> Result<Option<tg::build::GetOutput>> {
		let connection = database.get().await?;
		let statement = "
			select
				id,
				complete,
				count,
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
			from builds
			where id = ?1;
		";
		let params = sqlite_params![id.to_string()];
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|error| error!(source = error, "failed to prepare the query"))?;
		let mut rows = statement
			.query(params)
			.map_err(|error| error!(source = error, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|error| error!(source = error, "failed to retrieve the row"))?
		else {
			return Ok(None);
		};
		let id = row
			.get::<_, String>(0)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let _complete = row
			.get::<_, bool>(1)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let count = row
			.get::<_, Option<i64>>(2)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let host = row
			.get::<_, String>(3)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let log = row
			.get::<_, Option<String>>(4)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let outcome = row
			.get::<_, Option<SqliteJson<tg::build::outcome::Data>>>(5)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let retry = row
			.get::<_, String>(6)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let status = row
			.get::<_, String>(7)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let target = row
			.get::<_, String>(8)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let weight = row
			.get::<_, Option<i64>>(9)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let created_at = row
			.get::<_, String>(10)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let queued_at = row
			.get::<_, Option<String>>(11)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let started_at = row
			.get::<_, Option<String>>(12)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let finished_at = row
			.get::<_, Option<String>>(13)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let id = id.parse()?;
		let count = count.map(|count| count.to_u64().unwrap());
		let host = host.parse()?;
		let log = log.map(|log| log.parse()).transpose()?;
		let outcome = outcome.map(|outcome| outcome.0);
		let retry = retry.parse()?;
		let status = status.parse()?;
		let target = target.parse()?;
		let weight = weight.map(|weight| weight.to_u64().unwrap());
		let created_at = time::OffsetDateTime::parse(&created_at, &Rfc3339)
			.map_err(|error| error!(source = error, "failed to parse the timestamp"))?;
		let queued_at = queued_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.map_err(|error| error!(source = error, "failed to parse the timestamp"))
			})
			.transpose()?;
		let started_at = started_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.map_err(|error| error!(source = error, "failed to parse the timestamp"))
			})
			.transpose()?;
		let finished_at = finished_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.map_err(|error| error!(source = error, "failed to parse the timestamp"))
			})
			.transpose()?;
		let output = tg::build::GetOutput {
			id,
			count,
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
			finished_at,
		};
		Ok(Some(output))
	}

	async fn try_get_build_postgres(
		&self,
		id: &tg::build::Id,
		database: &Postgres,
	) -> Result<Option<tg::build::GetOutput>> {
		let connection = database.get().await?;
		let statement = "
			select
				id,
				complete,
				count,
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
			from builds
			where id = $1;
		";
		let params = postgres_params![id.to_string()];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "failed to prepare the query"))?;
		let row = connection
			.query_one(&statement, params)
			.await
			.map_err(|error| error!(source = error, "failed to execute the statement"))?;
		let id = row
			.try_get::<_, String>(0)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let _complete = row
			.try_get::<_, bool>(1)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let count = row
			.try_get::<_, Option<i64>>(2)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let host = row
			.try_get::<_, String>(3)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let log = row
			.try_get::<_, Option<String>>(4)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let outcome = row
			.try_get::<_, Option<PostgresJson<tg::build::outcome::Data>>>(5)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let retry = row
			.try_get::<_, String>(6)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let status = row
			.try_get::<_, String>(7)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let target = row
			.try_get::<_, String>(8)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let weight = row
			.try_get::<_, Option<i64>>(9)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let created_at = row
			.try_get::<_, String>(10)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let queued_at = row
			.try_get::<_, Option<String>>(11)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let started_at = row
			.try_get::<_, Option<String>>(12)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let finished_at = row
			.try_get::<_, Option<String>>(13)
			.map_err(|error| error!(source = error, "failed to deserialize the column"))?;
		let id = id.parse()?;
		let count = count.map(|count| count.to_u64().unwrap());
		let host = host.parse()?;
		let log = log.map(|log| log.parse()).transpose()?;
		let outcome = outcome.map(|outcome| outcome.0);
		let retry = retry.parse()?;
		let status = status.parse()?;
		let target = target.parse()?;
		let weight = weight.map(|weight| weight.to_u64().unwrap());
		let created_at = time::OffsetDateTime::parse(&created_at, &Rfc3339)
			.map_err(|error| error!(source = error, "failed to parse the timestamp"))?;
		let queued_at = queued_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.map_err(|error| error!(source = error, "failed to parse the timestamp"))
			})
			.transpose()?;
		let started_at = started_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.map_err(|error| error!(source = error, "failed to parse the timestamp"))
			})
			.transpose()?;
		let finished_at = finished_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.map_err(|error| error!(source = error, "failed to parse the timestamp"))
			})
			.transpose()?;
		let output = tg::build::GetOutput {
			id,
			count,
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
			finished_at,
		};
		Ok(Some(output))
	}

	async fn try_get_build_remote(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::GetOutput>> {
		// Get the remote.
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};

		// Get the build from the remote server.
		let arg = tg::build::GetArg::default();
		let Some(output) = remote.try_get_build(id, arg).await? else {
			return Ok(None);
		};

		// Insert the build if it is finished.
		if output.status == tg::build::Status::Finished {
			let arg = tg::build::children::GetArg {
				timeout: Some(std::time::Duration::ZERO),
				..Default::default()
			};
			let children = self
				.get_build_children(id, arg, None)
				.await?
				.map_ok(|chunk| stream::iter(chunk.items).map(Ok::<_, Error>))
				.try_flatten()
				.try_collect()
				.await?;
			let arg = tg::build::PutArg {
				id: output.id.clone(),
				children,
				count: output.count,
				host: output.host.clone(),
				log: output.log.clone(),
				outcome: output.outcome.clone(),
				retry: output.retry,
				status: output.status,
				target: output.target.clone(),
				weight: output.weight,
				created_at: output.created_at,
				queued_at: output.queued_at,
				started_at: output.started_at,
				finished_at: output.finished_at,
			};
			self.insert_build(id, &arg).await?;
		}

		Ok(Some(output))
	}
}

impl Http {
	pub async fn handle_get_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let id = id
			.parse()
			.map_err(|error| error!(source = error, "failed to parse the ID"))?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.map_err(|error| error!(source = error, "failed to deserialize the search params"))?
			.unwrap_or_default();

		// Get the build.
		let Some(output) = self.inner.tg.try_get_build(&id, arg).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|error| error!(source = error, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}
}
