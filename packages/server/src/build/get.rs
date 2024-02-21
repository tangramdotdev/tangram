use crate::{
	database::{Database, Postgres, PostgresJson, Sqlite, SqliteJson},
	postgres_params, sqlite_params, Http, Server,
};
use num::ToPrimitive;
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};
use tangram_util::http::{full, not_found, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn try_get_build(&self, id: &tg::build::Id) -> Result<Option<tg::build::GetOutput>> {
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
				children,
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
			.wrap_err("Failed to prepare the query.")?;
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to execute the statement.")?;
		let Some(row) = rows.next().wrap_err("Failed to retrieve the row.")? else {
			return Ok(None);
		};
		let id = row
			.get::<_, String>(0)
			.wrap_err("Failed to deserialize the column.")?;
		let children = row
			.get::<_, Option<SqliteJson<Vec<tg::build::Id>>>>(1)
			.wrap_err("Failed to deserialize the column.")?;
		let host = row
			.get::<_, String>(2)
			.wrap_err("Failed to deserialize the column.")?;
		let log = row
			.get::<_, Option<String>>(3)
			.wrap_err("Failed to deserialize the column.")?;
		let outcome = row
			.get::<_, Option<SqliteJson<tg::build::outcome::Data>>>(4)
			.wrap_err("Failed to deserialize the column.")?;
		let retry = row
			.get::<_, String>(5)
			.wrap_err("Failed to deserialize the column.")?;
		let status = row
			.get::<_, String>(6)
			.wrap_err("Failed to deserialize the column.")?;
		let target = row
			.get::<_, String>(7)
			.wrap_err("Failed to deserialize the column.")?;
		let weight = row
			.get::<_, Option<i64>>(8)
			.wrap_err("Failed to deserialize the column.")?;
		let created_at = row
			.get::<_, String>(9)
			.wrap_err("Failed to deserialize the column.")?;
		let queued_at = row
			.get::<_, Option<String>>(10)
			.wrap_err("Failed to deserialize the column.")?;
		let started_at = row
			.get::<_, Option<String>>(11)
			.wrap_err("Failed to deserialize the column.")?;
		let finished_at = row
			.get::<_, Option<String>>(12)
			.wrap_err("Failed to deserialize the column.")?;
		let id = id.parse()?;
		let children = children.map(|children| children.0);
		let host = host.parse()?;
		let log = log.map(|log| log.parse()).transpose()?;
		let outcome = outcome.map(|outcome| outcome.0);
		let retry = retry.parse()?;
		let status = status.parse()?;
		let target = target.parse()?;
		let weight = weight.map(|weight| weight.to_u64().unwrap());
		let created_at = time::OffsetDateTime::parse(&created_at, &Rfc3339)
			.wrap_err("Failed to parse the timestamp.")?;
		let queued_at = queued_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.wrap_err("Failed to parse the timestamp.")
			})
			.transpose()?;
		let started_at = started_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.wrap_err("Failed to parse the timestamp.")
			})
			.transpose()?;
		let finished_at = finished_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.wrap_err("Failed to parse the timestamp.")
			})
			.transpose()?;
		let output = tg::build::GetOutput {
			id,
			children,
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
				children,
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
			.wrap_err("Failed to prepare the query.")?;
		let row = connection
			.query_one(&statement, params)
			.await
			.wrap_err("Failed to execute the statement.")?;
		let id = row
			.try_get::<_, String>(0)
			.wrap_err("Failed to deserialize the column.")?;
		let children = row
			.try_get::<_, Option<PostgresJson<Vec<tg::build::Id>>>>(1)
			.wrap_err("Failed to deserialize the column.")?;
		let host = row
			.try_get::<_, String>(2)
			.wrap_err("Failed to deserialize the column.")?;
		let log = row
			.try_get::<_, Option<String>>(3)
			.wrap_err("Failed to deserialize the column.")?;
		let outcome = row
			.try_get::<_, Option<PostgresJson<tg::build::outcome::Data>>>(4)
			.wrap_err("Failed to deserialize the column.")?;
		let retry = row
			.try_get::<_, String>(5)
			.wrap_err("Failed to deserialize the column.")?;
		let status = row
			.try_get::<_, String>(6)
			.wrap_err("Failed to deserialize the column.")?;
		let target = row
			.try_get::<_, String>(7)
			.wrap_err("Failed to deserialize the column.")?;
		let weight = row
			.try_get::<_, Option<i64>>(8)
			.wrap_err("Failed to deserialize the column.")?;
		let created_at = row
			.try_get::<_, String>(9)
			.wrap_err("Failed to deserialize the column.")?;
		let queued_at = row
			.try_get::<_, Option<String>>(10)
			.wrap_err("Failed to deserialize the column.")?;
		let started_at = row
			.try_get::<_, Option<String>>(11)
			.wrap_err("Failed to deserialize the column.")?;
		let finished_at = row
			.try_get::<_, Option<String>>(12)
			.wrap_err("Failed to deserialize the column.")?;
		let id = id.parse()?;
		let children = children.map(|children| children.0);
		let host = host.parse()?;
		let log = log.map(|log| log.parse()).transpose()?;
		let outcome = outcome.map(|outcome| outcome.0);
		let retry = retry.parse()?;
		let status = status.parse()?;
		let target = target.parse()?;
		let weight = weight.map(|weight| weight.to_u64().unwrap());
		let created_at = time::OffsetDateTime::parse(&created_at, &Rfc3339)
			.wrap_err("Failed to parse the timestamp.")?;
		let queued_at = queued_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.wrap_err("Failed to parse the timestamp.")
			})
			.transpose()?;
		let started_at = started_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.wrap_err("Failed to parse the timestamp.")
			})
			.transpose()?;
		let finished_at = finished_at
			.map(|timestamp| {
				time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.wrap_err("Failed to parse the timestamp.")
			})
			.transpose()?;
		let output = tg::build::GetOutput {
			id,
			children,
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
		let Some(output) = remote.try_get_build(id).await? else {
			return Ok(None);
		};

		// Insert the build if it is finished.
		if output.status == tg::build::Status::Finished {
			let arg = tg::build::PutArg {
				id: output.id.clone(),
				children: output.children.clone(),
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
		let ["builds", build_id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let build_id = build_id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the build.
		let Some(output) = self.inner.tg.try_get_build(&build_id).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = serde_json::to_string(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();

		Ok(response)
	}
}
