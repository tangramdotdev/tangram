use crate::{
	database::{Database, Postgres, PostgresJson, Sqlite, SqliteJson},
	postgres_params, sqlite_params, Http, Server,
};
use itertools::Itertools;
use num::ToPrimitive;
use rusqlite as sqlite;
use tangram_client as tg;
use tangram_error::{error, Error, Result};
use tangram_util::{
	http::{bad_request, full, Incoming, Outgoing},
	iter::IterExt,
};
use time::format_description::well_known::Rfc3339;
use tokio_postgres as postgres;

impl Server {
	pub async fn list_builds(&self, arg: tg::build::ListArg) -> Result<tg::build::ListOutput> {
		match &self.inner.database {
			Database::Sqlite(database) => self.list_builds_sqlite(arg, database).await,
			Database::Postgres(database) => self.list_builds_postgres(arg, database).await,
		}
	}

	async fn list_builds_sqlite(
		&self,
		arg: tg::build::ListArg,
		database: &Sqlite,
	) -> Result<tg::build::ListOutput> {
		let connection = database.get().await?;
		let status = if arg.status.is_some() {
			"false"
		} else {
			"true"
		};
		let target = if arg.target.is_some() {
			"false"
		} else {
			"true"
		};
		let order = match arg.order.unwrap_or(tg::build::Order::CreatedAt) {
			tg::build::Order::CreatedAt => "order by created_at",
			tg::build::Order::CreatedAtDesc => "order by created_at desc",
		};
		let statement = &format!(
			"
				select
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
					finished_at
				from builds
				where
					(status = ?1 or {status}) and
					(target = ?2 or {target})
				{order}
				limit ?3
				offset ?4;
			"
		);
		let status = arg.status.map(|status| status.to_string());
		let target = arg.target.map(|target| target.to_string());
		let limit = arg.limit.map(|limit| limit.to_i64().unwrap());
		let offset = 0i64;
		let params = sqlite_params![status, target, limit, offset];
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|error| error!(source = error, "failed to prepare the query"))?;
		let items = statement
			.query(params)
			.map_err(|error| error!(source = error, "failed to execute the statment"))?
			.and_then(|row| {
				let id = row.get::<_, String>(0)?;
				let count = row.get::<_, Option<i64>>(1)?;
				let host = row.get::<_, String>(2)?;
				let log = row.get::<_, Option<String>>(3)?;
				let outcome = row.get::<_, Option<SqliteJson<tg::build::outcome::Data>>>(4)?;
				let retry = row.get::<_, String>(5)?;
				let status = row.get::<_, String>(6)?;
				let target = row.get::<_, String>(7)?;
				let weight = row.get::<_, Option<i64>>(8)?;
				let created_at = row.get::<_, String>(9)?;
				let queued_at = row.get::<_, Option<String>>(10)?;
				let started_at = row.get::<_, Option<String>>(11)?;
				let finished_at = row.get::<_, Option<String>>(12)?;
				Ok::<_, sqlite::Error>((
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
				))
			})
			.map_err(|error| error!(source = error, "failed to deserialize the rows"))
			.and_then(|row| {
				let (
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
				) = row;
				let id = id.parse()?;
				let count = count.map(|count| count.to_u64().unwrap());
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
						time::OffsetDateTime::parse(&timestamp, &Rfc3339).map_err(|error| {
							error!(source = error, "failed to parse the timestamp")
						})
					})
					.transpose()?;
				let started_at = started_at
					.map(|timestamp| {
						time::OffsetDateTime::parse(&timestamp, &Rfc3339).map_err(|error| {
							error!(source = error, "failed to parse the timestamp")
						})
					})
					.transpose()?;
				let finished_at = finished_at
					.map(|timestamp| {
						time::OffsetDateTime::parse(&timestamp, &Rfc3339).map_err(|error| {
							error!(source = error, "failed to parse the timestamp")
						})
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
				Ok::<_, Error>(output)
			})
			.try_collect()?;
		let output = tg::build::ListOutput { items };
		Ok(output)
	}

	async fn list_builds_postgres(
		&self,
		arg: tg::build::ListArg,
		database: &Postgres,
	) -> Result<tg::build::ListOutput> {
		let connection = database.get().await?;
		let status = if arg.status.is_some() {
			"false"
		} else {
			"true"
		};
		let target = if arg.target.is_some() {
			"false"
		} else {
			"true"
		};
		let order = match arg.order.unwrap_or(tg::build::Order::CreatedAt) {
			tg::build::Order::CreatedAt => "order by created_at",
			tg::build::Order::CreatedAtDesc => "order by created_at desc",
		};
		let statement = &format!(
			"
				select
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
					finished_at
				from builds
				where
					(status = $1 or {status}) and
					(target = $2 or {target})
				{order}
				limit $3
				offset $4;
			"
		);
		let status = arg.status.map(|status| status.to_string());
		let target = arg.target.map(|target| target.to_string());
		let limit = arg.limit.map(|limit| limit.to_i64().unwrap());
		let offset = 0i64;
		let params = postgres_params![status, target, limit, offset];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "failed to prepare the query"))?;
		let items = connection
			.query(&statement, params)
			.await
			.map_err(|error| error!(source = error, "failed to execute the statement"))?
			.into_iter()
			.map(|row| {
				let id = row.try_get::<_, String>(0)?;
				let count = row.try_get::<_, Option<i64>>(1)?;
				let host = row.try_get::<_, String>(2)?;
				let log = row.try_get::<_, Option<String>>(3)?;
				let outcome =
					row.try_get::<_, Option<PostgresJson<tg::build::outcome::Data>>>(4)?;
				let retry = row.try_get::<_, String>(5)?;
				let status = row.try_get::<_, String>(6)?;
				let target = row.try_get::<_, String>(7)?;
				let weight = row.try_get::<_, Option<i64>>(8)?;
				let created_at = row.try_get::<_, String>(9)?;
				let queued_at = row.try_get::<_, Option<String>>(10)?;
				let started_at = row.try_get::<_, Option<String>>(11)?;
				let finished_at = row.try_get::<_, Option<String>>(12)?;
				Ok::<_, postgres::Error>((
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
				))
			})
			.map_err(|error| error!(source = error, "failed to deserialize the rows"))
			.and_then(|row| {
				let (
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
				) = row;
				let id = id.parse()?;
				let count = count.map(|count| count.to_u64().unwrap());
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
						time::OffsetDateTime::parse(&timestamp, &Rfc3339).map_err(|error| {
							error!(source = error, "failed to parse the timestamp")
						})
					})
					.transpose()?;
				let started_at = started_at
					.map(|timestamp| {
						time::OffsetDateTime::parse(&timestamp, &Rfc3339).map_err(|error| {
							error!(source = error, "failed to parse the timestamp")
						})
					})
					.transpose()?;
				let finished_at = finished_at
					.map(|timestamp| {
						time::OffsetDateTime::parse(&timestamp, &Rfc3339).map_err(|error| {
							error!(source = error, "failed to parse the timestamp")
						})
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
				Ok::<_, Error>(output)
			})
			.try_collect()?;
		let output = tg::build::ListOutput { items };
		Ok(output)
	}
}

impl Http {
	pub async fn handle_list_builds_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Read the search params.
		let Some(query) = request.uri().query() else {
			return Ok(bad_request());
		};
		let arg = serde_urlencoded::from_str(query)
			.map_err(|error| error!(source = error, "failed to deserialize the search params"))?;

		let output = self.inner.tg.list_builds(arg).await?;

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
