use crate::{
	database::{Database, Postgres, PostgresJson, Sqlite, SqliteJson},
	postgres_params, sqlite_params, Http, Server,
};
use itertools::Itertools;
use num::ToPrimitive;
use rusqlite as sqlite;
use tangram_client as tg;
use tangram_error::{Error, Result, Wrap, WrapErr};
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
			.wrap_err("Failed to prepare the query.")?;
		let items = statement
			.query(params)
			.wrap_err("Failed to execute the statment.")?
			.and_then(|row| {
				let id = row.get::<_, String>(0)?;
				let children = row.get::<_, Option<SqliteJson<Vec<tg::build::Id>>>>(1)?;
				let descendants = row.get::<_, Option<i64>>(2)?;
				let host = row.get::<_, String>(3)?;
				let log = row.get::<_, Option<String>>(4)?;
				let outcome = row.get::<_, Option<SqliteJson<tg::build::outcome::Data>>>(5)?;
				let retry = row.get::<_, String>(6)?;
				let status = row.get::<_, String>(7)?;
				let target = row.get::<_, String>(8)?;
				let weight = row.get::<_, Option<i64>>(9)?;
				let created_at = row.get::<_, String>(10)?;
				let queued_at = row.get::<_, Option<String>>(11)?;
				let started_at = row.get::<_, Option<String>>(12)?;
				let finished_at = row.get::<_, Option<String>>(13)?;
				Ok::<_, sqlite::Error>((
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
					finished_at,
				))
			})
			.map_err(|error| error.wrap("Failed to deserialize the rows."))
			.and_then(|row| {
				let (
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
					finished_at,
				) = row;
				let id = id.parse()?;
				let children = children.map(|children| children.0);
				let descendants = descendants.map(|descendants| descendants.to_u64().unwrap());
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
			.wrap_err("Failed to prepare the query.")?;
		let items = connection
			.query(&statement, params)
			.await
			.wrap_err("Failed to execute the statement.")?
			.into_iter()
			.map(|row| {
				let id = row.try_get::<_, String>(0)?;
				let children = row.try_get::<_, Option<PostgresJson<Vec<tg::build::Id>>>>(1)?;
				let descendants = row.try_get::<_, Option<i64>>(2)?;
				let host = row.try_get::<_, String>(3)?;
				let log = row.try_get::<_, Option<String>>(4)?;
				let outcome =
					row.try_get::<_, Option<PostgresJson<tg::build::outcome::Data>>>(5)?;
				let retry = row.try_get::<_, String>(6)?;
				let status = row.try_get::<_, String>(7)?;
				let target = row.try_get::<_, String>(8)?;
				let weight = row.try_get::<_, Option<i64>>(9)?;
				let created_at = row.try_get::<_, String>(10)?;
				let queued_at = row.try_get::<_, Option<String>>(11)?;
				let started_at = row.try_get::<_, Option<String>>(12)?;
				let finished_at = row.try_get::<_, Option<String>>(13)?;
				Ok::<_, postgres::Error>((
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
					finished_at,
				))
			})
			.map_err(|error| error.wrap("Failed to deserialize the rows."))
			.and_then(|row| {
				let (
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
					finished_at,
				) = row;
				let id = id.parse()?;
				let children = children.map(|children| children.0);
				let descendants = descendants.map(|descendants| descendants.to_u64().unwrap());
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
			.wrap_err("Failed to deserialize the search params.")?;

		let output = self.inner.tg.list_builds(arg).await?;

		// Create the response.
		let body = serde_json::to_string(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();

		Ok(response)
	}
}
