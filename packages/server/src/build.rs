use super::Server;
use crate::{
	database::{Database, Postgres, PostgresJson, Sqlite, SqliteJson},
	postgres_params, sqlite_params, BuildState, Http, Permit,
};
use either::Either;
use futures::{future, stream::FuturesUnordered, FutureExt, TryFutureExt, TryStreamExt};
use http_body_util::BodyExt;
use itertools::Itertools;
use num::ToPrimitive;
use rusqlite as sqlite;
use std::{pin::pin, sync::Arc};
use tangram_client as tg;
use tangram_error::{error, Error, Result, Wrap, WrapErr};
use tangram_util::{
	http::{bad_request, empty, full, not_found, ok, Incoming, Outgoing},
	iter::IterExt,
};
use time::format_description::well_known::Rfc3339;
use tokio_postgres as postgres;
use tokio_stream::StreamExt;

mod children;
mod log;
mod outcome;
mod status;

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
		let r#where = match (&arg.status, &arg.target) {
			(Some(_), None) => "where status = ?1",
			(None, Some(_)) => "where target = ?2",
			_ => "",
		};
		let order = match arg.order {
			Some(tg::build::Order::CreatedAt) => "order by created_at",
			Some(tg::build::Order::CreatedAtDesc) => "order by created_at desc",
			None => "",
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
				{where}
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
		let r#where = match (&arg.status, &arg.target) {
			(Some(_), None) => "where status = ?1",
			(None, Some(_)) => "where target = ?2",
			_ => "",
		};
		let order = match arg.order {
			Some(tg::build::Order::CreatedAt) => "order by created_at",
			Some(tg::build::Order::CreatedAtDesc) => "order by created_at desc",
			None => "",
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
					queued_at,
					started_at,
					finished_at
				from builds
				{where}
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

	pub async fn try_get_build(&self, id: &tg::build::Id) -> Result<Option<tg::build::GetOutput>> {
		if let Some(output) = self.try_get_build_local(id).await? {
			Ok(Some(output))
		} else if let Some(output) = self.try_get_build_remote(id).await? {
			Ok(Some(output))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_local(
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
		let descendants = row
			.get::<_, Option<i64>>(2)
			.wrap_err("Failed to deserialize the column.")?;
		let host = row
			.get::<_, String>(3)
			.wrap_err("Failed to deserialize the column.")?;
		let log = row
			.get::<_, Option<String>>(4)
			.wrap_err("Failed to deserialize the column.")?;
		let outcome = row
			.get::<_, Option<SqliteJson<tg::build::outcome::Data>>>(5)
			.wrap_err("Failed to deserialize the column.")?;
		let retry = row
			.get::<_, String>(6)
			.wrap_err("Failed to deserialize the column.")?;
		let status = row
			.get::<_, String>(7)
			.wrap_err("Failed to deserialize the column.")?;
		let target = row
			.get::<_, String>(8)
			.wrap_err("Failed to deserialize the column.")?;
		let weight = row
			.get::<_, Option<i64>>(9)
			.wrap_err("Failed to deserialize the column.")?;
		let created_at = row
			.get::<_, String>(10)
			.wrap_err("Failed to deserialize the column.")?;
		let queued_at = row
			.get::<_, Option<String>>(11)
			.wrap_err("Failed to deserialize the column.")?;
		let started_at = row
			.get::<_, Option<String>>(12)
			.wrap_err("Failed to deserialize the column.")?;
		let finished_at = row
			.get::<_, Option<String>>(13)
			.wrap_err("Failed to deserialize the column.")?;
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
		Ok(Some(output))
	}

	async fn try_get_build_postgres(
		&self,
		id: &tg::build::Id,
		database: &Postgres,
	) -> Result<Option<tg::build::GetOutput>> {
		let connection = database.get().await?;
		let statement = "
			select id, children, log, outcome, status, target, timestamp
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
		let descendants = row
			.try_get::<_, Option<i64>>(2)
			.wrap_err("Failed to deserialize the column.")?;
		let host = row
			.try_get::<_, String>(3)
			.wrap_err("Failed to deserialize the column.")?;
		let log = row
			.try_get::<_, Option<String>>(4)
			.wrap_err("Failed to deserialize the column.")?;
		let outcome = row
			.try_get::<_, Option<PostgresJson<tg::build::outcome::Data>>>(5)
			.wrap_err("Failed to deserialize the column.")?;
		let retry = row
			.try_get::<_, String>(6)
			.wrap_err("Failed to deserialize the column.")?;
		let status = row
			.try_get::<_, String>(7)
			.wrap_err("Failed to deserialize the column.")?;
		let target = row
			.try_get::<_, String>(8)
			.wrap_err("Failed to deserialize the column.")?;
		let weight = row
			.try_get::<_, Option<i64>>(9)
			.wrap_err("Failed to deserialize the column.")?;
		let created_at = row
			.try_get::<_, String>(10)
			.wrap_err("Failed to deserialize the column.")?;
		let queued_at = row
			.try_get::<_, Option<String>>(11)
			.wrap_err("Failed to deserialize the column.")?;
		let started_at = row
			.try_get::<_, Option<String>>(12)
			.wrap_err("Failed to deserialize the column.")?;
		let finished_at = row
			.try_get::<_, Option<String>>(13)
			.wrap_err("Failed to deserialize the column.")?;
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
				descendants: output.descendants,
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

	async fn insert_build(&self, id: &tg::build::Id, arg: &tg::build::PutArg) -> Result<()> {
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

	pub async fn push_build(&self, user: Option<&tg::User>, id: &tg::build::Id) -> Result<()> {
		let remote = self
			.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?;
		tg::Build::with_id(id.clone())
			.push(user, self, remote.as_ref())
			.await
			.wrap_err("Failed to push the build.")?;
		Ok(())
	}

	pub async fn pull_build(&self, id: &tg::build::Id) -> Result<()> {
		let remote = self
			.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?;
		tg::Build::with_id(id.clone())
			.pull(self, remote.as_ref())
			.await
			.wrap_err("Failed to pull the build.")?;
		Ok(())
	}

	pub async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput> {
		// Get a local build if one exists that satisfies the retry constraint.
		let build = 'a: {
			// Find a build.
			let list_arg = tg::build::ListArg {
				limit: Some(1),
				order: Some(tg::build::Order::CreatedAtDesc),
				status: None,
				target: Some(arg.target.clone()),
			};
			let Some(build) = self
				.list_builds(list_arg)
				.await?
				.items
				.first()
				.cloned()
				.map(|state| tg::Build::with_id(state.id))
			else {
				break 'a None;
			};

			// Verify the build satisfies the retry constraint.
			let outcome_arg = tg::build::outcome::GetArg {
				timeout: Some(std::time::Duration::ZERO),
			};
			let outcome = build.get_outcome(self, outcome_arg).await?;
			if let Some(outcome) = outcome {
				if outcome.retry() <= arg.retry {
					break 'a None;
				}
			}

			Some(build)
		};

		// Get a remote build if one exists that satisfies the retry constraint.
		let build = 'a: {
			if let Some(build) = build {
				break 'a Some(build);
			}

			// Get the remote.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a None;
			};

			// Find a build.
			let list_arg = tg::build::ListArg {
				limit: Some(1),
				order: Some(tg::build::Order::CreatedAtDesc),
				status: None,
				target: Some(arg.target.clone()),
			};
			let Some(build) = remote
				.list_builds(list_arg)
				.await?
				.items
				.first()
				.cloned()
				.map(|state| tg::Build::with_id(state.id))
			else {
				break 'a None;
			};

			// Verify the build satisfies the retry constraint.
			let outcome_arg = tg::build::outcome::GetArg {
				timeout: Some(std::time::Duration::ZERO),
			};
			let outcome = build.get_outcome(self, outcome_arg).await?;
			if let Some(outcome) = outcome {
				if outcome.retry() <= arg.retry {
					break 'a None;
				}
			}

			Some(build)
		};

		// If a local or remote build was found that satisfies the retry constraint, then return it.
		if let Some(build) = build {
			// Add the build as a child of the parent.
			if let Some(parent) = arg.parent.as_ref() {
				self.add_build_child(user, parent, build.id()).await?;
			}

			let output = tg::build::GetOrCreateOutput {
				id: build.id().clone(),
			};

			return Ok(output);
		}

		// If the build has a parent and it is not local, or if the build has no parent and the remote flag is set, then attempt to get or create a remote build.
		'a: {
			// Determine if the build should be remote.
			let remote = if arg.remote {
				true
			} else if let Some(parent) = arg.parent.as_ref() {
				self.get_build_exists_local(parent).await?
			} else {
				false
			};
			if !remote {
				break 'a;
			}

			// Get the remote.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};

			// Push the target.
			let object = tg::object::Handle::with_id(arg.target.clone().into());
			let Ok(()) = object.push(self, remote.as_ref()).await else {
				break 'a;
			};

			// Get or create the build on the remote.
			let Ok(output) = remote.get_or_create_build(user, arg.clone()).await else {
				break 'a;
			};

			return Ok(output);
		}

		// Otherwise, create a new build.
		let build_id = tg::build::Id::new();

		// Get the host.
		let target = tg::Target::with_id(arg.target.clone());
		let host = target.host(self).await?;

		// Create the build state.
		let permit = Arc::new(tokio::sync::Mutex::new(None));
		let (stop, _) = tokio::sync::watch::channel(false);
		let state = Arc::new(BuildState {
			permit,
			stop,
			task: std::sync::Mutex::new(None),
		});
		self.inner
			.build_state
			.write()
			.unwrap()
			.insert(build_id.clone(), state);

		// Insert the build.
		let put_arg = tg::build::PutArg {
			id: build_id.clone(),
			children: None,
			descendants: None,
			host: host.clone(),
			log: None,
			outcome: None,
			retry: arg.retry,
			status: tg::build::Status::Created,
			target: arg.target.clone(),
			weight: None,
			created_at: time::OffsetDateTime::now_utc(),
			queued_at: None,
			started_at: None,
			finished_at: None,
		};
		self.insert_build(&build_id, &put_arg).await?;

		// Add the build to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			self.add_build_child(user, parent, &build_id).await?;
		}

		// Send the message.
		self.inner.messenger.publish_to_build_created().await?;

		let output = tg::build::GetOrCreateOutput { id: build_id };

		Ok(output)
	}

	pub(crate) async fn build_queue_task(
		&self,
		mut stop: tokio::sync::watch::Receiver<bool>,
	) -> Result<()> {
		let task = self.build_queue_task_inner();
		let stop = stop.wait_for(|stop| *stop);
		future::select(pin!(task), pin!(stop))
			.map(|output| match output {
				future::Either::Left((Err(error), _)) => Err(error),
				_ => Ok(()),
			})
			.await?;
		Ok(())
	}

	async fn build_queue_task_inner(&self) -> Result<()> {
		loop {
			// If the messenger is nats, then wait for a permit.
			let permit = match self.inner.messenger.kind() {
				crate::messenger::Kind::Channel => None,
				crate::messenger::Kind::Nats => {
					let Ok(permit) = self.inner.build_semaphore.clone().acquire_owned().await
					else {
						return Ok(());
					};
					Some(permit)
				},
			};

			// Wait for a message that a build was created.
			self.inner
				.messenger
				.subscribe_to_build_created()
				.await?
				.next()
				.await;

			// Attempt to get a build.
			let arg = tg::build::ListArg {
				limit: Some(1),
				order: Some(tg::build::Order::CreatedAt),
				status: Some(tg::build::Status::Created),
				target: None,
			};
			let output = self.list_builds(arg).await?;
			let Some(build) = output.items.first() else {
				continue;
			};
			let id = build.id.clone();

			let task = tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				async move {
					// Update the status to queued.
					server
						.set_build_status(None, &id, tg::build::Status::Queued)
						.await?;

					// If the build does not have a permit, then wait for one, either from the semaphore or one of the build's parents.
					let permit = if let Some(permit) = permit {
						Permit(Either::Left(permit))
					} else {
						let semaphore = server
							.inner
							.build_semaphore
							.clone()
							.acquire_owned()
							.map(|result| Permit(Either::Left(result.unwrap())));

						let parent = server.try_get_build_parent(&id).await?;
						let state = parent.and_then(|parent| {
							server
								.inner
								.build_state
								.read()
								.unwrap()
								.get(&parent)
								.cloned()
						});
						let parent = if let Some(state) = state {
							state
								.permit
								.clone()
								.lock_owned()
								.map(|guard| Permit(Either::Right(guard)))
								.left_future()
						} else {
							future::pending().right_future()
						};

						tokio::select! {
							permit = semaphore => permit,
							permit = parent => permit,
						}
					};

					// Set the permit in the build state.
					let state = server
						.inner
						.build_state
						.write()
						.unwrap()
						.get_mut(&id)
						.unwrap()
						.clone();
					state.permit.lock().await.replace(permit);

					// Update the status to started.
					server
						.set_build_status(None, &id, tg::build::Status::Started)
						.await?;

					let build = tg::Build::with_id(id.clone());
					let target = build.target(&server).await?;

					// Get the stop receiver.
					let stop = server
						.inner
						.build_state
						.read()
						.unwrap()
						.get(&id)
						.unwrap()
						.stop
						.subscribe();

					// Build the target with the appropriate runtime.
					let result = match target.host(&server).await?.os() {
						tg::system::Os::Js => {
							// Build the target on the server's local pool because it is a `!Send` future.
							tangram_runtime::js::build(&server, &build, stop).await
						},
						tg::system::Os::Darwin => {
							#[cfg(target_os = "macos")]
							{
								// If the VFS is disabled, then perform an internal checkout.
								if server.inner.vfs.lock().unwrap().is_none() {
									target
										.data(&server)
										.await?
										.children()
										.into_iter()
										.filter_map(|id| id.try_into().ok())
										.map(|id| {
											let server = server.clone();
											async move {
												let artifact = tg::Artifact::with_id(id);
												artifact.check_out(&server, None).await
											}
										})
										.collect::<FuturesUnordered<_>>()
										.try_collect::<Vec<_>>()
										.await?;
								}
								tangram_runtime::darwin::build(
									&server,
									&build,
									stop,
									&server.inner.path,
								)
								.await
							}
							#[cfg(not(target_os = "macos"))]
							{
								return Err(error!("Cannot build a darwin target on this host."));
							}
						},
						tg::system::Os::Linux => {
							#[cfg(target_os = "linux")]
							{
								// If the VFS is disabled, then perform an internal checkout.
								if server.inner.vfs.lock().unwrap().is_none() {
									target
										.data(&server)
										.await?
										.children()
										.into_iter()
										.filter_map(|id| id.try_into().ok())
										.map(|id| {
											let server = server.clone();
											async move {
												let artifact = tg::Artifact::with_id(id);
												artifact.check_out(&server, None).await
											}
										})
										.collect::<FuturesUnordered<_>>()
										.try_collect::<Vec<_>>()
										.await?;
								}
								tangram_runtime::linux::build(
									&server,
									&build,
									stop,
									&server.inner.path,
								)
								.await
							}
							#[cfg(not(target_os = "linux"))]
							{
								return Err(error!("Cannot build a linux target on this host."));
							}
						},
					};

					// Create the outcome.
					let outcome = match result {
						Ok(Some(value)) => tg::build::Outcome::Succeeded(value),
						Ok(None) => tg::build::Outcome::Terminated,
						Err(error) => tg::build::Outcome::Failed(error),
					};

					// If the build failed, add a message to the build's log.
					if let tg::build::Outcome::Failed(error) = &outcome {
						build
							.add_log(&server, error.trace().to_string().into())
							.await?;
					}

					// Set the outcome.
					build.set_outcome(&server, None, outcome).await?;

					Ok::<_, Error>(())
				}
				.inspect_err(|error| {
					let trace = error.trace();
					tracing::error!(%trace, "The build task failed.");
				})
				.map(|_| ())
			});

			// Set the task in the build state.
			self.inner
				.build_state
				.write()
				.unwrap()
				.get_mut(&id)
				.unwrap()
				.task
				.lock()
				.unwrap()
				.replace(task);
		}
	}

	async fn try_get_build_parent(&self, id: &tg::build::Id) -> Result<Option<tg::build::Id>> {
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					select build
					from build_children
					where child = ?1
					limit 1;
				";
				let id = id.to_string();
				let params = sqlite_params![id];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the statement.")?;
				let mut rows = statement
					.query(params)
					.wrap_err("Failed to execute the statement.")?;
				let row = rows.next().wrap_err("Failed to get the row.")?;
				let id = row
					.map(|row| {
						row.get::<_, String>(0)
							.wrap_err("Failed to deserialize the column.")
							.and_then(|id| id.parse().wrap_err("Failed to deserialize the column."))
					})
					.transpose()?;
				Ok(id)
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let statement = "
					select build
					from build_children
					where child = $1;
					limit 1;
				";
				let id = id.to_string();
				let params = postgres_params![id];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the statement.")?;
				let rows = connection
					.query(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
				let row = rows.first();
				let id = row
					.map(|row| {
						row.try_get::<_, String>(0)
							.wrap_err("Failed to deserilaize the column.")
							.and_then(|id| id.parse().wrap_err("Failed to deserialize the column."))
					})
					.transpose()?;
				Ok(id)
			},
		}
	}

	pub(crate) async fn get_build_exists_local(&self, id: &tg::build::Id) -> Result<bool> {
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					select count(*) != 0
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
				let exists = row
					.try_get::<_, bool>(0)
					.wrap_err("Failed to deserialize the column.")?;
				Ok(exists)
			},
		}
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

	pub async fn handle_get_or_create_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};

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

		// Get or create the build.
		let output = self
			.inner
			.tg
			.get_or_create_build(user.as_ref(), arg)
			.await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	pub async fn handle_push_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "push"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Push the build.
		self.inner.tg.push_build(user.as_ref(), &id).await?;

		Ok(ok())
	}

	pub async fn handle_pull_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["build", id, "pull"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Pull the build.
		self.inner.tg.pull_build(&id).await?;

		Ok(ok())
	}
}
