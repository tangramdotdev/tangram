use super::Server;
use crate::{
	database::{Database, Postgres, PostgresJson, Sqlite, SqliteJson},
	postgres_params, sqlite_params, BuildState, Http, LocalMessengerBuildChannels, Messenger,
};
use futures::{
	stream::{self, FuturesUnordered},
	StreamExt, TryStreamExt,
};
use http_body_util::BodyExt;
use itertools::Itertools;
use num::ToPrimitive;
use rusqlite as sqlite;
use std::sync::Arc;
use tangram_client as tg;
use tangram_error::{error, Error, Result, Wrap, WrapErr};
use tangram_util::{
	http::{bad_request, empty, full, not_found, ok, Incoming, Outgoing},
	iter::IterExt,
};
use time::format_description::well_known::Rfc3339;
use tokio_postgres as postgres;

mod children;
mod log;
mod outcome;
mod queue;
mod status;

impl Server {
	#[allow(clippy::too_many_lines)]
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
		let order = match arg.order {
			tg::build::ListOrder::Timestamp => "timestamp desc",
		};
		let statement = &format!(
			"
				select id, children, log, outcome, status, target, timestamp
				from builds
				where target = ?1
				order by {order}
				limit ?2
				offset ?3;
			"
		);
		let params = sqlite_params![arg.target.to_string(), arg.limit, 0];
		let mut statement = connection
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let items = statement
			.query(params)
			.wrap_err("Failed to execute the statment.")?
			.and_then(|row| {
				let id = row.get::<_, String>(0)?;
				let children = row.get::<_, Option<SqliteJson<Vec<tg::build::Id>>>>(1)?;
				let log = row.get::<_, Option<String>>(2)?;
				let outcome = row.get::<_, Option<SqliteJson<tg::build::outcome::Data>>>(3)?;
				let status = row.get::<_, String>(4)?;
				let target = row.get::<_, String>(5)?;
				let timestamp = row.get::<_, String>(6)?;
				Ok::<_, sqlite::Error>((id, children, log, outcome, status, target, timestamp))
			})
			.map_err(|error| error.wrap("Failed to deserialize the rows."))
			.and_then(|row| {
				let (id, children, log, outcome, status, target, timestamp) = row;
				let id = id.parse()?;
				let children = children.map(|children| children.0);
				let log = log.map(|log| log.parse()).transpose()?;
				let outcome = outcome.map(|outcome| outcome.0);
				let status = status.parse()?;
				let target = target.parse()?;
				let timestamp = time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.wrap_err("Failed to parse the timestamp.")?;
				let output = tg::build::GetOutput {
					id,
					children,
					log,
					outcome,
					status,
					target,
					timestamp,
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
		let order = match arg.order {
			tg::build::ListOrder::Timestamp => "timestamp desc",
		};
		let statement = &format!(
			"
				select id, children, log, outcome, status, target, timestamp
				from builds
				where target = $1
				order by {order}
				limit $2
				offset $3;
			"
		);
		let params = postgres_params![
			arg.target.to_string(),
			arg.limit.to_i64().unwrap(),
			0.to_i64().unwrap()
		];
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
				let log = row.try_get::<_, Option<String>>(2)?;
				let outcome =
					row.try_get::<_, Option<PostgresJson<tg::build::outcome::Data>>>(3)?;
				let status = row.try_get::<_, String>(4)?;
				let target = row.try_get::<_, String>(5)?;
				let timestamp = row.try_get::<_, String>(6)?;
				Ok::<_, postgres::Error>((id, children, log, outcome, status, target, timestamp))
			})
			.map_err(|error| error.wrap("Failed to deserialize the rows."))
			.and_then(|row| {
				let (id, children, log, outcome, status, target, timestamp) = row;
				let id = id.parse()?;
				let children = children.map(|children| children.0);
				let log = log.map(|log| log.parse()).transpose()?;
				let outcome = outcome.map(|outcome| outcome.0);
				let status = status.parse()?;
				let target = target.parse()?;
				let timestamp = time::OffsetDateTime::parse(&timestamp, &Rfc3339)
					.wrap_err("Failed to parse the timestamp.")?;
				let output = tg::build::GetOutput {
					id,
					children,
					log,
					outcome,
					status,
					target,
					timestamp,
				};
				Ok::<_, Error>(output)
			})
			.try_collect()?;
		let output = tg::build::ListOutput { items };
		Ok(output)
	}

	pub async fn get_build_exists(&self, id: &tg::build::Id) -> Result<bool> {
		if self.get_build_exists_local(id).await? {
			return Ok(true);
		}
		if self.get_build_exists_remote(id).await? {
			return Ok(true);
		}
		Ok(false)
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

	async fn get_build_exists_remote(&self, id: &tg::build::Id) -> Result<bool> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};
		let exists = remote.get_build_exists(id).await?;
		Ok(exists)
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

	#[allow(clippy::too_many_lines)]
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
			select id, children, log, outcome, status, target, timestamp
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
		let log = row
			.get::<_, Option<String>>(2)
			.wrap_err("Failed to deserialize the column.")?;
		let outcome = row
			.get::<_, Option<SqliteJson<tg::build::outcome::Data>>>(3)
			.wrap_err("Failed to deserialize the column.")?;
		let status = row
			.get::<_, String>(4)
			.wrap_err("Failed to deserialize the column.")?;
		let target = row
			.get::<_, String>(5)
			.wrap_err("Failed to deserialize the column.")?;
		let timestamp = row
			.get::<_, String>(6)
			.wrap_err("Failed to deserialize the column.")?;
		let id = id.parse()?;
		let children = children.map(|children| children.0);
		let log = log.map(|log| log.parse()).transpose()?;
		let outcome = outcome.map(|outcome| outcome.0);
		let status = status.parse()?;
		let target = target.parse()?;
		let timestamp = time::OffsetDateTime::parse(&timestamp, &Rfc3339)
			.wrap_err("Failed to parse the timestamp.")?;
		let output = tg::build::GetOutput {
			id,
			children,
			log,
			outcome,
			status,
			target,
			timestamp,
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
		let log = row
			.try_get::<_, Option<String>>(2)
			.wrap_err("Failed to deserialize the column.")?;
		let outcome = row
			.try_get::<_, Option<PostgresJson<tg::build::outcome::Data>>>(3)
			.wrap_err("Failed to deserialize the column.")?;
		let status = row
			.try_get::<_, String>(4)
			.wrap_err("Failed to deserialize the column.")?;
		let target = row
			.try_get::<_, String>(5)
			.wrap_err("Failed to deserialize the column.")?;
		let timestamp = row
			.try_get::<_, String>(6)
			.wrap_err("Failed to deserialize the column.")?;
		let id = id.parse()?;
		let children = children.map(|children| children.0);
		let log = log.map(|log| log.parse()).transpose()?;
		let outcome = outcome.map(|outcome| outcome.0);
		let status = status.parse()?;
		let target = target.parse()?;
		let timestamp = time::OffsetDateTime::parse(&timestamp, &Rfc3339)
			.wrap_err("Failed to parse the timestamp.")?;
		let output = tg::build::GetOutput {
			id,
			children,
			log,
			outcome,
			status,
			target,
			timestamp,
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
				log: output.log.clone(),
				outcome: output.outcome.clone(),
				status: output.status,
				target: output.target.clone(),
				timestamp: output.timestamp,
			};
			self.insert_build(id, &arg).await?;
		}

		Ok(Some(output))
	}

	pub async fn try_put_build(
		&self,
		_user: Option<&tg::User>,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> Result<tg::build::PutOutput> {
		// Verify the build is finished.
		if arg.status != tg::build::Status::Finished {
			return Err(error!("The build is not finished."));
		}

		// Get the missing builds.
		let children = arg.children.clone().wrap_err("The children must be set.")?;
		let builds = stream::iter(children)
			.map(Ok)
			.try_filter_map(|id| async move {
				let exists = self.get_build_exists(&id).await?;
				Ok::<_, Error>(if exists { None } else { Some(id) })
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Get the missing objects.
		let objects = stream::iter(arg.objects())
			.map(Ok)
			.try_filter_map(|id| async move {
				let exists = self.get_object_exists(&id).await?;
				Ok::<_, Error>(if exists { None } else { Some(id) })
			})
			.try_collect::<Vec<_>>()
			.await?;

		// If there are no missing builds or objects, then insert the build.
		if builds.is_empty() && objects.is_empty() {
			self.insert_build(id, arg).await?;
		}

		// Create the output.
		let output = tg::build::PutOutput {
			missing: tg::build::Missing { builds, objects },
		};

		Ok(output)
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
		if let Some(children) = arg.children.as_ref() {
			children
				.iter()
				.enumerate()
				.map(|(position, child)| {
					let params = sqlite_params![
						id.to_string(),
						position.to_i64().unwrap(),
						child.to_string(),
					];
					statement
						.execute(params)
						.wrap_err("Failed to execute the statement.")?;
					Ok::<_, Error>(())
				})
				.try_collect()?;
		}

		// Insert the build.
		let statement = "
			insert into builds (id, children, log, outcome, status, target, timestamp)
			values (?1, ?2, ?3, ?4, ?5, ?6, ?7)
			on conflict do update set id = ?1, children = ?2, log = ?3, outcome = ?4, status = ?5, target = ?6, timestamp = ?7;
		";
		let id = id.to_string();
		let children = arg.children.clone().map(SqliteJson);
		let log = arg.log.as_ref().map(ToString::to_string);
		let outcome = arg.outcome.clone().map(SqliteJson);
		let status = arg.status.to_string();
		let target = arg.target.to_string();
		let timestamp = arg
			.timestamp
			.format(&Rfc3339)
			.wrap_err("Failed to format the timestamp.")?;
		let params = sqlite_params![id, children, log, outcome, status, target, timestamp];
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
		if let Some(children) = arg.children.as_ref() {
			children
				.iter()
				.enumerate()
				.map(|(position, child)| {
					let connection = connection.clone();
					let statement = statement.clone();
					async move {
						let params = postgres_params![
							id.to_string(),
							position.to_i64().unwrap(),
							child.to_string(),
						];
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
		}

		// Insert the build.
		let statement = "
			upsert into builds (id, children, log, outcome, status, target, timestamp)
			values ($1, $2, $3, $4, $5, $6, $7);
		";
		let id = id.to_string();
		let children = arg.children.clone().map(PostgresJson);
		let log = arg.log.as_ref().map(ToString::to_string);
		let outcome = arg.outcome.clone().map(PostgresJson);
		let status = arg.status.to_string();
		let target = arg.target.to_string();
		let timestamp = arg
			.timestamp
			.format(&Rfc3339)
			.wrap_err("Failed to format the timestamp.")?;
		let params = postgres_params![id, children, log, outcome, status, target, timestamp];
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

	#[allow(clippy::unused_async)]
	pub async fn pull_build(&self, _id: &tg::build::Id) -> Result<()> {
		Err(error!("Not yet implemented."))
	}

	#[allow(clippy::too_many_lines)]
	pub async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput> {
		// Get a local build if one exists that satisfies the retry constraint.
		let build = 'a: {
			// Find a build.
			let list_arg = tg::build::ListArg {
				limit: 1,
				order: tg::build::ListOrder::Timestamp,
				target: arg.target.clone(),
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
				if outcome.retry() <= arg.options.retry {
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
				limit: 1,
				order: tg::build::ListOrder::Timestamp,
				target: arg.target.clone(),
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
				if outcome.retry() <= arg.options.retry {
					break 'a None;
				}
			}

			Some(build)
		};

		// If a local or remote build was found that satisfies the retry constraint, then return it.
		if let Some(build) = build {
			// Update the queue with the depth if it is greater.
			let updated = match &self.inner.database {
				Database::Sqlite(database) => {
					let connection = database.get().await?;
					let statement = "
						update build_queue
						set
							options = json_set(options, '$.depth', (select json(max(depth, ?1)))),
							depth = (select max(depth, ?1))
						where build = ?2;
					";
					let params = sqlite_params![arg.options.depth, build.id().to_string()];
					let mut statement = connection
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the query.")?;
					let n = statement
						.execute(params)
						.wrap_err("Failed to execute the statement.")?;
					n > 0
				},

				Database::Postgres(database) => {
					let connection = database.get().await?;
					let statement = "
						update build_queue
						set 
							options = json_set(options, '{depth}', (select to_json(greatest(depth, $1)))),
							depth = (select greatest(depth, $1))
						where build = $2;
					";
					let params = postgres_params![
						arg.options.depth.to_i64().unwrap(),
						build.id().to_string()
					];
					let statement = connection
						.prepare_cached(statement)
						.await
						.wrap_err("Failed to prepare the statement.")?;
					let n = connection
						.execute(&statement, params)
						.await
						.wrap_err("Failed to execute the statement.")?;
					n > 0
				},
			};
			if updated {
				self.inner.local_queue_task_wake_sender.send_replace(());
			}

			// Add the build as a child of the parent.
			if let Some(parent) = arg.options.parent.as_ref() {
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
			let remote = if let Some(parent) = arg.options.parent.as_ref() {
				self.get_build_exists_local(parent).await?
			} else {
				arg.options.remote
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
			let options = tg::build::Options {
				remote: false,
				..arg.options.clone()
			};
			let arg = tg::build::GetOrCreateArg {
				target: arg.target.clone(),
				options,
			};
			let Ok(output) = remote.get_or_create_build(user, arg).await else {
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
		let (stop, _) = tokio::sync::watch::channel(false);
		let state = Arc::new(BuildState {
			depth: arg.options.depth,
			stop,
			task: std::sync::Mutex::new(None),
		});
		self.inner
			.build_state
			.write()
			.unwrap()
			.insert(build_id.clone(), state);

		// Create the build channels if the messenger is local.
		if let Messenger::Local(messenger) = &self.inner.messenger {
			let (children, _) = tokio::sync::watch::channel(());
			let (log, _) = tokio::sync::watch::channel(());
			let (status, _) = tokio::sync::watch::channel(());
			let channels = Arc::new(LocalMessengerBuildChannels {
				children,
				log,
				status,
			});
			messenger
				.builds
				.write()
				.unwrap()
				.insert(build_id.clone(), channels);
		}

		// Insert the build.
		let put_arg = tg::build::PutArg {
			id: build_id.clone(),
			children: None,
			log: None,
			outcome: None,
			status: tg::build::Status::Queued,
			target: arg.target.clone(),
			timestamp: time::OffsetDateTime::now_utc(),
		};
		self.insert_build(&build_id, &put_arg).await?;

		// Add the build to the queue.
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					insert into build_queue (build, options, host, depth)
					values (?1, ?2, ?3, ?4);
				";
				let params = sqlite_params![
					build_id.to_string(),
					SqliteJson(arg.options.clone()),
					host.to_string(),
					arg.options.depth,
				];
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
					insert into build_queue (build, options, host, depth)
					values ($1, $2, $3, $4);
				";
				let params = postgres_params![
					build_id.to_string(),
					PostgresJson(arg.options.clone()),
					host.to_string(),
					arg.options.depth.to_i64().unwrap(),
				];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the statement.")?;
				connection
					.execute(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
			},
		}

		// Add the build to the parent.
		if let Some(parent) = arg.options.parent.as_ref() {
			self.add_build_child(user, parent, &build_id).await?;
		}

		// Send a message to the build queue task that the item has been added.
		self.inner.local_queue_task_wake_sender.send_replace(());

		let output = tg::build::GetOrCreateOutput { id: build_id };

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

	pub async fn handle_get_build_exists_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get whether the build exists.
		let exists = self.inner.tg.get_build_exists(&id).await?;

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
		let output = self
			.inner
			.tg
			.try_put_build(user.as_ref(), &build_id, &arg)
			.await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
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
