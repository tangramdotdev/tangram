use super::Server;
use futures::future;
use indoc::indoc;
use num::ToPrimitive as _;
use rusqlite::{self as sqlite, fallible_iterator::FallibleIterator as _};
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{Body, response::builder::Ext as _};
use time::format_description::well_known::Rfc3339;

struct InnerOutput {
	processes: Vec<tg::process::Id>,
	objects: Vec<tg::object::Id>,
	blobs: Vec<tg::blob::Id>,
}

impl Server {
	pub async fn clean(&self) -> tg::Result<()> {
		// Clean the temporary directory.
		tokio::fs::remove_dir_all(self.temp_path())
			.await
			.map_err(|source| tg::error!(!source, "failed to remove the temporary directory"))?;
		tokio::fs::create_dir_all(self.temp_path())
			.await
			.map_err(|error| {
				tg::error!(source = error, "failed to recreate the temporary directory")
			})?;

		// Clean until there are no more processes or objects to remove.
		let config = self.config.cleaner.clone().unwrap_or_default();
		loop {
			let output = self.cleaner_task_inner(&config).await?;
			let n = output.processes.len() + output.objects.len() + output.blobs.len();
			if n == 0 {
				break;
			}
		}

		Ok(())
	}

	pub(crate) async fn cleaner_task(&self, config: &crate::config::Cleaner) -> tg::Result<()> {
		loop {
			let result = self.cleaner_task_inner(config).await;
			match result {
				Ok(output) => {
					let n = output.processes.len() + output.objects.len() + output.blobs.len();
					if n == 0 {
						tokio::time::sleep(Duration::from_secs(1)).await;
					}
				},
				Err(error) => {
					tracing::error!(?error, "failed to clean");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	async fn cleaner_task_inner(&self, config: &crate::config::Cleaner) -> tg::Result<InnerOutput> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let output = match &self.database {
			Either::Left(database) => {
				self.cleaner_task_inner_sqlite(config, now, database)
					.await?
			},
			Either::Right(database) => {
				self.cleaner_task_inner_postgres(config, now, database)
					.await?
			},
		};
		let arg = crate::store::DeleteBatchArg {
			ids: output.objects.clone(),
			now,
			ttl: config.ttl.as_secs(),
		};
		self.store.delete_batch(arg).await?;
		future::try_join_all(output.blobs.iter().map(|blob| async {
			let path = self.blobs_path().join(blob.to_string());
			tokio::fs::remove_file(&path)
				.await
				.map_err(|source| tg::error!(!source, ?path, "failed to remove the blob"))?;
			Ok::<_, tg::Error>(())
		}))
		.await?;
		Ok(output)
	}

	async fn cleaner_task_inner_sqlite(
		&self,
		config: &crate::config::Cleaner,
		now: i64,
		database: &db::sqlite::Database,
	) -> tg::Result<InnerOutput> {
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		connection
			.with({
				let config = config.clone();
				move |connection| {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Delete processes.
					let statement = indoc!(
						"
							select id
							from processes
							where reference_count = 0 and touched_at <= ?1
							limit ?2;
						"
					);
					let max_touched_at = time::OffsetDateTime::from_unix_timestamp(
						now - config.ttl.as_secs().to_i64().unwrap(),
					)
					.unwrap()
					.format(&Rfc3339)
					.unwrap();
					let mut statement =
						transaction.prepare_cached(statement).map_err(|source| {
							tg::error!(!source, "failed to prepare the blobs statement")
						})?;
					let params = sqlite::params![max_touched_at, config.batch_size];
					let processes = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
						.map(|row| {
							let string = row.get::<_, String>(0).unwrap();
							let id = string.parse().map_err(|source: tg::Error| {
								sqlite::Error::FromSqlConversionFailure(
									0,
									sqlite::types::Type::Text,
									source.into(),
								)
							})?;
							Ok(id)
						})
						.collect::<Vec<tg::process::Id>>()
						.map_err(|source| tg::error!(!source, "failed to get the processes"))?;
					drop(statement);

					// Get objects to remove.
					let statement = indoc!(
						"
							select id
							from objects
							where reference_count = 0 and touched_at <= ?1
							limit ?2;
						"
					);
					let max_touched_at = time::OffsetDateTime::from_unix_timestamp(
						now - config.ttl.as_secs().to_i64().unwrap(),
					)
					.unwrap()
					.format(&Rfc3339)
					.unwrap();
					let mut statement = transaction
						.prepare_cached(statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![max_touched_at, config.batch_size];
					let objects = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
						.map(|row| {
							let string = row.get::<_, String>(0).unwrap();
							let id = string.parse().map_err(|source: tg::Error| {
								sqlite::Error::FromSqlConversionFailure(
									0,
									sqlite::types::Type::Text,
									source.into(),
								)
							})?;
							Ok(id)
						})
						.collect::<Vec<tg::object::Id>>()
						.map_err(|source| tg::error!(!source, "failed to get the objects"))?;
					drop(statement);

					// Get blobs to remove.
					let statement = indoc!(
						"
							select id
							from blobs
							where reference_count = 0
							limit ?2;
						"
					);
					let max_touched_at = time::OffsetDateTime::from_unix_timestamp(
						now - config.ttl.as_secs().to_i64().unwrap(),
					)
					.unwrap()
					.format(&Rfc3339)
					.unwrap();
					let mut statement = transaction
						.prepare_cached(statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![max_touched_at, config.batch_size];
					let blobs = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
						.map(|row| {
							let string = row.get::<_, String>(0).unwrap();
							let id = string.parse().map_err(|source: tg::Error| {
								sqlite::Error::FromSqlConversionFailure(
									0,
									sqlite::types::Type::Text,
									source.into(),
								)
							})?;
							Ok(id)
						})
						.collect::<Vec<tg::blob::Id>>()
						.map_err(|source| tg::error!(!source, "failed to get the objects"))?;
					drop(statement);

					let output = InnerOutput {
						processes,
						objects,
						blobs,
					};

					Ok::<_, tg::Error>(output)
				}
			})
			.await
	}

	async fn cleaner_task_inner_postgres(
		&self,
		config: &crate::config::Cleaner,
		now: i64,
		database: &db::postgres::Database,
	) -> tg::Result<InnerOutput> {
		// Get a database connection.
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let mut connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.client_mut()
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Delete processes and objects
		let statement = indoc!(
			"
				with deleted_processes AS (
					delete from processes
					where reference_count = 0 and touched_at <= $1
					limit $2
					returning id
				),
				deleted_process_children AS (
					delete from process_children
					where process in (select id from deleted_processes)
				),
				deleted_process_objects as (
					delete from process_objects
					where process in (select id from deleted_processes)
				),
				deleted_objects AS (
					delete from objects
					where reference_count = 0 and touched_at <= $1
					limit $2
					returning id
				),
				deleted_object_children AS (
					delete from object_children
					where object in (select id from deleted_objects)
				),
				deleted_blobs AS (
					delete from blobs
					where reference_count = 0
					limit $2
					returning id
				)
				select id from deleted_processes
				union all
				select id from deleted_objects
				union all
				select id from deleted_blobs;
			"
		);
		let max_touched_at =
			time::OffsetDateTime::from_unix_timestamp(now - config.ttl.as_secs().to_i64().unwrap())
				.unwrap()
				.format(&Rfc3339)
				.unwrap();
		let rows = transaction
			.query(
				statement,
				&[&max_touched_at, &config.batch_size.to_i64().unwrap()],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let mut processes = Vec::new();
		let mut objects = Vec::new();
		let mut blobs = Vec::new();
		for row in rows {
			let id = row.get::<_, String>(0);
			if let Ok(id) = id.parse() {
				processes.push(id);
			} else if let Ok(id) = id.parse() {
				objects.push(id);
			} else if let Ok(id) = id.parse() {
				blobs.push(id);
			} else {
				return Err(tg::error!(%id, "invalid id"));
			}
		}

		// Commit the transaction
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		let output = InnerOutput {
			processes,
			objects,
			blobs,
		};

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_server_clean_request<H>(
		handle: &H,
		_request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		handle.clean().await?;
		Ok(http::Response::builder().empty().unwrap())
	}
}
