use super::Server;
use indoc::indoc;
use num::ToPrimitive as _;
use rusqlite as sqlite;
use rusqlite::fallible_iterator::FallibleIterator;
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{Body, response::builder::Ext as _};
use time::format_description::well_known::Rfc3339;

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
			let n = self.cleaner_task_inner(&config).await?;
			if n == 0 {
				break;
			}
		}

		Ok(())
	}

	pub async fn cleaner_task(&self, config: &crate::config::Cleaner) -> tg::Result<()> {
		loop {
			let result = self.cleaner_task_inner(config).await;
			match result {
				Ok(0) => {
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
				Ok(_) => (),
				Err(error) => {
					tracing::error!(?error, "failed to clean");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	pub async fn cleaner_task_inner(&self, config: &crate::config::Cleaner) -> tg::Result<u64> {
		match &self.database {
			Either::Left(database) => {
				let connection = database
					.write_connection()
					.await
					.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

				connection
					.with({
						let config = config.clone();
						move |connection| {
							// Begin a transaction.
							let transaction = connection.transaction().map_err(|source| {
								tg::error!(!source, "failed to begin a transaction")
							})?;

							// Get processes to remove.
							let statement = indoc!(
								"
									select id
									from processes
									where reference_count = 0 and touched_at <= ?1
									limit ?2;
								"
							);
							let max_touched_at = (time::OffsetDateTime::now_utc()
								- config.touch_timeout)
								.format(&Rfc3339)
								.unwrap();
							let mut statement =
								transaction.prepare_cached(statement).map_err(|source| {
									tg::error!(!source, "failed to prepare the blobs statement")
								})?;
							let params = sqlite::params![max_touched_at, config.batch_size];
							let processes: Vec<tg::process::Id> = statement
								.query(params)
								.map_err(|source| {
									tg::error!(!source, "failed to execute the statement")
								})?
								.map(|r| {
									let s: String = r.get(0).unwrap();
									Ok(s.parse().unwrap())
								})
								.collect()
								.map_err(|source| tg::error!(!source, "failed to collect "))?;

							drop(statement);

							for id in &processes {
								// Remove the process.
								let statement = indoc!(
									"
										delete from processes
										where id = ?1;
									"
								);
								let params = sqlite::params![id.to_string()];
								transaction.execute(statement.into(), params).map_err(
									|source| tg::error!(!source, "failed to execute the statement"),
								)?;

								// Remove the process children.
								let statement = indoc!(
									"
										delete from process_children
										where process = ?1;
									"
								);
								let params = sqlite::params![id.to_string()];
								transaction.execute(statement.into(), params).map_err(
									|source| tg::error!(!source, "failed to execute the statement"),
								)?;

								// Remove the process objects.
								let statement = indoc!(
									"
										delete from process_objects
										where process = {p}1;
									"
								);
								let params = sqlite::params![id.to_string()];
								transaction.execute(statement.into(), params).map_err(
									|source| tg::error!(!source, "failed to execute the statement"),
								)?;
							}

							// Get objects to remove.
							let statement = indoc!(
								"
									select id
									from objects
									where reference_count = 0 and touched_at <= ?1
									limit ?2;
								"
							);
							let max_touched_at = (time::OffsetDateTime::now_utc()
								- config.touch_timeout)
								.format(&Rfc3339)
								.unwrap();
							let mut statement =
								transaction.prepare_cached(statement).map_err(|source| {
									tg::error!(!source, "failed to prepare the blobs statement")
								})?;
							let params = sqlite::params![max_touched_at, config.batch_size];
							let objects: Vec<tg::object::Id> = statement
								.query(params)
								.map_err(|source| {
									tg::error!(!source, "failed to execute the statement")
								})?
								.map(|r| {
									let s: String = r.get(0).unwrap();
									Ok(s.parse().unwrap())
								})
								.collect()
								.map_err(|source| tg::error!(!source, "failed to collect"))?;

							drop(statement);

							for id in &objects {
								// Remove the object.
								let statement = indoc!(
									"
										delete from objects
										where id = ?1;
									"
								);
								let params = sqlite::params![id.to_string()];
								transaction.execute(statement.into(), params).map_err(
									|source| tg::error!(!source, "failed to execute the statement"),
								)?;

								// Remove the object children.
								let statement = indoc!(
									"
										delete from object_children
										where object = {p}1;
									"
								);
								let params = sqlite::params![id.to_string()];
								transaction.execute(statement.into(), params).map_err(
									|source| tg::error!(!source, "failed to execute the statement"),
								)?;
							}

							// Commit the transaction.
							transaction.commit().map_err(|source| {
								tg::error!(!source, "failed to commit the transaction")
							})?;

							let n =
								processes.len().to_u64().unwrap() + objects.len().to_u64().unwrap();

							Ok::<_, tg::Error>(n)
						}
					})
					.await
			},
			Either::Right(database) => {
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

				// Calculate the timestamp for cleanup
				let max_touched_at = (time::OffsetDateTime::now_utc() - config.touch_timeout)
					.format(&Rfc3339)
					.unwrap();

				// Single query with CTEs to handle all deletions and return count
				let statement = indoc!(
					"
					with deleted_processes AS (
						delete from processes
						where reference_count = 0 
						and touched_at <= $1
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
						where reference_count = 0 
						and touched_at <= $1
						limit $2
						returning id
					),
					deleted_object_children AS (
						delete from object_children
						where object in (select id from deleted_objects)
					)
					select 
					(select count(*) from deleted_processes) + 
					(select count(*) from deleted_objects) as total_deleted
					"
				);

				let total_deleted: u64 = transaction
					.query_one(statement, &[
						&max_touched_at,
						&config.batch_size.to_i64().unwrap(),
					])
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to execute the cleanup statement")
					})?
					.get::<_, i64>(0)
					.to_u64()
					.unwrap();

				// Commit the transaction
				transaction
					.commit()
					.await
					.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

				Ok(total_deleted)
			},
		}
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
