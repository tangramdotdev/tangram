use super::Server;
use indoc::{formatdoc, indoc};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

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

		match &self.database {
			Either::Left(database) => {
				// Get a database connection.
				let mut connection = database
					.write_connection()
					.await
					.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

				// Remove processes.
				loop {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.await
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Get a process to remove.
					let statement = indoc!(
						"
							select id
							from processes
							where (
								select count(*) = 0
								from process_children
								where child = processes.id
							) and (
								select count(*) = 0
								from tags
								where item = processes.id
							)
							limit 100;
						"
					);
					let params = db::params![];
					let processes = transaction
						.query_all_value_into::<tg::process::Id>(statement.into(), params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

					// If there are no processes, then break.
					if processes.is_empty() {
						break;
					}

					for id in processes {
						// Remove the process.
						let p = transaction.p();
						let statement = formatdoc!(
							"
								delete from processes
								where id = {p}1;
							"
						);
						let params = db::params![id];
						transaction
							.execute(statement.into(), params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;

						// Remove the process children.
						let p = transaction.p();
						let statement = formatdoc!(
							"
								delete from process_children
								where process = {p}1;
							"
						);
						let params = db::params![id];
						transaction
							.execute(statement.into(), params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;

						// Remove the process objects.
						let p = transaction.p();
						let statement = formatdoc!(
							"
								delete from process_objects
								where process = {p}1;
							"
						);
						let params = db::params![id];
						transaction
							.execute(statement.into(), params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
					}

					// Commit the transaction.
					transaction.commit().await.map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;
				}

				// Remove objects.
				loop {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.await
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Get objects to remove.
					let statement = formatdoc!(
						"
							select id
							from objects
							where (
								select count(*) = 0
								from object_children
								where child = objects.id
							) and (
								select count(*) = 0
								from process_objects
								where object = objects.id
							) and (
								select count(*) = 0
								from tags
								where item = objects.id
							)
							limit 100;
						"
					);
					let params = db::params![];
					let objects = transaction
						.query_all_value_into::<tg::object::Id>(statement.into(), params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

					// If there are no objects, then break.
					if objects.is_empty() {
						break;
					}

					for id in objects {
						// Remove the object.
						let p = transaction.p();
						let statement = formatdoc!(
							"
								delete from objects
								where id = {p}1;
							"
						);
						let params = db::params![id];
						transaction
							.execute(statement.into(), params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;

						// Remove the object children.
						let p = transaction.p();
						let statement = formatdoc!(
							"
								delete from object_children
								where object = {p}1;
							"
						);
						let params = db::params![id];
						transaction
							.execute(statement.into(), params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
					}

					// Commit the transaction.
					transaction.commit().await.map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;
				}

				Ok(())
			},
			Either::Right(database) => {
				// Get a database connection.
				let mut connection = database
					.write_connection()
					.await
					.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

				// Remove processes.
				loop {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.await
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Get a process to remove.
					let statement = indoc!(
						"
						with eligible_processes as (
							select id
							from processes
							where (
								select count(*) = 0
								from process_children
								where child = processes.id
							) and (
								select count(*) = 0
								from tags
								where item = processes.id
							)
							limit 100
						), 
						deleted_processes as (
							delete from processes
							where id in (select id from eligible_processes)
						),
						deleted_process_children as (
							delete from process_children
							where process in (select id from eligible_processes) 
						),
						deleted_process_objects as (
							delete from process_objects
							where process in (select id from eligible_processes)
						)
						select count(*) from eligible_processes;
						"
					);
					let params = db::params![];
					let processes = transaction
						.query_one_value_into::<u64>(statement.into(), params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

					// If there are no processes, then break.
					if processes == 0 {
						break;
					}

					// Commit the transaction.
					transaction.commit().await.map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;
				}

				// Remove objects.
				loop {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.await
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Get objects to remove.
					let statement = formatdoc!(
						"
						with eligible_objects as (
							select id
							from objects
							where (
								select count(*) = 0
								from object_children
								where child = objects.id
							) and (
								select count(*) = 0
								from process_objects
								where object = objects.id
							) and (
								select count(*) = 0
								from tags
								where item = objects.id
							)
							limit 100
						), 
						deleted_objects as (
							delete from objects
							where id in (select id from eligible_objects)
						),
						deleted_object_children as (
							delete from object_children
							where object in (select id from eligible_objects)
						)
						select count(*) from eligible_objects;
						"
					);
					let params = db::params![];
					let objects = transaction
						.query_one_value_into::<u64>(statement.into(), params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

					// If there are no objects, then break.
					if objects == 0 {
						break;
					}

					// Commit the transaction.
					transaction.commit().await.map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;
				}

				Ok(())
			},
		}
	}
}

impl Server {
	pub(crate) async fn handle_server_clean_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		handle.clean().await?;
		Ok(http::Response::builder().empty().unwrap())
	}
}
