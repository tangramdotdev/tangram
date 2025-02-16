use super::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{response::builder::Ext as _, Body};
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

		// Get a database connection.
		let mut connection = self
			.database
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
			let max_touched_at = (time::OffsetDateTime::now_utc()
				- self.config().advanced.garbage_collection_grace_period)
				.format(&Rfc3339)
				.unwrap();

			// Get a process to remove.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					select id
					from processes
					where reference_count = 0 and touched_at <= {p}1
					limit 100;
				"
			);
			let params = db::params![max_touched_at];
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
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

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
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

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
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Commit the transaction.
			transaction
				.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		}

		// Remove objects.
		loop {
			// Begin a transaction.
			let transaction = connection
				.transaction()
				.await
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
			let max_touched_at = (time::OffsetDateTime::now_utc()
				- self.config().advanced.garbage_collection_grace_period)
				.format(&Rfc3339)
				.unwrap();

			// Get objects to remove.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					select id
					from objects
					where reference_count = 0 and touched_at <= {p}1
					limit 100;
				"
			);
			let params = db::params![max_touched_at];
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
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

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
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Commit the transaction.
			transaction
				.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		}

		Ok(())
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
