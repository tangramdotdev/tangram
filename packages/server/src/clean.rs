use super::Server;
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::time::Duration;
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
		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Get processes to remove.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select id
				from processes
				where reference_count = 0 and touched_at <= {p}1
				limit {p}2;
			"
		);
		let max_touched_at = (time::OffsetDateTime::now_utc() - config.touch_timeout)
			.format(&Rfc3339)
			.unwrap();
		let params = db::params![max_touched_at, config.batch_size];
		let processes = transaction
			.query_all_value_into::<tg::process::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		for id in &processes {
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

		// Get objects to remove.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select id
				from objects
				where reference_count = 0 and touched_at <= {p}1
				limit {p}2;
			"
		);
		let max_touched_at = (time::OffsetDateTime::now_utc() - config.touch_timeout)
			.format(&Rfc3339)
			.unwrap();
		let params = db::params![max_touched_at, config.batch_size];
		let objects = transaction
			.query_all_value_into::<tg::object::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		for id in &objects {
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

		let n = processes.len().to_u64().unwrap() + objects.len().to_u64().unwrap();

		Ok(n)
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
