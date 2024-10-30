use super::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
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

		// Get a database connection.
		let mut connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Remove builds.
		loop {
			// Begin a transaction.
			let transaction = connection
				.transaction()
				.await
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

			// Get a build to remove.
			let statement = formatdoc!(
				"
					select id
					from builds
					where (
						select count(*) = 0
						from build_children
						where child = builds.id
					) and (
						select count(*) = 0
						from tags
						where item = builds.id
					)
					limit 100;
				"
			);
			let params = db::params![];
			let builds = transaction
				.query_all_value_into::<tg::build::Id>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// If there are no builds, then break.
			if builds.is_empty() {
				break;
			}

			for id in builds {
				// Remove the build.
				let p = transaction.p();
				let statement = formatdoc!(
					"
						delete from builds
						where id = {p}1;
					"
				);
				let params = db::params![id];
				transaction
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				// Remove the build children.
				let p = transaction.p();
				let statement = formatdoc!(
					"
						delete from build_children
						where build = {p}1;
					"
				);
				let params = db::params![id];
				transaction
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				// Remove the build objects.
				let p = transaction.p();
				let statement = formatdoc!(
					"
						delete from build_objects
						where build = {p}1;
					"
				);
				let params = db::params![id];
				transaction
					.execute(statement, params)
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
						from build_objects
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
				.query_all_value_into::<tg::object::Id>(statement, params)
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
					.execute(statement, params)
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
					.execute(statement, params)
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
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		handle.clean().await?;
		Ok(http::Response::builder().empty().unwrap())
	}
}
