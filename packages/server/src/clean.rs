use super::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub async fn clean(&self) -> tg::Result<()> {
		// Clean the checkouts directory.
		tokio::fs::remove_dir_all(self.checkouts_path())
			.await
			.map_err(|source| tg::error!(!source, "failed to remove the checkouts directory"))?;
		tokio::fs::create_dir_all(self.checkouts_path())
			.await
			.map_err(|error| {
				tg::error!(source = error, "failed to recreate the checkouts directory")
			})?;

		// Clean the temporary directory.
		tokio::fs::remove_dir_all(self.tmp_path())
			.await
			.map_err(|source| tg::error!(!source, "failed to remove the temporary directory"))?;
		tokio::fs::create_dir_all(self.tmp_path())
			.await
			.map_err(|error| {
				tg::error!(source = error, "failed to recreate the temporary directory")
			})?;

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Remove builds.
		loop {
			// Get a build to remove.
			let statement = formatdoc!(
				"
					select id
					from builds
					where (
						select count(*) = 0
						from build_children
						where child = builds.id
					)
					limit 100;
				"
			);
			let params = db::params![];
			let builds = connection
				.query_all_value_into::<tg::build::Id>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// If there are no builds, then break.
			if builds.is_empty() {
				break;
			}

			for id in builds {
				// Remove the build.
				let p = connection.p();
				let statement = formatdoc!(
					"
						delete from builds
						where id = {p}1;
					"
				);
				let params = db::params![id];
				connection
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				// Remove the build children.
				let p = connection.p();
				let statement = formatdoc!(
					"
						delete from build_children
						where object = {p}1;
					"
				);
				let params = db::params![id];
				connection
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				// Remove the build objects.
				let p = connection.p();
				let statement = formatdoc!(
					"
						delete from build_objects
						where object = {p}1;
					"
				);
				let params = db::params![id];
				connection
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		// Remove objects.
		loop {
			// Get an object to remove.
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
						from package_versions
						where id = objects.id
					)
					limit 100;
				"
			);
			let params = db::params![];
			let objects = connection
				.query_all_value_into::<tg::object::Id>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// If there are no objects, then break.
			if objects.is_empty() {
				break;
			}

			for id in objects {
				// Remove the object.
				let p = connection.p();
				let statement = formatdoc!(
					"
						delete from objects
						where id = {p}1;
					"
				);
				let params = db::params![id];
				connection
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				// Remove the object children.
				let p = connection.p();
				let statement = formatdoc!(
					"
						delete from object_children
						where object = {p}1;
					"
				);
				let params = db::params![id];
				connection
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		Ok(())
	}
}
