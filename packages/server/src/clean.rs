use super::Server;
use crate::{
	database::{Database, Postgres, Sqlite},
	sqlite_params,
};
use itertools::Itertools;
use tangram_client as tg;
use tangram_error::{error, Result, Wrap, WrapErr};
use tangram_util::iter::IterExt;

impl Server {
	pub async fn clean(&self) -> Result<()> {
		// Clean the checkouts directory.
		tokio::fs::remove_dir_all(self.checkouts_path())
			.await
			.wrap_err("Failed to remove the checkouts directory.")?;
		tokio::fs::create_dir_all(self.checkouts_path())
			.await
			.wrap_err("Failed to recreate the checkouts directory.")?;

		// Clean the temporary directory.
		tokio::fs::remove_dir_all(self.tmp_path())
			.await
			.wrap_err("Failed to remove the temporary directory.")?;
		tokio::fs::create_dir_all(self.tmp_path())
			.await
			.wrap_err("Failed to recreate the temporary directory.")?;

		// Clean the database.
		match &self.inner.database {
			Database::Sqlite(database) => self.clean_sqlite(database).await?,
			Database::Postgres(database) => self.clean_postgres(database).await?,
		}

		Ok(())
	}

	async fn clean_sqlite(&self, database: &Sqlite) -> Result<()> {
		let mut connection = database.get().await?;

		// Remove builds.
		loop {
			// Begin a transaction.
			let txn = connection
				.transaction()
				.wrap_err("Failed to begin the transaction.")?;

			// Get a build to remove.
			let builds: Vec<tg::build::Id> = {
				let statement = "
					select id
					from builds
					where (
						select count(*) = 0
						from build_children
						where child = builds.id
					)
					limit 100;
				";
				let params = sqlite_params![];
				let mut statement = txn
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let rows = statement
					.query(params)
					.wrap_err("Failed to execute the statement.")?;
				rows.and_then(|row| row.get::<_, String>(0))
					.map_err(|error| error.wrap("Failed to deserialize the rows."))
					.and_then(|id| id.parse())
					.try_collect()?
			};

			// If there are no builds, then break.
			if builds.is_empty() {
				break;
			}

			for id in builds {
				// Remove the build.
				{
					let statement = "
						delete from builds
						where id = ?1;
					";
					let id = id.to_string();
					let params = sqlite_params![id];
					let mut statement = txn
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the query.")?;
					statement
						.execute(params)
						.wrap_err("Failed to execute the statement.")?;
				}

				// Remove the build children.
				{
					let statement = "
						delete from build_children
						where object = ?1;
					";
					let object = id.to_string();
					let params = sqlite_params![object];
					let mut statement = txn
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the query.")?;
					statement
						.execute(params)
						.wrap_err("Failed to execute the statement.")?;
				}

				// Remove the build objects.
				{
					let statement = "
						delete from build_objects
						where object = ?1;
					";
					let object = id.to_string();
					let params = sqlite_params![object];
					let mut statement = txn
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the query.")?;
					statement
						.execute(params)
						.wrap_err("Failed to execute the statement.")?;
				}
			}

			// Commit the transaction.
			txn.commit().wrap_err("Failed to commit the transaction.")?;
		}

		// Remove objects.
		loop {
			// Begin a transaction.
			let txn = connection
				.transaction()
				.wrap_err("Failed to begin the transaction.")?;

			// Get an object to remove.
			let objects: Vec<tg::object::Id> = {
				let statement = "
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
				";
				let params = sqlite_params![];
				let mut statement = txn
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let rows = statement
					.query(params)
					.wrap_err("Failed to execute the statement.")?;
				rows.and_then(|row| row.get::<_, String>(0))
					.map_err(|error| error.wrap("Failed to deserialize the rows."))
					.and_then(|id| id.parse())
					.try_collect()?
			};

			// If there are no objects, then break.
			if objects.is_empty() {
				break;
			}

			for id in objects {
				// Remove the object.
				{
					let statement = "
						delete from objects
						where id = ?1;
					";
					let id = id.to_string();
					let params = sqlite_params![id];
					let mut statement = txn
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the query.")?;
					statement
						.execute(params)
						.wrap_err("Failed to execute the statement.")?;
				}

				// Remove the object children.
				{
					let statement = "
						delete from object_children
						where object = ?1;
					";
					let object = id.to_string();
					let params = sqlite_params![object];
					let mut statement = txn
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the query.")?;
					statement
						.execute(params)
						.wrap_err("Failed to execute the statement.")?;
				}
			}

			// Commit the transaction.
			txn.commit().wrap_err("Failed to commit the transaction.")?;
		}

		Ok(())
	}

	async fn clean_postgres(&self, _database: &Postgres) -> Result<()> {
		Err(error!("Not yet implemented."))
	}
}
