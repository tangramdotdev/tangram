use super::Server;
use crate::database::Database;
use tangram_error::{Result, WrapErr};

impl Server {
	pub async fn clean(&self) -> Result<()> {
		// Clean the database.
		if let Database::Sqlite(database) = &self.inner.database {
			let connection = database.get().await?;
			connection.execute_batch(
				"
					delete from builds;
					delete from build_logs;
					delete from build_queue;
					delete from objects;
				",
			)
			.wrap_err("Failed to clear the database.")?;
		}

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

		// Clean the checkouts directory.
		tokio::fs::remove_dir_all(self.checkouts_path())
			.await
			.wrap_err("Failed to remove the temporary directory.")?;
		tokio::fs::create_dir_all(self.checkouts_path())
			.await
			.wrap_err("Failed to recreate the temporary directory.")?;

		Ok(())
	}
}
