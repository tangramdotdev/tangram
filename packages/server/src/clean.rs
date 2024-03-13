use super::Server;
use crate::database::Connection;
use tangram_error::{error, Result};

impl Server {
	pub async fn clean(&self) -> Result<()> {
		// Clean the database.
		if let Connection::Sqlite(database) = self.inner.database.get().await? {
			database
				.execute_batch(
					"
					delete from builds;
					delete from build_children;
					delete from build_logs;
					delete from objects;
				",
				)
				.map_err(|error| error!(source = error, "Failed to clear the database."))?;
		}

		// Clean the checkouts directory.
		tokio::fs::remove_dir_all(self.checkouts_path())
			.await
			.map_err(|error| error!(source = error, "Failed to remove the checkouts directory."))?;
		tokio::fs::create_dir_all(self.checkouts_path())
			.await
			.map_err(|error| {
				error!(
					source = error,
					"Failed to recreate the checkouts directory."
				)
			})?;

		// Clean the temporary directory.
		tokio::fs::remove_dir_all(self.tmp_path())
			.await
			.map_err(|error| error!(source = error, "Failed to remove the temporary directory."))?;
		tokio::fs::create_dir_all(self.tmp_path())
			.await
			.map_err(|error| {
				error!(
					source = error,
					"Failed to recreate the temporary directory."
				)
			})?;

		Ok(())
	}
}
