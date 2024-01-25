use super::Server;
use tangram_error::{Result, WrapErr};

impl Server {
	pub async fn clean(&self) -> Result<()> {
		// Clean the database.
		{
			let db = self.inner.database.get().await?;
			db.execute_batch(
				"
					delete from objects;
					delete from builds;
					delete from logs;
					delete from queue;
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
