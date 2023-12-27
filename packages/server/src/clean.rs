use super::Server;
use lmdb::Transaction;
use tangram_error::{Result, WrapErr};

impl Server {
	pub async fn clean(&self) -> Result<()> {
		// Clear the store.
		{
			let mut txn = self
				.inner
				.store
				.env
				.begin_rw_txn()
				.wrap_err("Failed to begin a transaction.")?;
			txn.clear_db(self.inner.store.objects)
				.wrap_err("Failed to clear the objects.")?;
			txn.commit().wrap_err("Failed to commit the transaction.")?;
		}

		// Clear the temporary path.
		tokio::fs::remove_dir_all(self.tmp_path())
			.await
			.wrap_err("Failed to remove the temporary directory.")?;
		tokio::fs::create_dir_all(self.tmp_path())
			.await
			.wrap_err("Failed to recreate the temporary directory.")?;

		Ok(())
	}
}
