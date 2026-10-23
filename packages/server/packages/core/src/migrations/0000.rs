use tangram_client::prelude::*;

impl crate::Server {
	pub(super) async fn migrate_0000(&self) -> tg::Result<()> {
		// Create the database schema.
		#[cfg(feature = "sqlite")]
		if let Ok(database) = self.database.try_unwrap_sqlite_ref() {
			crate::database::sqlite::create_schema(database).await?;
		}
		#[cfg(feature = "turso")]
		if let Ok(database) = self.database.try_unwrap_turso_ref() {
			crate::database::turso::create_schema(database).await?;
		}

		// Create the checkout directories.
		if self.checkouts_enabled() {
			tokio::fs::create_dir_all(self.store_path())
				.await
				.map_err(|error| tg::error!(!error, "failed to create the store directory"))?;
			if self.config.vfs.is_some() {
				tokio::fs::create_dir_all(self.path.join("checkouts"))
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to create the checkouts directory")
					})?;
			}
		}

		Ok(())
	}
}
