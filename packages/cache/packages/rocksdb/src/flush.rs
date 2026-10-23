use {super::Cache, tangram_client::prelude::*};

impl Cache {
	pub fn flush_sync(&self) -> tg::Result<()> {
		self.db.flush()?;
		Ok(())
	}

	pub(super) async fn flush(&self) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let db = self.db.clone();
			move || db.flush()
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))??;
		Ok(())
	}
}
