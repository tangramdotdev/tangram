use {super::Store, tangram_client::prelude::*};

impl Store {
	pub fn flush_sync(&self) -> tg::Result<()> {
		self.env
			.force_sync()
			.map_err(|error| tg::error!(!error, "failed to sync"))?;
		Ok(())
	}

	pub(super) async fn flush(&self) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|error| tg::error!(!error, "failed to sync"))
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}
}
