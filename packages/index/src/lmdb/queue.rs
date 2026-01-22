use {super::Index, tangram_client::prelude::*};

impl Index {
	pub async fn queue(&self, _batch_size: usize) -> tg::Result<usize> {
		Ok(0)
	}

	pub async fn get_transaction_id(&self) -> tg::Result<u64> {
		Ok(0)
	}

	pub async fn get_queue_size(&self, _transaction_id: u64) -> tg::Result<u64> {
		Ok(0)
	}

	pub async fn sync(&self) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|source| tg::error!(!source, "failed to sync"))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))??;
		Ok(())
	}
}
