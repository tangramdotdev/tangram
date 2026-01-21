use {super::Index, tangram_client::prelude::*};

impl Index {
	pub async fn get_transaction_id(&self) -> tg::Result<u64> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;
		let version = txn
			.get_read_version()
			.await
			.map_err(|source| tg::error!(!source, "failed to get read version"))?;
		Ok(version.cast_unsigned())
	}

	pub async fn get_queue_size(&self, _transaction_id: u64) -> tg::Result<u64> {
		todo!()
	}
}
