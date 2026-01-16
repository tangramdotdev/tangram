use {super::Index, tangram_client::prelude::*};

impl Index {
	pub async fn get_transaction_id(&self) -> tg::Result<u64> {
		todo!()
	}

	pub async fn get_queue_size(&self, _transaction_id: u64) -> tg::Result<u64> {
		todo!()
	}
}
