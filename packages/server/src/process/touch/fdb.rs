use {
	crate::{Server, index::message::TouchProcess},
	foundationdb as fdb,
	std::sync::Arc,
	tangram_client as tg,
};

impl Server {
	pub(crate) async fn touch_process_fdb(
		&self,
		database: &Arc<fdb::Database>,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let message = TouchProcess {
			id: id.clone(),
			touched_at,
		};
		self.touch_process_fdb_inner(&txn, &message).await?;

		txn.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		Ok(())
	}
}
