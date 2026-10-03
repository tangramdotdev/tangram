use {
	crate::transaction::Transaction,
	std::{collections::BTreeMap, sync::Mutex},
	tangram_client::prelude::*,
};

pub struct Database {
	pub(super) db: fjall::Database,
	pub(super) keyspace: fjall::Keyspace,
	pub(super) writer_lock: Mutex<()>,
}

impl Database {
	#[must_use]
	pub(super) fn write_transaction(&self) -> Transaction<'_> {
		let guard = self.writer_lock.lock().unwrap();
		let mut transaction = self.read_transaction();
		transaction.pending = Some(BTreeMap::new());
		transaction.writer_guard = Some(guard);
		transaction
	}

	#[must_use]
	pub fn read_transaction(&self) -> Transaction<'_> {
		let snapshot = self.db.snapshot();
		Transaction {
			database: self,
			pending: None,
			snapshot,
			writer_guard: None,
		}
	}

	pub(super) fn flush(&self) -> tg::Result<()> {
		self.db
			.persist(fjall::PersistMode::SyncAll)
			.map_err(|error| tg::error!(!error, "failed to flush the cache"))?;
		Ok(())
	}
}
