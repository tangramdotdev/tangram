use {
	crate::transaction::Transaction,
	std::{collections::BTreeMap, sync::Mutex},
	tangram_client::prelude::*,
};

pub struct Database {
	pub(super) catch_up_lock: Mutex<()>,
	pub(super) db: rocksdb::DB,
	pub(super) secondary: bool,
	pub(super) writer_lock: Mutex<()>,
}

impl Database {
	pub(super) fn write_transaction(&self) -> tg::Result<Transaction<'_>> {
		if self.secondary {
			return Err(tg::error!("the writer is unavailable"));
		}
		let guard = self.writer_lock.lock().unwrap();
		let mut transaction = self.read_transaction();
		transaction.pending = Some(BTreeMap::new());
		transaction.writer_guard = Some(guard);
		Ok(transaction)
	}

	#[must_use]
	pub fn read_transaction(&self) -> Transaction<'_> {
		// Secondary instances do not support snapshots.
		let snapshot = (!self.secondary).then(|| self.db.snapshot());
		Transaction {
			database: self,
			pending: None,
			#[cfg(test)]
			sequence: snapshot.as_ref().map_or_else(
				|| self.db.latest_sequence_number(),
				rocksdb::Snapshot::sequence_number,
			),
			snapshot,
			writer_guard: None,
		}
	}

	pub fn catch_up(&self) -> tg::Result<()> {
		if !self.secondary {
			return Ok(());
		}
		let _guard = self.catch_up_lock.lock().unwrap();
		self.db
			.try_catch_up_with_primary()
			.map_err(|error| tg::error!(!error, "failed to catch up with the primary cache"))?;
		Ok(())
	}

	pub(super) fn flush(&self) -> tg::Result<()> {
		if self.secondary {
			return Err(tg::error!("the writer is unavailable"));
		}
		self.db
			.flush()
			.map_err(|error| tg::error!(!error, "failed to flush the cache"))?;
		Ok(())
	}
}
