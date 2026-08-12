use {
	super::{Db, Index, Kind as KeyKind, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

mod key;

pub(super) use key::Key;

impl Index {
	pub async fn complete_log_compaction(&self, entry: &crate::log::Entry) -> tg::Result<()> {
		self.send_log_compaction_request(Request::CompleteLogCompaction(entry.clone()))
			.await
	}

	pub async fn enqueue_log_compaction(&self, process: &tg::process::Id) -> tg::Result<()> {
		self.send_log_compaction_request(Request::EnqueueLogCompaction(process.clone()))
			.await
	}

	async fn send_log_compaction_request(&self, request: Request) -> tg::Result<()> {
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub async fn log_compaction_batch(
		&self,
		batch_size: usize,
	) -> tg::Result<Vec<crate::log::Entry>> {
		let request = crate::read::Request::LmdbLogCompactionBatch { batch_size };
		let response = self.send_read_request(request).await?;
		let crate::read::Response::LogCompactionBatch(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn log_compaction_batch_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		batch_size: usize,
	) -> tg::Result<Vec<crate::log::Entry>> {
		let key_kind = KeyKind::LogCompactionVersion.to_i32().unwrap();
		let prefix = Self::pack(subspace, &(key_kind,));
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the log compaction range"))?
			.take(batch_size)
			.map(|entry| {
				let (key, _) = entry.map_err(|error| {
					tg::error!(!error, "failed to read the log compaction entry")
				})?;
				let key = Self::unpack(subspace, key)?;
				let crate::lmdb::Key::LogCompaction(Key::Version { process, version }) = key else {
					return Err(tg::error!("unexpected log compaction key"));
				};
				let version = Self::log_compaction_version(version);
				Ok(crate::log::Entry {
					position: crate::log::Position::Lmdb { version },
					process,
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(entries)
	}

	pub async fn try_get_oldest_log_compaction_transaction_id(&self) -> tg::Result<Option<u64>> {
		let request = crate::read::Request::TryGetOldestLogCompactionTransactionId;
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetOldestLogCompactionTransactionId(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_oldest_log_compaction_transaction_id_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
	) -> tg::Result<Option<u64>> {
		let key_kind = KeyKind::LogCompactionVersion.to_i32().unwrap();
		let prefix = Self::pack(subspace, &(key_kind,));
		let entry = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the log compaction range"))?
			.next()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to read the log compaction entry"))?;
		let Some((key, _)) = entry else {
			return Ok(None);
		};
		let key = Self::unpack(subspace, key)?;
		let crate::lmdb::Key::LogCompaction(Key::Version { version, .. }) = key else {
			return Err(tg::error!("unexpected log compaction key"));
		};

		Ok(Some(version))
	}

	pub(super) fn complete_log_compaction_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		entry: &crate::log::Entry,
	) -> tg::Result<()> {
		let identity = Self::log_compaction_identity_key(&entry.process);
		let identity_key = Self::pack(subspace, &identity);
		let value = db
			.get(transaction, &identity_key)
			.map_err(|error| tg::error!(!error, "failed to get the log compaction identity"))?;
		let Some(value) = value else {
			return Ok(());
		};
		let version = u64::from_be_bytes(
			value
				.try_into()
				.map_err(|_| tg::error!("invalid log compaction identity"))?,
		);
		let crate::log::Position::Lmdb {
			version: entry_version,
		} = entry.position
		else {
			return Err(tg::error!("unexpected log compaction position"));
		};
		if Self::log_compaction_version(version) != entry_version {
			return Ok(());
		}

		db.delete(transaction, &identity_key)
			.map_err(|error| tg::error!(!error, "failed to delete the log compaction identity"))?;
		let version = Self::log_compaction_version_key(&entry.process, version);
		let version = Self::pack(subspace, &version);
		db.delete(transaction, &version)
			.map_err(|error| tg::error!(!error, "failed to delete the log compaction entry"))?;

		Ok(())
	}

	pub(super) fn enqueue_log_compaction_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		let identity = Self::log_compaction_identity_key(process);
		let identity_key = Self::pack(subspace, &identity);
		let exists = db
			.get(transaction, &identity_key)
			.map_err(|error| tg::error!(!error, "failed to get the log compaction identity"))?
			.is_some();
		if exists {
			return Ok(());
		}

		let version = transaction.id() as u64;
		db.put(transaction, &identity_key, &version.to_be_bytes())
			.map_err(|error| tg::error!(!error, "failed to put the log compaction identity"))?;
		let version_key = Self::log_compaction_version_key(process, version);
		let version_key = Self::pack(subspace, &version_key);
		db.put(transaction, &version_key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the log compaction entry"))?;

		Ok(())
	}

	fn log_compaction_identity_key(process: &tg::process::Id) -> crate::lmdb::Key {
		crate::lmdb::Key::LogCompaction(Key::Identity(process.clone()))
	}

	fn log_compaction_version(version: u64) -> crate::log::Version {
		let mut bytes = [0u8; 12];
		bytes[..8].copy_from_slice(&version.to_be_bytes());
		crate::log::Version::new(bytes)
	}

	fn log_compaction_version_key(process: &tg::process::Id, version: u64) -> crate::lmdb::Key {
		crate::lmdb::Key::LogCompaction(Key::Version {
			process: process.clone(),
			version,
		})
	}
}
