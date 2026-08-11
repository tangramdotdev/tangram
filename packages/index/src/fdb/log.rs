use {
	super::{Index, Kind as KeyKind, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::{self as fdbt, Subspace},
	futures::future,
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
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<crate::log::Entry>> {
		let request = crate::read::Request::LogCompactionBatch {
			batch_size,
			partition_end,
			partition_start,
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::LogCompactionBatch(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn log_compaction_batch_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<crate::log::Entry>> {
		let key_kind = KeyKind::LogCompactionVersion.to_i32().unwrap();
		let mut output = Vec::new();
		for partition in partition_start..partition_end {
			let remaining = batch_size.saturating_sub(output.len());
			if remaining == 0 {
				break;
			}
			let begin = Self::pack(subspace, &(key_kind, partition));
			let end = Self::pack(subspace, &(key_kind, partition.saturating_add(1)));
			let range = fdb::RangeOption {
				begin: fdb::KeySelector::first_greater_or_equal(begin),
				end: fdb::KeySelector::first_greater_or_equal(end),
				limit: Some(remaining),
				mode: fdb::options::StreamingMode::WantAll,
				..Default::default()
			};
			let entries = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the log compaction range"))?;
			for entry in entries {
				let key = Self::unpack(subspace, entry.key())?;
				let crate::fdb::Key::LogCompaction(Key::Version {
					partition,
					process,
					version,
				}) = key
				else {
					return Err(tg::error!("unexpected log compaction key"));
				};
				let version = crate::log::Version::new(*version.as_bytes());
				output.push(crate::log::Entry {
					partition,
					process,
					version,
				});
			}
		}

		Ok(output)
	}

	pub async fn try_get_oldest_log_compaction_transaction_id(&self) -> tg::Result<Option<u64>> {
		let request = crate::read::Request::TryGetOldestLogCompactionTransactionId;
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetOldestLogCompactionTransactionId(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_oldest_log_compaction_transaction_id_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		partition_total: u64,
	) -> tg::Result<Option<u64>> {
		let key_kind = KeyKind::LogCompactionVersion.to_i32().unwrap();
		let futures = (0..partition_total).map(|partition| {
			let begin = Self::pack(subspace, &(key_kind, partition));
			let end = Self::pack(subspace, &(key_kind, partition.saturating_add(1)));
			let range = fdb::RangeOption {
				begin: fdb::KeySelector::first_greater_or_equal(begin),
				end: fdb::KeySelector::first_greater_or_equal(end),
				limit: Some(1),
				mode: fdb::options::StreamingMode::WantAll,
				..Default::default()
			};
			async move {
				let entries = txn.get_range(&range, 1, false).await.map_err(|error| {
					tg::error!(!error, "failed to get the log compaction range")
				})?;
				let Some(entry) = entries.first() else {
					return Ok(None);
				};
				let key = Self::unpack(subspace, entry.key())?;
				let crate::fdb::Key::LogCompaction(Key::Version { version, .. }) = key else {
					return Err(tg::error!("unexpected log compaction key"));
				};
				let transaction_id =
					u64::from_be_bytes(version.as_bytes()[..8].try_into().unwrap());
				Ok(Some(transaction_id))
			}
		});
		let transaction_id = future::try_join_all(futures)
			.await?
			.into_iter()
			.flatten()
			.min();

		Ok(transaction_id)
	}

	pub(super) async fn complete_log_compaction_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		entry: &crate::log::Entry,
	) -> tg::Result<()> {
		let identity = Self::log_compaction_identity_key(&entry.process);
		let identity_key = Self::pack(subspace, &identity);
		let value = txn
			.get(&identity_key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the log compaction identity"))?;
		let Some(value) = value else {
			return Ok(());
		};
		let (partition, version) = fdbt::unpack::<(u64, fdbt::Versionstamp)>(&value)
			.map_err(|error| tg::error!(!error, "failed to unpack the log compaction identity"))?;
		let entry_version = fdbt::Versionstamp::from(*entry.version.bytes());
		if partition != entry.partition || version != entry_version {
			return Ok(());
		}

		txn.clear(&identity_key);
		let version = Self::log_compaction_version_key(&entry.process, partition, version);
		let version = Self::pack(subspace, &version);
		txn.clear(&version);

		Ok(())
	}

	pub(super) async fn enqueue_log_compaction_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let identity = Self::log_compaction_identity_key(process);
		let identity_key = Self::pack(subspace, &identity);
		let exists = txn
			.get(&identity_key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the log compaction identity"))?
			.is_some();
		if exists {
			return Ok(());
		}

		let process_bytes = process.to_bytes();
		let partition = Self::partition_for_id(process_bytes.as_ref(), partition_total);
		let version = fdbt::Versionstamp::incomplete(0);
		let value = fdbt::pack_with_versionstamp(&(partition, version.clone()));
		txn.atomic_op(
			&identity_key,
			&value,
			fdb::options::MutationType::SetVersionstampedValue,
		);

		let version_key = Self::log_compaction_version_key(process, partition, version);
		let version_key = Self::pack_with_versionstamp(subspace, &version_key);
		txn.atomic_op(
			&version_key,
			&[],
			fdb::options::MutationType::SetVersionstampedKey,
		);

		Ok(())
	}

	fn log_compaction_identity_key(process: &tg::process::Id) -> crate::fdb::Key {
		crate::fdb::Key::LogCompaction(Key::Identity(process.clone()))
	}

	fn log_compaction_version_key(
		process: &tg::process::Id,
		partition: u64,
		version: fdbt::Versionstamp,
	) -> crate::fdb::Key {
		crate::fdb::Key::LogCompaction(Key::Version {
			partition,
			process: process.clone(),
			version,
		})
	}
}
