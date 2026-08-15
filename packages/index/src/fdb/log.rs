use {
	super::{Index, Kind as KeyKind, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::{self as fdbt, Subspace},
	futures::future,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
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
		let request = crate::read::Request::FdbLogCompactionBatch {
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
	) -> tg::Result<ControlFlow<Vec<crate::log::Entry>, fdb::FdbError>> {
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
			let result = txn.get_range(&range, 1, false).await;
			let entries = crate::fdb::retry!(result);
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
					position: crate::log::Position::Fdb { partition, version },
					process,
				});
			}
		}

		Ok(ControlFlow::Break(output))
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
	) -> tg::Result<ControlFlow<Option<u64>, fdb::FdbError>> {
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
				let result = txn.get_range(&range, 1, false).await;
				let entries = crate::fdb::retry!(result);
				let Some(entry) = entries.first() else {
					return Ok(ControlFlow::Break(None));
				};
				let key = Self::unpack(subspace, entry.key())?;
				let crate::fdb::Key::LogCompaction(Key::Version { version, .. }) = key else {
					return Err(tg::error!("unexpected log compaction key"));
				};
				let transaction_id =
					u64::from_be_bytes(version.as_bytes()[..8].try_into().unwrap());
				Ok(ControlFlow::Break(Some(transaction_id)))
			}
		});
		let transaction_id = {
			let result = future::try_join_all(futures).await;
			let results = result?;
			let mut values = Vec::with_capacity(results.len());
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		}
		.into_iter()
		.flatten()
		.min();

		Ok(ControlFlow::Break(transaction_id))
	}

	pub(super) async fn complete_log_compaction_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		entry: &crate::log::Entry,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let identity = Self::log_compaction_identity_key(&entry.process);
		let identity_key = Self::pack(subspace, &identity);
		let result = txn.get(&identity_key, false).await;
		let value = crate::fdb::retry!(result);
		let Some(value) = value else {
			return Ok(ControlFlow::Break(()));
		};
		let (partition, version) = fdbt::unpack::<(u64, fdbt::Versionstamp)>(&value)
			.map_err(|error| tg::error!(!error, "failed to unpack the log compaction identity"))?;
		let (entry_partition, entry_version) = match &entry.position {
			crate::log::Position::Fdb { partition, version } => (*partition, *version),
			#[cfg(feature = "lmdb")]
			crate::log::Position::Lmdb { .. } => {
				return Err(tg::error!("unexpected log compaction position"));
			},
		};
		let entry_version = fdbt::Versionstamp::from(*entry_version.bytes());
		if partition != entry_partition || version != entry_version {
			return Ok(ControlFlow::Break(()));
		}

		txn.clear(&identity_key);
		let version = Self::log_compaction_version_key(&entry.process, partition, version);
		let version = Self::pack(subspace, &version);
		txn.clear(&version);

		Ok(ControlFlow::Break(()))
	}

	pub(super) async fn enqueue_log_compaction_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let identity = Self::log_compaction_identity_key(process);
		let identity_key = Self::pack(subspace, &identity);
		let result = txn.get(&identity_key, false).await;
		let exists = crate::fdb::retry!(result).is_some();
		if exists {
			return Ok(ControlFlow::Break(()));
		}

		let partition = rand::random_range(0..partition_total);
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

		Ok(ControlFlow::Break(()))
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
