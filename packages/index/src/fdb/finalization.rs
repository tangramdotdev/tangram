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
	pub async fn complete_finalization(
		&self,
		entry: &crate::finalization::Entry,
	) -> tg::Result<()> {
		self.send_finalization_request(Request::CompleteFinalization(entry.clone()))
			.await
	}

	pub async fn enqueue_finalization(&self, item: &crate::finalization::Item) -> tg::Result<()> {
		self.send_finalization_request(Request::EnqueueFinalization(item.clone()))
			.await
	}

	async fn send_finalization_request(&self, request: Request) -> tg::Result<()> {
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub async fn finalization_batch(
		&self,
		kind: crate::finalization::Kind,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<crate::finalization::Entry>> {
		let request = crate::read::Request::FinalizationBatch {
			batch_size,
			kind,
			partition_end,
			partition_start,
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::FinalizationBatch(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn finalization_batch_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		kind: crate::finalization::Kind,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<crate::finalization::Entry>> {
		let key_kind = Self::finalization_version_kind(kind).to_i32().unwrap();
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
				.map_err(|error| tg::error!(!error, "failed to get the finalization range"))?;
			for entry in entries {
				let key = Self::unpack(subspace, entry.key())?;
				let (item, partition, version) = match key {
					crate::fdb::Key::Finalization(Key::ProcessVersion {
						id,
						partition,
						version,
					}) => (crate::finalization::Item::Process(id), partition, version),
					crate::fdb::Key::Finalization(Key::SandboxVersion {
						id,
						partition,
						version,
					}) => (crate::finalization::Item::Sandbox(id), partition, version),
					_ => return Err(tg::error!("unexpected finalization key")),
				};
				let version = crate::finalization::Version::new(*version.as_bytes());
				output.push(crate::finalization::Entry {
					item,
					partition,
					version,
				});
			}
		}

		Ok(output)
	}

	pub async fn try_get_oldest_finalization_transaction_id(
		&self,
		kind: crate::finalization::Kind,
	) -> tg::Result<Option<u64>> {
		let request = crate::read::Request::TryGetOldestFinalizationTransactionId { kind };
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetOldestFinalizationTransactionId(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_oldest_finalization_transaction_id_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		partition_total: u64,
		kind: crate::finalization::Kind,
	) -> tg::Result<Option<u64>> {
		let key_kind = Self::finalization_version_kind(kind).to_i32().unwrap();
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
				let entries = txn
					.get_range(&range, 1, false)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the finalization range"))?;
				let Some(entry) = entries.first() else {
					return Ok(None);
				};
				let key = Self::unpack(subspace, entry.key())?;
				let crate::fdb::Key::Finalization(
					Key::ProcessVersion { version, .. } | Key::SandboxVersion { version, .. },
				) = key
				else {
					return Err(tg::error!("unexpected finalization key"));
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

	pub(super) async fn complete_finalization_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		entry: &crate::finalization::Entry,
	) -> tg::Result<()> {
		let identity = Self::finalization_identity_key(&entry.item);
		let identity_key = Self::pack(subspace, &identity);
		let value = txn
			.get(&identity_key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the finalization identity"))?;
		let Some(value) = value else {
			return Ok(());
		};
		let (partition, version) = fdbt::unpack::<(u64, fdbt::Versionstamp)>(&value)
			.map_err(|error| tg::error!(!error, "failed to unpack the finalization identity"))?;
		let entry_version = fdbt::Versionstamp::from(*entry.version.bytes());
		if partition != entry.partition || version != entry_version {
			return Ok(());
		}

		txn.clear(&identity_key);
		let worker = Self::finalization_version_key(&entry.item, partition, version.clone());
		let worker = Self::pack(subspace, &worker);
		txn.clear(&worker);

		Ok(())
	}

	pub(super) async fn enqueue_finalization_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		item: &crate::finalization::Item,
		partition_total: u64,
	) -> tg::Result<()> {
		let identity = Self::finalization_identity_key(item);
		let identity_key = Self::pack(subspace, &identity);
		let exists = txn
			.get(&identity_key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the finalization identity"))?
			.is_some();
		if exists {
			return Ok(());
		}

		let id_bytes = match item {
			crate::finalization::Item::Process(id) => id.to_bytes(),
			crate::finalization::Item::Sandbox(id) => id.to_bytes(),
		};
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		let version = fdbt::Versionstamp::incomplete(0);
		let value = fdbt::pack_with_versionstamp(&(partition, version.clone()));
		txn.atomic_op(
			&identity_key,
			&value,
			fdb::options::MutationType::SetVersionstampedValue,
		);

		let worker = Self::finalization_version_key(item, partition, version.clone());
		let worker = Self::pack_with_versionstamp(subspace, &worker);
		txn.atomic_op(
			&worker,
			&[],
			fdb::options::MutationType::SetVersionstampedKey,
		);

		Ok(())
	}

	fn finalization_identity_key(item: &crate::finalization::Item) -> crate::fdb::Key {
		let key = match item {
			crate::finalization::Item::Process(id) => Key::Process(id.clone()),
			crate::finalization::Item::Sandbox(id) => Key::Sandbox(id.clone()),
		};
		crate::fdb::Key::Finalization(key)
	}

	fn finalization_version_key(
		item: &crate::finalization::Item,
		partition: u64,
		version: fdbt::Versionstamp,
	) -> crate::fdb::Key {
		let key = match item {
			crate::finalization::Item::Process(id) => Key::ProcessVersion {
				id: id.clone(),
				partition,
				version,
			},
			crate::finalization::Item::Sandbox(id) => Key::SandboxVersion {
				id: id.clone(),
				partition,
				version,
			},
		};
		crate::fdb::Key::Finalization(key)
	}

	fn finalization_version_kind(kind: crate::finalization::Kind) -> KeyKind {
		match kind {
			crate::finalization::Kind::Process => KeyKind::ProcessFinalizationVersion,
			crate::finalization::Kind::Sandbox => KeyKind::SandboxFinalizationVersion,
		}
	}
}
