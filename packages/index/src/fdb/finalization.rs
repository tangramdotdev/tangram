use {
	super::{Index, Kind as KeyKind, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::{self as fdbt, Subspace},
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
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.sender_medium
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected response"));
		};

		Ok(())
	}

	pub async fn finalization_batch(
		&self,
		kind: crate::finalization::Kind,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<Vec<crate::finalization::Entry>> {
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		let key_kind = Self::finalization_version_kind(kind).to_i32().unwrap();
		let partition_end = partition_start.saturating_add(partition_count);
		let mut output = Vec::new();
		for partition in partition_start..partition_end {
			let remaining = batch_size.saturating_sub(output.len());
			if remaining == 0 {
				break;
			}
			let begin = Self::pack(&self.subspace, &(key_kind, partition));
			let end = Self::pack(&self.subspace, &(key_kind, partition.saturating_add(1)));
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
				let key = Self::unpack(&self.subspace, entry.key())?;
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

	pub async fn finalizations_finished(
		&self,
		kind: crate::finalization::Kind,
		transaction_id: u64,
	) -> tg::Result<bool> {
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		let key_kind = Self::finalization_barrier_kind(kind).to_i32().unwrap();
		let mut bytes = [0u8; 10];
		bytes[..8].copy_from_slice(&transaction_id.to_be_bytes());
		bytes[8..].copy_from_slice(&u16::MAX.to_be_bytes());
		let version = fdbt::Versionstamp::complete(bytes, 0);
		let begin = Self::pack(&self.subspace, &(key_kind,));
		let end = Self::pack(&self.subspace, &(key_kind, version));
		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(begin),
			end: fdb::KeySelector::first_greater_or_equal(end),
			limit: Some(1),
			mode: fdb::options::StreamingMode::WantAll,
			..Default::default()
		};
		let entries = txn.get_range(&range, 1, false).await.map_err(|error| {
			tg::error!(!error, "failed to check whether finalizations are finished")
		})?;

		Ok(entries.is_empty())
	}

	pub(super) async fn task_complete_finalization(
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
		let barrier = Self::finalization_barrier_key(&entry.item, partition, version);
		let barrier = Self::pack(subspace, &barrier);
		txn.clear(&barrier);

		Ok(())
	}

	pub(super) async fn task_enqueue_finalization(
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

		let barrier = Self::finalization_barrier_key(item, partition, version);
		let barrier = Self::pack_with_versionstamp(subspace, &barrier);
		txn.atomic_op(
			&barrier,
			&[],
			fdb::options::MutationType::SetVersionstampedKey,
		);

		Ok(())
	}

	fn finalization_barrier_key(
		item: &crate::finalization::Item,
		partition: u64,
		version: fdbt::Versionstamp,
	) -> crate::fdb::Key {
		let key = match item {
			crate::finalization::Item::Process(id) => Key::ProcessBarrier {
				id: id.clone(),
				partition,
				version,
			},
			crate::finalization::Item::Sandbox(id) => Key::SandboxBarrier {
				id: id.clone(),
				partition,
				version,
			},
		};
		crate::fdb::Key::Finalization(key)
	}

	fn finalization_barrier_kind(kind: crate::finalization::Kind) -> KeyKind {
		match kind {
			crate::finalization::Kind::Process => KeyKind::ProcessFinalizationBarrier,
			crate::finalization::Kind::Sandbox => KeyKind::SandboxFinalizationBarrier,
		}
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
