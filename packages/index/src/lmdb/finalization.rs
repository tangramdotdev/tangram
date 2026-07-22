use {
	super::{Db, Index, Kind as KeyKind, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
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
			.as_ref()
			.unwrap()
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
		partition_end: u64,
	) -> tg::Result<Vec<crate::finalization::Entry>> {
		if partition_start > 0 || partition_end == 0 {
			return Ok(Vec::new());
		}
		let db = self.db;
		let env = self.env.clone();
		let subspace = self.subspace.clone();
		tokio::task::spawn_blocking(move || {
			let transaction = env
				.read_txn()
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
			let key_kind = Self::finalization_version_kind(kind).to_i32().unwrap();
			let prefix = Self::pack(&subspace, &(key_kind,));
			let entries = db
				.prefix_iter(&transaction, &prefix)
				.map_err(|error| tg::error!(!error, "failed to get the finalization range"))?
				.take(batch_size)
				.map(|entry| {
					let (key, _) = entry.map_err(|error| {
						tg::error!(!error, "failed to read the finalization entry")
					})?;
					let key = Self::unpack(&subspace, key)?;
					let (item, version) = match key {
						crate::lmdb::Key::Finalization(Key::ProcessVersion { id, version }) => {
							(crate::finalization::Item::Process(id), version)
						},
						crate::lmdb::Key::Finalization(Key::SandboxVersion { id, version }) => {
							(crate::finalization::Item::Sandbox(id), version)
						},
						_ => return Err(tg::error!("unexpected finalization key")),
					};
					let version = Self::finalization_version(version);
					Ok(crate::finalization::Entry {
						item,
						partition: 0,
						version,
					})
				})
				.collect::<tg::Result<Vec<_>>>()?;

			Ok(entries)
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub async fn try_get_oldest_finalization_transaction_id(
		&self,
		kind: crate::finalization::Kind,
	) -> tg::Result<Option<u64>> {
		let db = self.db;
		let env = self.env.clone();
		let subspace = self.subspace.clone();
		tokio::task::spawn_blocking(move || {
			let transaction = env
				.read_txn()
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
			let key_kind = Self::finalization_version_kind(kind).to_i32().unwrap();
			let prefix = Self::pack(&subspace, &(key_kind,));
			let entry = db
				.prefix_iter(&transaction, &prefix)
				.map_err(|error| tg::error!(!error, "failed to get the finalization range"))?
				.next()
				.transpose()
				.map_err(|error| tg::error!(!error, "failed to read the finalization entry"))?;
			let Some((key, _)) = entry else {
				return Ok(None);
			};
			let key = Self::unpack(&subspace, key)?;
			let crate::lmdb::Key::Finalization(
				Key::ProcessVersion { version, .. } | Key::SandboxVersion { version, .. },
			) = key
			else {
				return Err(tg::error!("unexpected finalization key"));
			};

			Ok(Some(version))
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub(super) fn task_complete_finalization(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		entry: &crate::finalization::Entry,
	) -> tg::Result<()> {
		let identity = Self::finalization_identity_key(&entry.item);
		let identity_key = Self::pack(subspace, &identity);
		let value = db
			.get(transaction, &identity_key)
			.map_err(|error| tg::error!(!error, "failed to get the finalization identity"))?;
		let Some(value) = value else {
			return Ok(());
		};
		let version = u64::from_be_bytes(
			value
				.try_into()
				.map_err(|_| tg::error!("invalid finalization identity"))?,
		);
		if entry.partition != 0 || Self::finalization_version(version) != entry.version {
			return Ok(());
		}

		db.delete(transaction, &identity_key)
			.map_err(|error| tg::error!(!error, "failed to delete the finalization identity"))?;
		let worker = Self::finalization_version_key(&entry.item, version);
		let worker = Self::pack(subspace, &worker);
		db.delete(transaction, &worker)
			.map_err(|error| tg::error!(!error, "failed to delete the finalization entry"))?;

		Ok(())
	}

	pub(super) fn task_enqueue_finalization(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		item: &crate::finalization::Item,
	) -> tg::Result<()> {
		let identity = Self::finalization_identity_key(item);
		let identity_key = Self::pack(subspace, &identity);
		let exists = db
			.get(transaction, &identity_key)
			.map_err(|error| tg::error!(!error, "failed to get the finalization identity"))?
			.is_some();
		if exists {
			return Ok(());
		}

		let version = transaction.id() as u64;
		db.put(transaction, &identity_key, &version.to_be_bytes())
			.map_err(|error| tg::error!(!error, "failed to put the finalization identity"))?;
		let worker = Self::finalization_version_key(item, version);
		let worker = Self::pack(subspace, &worker);
		db.put(transaction, &worker, &[])
			.map_err(|error| tg::error!(!error, "failed to put the finalization entry"))?;

		Ok(())
	}

	fn finalization_identity_key(item: &crate::finalization::Item) -> crate::lmdb::Key {
		let key = match item {
			crate::finalization::Item::Process(id) => Key::Process(id.clone()),
			crate::finalization::Item::Sandbox(id) => Key::Sandbox(id.clone()),
		};
		crate::lmdb::Key::Finalization(key)
	}

	fn finalization_version(version: u64) -> crate::finalization::Version {
		let mut bytes = [0u8; 12];
		bytes[..8].copy_from_slice(&version.to_be_bytes());
		crate::finalization::Version::new(bytes)
	}

	fn finalization_version_key(
		item: &crate::finalization::Item,
		version: u64,
	) -> crate::lmdb::Key {
		let key = match item {
			crate::finalization::Item::Process(id) => Key::ProcessVersion {
				id: id.clone(),
				version,
			},
			crate::finalization::Item::Sandbox(id) => Key::SandboxVersion {
				id: id.clone(),
				version,
			},
		};
		crate::lmdb::Key::Finalization(key)
	}

	fn finalization_version_kind(kind: crate::finalization::Kind) -> KeyKind {
		match kind {
			crate::finalization::Kind::Process => KeyKind::ProcessFinalizationVersion,
			crate::finalization::Kind::Sandbox => KeyKind::SandboxFinalizationVersion,
		}
	}
}
