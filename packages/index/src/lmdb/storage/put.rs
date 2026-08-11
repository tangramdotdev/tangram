use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn touch_account_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ObjectArg,
		time_to_touch: std::time::Duration,
	) -> tg::Result<()> {
		let key = Key::Storage(crate::lmdb::storage::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let key = Self::pack(subspace, &key);
		let Some(value) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the account object"))?
		else {
			return Ok(());
		};
		let mut entry = crate::storage::Entry::deserialize(value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(entry.touched_at) >= time_to_touch {
			entry.touched_at = arg.touched_at;
			db.put(transaction, &key, &entry.serialize()?)
				.map_err(|error| tg::error!(!error, "failed to touch the account object"))?;
			Self::put_account_object_clean_key(db, subspace, transaction, arg)?;
		}

		Ok(())
	}

	pub(crate) fn touch_account_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ProcessArg,
		time_to_touch: std::time::Duration,
	) -> tg::Result<()> {
		let key = Key::Storage(crate::lmdb::storage::Key::AccountProcess {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let key = Self::pack(subspace, &key);
		let Some(value) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the account process"))?
		else {
			return Ok(());
		};
		let mut entry = crate::storage::Entry::deserialize(value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(entry.touched_at) >= time_to_touch {
			entry.touched_at = arg.touched_at;
			db.put(transaction, &key, &entry.serialize()?)
				.map_err(|error| tg::error!(!error, "failed to touch the account process"))?;
			Self::put_account_process_clean_key(db, subspace, transaction, arg)?;
		}

		Ok(())
	}

	pub(crate) fn enqueue_account_object_from_parents(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		object: &tg::object::Id,
	) -> tg::Result<()> {
		let mut accounts = BTreeSet::new();
		let parents = Self::get_object_parents_with_transaction(db, subspace, transaction, object)?;
		for parent in parents {
			accounts.extend(Self::get_object_accounts_with_transaction(
				db,
				subspace,
				transaction,
				&parent,
			)?);
		}
		let processes =
			Self::get_object_processes_with_transaction(db, subspace, transaction, object)?;
		for (process, _) in processes {
			accounts.extend(Self::get_process_accounts_with_transaction(
				db,
				subspace,
				transaction,
				&process,
			)?);
		}
		for account in accounts {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Left(object.clone()),
				crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add(account)),
				crate::lmdb::update::Source::Put,
				None,
			)?;
		}

		Ok(())
	}

	pub(crate) fn enqueue_account_process_from_parents(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		let mut accounts = BTreeSet::new();
		let parents =
			Self::get_process_parents_with_transaction(db, subspace, transaction, process)?;
		for parent in parents {
			accounts.extend(Self::get_process_accounts_with_transaction(
				db,
				subspace,
				transaction,
				&parent,
			)?);
		}
		for account in accounts {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Right(process.clone()),
				crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add(account)),
				crate::lmdb::update::Source::Put,
				None,
			)?;
		}

		Ok(())
	}

	pub(crate) fn enqueue_account_process_relationships(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		let accounts =
			Self::get_process_accounts_with_transaction(db, subspace, transaction, process)?;
		if accounts.is_empty() {
			return Ok(());
		}
		let children =
			Self::get_process_children_with_transaction(db, subspace, transaction, process)?;
		let objects =
			Self::get_process_objects_with_transaction(db, subspace, transaction, process)?;
		for account in accounts {
			let kind =
				crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add(account));
			for child in &children {
				Self::enqueue_update_with_kind(
					db,
					subspace,
					transaction,
					tg::Either::Right(child.clone()),
					kind.clone(),
					crate::lmdb::update::Source::Put,
					None,
				)?;
			}
			for (object, _) in &objects {
				Self::enqueue_update_with_kind(
					db,
					subspace,
					transaction,
					tg::Either::Left(object.clone()),
					kind.clone(),
					crate::lmdb::update::Source::Put,
					None,
				)?;
			}
		}

		Ok(())
	}

	fn get_object_accounts_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		object: &tg::object::Id,
	) -> tg::Result<Vec<crate::storage::Account>> {
		let object_bytes = object.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::lmdb::Kind::ObjectAccount.to_i32().unwrap(),
				object_bytes.as_ref(),
			),
		);
		let accounts = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the object accounts"))?
			.map(|entry| {
				let (key, _) = entry
					.map_err(|error| tg::error!(!error, "failed to read an object account"))?;
				let key = Self::unpack(subspace, key)?;
				let Key::Storage(crate::lmdb::storage::Key::ObjectAccount { account, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(account)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(accounts)
	}

	fn get_process_accounts_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		process: &tg::process::Id,
	) -> tg::Result<Vec<crate::storage::Account>> {
		let process_bytes = process.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::lmdb::Kind::ProcessAccount.to_i32().unwrap(),
				process_bytes.as_ref(),
			),
		);
		let accounts = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the process accounts"))?
			.map(|entry| {
				let (key, _) = entry
					.map_err(|error| tg::error!(!error, "failed to read a process account"))?;
				let key = Self::unpack(subspace, key)?;
				let Key::Storage(crate::lmdb::storage::Key::ProcessAccount { account, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(account)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(accounts)
	}

	pub(crate) fn put_account_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ObjectArg,
		storage_partition_total: u64,
		touch_existing: bool,
		version: Option<u64>,
	) -> tg::Result<bool> {
		let entry_key = Key::Storage(crate::lmdb::storage::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		if let Some(value) = db
			.get(transaction, &entry_key)
			.map_err(|error| tg::error!(!error, "failed to get the account object"))?
		{
			let mut entry = crate::storage::Entry::deserialize(value)?;
			if touch_existing && arg.touched_at > entry.touched_at {
				entry.touched_at = arg.touched_at;
				let value = entry.serialize()?;
				db.put(transaction, &entry_key, &value)
					.map_err(|error| tg::error!(!error, "failed to touch the account object"))?;
				Self::put_account_object_clean_key(db, subspace, transaction, arg)?;
			}
			return Ok(false);
		}

		let object = Self::try_get_object_with_transaction(db, subspace, transaction, &arg.object)?;
		let Some(object) = object else {
			if touch_existing {
				return Err(
					tg::error!(object = %arg.object, "cannot add a missing object to a storage account"),
				);
			}
			return Ok(false);
		};
		let entry = crate::storage::Entry {
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		db.put(transaction, &entry_key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the account object"))?;

		let reverse_key = Key::Storage(crate::lmdb::storage::Key::ObjectAccount {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		db.put(transaction, &reverse_key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the object account"))?;
		Self::put_account_object_clean_key(db, subspace, transaction, arg)?;

		Self::add_account_usage(
			db,
			subspace,
			transaction,
			&arg.account,
			crate::storage::Kind::ObjectCount,
			1,
			storage_partition_total,
		)?;
		let size = i64::try_from(object.metadata.node.size)
			.map_err(|_| tg::error!(object = %arg.object, "the object size is too large"))?;
		Self::add_account_usage(
			db,
			subspace,
			transaction,
			&arg.account,
			crate::storage::Kind::ObjectSize,
			size,
			storage_partition_total,
		)?;

		let children =
			Self::get_object_children_with_transaction(db, subspace, transaction, &arg.object)?;
		for child in children {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Left(child),
				crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add(
					arg.account.clone(),
				)),
				crate::lmdb::update::Source::Put,
				version,
			)?;
		}

		Ok(true)
	}

	pub(crate) fn put_account_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ProcessArg,
		storage_partition_total: u64,
		touch_existing: bool,
		version: Option<u64>,
	) -> tg::Result<bool> {
		let entry_key = Key::Storage(crate::lmdb::storage::Key::AccountProcess {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		if let Some(value) = db
			.get(transaction, &entry_key)
			.map_err(|error| tg::error!(!error, "failed to get the account process"))?
		{
			let mut entry = crate::storage::Entry::deserialize(value)?;
			if touch_existing && arg.touched_at > entry.touched_at {
				entry.touched_at = arg.touched_at;
				let value = entry.serialize()?;
				db.put(transaction, &entry_key, &value)
					.map_err(|error| tg::error!(!error, "failed to touch the account process"))?;
				Self::put_account_process_clean_key(db, subspace, transaction, arg)?;
			}
			return Ok(false);
		}

		let process =
			Self::try_get_process_with_transaction(db, subspace, transaction, &arg.process)?;
		if process.is_none() {
			if touch_existing {
				return Err(
					tg::error!(process = %arg.process, "cannot add a missing process to a storage account"),
				);
			}
			return Ok(false);
		}
		let entry = crate::storage::Entry {
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		db.put(transaction, &entry_key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the account process"))?;

		let reverse_key = Key::Storage(crate::lmdb::storage::Key::ProcessAccount {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		db.put(transaction, &reverse_key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the process account"))?;
		Self::put_account_process_clean_key(db, subspace, transaction, arg)?;

		Self::add_account_usage(
			db,
			subspace,
			transaction,
			&arg.account,
			crate::storage::Kind::ProcessCount,
			1,
			storage_partition_total,
		)?;

		let children =
			Self::get_process_children_with_transaction(db, subspace, transaction, &arg.process)?;
		for child in children {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Right(child),
				crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add(
					arg.account.clone(),
				)),
				crate::lmdb::update::Source::Put,
				version,
			)?;
		}
		let objects =
			Self::get_process_objects_with_transaction(db, subspace, transaction, &arg.process)?;
		for (object, _) in objects {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Left(object),
				crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add(
					arg.account.clone(),
				)),
				crate::lmdb::update::Source::Put,
				version,
			)?;
		}

		Ok(true)
	}

	pub(crate) fn add_account_usage(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &crate::storage::Account,
		kind: crate::storage::Kind,
		delta: i64,
		storage_partition_total: u64,
	) -> tg::Result<()> {
		let partition = rand::random_range(0..storage_partition_total);
		let key = Key::Storage(crate::lmdb::storage::Key::AccountUsage {
			account: account.clone(),
			kind,
			partition,
		});
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the account storage value"))?
			.map(|value| {
				i64::from_le_bytes(
					value
						.try_into()
						.map_err(|_| tg::error!("invalid account storage value"))?,
				)
				.checked_add(delta)
				.ok_or_else(|| tg::error!("the account storage value overflowed"))
			})
			.transpose()?
			.unwrap_or(delta);
		db.put(transaction, &key, &value.to_le_bytes())
			.map_err(|error| tg::error!(!error, "failed to put the account storage value"))?;

		Ok(())
	}

	fn put_account_object_clean_key(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ObjectArg,
	) -> tg::Result<()> {
		let key = Key::Clean(crate::lmdb::clean::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
			touched_at: arg.touched_at,
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the account object clean key"))?;

		Ok(())
	}

	fn put_account_process_clean_key(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ProcessArg,
	) -> tg::Result<()> {
		let key = Key::Clean(crate::lmdb::clean::Key::AccountProcess {
			account: arg.account.clone(),
			process: arg.process.clone(),
			touched_at: arg.touched_at,
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the account process clean key"))?;

		Ok(())
	}
}
