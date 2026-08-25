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
		arg: &crate::usage::storage::put::ObjectArg,
		time_to_touch: std::time::Duration,
	) -> tg::Result<()> {
		let key = Key::Usage(crate::lmdb::usage::Key::AccountObject {
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
		let mut entry = crate::usage::storage::Entry::deserialize(value)?;
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
		arg: &crate::usage::storage::put::ProcessArg,
		time_to_touch: std::time::Duration,
	) -> tg::Result<()> {
		let key = Key::Usage(crate::lmdb::usage::Key::AccountProcess {
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
		let mut entry = crate::usage::storage::Entry::deserialize(value)?;
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
		touched_at: i64,
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
		let object_bytes = object.to_bytes();
		let required = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		accounts.extend(Self::get_target_tag_accounts_with_transaction(
			db,
			subspace,
			transaction,
			object_bytes.as_ref(),
			required,
		)?);
		for account in accounts {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Left(object.clone()),
				crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add {
					account,
					touched_at,
				}),
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
		touched_at: i64,
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
		let process_bytes = process.to_bytes();
		let required = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		accounts.extend(Self::get_target_tag_accounts_with_transaction(
			db,
			subspace,
			transaction,
			process_bytes.as_ref(),
			required,
		)?);
		for account in accounts {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Right(process.clone()),
				crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add {
					account,
					touched_at,
				}),
				crate::lmdb::update::Source::Put,
				None,
			)?;
		}

		Ok(())
	}

	fn get_target_tag_accounts_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		target: &[u8],
		required: tg::authorization::Permission,
	) -> tg::Result<BTreeSet<crate::usage::Account>> {
		let tags = Self::get_target_tags_with_transaction(db, subspace, transaction, target)?;
		let tags = Self::try_get_tags_with_transaction(db, subspace, transaction, &tags)?;
		let accounts = tags
			.into_iter()
			.flatten()
			.filter(|tag| {
				tag.permissions
					.iter()
					.any(|permission| permission.implies(required))
			})
			.filter_map(|tag| tag.account)
			.collect();

		Ok(accounts)
	}

	pub(crate) fn enqueue_account_process_relationships(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		process: &tg::process::Id,
		touched_at: i64,
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
			let kind = crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add {
				account,
				touched_at,
			});
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
	) -> tg::Result<Vec<crate::usage::Account>> {
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
				let Key::Usage(crate::lmdb::usage::Key::ObjectAccount { account, .. }) = key else {
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
	) -> tg::Result<Vec<crate::usage::Account>> {
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
				let Key::Usage(crate::lmdb::usage::Key::ProcessAccount { account, .. }) = key
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
		arg: &crate::usage::storage::put::ObjectArg,
		usage_partition_total: u64,
		touch_existing: bool,
		version: Option<u64>,
	) -> tg::Result<bool> {
		let entry_key = Key::Usage(crate::lmdb::usage::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		if let Some(value) = db
			.get(transaction, &entry_key)
			.map_err(|error| tg::error!(!error, "failed to get the account object"))?
		{
			let mut entry = crate::usage::storage::Entry::deserialize(value)?;
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
			return Ok(false);
		};
		let entry = crate::usage::storage::Entry {
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		db.put(transaction, &entry_key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the account object"))?;

		let reverse_key = Key::Usage(crate::lmdb::usage::Key::ObjectAccount {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		db.put(transaction, &reverse_key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the object account"))?;
		Self::put_account_object_clean_key(db, subspace, transaction, arg)?;
		let usage_partition = rand::random_range(0..usage_partition_total);

		let entry = crate::usage::DeltaArg {
			account: &arg.account,
			at: arg.touched_at,
			delta: 1,
			kind: crate::usage::DeltaKind::ObjectCount,
			partition: usage_partition,
		};
		Self::add_usage_delta(db, subspace, transaction, entry)?;
		let size = i64::try_from(object.metadata.node.size)
			.map_err(|_| tg::error!(object = %arg.object, "the object size is too large"))?;
		let entry = crate::usage::DeltaArg {
			account: &arg.account,
			at: arg.touched_at,
			delta: size,
			kind: crate::usage::DeltaKind::ObjectSize,
			partition: usage_partition,
		};
		Self::add_usage_delta(db, subspace, transaction, entry)?;

		let children =
			Self::get_object_children_with_transaction(db, subspace, transaction, &arg.object)?;
		for child in children {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Left(child),
				crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add {
					account: arg.account.clone(),
					touched_at: arg.touched_at,
				}),
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
		arg: &crate::usage::storage::put::ProcessArg,
		usage_partition_total: u64,
		touch_existing: bool,
		version: Option<u64>,
	) -> tg::Result<bool> {
		let entry_key = Key::Usage(crate::lmdb::usage::Key::AccountProcess {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		if let Some(value) = db
			.get(transaction, &entry_key)
			.map_err(|error| tg::error!(!error, "failed to get the account process"))?
		{
			let mut entry = crate::usage::storage::Entry::deserialize(value)?;
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
			return Ok(false);
		}
		let entry = crate::usage::storage::Entry {
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		db.put(transaction, &entry_key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the account process"))?;

		let reverse_key = Key::Usage(crate::lmdb::usage::Key::ProcessAccount {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		db.put(transaction, &reverse_key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the process account"))?;
		Self::put_account_process_clean_key(db, subspace, transaction, arg)?;
		let usage_partition = rand::random_range(0..usage_partition_total);

		let entry = crate::usage::DeltaArg {
			account: &arg.account,
			at: arg.touched_at,
			delta: 1,
			kind: crate::usage::DeltaKind::ProcessCount,
			partition: usage_partition,
		};
		Self::add_usage_delta(db, subspace, transaction, entry)?;

		let children =
			Self::get_process_children_with_transaction(db, subspace, transaction, &arg.process)?;
		for child in children {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Right(child),
				crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add {
					account: arg.account.clone(),
					touched_at: arg.touched_at,
				}),
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
				crate::lmdb::update::Kind::Storage(crate::lmdb::update::StorageKind::Add {
					account: arg.account.clone(),
					touched_at: arg.touched_at,
				}),
				crate::lmdb::update::Source::Put,
				version,
			)?;
		}

		Ok(true)
	}

	#[allow(clippy::too_many_arguments)]
	fn put_account_object_clean_key(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::usage::storage::put::ObjectArg,
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
		arg: &crate::usage::storage::put::ProcessArg,
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
