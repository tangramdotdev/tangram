use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn touch_account_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::usage::storage::put::ObjectArg,
		time_to_touch: std::time::Duration,
	) -> tg::Result<()> {
		let key = Key::Usage(crate::usage::Key::AccountObject {
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
		let mut entry = tangram_index::usage::storage::Entry::deserialize(value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(entry.touched_at) >= time_to_touch {
			entry.touched_at = arg.touched_at;
			db.put(transaction, &key, &entry.serialize()?)
				.map_err(|error| tg::error!(!error, "failed to touch the account object"))?;
			if entry.reference_count == 0 {
				Self::put_account_object_clean_key(db, subspace, transaction, arg)?;
			}
		}

		Ok(())
	}

	pub(crate) fn touch_account_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::usage::storage::put::ProcessArg,
		time_to_touch: std::time::Duration,
	) -> tg::Result<()> {
		let key = Key::Usage(crate::usage::Key::AccountProcess {
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
		let mut entry = tangram_index::usage::storage::Entry::deserialize(value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(entry.touched_at) >= time_to_touch {
			entry.touched_at = arg.touched_at;
			db.put(transaction, &key, &entry.serialize()?)
				.map_err(|error| tg::error!(!error, "failed to touch the account process"))?;
			if entry.reference_count == 0 {
				Self::put_account_process_clean_key(db, subspace, transaction, arg)?;
			}
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
		let mut accounts = BTreeMap::new();
		let mut insert = |account, permissions: tg::authorization::permission::Set| {
			if !permissions.is_empty() {
				accounts
					.entry(account)
					.and_modify(|current: &mut tg::authorization::permission::Set| {
						current.insert(permissions);
					})
					.or_insert(permissions);
			}
		};
		let parents = Self::get_object_parents_with_transaction(db, subspace, transaction, object)?;
		for parent in parents {
			for account in
				Self::get_object_accounts_with_transaction(db, subspace, transaction, &parent)?
			{
				if let Some(entry) = Self::get_account_storage_entry_with_transaction(
					db,
					subspace,
					transaction,
					&account,
					&tg::Either::Left(parent.clone()),
				)? {
					insert(
						account,
						tangram_index::usage::storage::child_permissions(entry.permissions),
					);
				}
			}
		}
		let processes =
			Self::get_object_processes_with_transaction(db, subspace, transaction, object)?;
		for (process, kind) in processes {
			for account in
				Self::get_process_accounts_with_transaction(db, subspace, transaction, &process)?
			{
				if let Some(entry) = Self::get_account_storage_entry_with_transaction(
					db,
					subspace,
					transaction,
					&account,
					&tg::Either::Right(process.clone()),
				)? {
					insert(
						account,
						tangram_index::usage::storage::object_permissions(entry.permissions, kind),
					);
				}
			}
		}
		let resource = tg::Id::from(object.clone());
		for (account, permissions) in Self::get_tag_storage_permissions_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
		)? {
			insert(account, permissions);
		}
		for (account, permissions) in accounts {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Left(object.clone()),
				crate::update::Kind::Usage(crate::update::UsageKind::Put {
					account,
					permissions,
					touched_at,
				}),
				crate::update::Source::Put,
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
		let mut accounts = BTreeMap::new();
		let mut insert = |account, permissions: tg::authorization::permission::Set| {
			if !permissions.is_empty() {
				accounts
					.entry(account)
					.and_modify(|current: &mut tg::authorization::permission::Set| {
						current.insert(permissions);
					})
					.or_insert(permissions);
			}
		};
		let parents =
			Self::get_process_parents_with_transaction(db, subspace, transaction, process)?;
		for parent in parents {
			for account in
				Self::get_process_accounts_with_transaction(db, subspace, transaction, &parent)?
			{
				if let Some(entry) = Self::get_account_storage_entry_with_transaction(
					db,
					subspace,
					transaction,
					&account,
					&tg::Either::Right(parent.clone()),
				)? {
					insert(
						account,
						tangram_index::usage::storage::child_permissions(entry.permissions),
					);
				}
			}
		}
		let resource = tg::Id::from(process.clone());
		for (account, permissions) in Self::get_tag_storage_permissions_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
		)? {
			insert(account, permissions);
		}
		for (account, permissions) in accounts {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Right(process.clone()),
				crate::update::Kind::Usage(crate::update::UsageKind::Put {
					account,
					permissions,
					touched_at,
				}),
				crate::update::Source::Put,
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
			let Some(entry) = Self::get_account_storage_entry_with_transaction(
				db,
				subspace,
				transaction,
				&account,
				&tg::Either::Right(process.clone()),
			)?
			else {
				continue;
			};
			let permissions = tangram_index::usage::storage::child_permissions(entry.permissions);
			let kind = crate::update::Kind::Usage(crate::update::UsageKind::Put {
				account: account.clone(),
				permissions,
				touched_at,
			});
			for child in &children {
				if permissions.is_empty() {
					break;
				}
				Self::enqueue_update_with_kind(
					db,
					subspace,
					transaction,
					tg::Either::Right(child.clone()),
					kind.clone(),
					crate::update::Source::Put,
					None,
				)?;
			}
			for (object, object_kind) in &objects {
				let permissions = tangram_index::usage::storage::object_permissions(
					entry.permissions,
					*object_kind,
				);
				if permissions.is_empty() {
					continue;
				}
				let kind = crate::update::Kind::Usage(crate::update::UsageKind::Put {
					account: account.clone(),
					permissions,
					touched_at,
				});
				Self::enqueue_update_with_kind(
					db,
					subspace,
					transaction,
					tg::Either::Left(object.clone()),
					kind.clone(),
					crate::update::Source::Put,
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
	) -> tg::Result<Vec<tangram_index::usage::Account>> {
		let object_bytes = object.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::Kind::ObjectAccount.to_i32().unwrap(),
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
				let Key::Usage(crate::usage::Key::ObjectAccount { account, .. }) = key else {
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
	) -> tg::Result<Vec<tangram_index::usage::Account>> {
		let process_bytes = process.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::Kind::ProcessAccount.to_i32().unwrap(),
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
				let Key::Usage(crate::usage::Key::ProcessAccount { account, .. }) = key else {
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
		arg: &tangram_index::usage::storage::put::ObjectArg,
		usage_partition_total: u64,
		permissions: Option<tg::authorization::permission::Set>,
		version: Option<u64>,
	) -> tg::Result<bool> {
		let entry_key = Key::Usage(crate::usage::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		let value = db
			.get(transaction, &entry_key)
			.map_err(|error| tg::error!(!error, "failed to get the account object"))?;
		let entry = value
			.map(tangram_index::usage::storage::Entry::deserialize)
			.transpose()?;
		let touch_existing = permissions.is_none();
		let permissions = permissions.unwrap_or(tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::SUBTREE,
		));
		if permissions.is_empty() {
			return Ok(false);
		}
		if let Some(mut entry) = entry {
			let previous = entry.permissions;
			entry.permissions.insert(permissions);
			if previous != entry.permissions {
				// Recheck additions because the queued permissions may no longer retain storage.
				entry.reference_count = 0;
				db.put(transaction, &entry_key, &entry.serialize()?)
					.map_err(|error| {
						tg::error!(!error, "failed to update the storage permissions")
					})?;
				let clean_arg = tangram_index::usage::storage::put::ObjectArg {
					touched_at: entry.touched_at,
					..arg.clone()
				};
				Self::put_account_object_clean_key(db, subspace, transaction, &clean_arg)?;
				Self::clear_usage_update_versions(
					db,
					subspace,
					transaction,
					&tg::Either::Left(arg.object.clone()),
					&arg.account,
				)?;
				Self::propagate_account_object(
					db,
					subspace,
					transaction,
					arg,
					entry.permissions,
					version.unwrap_or_else(|| transaction.id() as u64),
				)?;
			}

			if touch_existing && arg.touched_at > entry.touched_at {
				entry.reference_count = 0;
				entry.touched_at = arg.touched_at;
				let value = entry.serialize()?;
				db.put(transaction, &entry_key, &value)
					.map_err(|error| tg::error!(!error, "failed to touch the account object"))?;
				Self::put_account_object_clean_key(db, subspace, transaction, arg)?;
			}
			if let Some(version) = version {
				Self::propagate_account_object(
					db,
					subspace,
					transaction,
					arg,
					entry.permissions,
					version,
				)?;
			}
			return Ok(false);
		}

		let object = Self::try_get_object_with_transaction(db, subspace, transaction, &arg.object)?;
		let Some(object) = object else {
			return Ok(false);
		};
		let entry = tangram_index::usage::storage::Entry {
			permissions,
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		db.put(transaction, &entry_key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the account object"))?;

		let reverse_key = Key::Usage(crate::usage::Key::ObjectAccount {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		db.put(transaction, &reverse_key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the object account"))?;
		Self::put_account_object_clean_key(db, subspace, transaction, arg)?;
		let usage_partition = rand::random_range(0..usage_partition_total);

		let entry = tangram_index::usage::DeltaArg {
			account: &arg.account,
			at: arg.touched_at,
			delta: 1,
			kind: tangram_index::usage::DeltaKind::ObjectCount,
			partition: usage_partition,
		};
		Self::add_usage_delta(db, subspace, transaction, entry)?;
		let size = i64::try_from(object.metadata.node.size)
			.map_err(|_| tg::error!(object = %arg.object, "the object size is too large"))?;
		let entry = tangram_index::usage::DeltaArg {
			account: &arg.account,
			at: arg.touched_at,
			delta: size,
			kind: tangram_index::usage::DeltaKind::ObjectSize,
			partition: usage_partition,
		};
		Self::add_usage_delta(db, subspace, transaction, entry)?;

		let version = version.unwrap_or_else(|| transaction.id() as u64);
		Self::propagate_account_object(db, subspace, transaction, arg, permissions, version)?;

		Ok(true)
	}

	pub(crate) fn put_account_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::usage::storage::put::ProcessArg,
		usage_partition_total: u64,
		permissions: Option<tg::authorization::permission::Set>,
		version: Option<u64>,
	) -> tg::Result<bool> {
		let entry_key = Key::Usage(crate::usage::Key::AccountProcess {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		let value = db
			.get(transaction, &entry_key)
			.map_err(|error| tg::error!(!error, "failed to get the account process"))?;
		let entry = value
			.map(tangram_index::usage::storage::Entry::deserialize)
			.transpose()?;
		let touch_existing = permissions.is_none();
		let permissions = permissions.unwrap_or(tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::all(),
		));
		if permissions.is_empty() {
			return Ok(false);
		}
		if let Some(mut entry) = entry {
			let previous = entry.permissions;
			let stored = entry.stores_node();
			entry.permissions.insert(permissions);
			if previous != entry.permissions {
				// Recheck additions because the queued permissions may no longer retain storage.
				entry.reference_count = 0;
				if !stored && entry.stores_node() {
					let delta = tangram_index::usage::DeltaArg {
						account: &arg.account,
						at: arg.touched_at,
						delta: 1,
						kind: tangram_index::usage::DeltaKind::ProcessCount,
						partition: rand::random_range(0..usage_partition_total),
					};
					Self::add_usage_delta(db, subspace, transaction, delta)?;
				}
				db.put(transaction, &entry_key, &entry.serialize()?)
					.map_err(|error| {
						tg::error!(!error, "failed to update the storage permissions")
					})?;
				let clean_arg = tangram_index::usage::storage::put::ProcessArg {
					touched_at: entry.touched_at,
					..arg.clone()
				};
				Self::put_account_process_clean_key(db, subspace, transaction, &clean_arg)?;
				Self::clear_usage_update_versions(
					db,
					subspace,
					transaction,
					&tg::Either::Right(arg.process.clone()),
					&arg.account,
				)?;
				Self::propagate_account_process(
					db,
					subspace,
					transaction,
					arg,
					entry.permissions,
					version.unwrap_or_else(|| transaction.id() as u64),
				)?;
			}

			if touch_existing && arg.touched_at > entry.touched_at {
				entry.reference_count = 0;
				entry.touched_at = arg.touched_at;
				let value = entry.serialize()?;
				db.put(transaction, &entry_key, &value)
					.map_err(|error| tg::error!(!error, "failed to touch the account process"))?;
				Self::put_account_process_clean_key(db, subspace, transaction, arg)?;
			}
			if let Some(version) = version {
				Self::propagate_account_process(
					db,
					subspace,
					transaction,
					arg,
					entry.permissions,
					version,
				)?;
			}
			return Ok(false);
		}

		let process =
			Self::try_get_process_with_transaction(db, subspace, transaction, &arg.process)?;
		if process.is_none() {
			return Ok(false);
		}
		let entry = tangram_index::usage::storage::Entry {
			permissions,
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		db.put(transaction, &entry_key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the account process"))?;

		let reverse_key = Key::Usage(crate::usage::Key::ProcessAccount {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		db.put(transaction, &reverse_key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the process account"))?;
		Self::put_account_process_clean_key(db, subspace, transaction, arg)?;
		let usage_partition = rand::random_range(0..usage_partition_total);

		if entry.stores_node() {
			let entry = tangram_index::usage::DeltaArg {
				account: &arg.account,
				at: arg.touched_at,
				delta: 1,
				kind: tangram_index::usage::DeltaKind::ProcessCount,
				partition: usage_partition,
			};
			Self::add_usage_delta(db, subspace, transaction, entry)?;
		}

		let version = version.unwrap_or_else(|| transaction.id() as u64);
		Self::propagate_account_process(db, subspace, transaction, arg, permissions, version)?;

		Ok(true)
	}

	fn propagate_account_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::usage::storage::put::ObjectArg,
		permissions: tg::authorization::permission::Set,
		version: u64,
	) -> tg::Result<()> {
		let id = tg::Either::Left(arg.object.clone());
		if !Self::lower_usage_update_put_version(
			db,
			subspace,
			transaction,
			&id,
			&arg.account,
			version,
		)? {
			return Ok(());
		}
		let inherited = tangram_index::usage::storage::child_permissions(permissions);
		let children = if inherited.is_empty() {
			Vec::new()
		} else {
			Self::get_object_children_with_transaction(db, subspace, transaction, &arg.object)?
		};
		for child in children {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Left(child),
				crate::update::Kind::Usage(crate::update::UsageKind::Put {
					account: arg.account.clone(),
					permissions: inherited,
					touched_at: arg.touched_at,
				}),
				crate::update::Source::Put,
				Some(version),
			)?;
		}

		Ok(())
	}

	fn propagate_account_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::usage::storage::put::ProcessArg,
		permissions: tg::authorization::permission::Set,
		version: u64,
	) -> tg::Result<()> {
		let id = tg::Either::Right(arg.process.clone());
		if !Self::lower_usage_update_put_version(
			db,
			subspace,
			transaction,
			&id,
			&arg.account,
			version,
		)? {
			return Ok(());
		}
		let inherited = tangram_index::usage::storage::child_permissions(permissions);
		let children = if inherited.is_empty() {
			Vec::new()
		} else {
			Self::get_process_children_with_transaction(db, subspace, transaction, &arg.process)?
		};
		for child in children {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Right(child),
				crate::update::Kind::Usage(crate::update::UsageKind::Put {
					account: arg.account.clone(),
					permissions: inherited,
					touched_at: arg.touched_at,
				}),
				crate::update::Source::Put,
				Some(version),
			)?;
		}
		let objects =
			Self::get_process_objects_with_transaction(db, subspace, transaction, &arg.process)?;
		for (object, kind) in objects {
			let permissions = tangram_index::usage::storage::object_permissions(permissions, kind);
			if permissions.is_empty() {
				continue;
			}
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Left(object),
				crate::update::Kind::Usage(crate::update::UsageKind::Put {
					account: arg.account.clone(),
					permissions,
					touched_at: arg.touched_at,
				}),
				crate::update::Source::Put,
				Some(version),
			)?;
		}

		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	fn put_account_object_clean_key(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::usage::storage::put::ObjectArg,
	) -> tg::Result<()> {
		let key = Key::Clean(crate::clean::Key::AccountObject {
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
		arg: &tangram_index::usage::storage::put::ProcessArg,
	) -> tg::Result<()> {
		let key = Key::Clean(crate::clean::Key::AccountProcess {
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
