use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

enum Candidate {
	Object {
		account: tangram_index::usage::Account,
		object: tg::object::Id,
		touched_at: i64,
	},
	Process {
		account: tangram_index::usage::Account,
		process: tg::process::Id,
		touched_at: i64,
	},
}

impl Index {
	pub(crate) fn schedule_object_accounts_for_cleaning(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		object: &tg::object::Id,
	) -> tg::Result<()> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::ObjectAccount.to_i32().unwrap(),
				object.to_bytes().as_ref(),
			),
		);
		let accounts = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the object accounts"))?
			.map(|entry| {
				let (key, _) = entry
					.map_err(|error| tg::error!(!error, "failed to read an object account"))?;
				let Key::Usage(crate::usage::Key::ObjectAccount { account, .. }) =
					Self::unpack(subspace, key)?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(account)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for account in accounts {
			Self::schedule_account_object_for_cleaning(
				db,
				subspace,
				transaction,
				&account,
				object,
			)?;
		}

		Ok(())
	}

	pub(crate) fn schedule_process_accounts_for_cleaning(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::ProcessAccount.to_i32().unwrap(),
				process.to_bytes().as_ref(),
			),
		);
		let accounts = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the process accounts"))?
			.map(|entry| {
				let (key, _) = entry
					.map_err(|error| tg::error!(!error, "failed to read a process account"))?;
				let Key::Usage(crate::usage::Key::ProcessAccount { account, .. }) =
					Self::unpack(subspace, key)?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(account)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for account in accounts {
			Self::schedule_account_process_for_cleaning(
				db,
				subspace,
				transaction,
				&account,
				process,
			)?;
		}

		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	pub(crate) fn clean_account_object_entry(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &tangram_index::usage::Account,
		object: &tg::object::Id,
		now: i64,
		touched_at: i64,
		usage_partition_total: u64,
	) -> tg::Result<()> {
		let candidate = Candidate::Object {
			account: account.clone(),
			object: object.clone(),
			touched_at,
		};
		Self::clean_account_entry(
			db,
			subspace,
			transaction,
			&candidate,
			now,
			usage_partition_total,
		)
	}

	#[allow(clippy::too_many_arguments)]
	pub(crate) fn clean_account_process_entry(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &tangram_index::usage::Account,
		process: &tg::process::Id,
		now: i64,
		touched_at: i64,
		usage_partition_total: u64,
	) -> tg::Result<()> {
		let candidate = Candidate::Process {
			account: account.clone(),
			process: process.clone(),
			touched_at,
		};
		Self::clean_account_entry(
			db,
			subspace,
			transaction,
			&candidate,
			now,
			usage_partition_total,
		)
	}

	fn clean_account_entry(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		candidate: &Candidate,
		now: i64,
		usage_partition_total: u64,
	) -> tg::Result<()> {
		let clean_key = match candidate {
			Candidate::Object {
				account,
				object,
				touched_at,
			} => Key::Clean(crate::clean::Key::AccountObject {
				account: account.clone(),
				object: object.clone(),
				touched_at: *touched_at,
			}),
			Candidate::Process {
				account,
				process,
				touched_at,
			} => Key::Clean(crate::clean::Key::AccountProcess {
				account: account.clone(),
				process: process.clone(),
				touched_at: *touched_at,
			}),
		};
		let clean_key = Self::pack(subspace, &clean_key);
		let entry_key = match candidate {
			Candidate::Object {
				account, object, ..
			} => Key::Usage(crate::usage::Key::AccountObject {
				account: account.clone(),
				object: object.clone(),
			}),
			Candidate::Process {
				account, process, ..
			} => Key::Usage(crate::usage::Key::AccountProcess {
				account: account.clone(),
				process: process.clone(),
			}),
		};
		let entry_key = Self::pack(subspace, &entry_key);
		let Some(value) = db
			.get(transaction, &entry_key)
			.map_err(|error| tg::error!(!error, "failed to get a storage entry"))?
		else {
			db.delete(transaction, &clean_key)
				.map_err(|error| tg::error!(!error, "failed to delete a storage clean key"))?;
			return Ok(());
		};
		let mut entry = tangram_index::usage::storage::Entry::deserialize(value)?;
		let touched_at = match candidate {
			Candidate::Object { touched_at, .. } | Candidate::Process { touched_at, .. } => {
				*touched_at
			},
		};
		if entry.touched_at != touched_at {
			db.delete(transaction, &clean_key)
				.map_err(|error| tg::error!(!error, "failed to delete a storage clean key"))?;
			return Ok(());
		}

		let (account, id) = match candidate {
			Candidate::Object {
				account, object, ..
			} => (account, tg::Either::Left(object.clone())),
			Candidate::Process {
				account, process, ..
			} => (account, tg::Either::Right(process.clone())),
		};
		let (permissions, reference_count) =
			Self::compute_account_storage_permissions_with_transaction(
				db,
				subspace,
				transaction,
				account,
				&id,
			)?;
		if !permissions.is_empty() {
			if permissions != entry.permissions {
				let stored = entry.stores_node();
				entry.permissions = permissions;
				if stored != entry.stores_node() {
					let delta = i64::from(entry.stores_node()) - i64::from(stored);
					let delta = tangram_index::usage::DeltaArg {
						account,
						at: now,
						delta,
						kind: tangram_index::usage::DeltaKind::ProcessCount,
						partition: rand::random_range(0..usage_partition_total),
					};
					Self::add_usage_delta(db, subspace, transaction, delta)?;
				}
				Self::schedule_account_storage_children_for_cleaning(
					db,
					subspace,
					transaction,
					account,
					&id,
				)?;
			}
			entry.reference_count = reference_count;
			let value = entry.serialize()?;
			db.put(transaction, &entry_key, &value)
				.map_err(|error| tg::error!(!error, "failed to update a storage entry"))?;
			db.delete(transaction, &clean_key)
				.map_err(|error| tg::error!(!error, "failed to delete a storage clean key"))?;
			return Ok(());
		}

		match candidate {
			Candidate::Object {
				account, object, ..
			} => Self::delete_account_object(
				db,
				subspace,
				transaction,
				account,
				object,
				now,
				usage_partition_total,
			)?,
			Candidate::Process {
				account, process, ..
			} => Self::delete_account_process(
				db,
				subspace,
				transaction,
				account,
				process,
				now,
				usage_partition_total,
			)?,
		}
		db.delete(transaction, &clean_key)
			.map_err(|error| tg::error!(!error, "failed to delete a storage clean key"))?;

		Ok(())
	}

	fn schedule_account_storage_children_for_cleaning(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &tangram_index::usage::Account,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
	) -> tg::Result<()> {
		match id {
			tg::Either::Left(object) => {
				for child in
					Self::get_object_children_with_transaction(db, subspace, transaction, object)?
				{
					Self::schedule_account_object_for_cleaning(
						db,
						subspace,
						transaction,
						account,
						&child,
					)?;
				}
			},
			tg::Either::Right(process) => {
				for child in
					Self::get_process_children_with_transaction(db, subspace, transaction, process)?
				{
					Self::schedule_account_process_for_cleaning(
						db,
						subspace,
						transaction,
						account,
						&child,
					)?;
				}
				for (object, _) in
					Self::get_process_objects_with_transaction(db, subspace, transaction, process)?
				{
					Self::schedule_account_object_for_cleaning(
						db,
						subspace,
						transaction,
						account,
						&object,
					)?;
				}
			},
		}

		Ok(())
	}

	fn delete_account_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &tangram_index::usage::Account,
		object: &tg::object::Id,
		now: i64,
		usage_partition_total: u64,
	) -> tg::Result<()> {
		let children =
			Self::get_object_children_with_transaction(db, subspace, transaction, object)?;
		for child in children {
			Self::schedule_account_object_for_cleaning(db, subspace, transaction, account, &child)?;
		}
		let key = Key::Usage(crate::usage::Key::AccountObject {
			account: account.clone(),
			object: object.clone(),
		});
		db.delete(transaction, &Self::pack(subspace, &key))
			.map_err(|error| tg::error!(!error, "failed to delete the account object"))?;
		let key = Key::Usage(crate::usage::Key::ObjectAccount {
			account: account.clone(),
			object: object.clone(),
		});
		db.delete(transaction, &Self::pack(subspace, &key))
			.map_err(|error| tg::error!(!error, "failed to delete the object account"))?;
		Self::clear_usage_update_versions(
			db,
			subspace,
			transaction,
			&tg::Either::Left(object.clone()),
			account,
		)?;
		let usage_partition = rand::random_range(0..usage_partition_total);
		let entry = tangram_index::usage::DeltaArg {
			account,
			at: now,
			delta: -1,
			kind: tangram_index::usage::DeltaKind::ObjectCount,
			partition: usage_partition,
		};
		Self::add_usage_delta(db, subspace, transaction, entry)?;
		let object_value =
			Self::try_get_object_with_transaction(db, subspace, transaction, object)?
				.ok_or_else(|| tg::error!(%object, "an object with a storage entry is missing"))?;
		let size = i64::try_from(object_value.metadata.node.size)
			.map_err(|_| tg::error!("the object size is too large"))?;
		let entry = tangram_index::usage::DeltaArg {
			account,
			at: now,
			delta: -size,
			kind: tangram_index::usage::DeltaKind::ObjectSize,
			partition: usage_partition,
		};
		Self::add_usage_delta(db, subspace, transaction, entry)?;
		let key = Key::Clean(crate::clean::Key::Object {
			id: object.clone(),
			touched_at: object_value.touched_at,
		});
		db.put(transaction, &Self::pack(subspace, &key), &[])
			.map_err(|error| tg::error!(!error, "failed to schedule the object for cleaning"))?;

		Ok(())
	}

	fn delete_account_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &tangram_index::usage::Account,
		process: &tg::process::Id,
		now: i64,
		usage_partition_total: u64,
	) -> tg::Result<()> {
		let stored = Self::get_account_storage_entry_with_transaction(
			db,
			subspace,
			transaction,
			account,
			&tg::Either::Right(process.clone()),
		)?
		.is_some_and(|entry| entry.stores_node());
		let children =
			Self::get_process_children_with_transaction(db, subspace, transaction, process)?;
		for child in children {
			Self::schedule_account_process_for_cleaning(
				db,
				subspace,
				transaction,
				account,
				&child,
			)?;
		}
		let objects =
			Self::get_process_objects_with_transaction(db, subspace, transaction, process)?;
		for (object, _) in objects {
			Self::schedule_account_object_for_cleaning(
				db,
				subspace,
				transaction,
				account,
				&object,
			)?;
		}
		let key = Key::Usage(crate::usage::Key::AccountProcess {
			account: account.clone(),
			process: process.clone(),
		});
		db.delete(transaction, &Self::pack(subspace, &key))
			.map_err(|error| tg::error!(!error, "failed to delete the account process"))?;
		let key = Key::Usage(crate::usage::Key::ProcessAccount {
			account: account.clone(),
			process: process.clone(),
		});
		db.delete(transaction, &Self::pack(subspace, &key))
			.map_err(|error| tg::error!(!error, "failed to delete the process account"))?;
		Self::clear_usage_update_versions(
			db,
			subspace,
			transaction,
			&tg::Either::Right(process.clone()),
			account,
		)?;
		let usage_partition = rand::random_range(0..usage_partition_total);
		if stored {
			let entry = tangram_index::usage::DeltaArg {
				account,
				at: now,
				delta: -1,
				kind: tangram_index::usage::DeltaKind::ProcessCount,
				partition: usage_partition,
			};
			Self::add_usage_delta(db, subspace, transaction, entry)?;
		}

		let process_value =
			Self::try_get_process_with_transaction(db, subspace, transaction, process)?
				.ok_or_else(|| tg::error!(%process, "a process with a storage entry is missing"))?;
		let key = Key::Clean(crate::clean::Key::Process {
			id: process.clone(),
			touched_at: process_value.touched_at,
		});
		db.put(transaction, &Self::pack(subspace, &key), &[])
			.map_err(|error| tg::error!(!error, "failed to schedule the process for cleaning"))?;

		Ok(())
	}

	fn schedule_account_object_for_cleaning(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &tangram_index::usage::Account,
		object: &tg::object::Id,
	) -> tg::Result<()> {
		let entry_key = Key::Usage(crate::usage::Key::AccountObject {
			account: account.clone(),
			object: object.clone(),
		});
		let Some(value) = db
			.get(transaction, &Self::pack(subspace, &entry_key))
			.map_err(|error| tg::error!(!error, "failed to get an account object"))?
		else {
			return Ok(());
		};
		let mut entry = tangram_index::usage::storage::Entry::deserialize(value)?;
		// Invalidate the cached references because the retaining scopes may have changed.
		if entry.reference_count != 0 {
			entry.reference_count = 0;
			db.put(
				transaction,
				&Self::pack(subspace, &entry_key),
				&entry.serialize()?,
			)
			.map_err(|error| tg::error!(!error, "failed to invalidate the storage references"))?;
		}

		let key = Key::Clean(crate::clean::Key::AccountObject {
			account: account.clone(),
			object: object.clone(),
			touched_at: entry.touched_at,
		});
		db.put(transaction, &Self::pack(subspace, &key), &[])
			.map_err(|error| {
				tg::error!(!error, "failed to schedule an account object for cleaning")
			})?;

		Ok(())
	}

	fn schedule_account_process_for_cleaning(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &tangram_index::usage::Account,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		let entry_key = Key::Usage(crate::usage::Key::AccountProcess {
			account: account.clone(),
			process: process.clone(),
		});
		let Some(value) = db
			.get(transaction, &Self::pack(subspace, &entry_key))
			.map_err(|error| tg::error!(!error, "failed to get an account process"))?
		else {
			return Ok(());
		};
		let mut entry = tangram_index::usage::storage::Entry::deserialize(value)?;
		// Invalidate the cached references because the retaining scopes may have changed.
		if entry.reference_count != 0 {
			entry.reference_count = 0;
			db.put(
				transaction,
				&Self::pack(subspace, &entry_key),
				&entry.serialize()?,
			)
			.map_err(|error| tg::error!(!error, "failed to invalidate the storage references"))?;
		}

		let key = Key::Clean(crate::clean::Key::AccountProcess {
			account: account.clone(),
			process: process.clone(),
			touched_at: entry.touched_at,
		});
		db.put(transaction, &Self::pack(subspace, &key), &[])
			.map_err(|error| {
				tg::error!(!error, "failed to schedule an account process for cleaning")
			})?;

		Ok(())
	}
}
