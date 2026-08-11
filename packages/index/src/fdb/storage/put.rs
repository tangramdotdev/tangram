use {
	crate::fdb::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	num_traits::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn touch_account_object(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ObjectArg,
		time_to_touch: std::time::Duration,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Storage(crate::fdb::storage::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let key = Self::pack(subspace, &key);
		let Some(value) = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the account object"))?
		else {
			return Ok(());
		};
		let mut entry = crate::storage::Entry::deserialize(&value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(entry.touched_at) >= time_to_touch {
			entry.touched_at = arg.touched_at;
			txn.set(&key, &entry.serialize()?);
			Self::put_account_object_clean_key(txn, subspace, arg, partition_total);
		}

		Ok(())
	}

	pub(crate) async fn touch_account_process(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ProcessArg,
		time_to_touch: std::time::Duration,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Storage(crate::fdb::storage::Key::AccountProcess {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let key = Self::pack(subspace, &key);
		let Some(value) = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the account process"))?
		else {
			return Ok(());
		};
		let mut entry = crate::storage::Entry::deserialize(&value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(entry.touched_at) >= time_to_touch {
			entry.touched_at = arg.touched_at;
			txn.set(&key, &entry.serialize()?);
			Self::put_account_process_clean_key(txn, subspace, arg, partition_total);
		}

		Ok(())
	}

	pub(crate) async fn enqueue_account_object_from_parents(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		object: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let mut accounts = BTreeSet::new();
		let parents = Self::get_object_parents_with_transaction(txn, subspace, object).await?;
		for parent in parents {
			accounts
				.extend(Self::get_object_accounts_with_transaction(txn, subspace, &parent).await?);
		}
		let processes = Self::get_object_processes_with_transaction(txn, subspace, object).await?;
		for (process, _) in processes {
			accounts.extend(
				Self::get_process_accounts_with_transaction(txn, subspace, &process).await?,
			);
		}
		for account in accounts {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Left(object.clone()),
				&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Add(account)),
				crate::fdb::update::Source::Put,
				partition_total,
			);
		}

		Ok(())
	}

	pub(crate) async fn enqueue_account_process_from_parents(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let mut accounts = BTreeSet::new();
		let parents = Self::get_process_parents_with_transaction(txn, subspace, process).await?;
		for parent in parents {
			accounts
				.extend(Self::get_process_accounts_with_transaction(txn, subspace, &parent).await?);
		}
		for account in accounts {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Right(process.clone()),
				&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Add(account)),
				crate::fdb::update::Source::Put,
				partition_total,
			);
		}

		Ok(())
	}

	pub(crate) async fn enqueue_account_process_relationships(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let accounts = Self::get_process_accounts_with_transaction(txn, subspace, process).await?;
		for account in accounts {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Right(process.clone()),
				&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Propagate(
					account,
				)),
				crate::fdb::update::Source::Put,
				partition_total,
			);
		}

		Ok(())
	}

	async fn get_object_accounts_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		object: &tg::object::Id,
	) -> tg::Result<Vec<crate::usage::Account>> {
		let object_bytes = object.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::fdb::Kind::ObjectAccount.to_i32().unwrap(),
				object_bytes.as_ref(),
			),
		);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix))
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the object accounts"))?;
		let accounts = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Storage(crate::fdb::storage::Key::ObjectAccount { account, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(account)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(accounts)
	}

	async fn get_process_accounts_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
	) -> tg::Result<Vec<crate::usage::Account>> {
		let process_bytes = process.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::fdb::Kind::ProcessAccount.to_i32().unwrap(),
				process_bytes.as_ref(),
			),
		);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix))
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process accounts"))?;
		let accounts = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Storage(crate::fdb::storage::Key::ProcessAccount { account, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(account)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(accounts)
	}

	pub(crate) async fn put_account_object(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ObjectArg,
		partition_total: u64,
		usage_partition_total: u64,
		touch_existing: bool,
		version: Option<&fdbt::Versionstamp>,
	) -> tg::Result<bool> {
		let entry_key = Key::Storage(crate::fdb::storage::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		if let Some(value) = txn
			.get(&entry_key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the account object"))?
		{
			let mut entry = crate::storage::Entry::deserialize(&value)?;
			if touch_existing && arg.touched_at > entry.touched_at {
				entry.touched_at = arg.touched_at;
				let value = entry.serialize()?;
				txn.set(&entry_key, &value);
				Self::put_account_object_clean_key(txn, subspace, arg, partition_total);
			}
			return Ok(false);
		}

		let object = Self::try_get_object_with_transaction(txn, subspace, &arg.object).await?;
		let Some(object) = object else {
			if touch_existing {
				return Err(
					tg::error!(object = %arg.object, "cannot add a missing object to a usage account"),
				);
			}
			return Ok(false);
		};
		let entry = crate::storage::Entry {
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		txn.set(&entry_key, &value);

		let reverse_key = Key::Storage(crate::fdb::storage::Key::ObjectAccount {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		txn.set(&reverse_key, &[]);
		Self::put_account_object_clean_key(txn, subspace, arg, partition_total);

		Self::add_account_usage(
			txn,
			subspace,
			&arg.account,
			crate::usage::Kind::ObjectCount,
			1,
			usage_partition_total,
		);
		let size = i64::try_from(object.metadata.node.size)
			.map_err(|_| tg::error!(object = %arg.object, "the object size is too large"))?;
		Self::add_account_usage(
			txn,
			subspace,
			&arg.account,
			crate::usage::Kind::ObjectSize,
			size,
			usage_partition_total,
		);

		Self::enqueue_update_with_kind_at_version(
			txn,
			subspace,
			&tg::Either::Left(arg.object.clone()),
			&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Propagate(
				arg.account.clone(),
			)),
			crate::fdb::update::Source::Put,
			partition_total,
			version,
		);

		Ok(true)
	}

	pub(crate) async fn put_account_process(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ProcessArg,
		partition_total: u64,
		usage_partition_total: u64,
		touch_existing: bool,
		version: Option<&fdbt::Versionstamp>,
	) -> tg::Result<bool> {
		let entry_key = Key::Storage(crate::fdb::storage::Key::AccountProcess {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		if let Some(value) = txn
			.get(&entry_key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the account process"))?
		{
			let mut entry = crate::storage::Entry::deserialize(&value)?;
			if touch_existing && arg.touched_at > entry.touched_at {
				entry.touched_at = arg.touched_at;
				let value = entry.serialize()?;
				txn.set(&entry_key, &value);
				Self::put_account_process_clean_key(txn, subspace, arg, partition_total);
			}
			return Ok(false);
		}

		let process = Self::try_get_process_with_transaction(txn, subspace, &arg.process).await?;
		if process.is_none() {
			if touch_existing {
				return Err(
					tg::error!(process = %arg.process, "cannot add a missing process to a usage account"),
				);
			}
			return Ok(false);
		}
		let entry = crate::storage::Entry {
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		txn.set(&entry_key, &value);

		let reverse_key = Key::Storage(crate::fdb::storage::Key::ProcessAccount {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		txn.set(&reverse_key, &[]);
		Self::put_account_process_clean_key(txn, subspace, arg, partition_total);

		Self::add_account_usage(
			txn,
			subspace,
			&arg.account,
			crate::usage::Kind::ProcessCount,
			1,
			usage_partition_total,
		);

		Self::enqueue_update_with_kind_at_version(
			txn,
			subspace,
			&tg::Either::Right(arg.process.clone()),
			&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Propagate(
				arg.account.clone(),
			)),
			crate::fdb::update::Source::Put,
			partition_total,
			version,
		);

		Ok(true)
	}

	pub(crate) fn add_account_usage(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		kind: crate::usage::Kind,
		delta: i64,
		usage_partition_total: u64,
	) {
		let partition = rand::random_range(0..usage_partition_total);
		let key = Key::Storage(crate::fdb::storage::Key::AccountUsage {
			account: account.clone(),
			kind,
			partition,
		});
		let key = Self::pack(subspace, &key);
		txn.atomic_op(&key, &delta.to_le_bytes(), fdb::options::MutationType::Add);
	}

	fn put_account_object_clean_key(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ObjectArg,
		partition_total: u64,
	) {
		let partition = Self::partition_for_id(arg.object.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::fdb::clean::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
			partition,
			touched_at: arg.touched_at,
		});
		let key = Self::pack(subspace, &key);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &[]);
	}

	fn put_account_process_clean_key(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ProcessArg,
		partition_total: u64,
	) {
		let partition = Self::partition_for_id(arg.process.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::fdb::clean::Key::AccountProcess {
			account: arg.account.clone(),
			partition,
			process: arg.process.clone(),
			touched_at: arg.touched_at,
		});
		let key = Self::pack(subspace, &key);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &[]);
	}
}
