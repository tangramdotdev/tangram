use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	num_traits::ToPrimitive as _,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn touch_account_object(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::usage::storage::put::ObjectArg,
		time_to_touch: std::time::Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Key::Usage(crate::usage::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let Some(value) = crate::retry!(result) else {
			return Ok(ControlFlow::Break(()));
		};
		let mut entry = tangram_index::usage::storage::Entry::deserialize(&value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(entry.touched_at) >= time_to_touch {
			entry.touched_at = arg.touched_at;
			txn.set(&key, &entry.serialize()?);
			if entry.reference_count == 0 {
				Self::put_account_object_clean_key(txn, subspace, arg, partition_total);
			}
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn touch_account_process(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::usage::storage::put::ProcessArg,
		time_to_touch: std::time::Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Key::Usage(crate::usage::Key::AccountProcess {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let Some(value) = crate::retry!(result) else {
			return Ok(ControlFlow::Break(()));
		};
		let mut entry = tangram_index::usage::storage::Entry::deserialize(&value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(entry.touched_at) >= time_to_touch {
			entry.touched_at = arg.touched_at;
			txn.set(&key, &entry.serialize()?);
			if entry.reference_count == 0 {
				Self::put_account_process_clean_key(txn, subspace, arg, partition_total);
			}
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn enqueue_account_object_from_parents(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		object: &tg::object::Id,
		partition_total: u64,
		touched_at: i64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
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
		let parents = crate::propagate!(
			Self::get_object_parents_with_transaction(txn, subspace, object).await
		);
		for parent in parents {
			for account in crate::propagate!(
				Self::get_object_accounts_with_transaction(txn, subspace, &parent).await
			) {
				if let Some(entry) = crate::propagate!(
					Self::get_account_storage_entry_with_transaction(
						txn,
						subspace,
						&account,
						&tg::Either::Left(parent.clone())
					)
					.await
				) {
					insert(
						account,
						tangram_index::usage::storage::child_permissions(entry.permissions),
					);
				}
			}
		}
		let processes = crate::propagate!(
			Self::get_object_processes_with_transaction(txn, subspace, object).await
		);
		for (process, kind) in processes {
			for account in crate::propagate!(
				Self::get_process_accounts_with_transaction(txn, subspace, &process).await
			) {
				if let Some(entry) = crate::propagate!(
					Self::get_account_storage_entry_with_transaction(
						txn,
						subspace,
						&account,
						&tg::Either::Right(process.clone())
					)
					.await
				) {
					insert(
						account,
						tangram_index::usage::storage::object_permissions(entry.permissions, kind),
					);
				}
			}
		}
		let resource = tg::Id::from(object.clone());
		for (account, permissions) in crate::propagate!(
			Self::get_tag_storage_permissions_with_transaction(txn, subspace, &resource).await
		) {
			insert(account, permissions);
		}
		for (account, permissions) in accounts {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Left(object.clone()),
				&crate::update::Kind::Usage(crate::update::UsageKind::Put {
					account,
					permissions,
					touched_at,
				}),
				crate::update::Source::Put,
				partition_total,
			);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn enqueue_account_process_from_parents(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
		touched_at: i64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
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
		let parents = crate::propagate!(
			Self::get_process_parents_with_transaction(txn, subspace, process).await
		);
		for parent in parents {
			for account in crate::propagate!(
				Self::get_process_accounts_with_transaction(txn, subspace, &parent).await
			) {
				if let Some(entry) = crate::propagate!(
					Self::get_account_storage_entry_with_transaction(
						txn,
						subspace,
						&account,
						&tg::Either::Right(parent.clone())
					)
					.await
				) {
					insert(
						account,
						tangram_index::usage::storage::child_permissions(entry.permissions),
					);
				}
			}
		}
		let resource = tg::Id::from(process.clone());
		for (account, permissions) in crate::propagate!(
			Self::get_tag_storage_permissions_with_transaction(txn, subspace, &resource).await
		) {
			insert(account, permissions);
		}
		for (account, permissions) in accounts {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Right(process.clone()),
				&crate::update::Kind::Usage(crate::update::UsageKind::Put {
					account,
					permissions,
					touched_at,
				}),
				crate::update::Source::Put,
				partition_total,
			);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn enqueue_account_process_relationships(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
		touched_at: i64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let accounts = crate::propagate!(
			Self::get_process_accounts_with_transaction(txn, subspace, process).await
		);
		for account in accounts {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Right(process.clone()),
				&crate::update::Kind::Usage(crate::update::UsageKind::Propagate {
					account,
					touched_at,
				}),
				crate::update::Source::Put,
				partition_total,
			);
		}

		Ok(ControlFlow::Break(()))
	}

	async fn get_object_accounts_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		object: &tg::object::Id,
	) -> tg::Result<ControlFlow<Vec<tangram_index::usage::Account>, fdb::FdbError>> {
		let object_bytes = object.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::Kind::ObjectAccount.to_i32().unwrap(),
				object_bytes.as_ref(),
			),
		);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix))
		};
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::retry!(result);
		let accounts = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Usage(crate::usage::Key::ObjectAccount { account, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(account)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(accounts))
	}

	async fn get_process_accounts_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
	) -> tg::Result<ControlFlow<Vec<tangram_index::usage::Account>, fdb::FdbError>> {
		let process_bytes = process.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::Kind::ProcessAccount.to_i32().unwrap(),
				process_bytes.as_ref(),
			),
		);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix))
		};
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::retry!(result);
		let accounts = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Usage(crate::usage::Key::ProcessAccount { account, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(account)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(accounts))
	}

	pub(crate) async fn put_account_object(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::usage::storage::put::ObjectArg,
		partition_totals: crate::PartitionTotals,
		permissions: Option<tg::authorization::permission::Set>,
		version: Option<&fdbt::Versionstamp>,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let cleaning_partition_total = partition_totals.cleaning;
		let usage_partition_total = partition_totals.usage;
		let entry_key = Key::Usage(crate::usage::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		let result = txn.get(&entry_key, false).await;
		let value = crate::retry!(result);
		let entry = value
			.map(|value| tangram_index::usage::storage::Entry::deserialize(&value))
			.transpose()?;
		let touch_existing = permissions.is_none();
		let permissions = permissions.unwrap_or(tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::SUBTREE,
		));
		if permissions.is_empty() {
			return Ok(ControlFlow::Break(false));
		}
		if let Some(mut entry) = entry {
			let previous = entry.permissions;
			entry.permissions.insert(permissions);
			if previous != entry.permissions {
				// Recheck additions because the queued permissions may no longer retain storage.
				entry.reference_count = 0;
				txn.set(&entry_key, &entry.serialize()?);
				let clean_arg = tangram_index::usage::storage::put::ObjectArg {
					touched_at: entry.touched_at,
					..arg.clone()
				};
				Self::put_account_object_clean_key(
					txn,
					subspace,
					&clean_arg,
					cleaning_partition_total,
				);
				Self::clear_usage_update_versions(
					txn,
					subspace,
					&tg::Either::Left(arg.object.clone()),
					&arg.account,
				);
				crate::propagate!(
					Self::propagate_account_storage(
						txn,
						subspace,
						&tg::Either::Left(arg.object.clone()),
						&arg.account,
						arg.touched_at,
						partition_totals.usage_update,
						version
					)
					.await
				);
			}

			if touch_existing && arg.touched_at > entry.touched_at {
				entry.reference_count = 0;
				entry.touched_at = arg.touched_at;
				let value = entry.serialize()?;
				txn.set(&entry_key, &value);
				Self::put_account_object_clean_key(txn, subspace, arg, cleaning_partition_total);
			}
			if let Some(version) = version {
				crate::propagate!(
					Self::propagate_account_storage(
						txn,
						subspace,
						&tg::Either::Left(arg.object.clone()),
						&arg.account,
						arg.touched_at,
						partition_totals.usage_update,
						Some(version)
					)
					.await
				);
			}
			return Ok(ControlFlow::Break(false));
		}

		let object = crate::propagate!(
			Self::try_get_object_with_transaction(txn, subspace, &arg.object).await
		);
		let Some(object) = object else {
			return Ok(ControlFlow::Break(false));
		};
		let entry = tangram_index::usage::storage::Entry {
			permissions,
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		txn.set(&entry_key, &value);

		let reverse_key = Key::Usage(crate::usage::Key::ObjectAccount {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		txn.set(&reverse_key, &[]);
		Self::put_account_object_clean_key(txn, subspace, arg, cleaning_partition_total);
		let usage_partition = rand::random_range(0..usage_partition_total);

		Self::add_usage_delta(
			txn,
			subspace,
			&arg.account,
			arg.touched_at,
			tangram_index::usage::DeltaKind::ObjectCount,
			1,
			usage_partition,
		);
		let size = i64::try_from(object.metadata.node.size)
			.map_err(|_| tg::error!(object = %arg.object, "the object size is too large"))?;
		Self::add_usage_delta(
			txn,
			subspace,
			&arg.account,
			arg.touched_at,
			tangram_index::usage::DeltaKind::ObjectSize,
			size,
			usage_partition,
		);

		crate::propagate!(
			Self::propagate_account_storage(
				txn,
				subspace,
				&tg::Either::Left(arg.object.clone()),
				&arg.account,
				arg.touched_at,
				partition_totals.usage_update,
				version
			)
			.await
		);

		Ok(ControlFlow::Break(true))
	}

	pub(crate) async fn put_account_process(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::usage::storage::put::ProcessArg,
		partition_totals: crate::PartitionTotals,
		permissions: Option<tg::authorization::permission::Set>,
		version: Option<&fdbt::Versionstamp>,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let cleaning_partition_total = partition_totals.cleaning;
		let usage_partition_total = partition_totals.usage;
		let entry_key = Key::Usage(crate::usage::Key::AccountProcess {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		let result = txn.get(&entry_key, false).await;
		let value = crate::retry!(result);
		let entry = value
			.map(|value| tangram_index::usage::storage::Entry::deserialize(&value))
			.transpose()?;
		let touch_existing = permissions.is_none();
		let permissions = permissions.unwrap_or(tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::all(),
		));
		if permissions.is_empty() {
			return Ok(ControlFlow::Break(false));
		}
		if let Some(mut entry) = entry {
			let previous = entry.permissions;
			let stored = entry.stores_node();
			entry.permissions.insert(permissions);
			if previous != entry.permissions {
				// Recheck additions because the queued permissions may no longer retain storage.
				entry.reference_count = 0;
				if !stored && entry.stores_node() {
					Self::add_usage_delta(
						txn,
						subspace,
						&arg.account,
						arg.touched_at,
						tangram_index::usage::DeltaKind::ProcessCount,
						1,
						rand::random_range(0..usage_partition_total),
					);
				}
				txn.set(&entry_key, &entry.serialize()?);
				let clean_arg = tangram_index::usage::storage::put::ProcessArg {
					touched_at: entry.touched_at,
					..arg.clone()
				};
				Self::put_account_process_clean_key(
					txn,
					subspace,
					&clean_arg,
					cleaning_partition_total,
				);
				Self::clear_usage_update_versions(
					txn,
					subspace,
					&tg::Either::Right(arg.process.clone()),
					&arg.account,
				);
				crate::propagate!(
					Self::propagate_account_storage(
						txn,
						subspace,
						&tg::Either::Right(arg.process.clone()),
						&arg.account,
						arg.touched_at,
						partition_totals.usage_update,
						version
					)
					.await
				);
			}

			if touch_existing && arg.touched_at > entry.touched_at {
				entry.reference_count = 0;
				entry.touched_at = arg.touched_at;
				let value = entry.serialize()?;
				txn.set(&entry_key, &value);
				Self::put_account_process_clean_key(txn, subspace, arg, cleaning_partition_total);
			}
			if let Some(version) = version {
				crate::propagate!(
					Self::propagate_account_storage(
						txn,
						subspace,
						&tg::Either::Right(arg.process.clone()),
						&arg.account,
						arg.touched_at,
						partition_totals.usage_update,
						Some(version)
					)
					.await
				);
			}
			return Ok(ControlFlow::Break(false));
		}

		let process = crate::propagate!(
			Self::try_get_process_with_transaction(txn, subspace, &arg.process).await
		);
		if process.is_none() {
			return Ok(ControlFlow::Break(false));
		}
		let entry = tangram_index::usage::storage::Entry {
			permissions,
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		txn.set(&entry_key, &value);

		let reverse_key = Key::Usage(crate::usage::Key::ProcessAccount {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		txn.set(&reverse_key, &[]);
		Self::put_account_process_clean_key(txn, subspace, arg, cleaning_partition_total);
		let usage_partition = rand::random_range(0..usage_partition_total);

		if entry.stores_node() {
			Self::add_usage_delta(
				txn,
				subspace,
				&arg.account,
				arg.touched_at,
				tangram_index::usage::DeltaKind::ProcessCount,
				1,
				usage_partition,
			);
		}

		crate::propagate!(
			Self::propagate_account_storage(
				txn,
				subspace,
				&tg::Either::Right(arg.process.clone()),
				&arg.account,
				arg.touched_at,
				partition_totals.usage_update,
				version
			)
			.await
		);

		Ok(ControlFlow::Break(true))
	}

	async fn propagate_account_storage(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		account: &tangram_index::usage::Account,
		touched_at: i64,
		partition_total: u64,
		version: Option<&fdbt::Versionstamp>,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		if let Some(version) = version {
			let lowered = crate::propagate!(
				Self::lower_usage_update_put_version(txn, subspace, id, account, version).await
			);
			if !lowered {
				return Ok(ControlFlow::Break(()));
			}
		}
		let kind = crate::update::Kind::Usage(crate::update::UsageKind::Propagate {
			account: account.clone(),
			touched_at,
		});
		Self::enqueue_update_with_kind_at_version(
			txn,
			subspace,
			id,
			&kind,
			crate::update::Source::Put,
			partition_total,
			version,
		);
		Ok(ControlFlow::Break(()))
	}

	fn put_account_object_clean_key(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::usage::storage::put::ObjectArg,
		partition_total: u64,
	) {
		let partition = Self::partition_for_id(arg.object.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::clean::Key::AccountObject {
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
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::usage::storage::put::ProcessArg,
		partition_total: u64,
	) {
		let partition = Self::partition_for_id(arg.process.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::clean::Key::AccountProcess {
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
