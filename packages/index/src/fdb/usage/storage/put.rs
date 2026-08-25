use {
	crate::fdb::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	num_traits::ToPrimitive as _,
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn touch_account_object(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::storage::put::ObjectArg,
		time_to_touch: std::time::Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Key::Usage(crate::fdb::usage::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let Some(value) = crate::fdb::retry!(result) else {
			return Ok(ControlFlow::Break(()));
		};
		let mut entry = crate::usage::storage::Entry::deserialize(&value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(entry.touched_at) >= time_to_touch {
			entry.touched_at = arg.touched_at;
			txn.set(&key, &entry.serialize()?);
			Self::put_account_object_clean_key(txn, subspace, arg, partition_total);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn touch_account_process(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::storage::put::ProcessArg,
		time_to_touch: std::time::Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Key::Usage(crate::fdb::usage::Key::AccountProcess {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let Some(value) = crate::fdb::retry!(result) else {
			return Ok(ControlFlow::Break(()));
		};
		let mut entry = crate::usage::storage::Entry::deserialize(&value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(entry.touched_at) >= time_to_touch {
			entry.touched_at = arg.touched_at;
			txn.set(&key, &entry.serialize()?);
			Self::put_account_process_clean_key(txn, subspace, arg, partition_total);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn enqueue_account_object_from_parents(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		object: &tg::object::Id,
		partition_total: u64,
		touched_at: i64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let object_bytes = object.to_bytes();
		let required = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let (parents, processes, accounts) = futures::future::try_join3(
			Self::get_object_parents_with_transaction(txn, subspace, object),
			Self::get_object_processes_with_transaction(txn, subspace, object),
			Self::get_target_tag_accounts_with_transaction(
				txn,
				subspace,
				object_bytes.as_ref(),
				required,
			),
		)
		.await?;
		let parents = match parents {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let processes = match processes {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let mut accounts = match accounts {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		for parent in parents {
			accounts.extend(crate::fdb::propagate!(
				Self::get_object_accounts_with_transaction(txn, subspace, &parent).await
			));
		}
		for (process, _) in processes {
			accounts.extend(crate::fdb::propagate!(
				Self::get_process_accounts_with_transaction(txn, subspace, &process).await
			));
		}
		for account in accounts {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Left(object.clone()),
				&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Add {
					account,
					touched_at,
				}),
				crate::fdb::update::Source::Put,
				partition_total,
			);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn enqueue_account_process_from_parents(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
		touched_at: i64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let process_bytes = process.to_bytes();
		let required = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		let (parents, accounts) = futures::future::try_join(
			Self::get_process_parents_with_transaction(txn, subspace, process),
			Self::get_target_tag_accounts_with_transaction(
				txn,
				subspace,
				process_bytes.as_ref(),
				required,
			),
		)
		.await?;
		let parents = match parents {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let mut accounts = match accounts {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		for parent in parents {
			accounts.extend(crate::fdb::propagate!(
				Self::get_process_accounts_with_transaction(txn, subspace, &parent).await
			));
		}
		for account in accounts {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Right(process.clone()),
				&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Add {
					account,
					touched_at,
				}),
				crate::fdb::update::Source::Put,
				partition_total,
			);
		}

		Ok(ControlFlow::Break(()))
	}

	async fn get_target_tag_accounts_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		target: &[u8],
		required: tg::authorization::Permission,
	) -> tg::Result<ControlFlow<BTreeSet<crate::usage::Account>, fdb::FdbError>> {
		let tags = crate::fdb::propagate!(
			Self::get_target_tags_with_transaction(txn, subspace, target).await
		);
		let tags =
			crate::fdb::propagate!(Self::try_get_tags_with_transaction(txn, subspace, &tags).await);
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

		Ok(ControlFlow::Break(accounts))
	}

	pub(crate) async fn enqueue_account_process_relationships(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
		touched_at: i64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let accounts = crate::fdb::propagate!(
			Self::get_process_accounts_with_transaction(txn, subspace, process).await
		);
		for account in accounts {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Right(process.clone()),
				&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Propagate {
					account,
					touched_at,
				}),
				crate::fdb::update::Source::Put,
				partition_total,
			);
		}

		Ok(ControlFlow::Break(()))
	}

	async fn get_object_accounts_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		object: &tg::object::Id,
	) -> tg::Result<ControlFlow<Vec<crate::usage::Account>, fdb::FdbError>> {
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
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);
		let accounts = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Usage(crate::fdb::usage::Key::ObjectAccount { account, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(account)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(accounts))
	}

	async fn get_process_accounts_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
	) -> tg::Result<ControlFlow<Vec<crate::usage::Account>, fdb::FdbError>> {
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
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);
		let accounts = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Usage(crate::fdb::usage::Key::ProcessAccount { account, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(account)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(accounts))
	}

	pub(crate) async fn put_account_object(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::storage::put::ObjectArg,
		partition_total: u64,
		usage_partition_total: u64,
		touch_existing: bool,
		version: Option<&fdbt::Versionstamp>,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let entry_key = Key::Usage(crate::fdb::usage::Key::AccountObject {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		let result = txn.get(&entry_key, false).await;
		if let Some(value) = crate::fdb::retry!(result) {
			let mut entry = crate::usage::storage::Entry::deserialize(&value)?;
			if touch_existing && arg.touched_at > entry.touched_at {
				entry.touched_at = arg.touched_at;
				let value = entry.serialize()?;
				txn.set(&entry_key, &value);
				Self::put_account_object_clean_key(txn, subspace, arg, partition_total);
			}
			return Ok(ControlFlow::Break(false));
		}

		let object = crate::fdb::propagate!(
			Self::try_get_object_with_transaction(txn, subspace, &arg.object).await
		);
		let Some(object) = object else {
			return Ok(ControlFlow::Break(false));
		};
		let entry = crate::usage::storage::Entry {
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		txn.set(&entry_key, &value);

		let reverse_key = Key::Usage(crate::fdb::usage::Key::ObjectAccount {
			account: arg.account.clone(),
			object: arg.object.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		txn.set(&reverse_key, &[]);
		Self::put_account_object_clean_key(txn, subspace, arg, partition_total);
		let usage_partition = rand::random_range(0..usage_partition_total);

		Self::add_usage_delta(
			txn,
			subspace,
			&arg.account,
			arg.touched_at,
			crate::usage::DeltaKind::ObjectCount,
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
			crate::usage::DeltaKind::ObjectSize,
			size,
			usage_partition,
		);

		Self::enqueue_update_with_kind_at_version(
			txn,
			subspace,
			&tg::Either::Left(arg.object.clone()),
			&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Propagate {
				account: arg.account.clone(),
				touched_at: arg.touched_at,
			}),
			crate::fdb::update::Source::Put,
			partition_total,
			version,
		);

		Ok(ControlFlow::Break(true))
	}

	pub(crate) async fn put_account_process(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::storage::put::ProcessArg,
		partition_total: u64,
		usage_partition_total: u64,
		touch_existing: bool,
		version: Option<&fdbt::Versionstamp>,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let entry_key = Key::Usage(crate::fdb::usage::Key::AccountProcess {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let entry_key = Self::pack(subspace, &entry_key);
		let result = txn.get(&entry_key, false).await;
		if let Some(value) = crate::fdb::retry!(result) {
			let mut entry = crate::usage::storage::Entry::deserialize(&value)?;
			if touch_existing && arg.touched_at > entry.touched_at {
				entry.touched_at = arg.touched_at;
				let value = entry.serialize()?;
				txn.set(&entry_key, &value);
				Self::put_account_process_clean_key(txn, subspace, arg, partition_total);
			}
			return Ok(ControlFlow::Break(false));
		}

		let process = crate::fdb::propagate!(
			Self::try_get_process_with_transaction(txn, subspace, &arg.process).await
		);
		if process.is_none() {
			return Ok(ControlFlow::Break(false));
		}
		let entry = crate::usage::storage::Entry {
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = entry.serialize()?;
		txn.set(&entry_key, &value);

		let reverse_key = Key::Usage(crate::fdb::usage::Key::ProcessAccount {
			account: arg.account.clone(),
			process: arg.process.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		txn.set(&reverse_key, &[]);
		Self::put_account_process_clean_key(txn, subspace, arg, partition_total);
		let usage_partition = rand::random_range(0..usage_partition_total);

		Self::add_usage_delta(
			txn,
			subspace,
			&arg.account,
			arg.touched_at,
			crate::usage::DeltaKind::ProcessCount,
			1,
			usage_partition,
		);

		Self::enqueue_update_with_kind_at_version(
			txn,
			subspace,
			&tg::Either::Right(arg.process.clone()),
			&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Propagate {
				account: arg.account.clone(),
				touched_at: arg.touched_at,
			}),
			crate::fdb::update::Source::Put,
			partition_total,
			version,
		);

		Ok(ControlFlow::Break(true))
	}

	fn put_account_object_clean_key(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::storage::put::ObjectArg,
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
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::storage::put::ProcessArg,
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
