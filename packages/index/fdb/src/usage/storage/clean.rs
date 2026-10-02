use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

enum Candidate {
	Object {
		account: tangram_index::usage::Account,
		object: tg::object::Id,
		partition: u64,
		touched_at: i64,
	},
	Process {
		account: tangram_index::usage::Account,
		partition: u64,
		process: tg::process::Id,
		touched_at: i64,
	},
}

impl Index {
	pub(crate) async fn schedule_object_accounts_for_cleaning(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		object: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Left(object.clone()),
			&crate::update::Kind::Usage(crate::update::UsageKind::CleanAll),
			crate::update::Source::Put,
			partition_total,
		);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn schedule_process_accounts_for_cleaning(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Right(process.clone()),
			&crate::update::Kind::Usage(crate::update::UsageKind::CleanAll),
			crate::update::Source::Put,
			partition_total,
		);

		Ok(ControlFlow::Break(()))
	}

	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn clean_account_object_entry(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		object: &tg::object::Id,
		now: i64,
		partition: u64,
		touched_at: i64,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let candidate = Candidate::Object {
			account: account.clone(),
			object: object.clone(),
			partition,
			touched_at,
		};
		Self::clean_account_entry(txn, subspace, &candidate, now, partition_totals).await
	}

	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn clean_account_process_entry(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		process: &tg::process::Id,
		now: i64,
		partition: u64,
		touched_at: i64,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let candidate = Candidate::Process {
			account: account.clone(),
			partition,
			process: process.clone(),
			touched_at,
		};
		Self::clean_account_entry(txn, subspace, &candidate, now, partition_totals).await
	}

	async fn clean_account_entry(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		candidate: &Candidate,
		now: i64,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let (entry_key, clean_key, touched_at) = match candidate {
			Candidate::Object {
				account,
				object,
				partition,
				touched_at,
			} => (
				Key::Usage(crate::usage::Key::AccountObject {
					account: account.clone(),
					object: object.clone(),
				}),
				Key::Clean(crate::clean::Key::AccountObject {
					account: account.clone(),
					object: object.clone(),
					partition: *partition,
					touched_at: *touched_at,
				}),
				*touched_at,
			),
			Candidate::Process {
				account,
				partition,
				process,
				touched_at,
			} => (
				Key::Usage(crate::usage::Key::AccountProcess {
					account: account.clone(),
					process: process.clone(),
				}),
				Key::Clean(crate::clean::Key::AccountProcess {
					account: account.clone(),
					partition: *partition,
					process: process.clone(),
					touched_at: *touched_at,
				}),
				*touched_at,
			),
		};
		let entry_key = Self::pack(subspace, &entry_key);
		let clean_key = Self::pack(subspace, &clean_key);
		let result = txn.get(&entry_key, false).await;
		let Some(value) = crate::retry!(result) else {
			txn.clear(&clean_key);
			return Ok(ControlFlow::Break(()));
		};
		let mut entry = tangram_index::usage::storage::Entry::deserialize(&value)?;
		if entry.touched_at != touched_at {
			txn.clear(&clean_key);
			return Ok(ControlFlow::Break(()));
		}
		let (account, id) = match candidate {
			Candidate::Object {
				account, object, ..
			} => (account, tg::Either::Left(object.clone())),
			Candidate::Process {
				account, process, ..
			} => (account, tg::Either::Right(process.clone())),
		};
		let (permissions, reference_count) = crate::propagate!(
			Self::compute_account_storage_permissions_with_transaction(txn, subspace, account, &id)
				.await
		);
		if !permissions.is_empty() {
			if permissions != entry.permissions {
				let stored = entry.stores_node();
				entry.permissions = permissions;
				if stored != entry.stores_node() {
					let delta = i64::from(entry.stores_node()) - i64::from(stored);
					Self::add_usage_delta(
						txn,
						subspace,
						account,
						now,
						tangram_index::usage::DeltaKind::ProcessCount,
						delta,
						rand::random_range(0..partition_totals.usage),
					);
				}
				Self::enqueue_update_with_kind(
					txn,
					subspace,
					&id,
					&crate::update::Kind::Usage(crate::update::UsageKind::Clean(account.clone())),
					crate::update::Source::Put,
					partition_totals.usage_update,
				);
			}
			entry.reference_count = reference_count;
			txn.set(&entry_key, &entry.serialize()?);
			txn.clear(&clean_key);
			return Ok(ControlFlow::Break(()));
		}

		match candidate {
			Candidate::Object {
				account, object, ..
			} => {
				crate::propagate!(
					Self::delete_account_object(
						txn,
						subspace,
						account,
						object,
						now,
						partition_totals,
					)
					.await
				);
			},
			Candidate::Process {
				account, process, ..
			} => {
				crate::propagate!(
					Self::delete_account_process(
						txn,
						subspace,
						account,
						process,
						now,
						partition_totals,
					)
					.await
				);
			},
		}
		txn.clear(&clean_key);

		Ok(ControlFlow::Break(()))
	}

	async fn delete_account_object(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		object: &tg::object::Id,
		now: i64,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let usage_partition_total = partition_totals.usage;
		let key = Key::Usage(crate::usage::Key::AccountObject {
			account: account.clone(),
			object: object.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		let key = Key::Usage(crate::usage::Key::ObjectAccount {
			account: account.clone(),
			object: object.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		Self::clear_usage_update_versions(
			txn,
			subspace,
			&tg::Either::Left(object.clone()),
			account,
		);
		let usage_partition = rand::random_range(0..usage_partition_total);
		Self::add_usage_delta(
			txn,
			subspace,
			account,
			now,
			tangram_index::usage::DeltaKind::ObjectCount,
			-1,
			usage_partition,
		);
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Left(object.clone()),
			&crate::update::Kind::Usage(crate::update::UsageKind::Clean(account.clone())),
			crate::update::Source::Put,
			partition_totals.usage_update,
		);
		let value =
			crate::propagate!(Self::try_get_object_with_transaction(txn, subspace, object).await)
				.ok_or_else(|| tg::error!(%object, "an object with a storage entry is missing"))?;
		let size = i64::try_from(value.metadata.node.size)
			.map_err(|_| tg::error!("the object size is too large"))?;
		Self::add_usage_delta(
			txn,
			subspace,
			account,
			now,
			tangram_index::usage::DeltaKind::ObjectSize,
			-size,
			usage_partition,
		);
		let partition =
			Self::partition_for_id(object.to_bytes().as_ref(), partition_totals.cleaning);
		let key = Key::Clean(crate::clean::Key::Object {
			id: object.clone(),
			partition,
			touched_at: value.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(ControlFlow::Break(()))
	}

	async fn delete_account_process(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		process: &tg::process::Id,
		now: i64,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let stored = crate::propagate!(
			Self::get_account_storage_entry_with_transaction(
				txn,
				subspace,
				account,
				&tg::Either::Right(process.clone())
			)
			.await
		)
		.is_some_and(|entry| entry.stores_node());
		let usage_partition_total = partition_totals.usage;
		let key = Key::Usage(crate::usage::Key::AccountProcess {
			account: account.clone(),
			process: process.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		let key = Key::Usage(crate::usage::Key::ProcessAccount {
			account: account.clone(),
			process: process.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		Self::clear_usage_update_versions(
			txn,
			subspace,
			&tg::Either::Right(process.clone()),
			account,
		);
		let usage_partition = rand::random_range(0..usage_partition_total);
		if stored {
			Self::add_usage_delta(
				txn,
				subspace,
				account,
				now,
				tangram_index::usage::DeltaKind::ProcessCount,
				-1,
				usage_partition,
			);
		}

		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Right(process.clone()),
			&crate::update::Kind::Usage(crate::update::UsageKind::Clean(account.clone())),
			crate::update::Source::Put,
			partition_totals.usage_update,
		);
		let value =
			crate::propagate!(Self::try_get_process_with_transaction(txn, subspace, process).await)
				.ok_or_else(|| tg::error!(%process, "a process with a storage entry is missing"))?;
		let partition =
			Self::partition_for_id(process.to_bytes().as_ref(), partition_totals.cleaning);
		let key = Key::Clean(crate::clean::Key::Process {
			id: process.clone(),
			partition,
			touched_at: value.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn schedule_account_object_for_cleaning(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		object: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let entry_key = Key::Usage(crate::usage::Key::AccountObject {
			account: account.clone(),
			object: object.clone(),
		});
		let result = txn.get(&Self::pack(subspace, &entry_key), false).await;
		let Some(value) = crate::retry!(result) else {
			return Ok(ControlFlow::Break(()));
		};
		let mut entry = tangram_index::usage::storage::Entry::deserialize(&value)?;
		// Invalidate the cached references because the retaining scopes may have changed.
		if entry.reference_count != 0 {
			entry.reference_count = 0;
			txn.set(&Self::pack(subspace, &entry_key), &entry.serialize()?);
		}

		let partition = Self::partition_for_id(object.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::clean::Key::AccountObject {
			account: account.clone(),
			object: object.clone(),
			partition,
			touched_at: entry.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn schedule_account_process_for_cleaning(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let entry_key = Key::Usage(crate::usage::Key::AccountProcess {
			account: account.clone(),
			process: process.clone(),
		});
		let result = txn.get(&Self::pack(subspace, &entry_key), false).await;
		let Some(value) = crate::retry!(result) else {
			return Ok(ControlFlow::Break(()));
		};
		let mut entry = tangram_index::usage::storage::Entry::deserialize(&value)?;
		// Invalidate the cached references because the retaining scopes may have changed.
		if entry.reference_count != 0 {
			entry.reference_count = 0;
			txn.set(&Self::pack(subspace, &entry_key), &entry.serialize()?);
		}

		let partition = Self::partition_for_id(process.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::clean::Key::AccountProcess {
			account: account.clone(),
			partition,
			process: process.clone(),
			touched_at: entry.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(ControlFlow::Break(()))
	}
}
