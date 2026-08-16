use {
	crate::fdb::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

enum Candidate {
	Object {
		account: crate::usage::Account,
		object: tg::object::Id,
		partition: u64,
		touched_at: i64,
	},
	Process {
		account: crate::usage::Account,
		partition: u64,
		process: tg::process::Id,
		touched_at: i64,
	},
}

impl Index {
	pub(crate) async fn schedule_object_accounts_for_cleaning(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		object: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Left(object.clone()),
			&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::CleanAll),
			crate::fdb::update::Source::Put,
			partition_total,
		);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn schedule_process_accounts_for_cleaning(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Right(process.clone()),
			&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::CleanAll),
			crate::fdb::update::Source::Put,
			partition_total,
		);

		Ok(ControlFlow::Break(()))
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::fdb) async fn clean_account_object_entry(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		object: &tg::object::Id,
		now: i64,
		partition: u64,
		touched_at: i64,
		partition_total: u64,
		usage_partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let candidate = Candidate::Object {
			account: account.clone(),
			object: object.clone(),
			partition,
			touched_at,
		};
		Self::clean_account_entry(
			txn,
			subspace,
			&candidate,
			now,
			partition_total,
			usage_partition_total,
		)
		.await
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::fdb) async fn clean_account_process_entry(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		process: &tg::process::Id,
		now: i64,
		partition: u64,
		touched_at: i64,
		partition_total: u64,
		usage_partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let candidate = Candidate::Process {
			account: account.clone(),
			partition,
			process: process.clone(),
			touched_at,
		};
		Self::clean_account_entry(
			txn,
			subspace,
			&candidate,
			now,
			partition_total,
			usage_partition_total,
		)
		.await
	}

	async fn clean_account_entry(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		candidate: &Candidate,
		now: i64,
		partition_total: u64,
		usage_partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let (entry_key, clean_key, touched_at) = match candidate {
			Candidate::Object {
				account,
				object,
				partition,
				touched_at,
			} => (
				Key::Usage(crate::fdb::usage::Key::AccountObject {
					account: account.clone(),
					object: object.clone(),
				}),
				Key::Clean(crate::fdb::clean::Key::AccountObject {
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
				Key::Usage(crate::fdb::usage::Key::AccountProcess {
					account: account.clone(),
					process: process.clone(),
				}),
				Key::Clean(crate::fdb::clean::Key::AccountProcess {
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
		let Some(value) = crate::fdb::retry!(result) else {
			txn.clear(&clean_key);
			return Ok(ControlFlow::Break(()));
		};
		let mut entry = crate::usage::storage::Entry::deserialize(&value)?;
		if entry.touched_at != touched_at {
			txn.clear(&clean_key);
			return Ok(ControlFlow::Break(()));
		}
		let reference_count = match candidate {
			Candidate::Object {
				account, object, ..
			} => {
				crate::fdb::propagate!(
					Self::compute_account_object_reference_count(txn, subspace, account, object)
						.await
				)
			},
			Candidate::Process {
				account, process, ..
			} => {
				crate::fdb::propagate!(
					Self::compute_account_process_reference_count(txn, subspace, account, process,)
						.await
				)
			},
		};
		if reference_count > 0 {
			entry.reference_count = reference_count;
			txn.set(&entry_key, &entry.serialize()?);
			txn.clear(&clean_key);
			return Ok(ControlFlow::Break(()));
		}
		match candidate {
			Candidate::Object {
				account, object, ..
			} => {
				crate::fdb::propagate!(
					Self::delete_account_object(
						txn,
						subspace,
						account,
						object,
						now,
						partition_total,
						usage_partition_total,
					)
					.await
				);
			},
			Candidate::Process {
				account, process, ..
			} => {
				crate::fdb::propagate!(
					Self::delete_account_process(
						txn,
						subspace,
						account,
						process,
						now,
						partition_total,
						usage_partition_total,
					)
					.await
				);
			},
		}
		txn.clear(&clean_key);

		Ok(ControlFlow::Break(()))
	}

	async fn compute_account_object_reference_count(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		object: &tg::object::Id,
	) -> tg::Result<ControlFlow<u64, fdb::FdbError>> {
		let (parents, processes) = futures::future::try_join(
			Self::get_object_parents_with_transaction(txn, subspace, object),
			Self::get_object_processes_with_transaction(txn, subspace, object),
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
		let keys = parents
			.into_iter()
			.map(|object| {
				Key::Usage(crate::fdb::usage::Key::AccountObject {
					account: account.clone(),
					object,
				})
			})
			.chain(processes.into_iter().map(|(process, _)| {
				Key::Usage(crate::fdb::usage::Key::AccountProcess {
					account: account.clone(),
					process,
				})
			}))
			.map(|key| Self::pack(subspace, &key))
			.collect::<Vec<_>>();
		let entries_future = async {
			let result = futures::future::try_join_all(
				keys.iter()
					.map(|key| async move { txn.get(key, false).await }),
			)
			.await;
			let entries = crate::fdb::retry!(result);

			Ok::<_, tg::Error>(ControlFlow::Break(entries))
		};
		let object_bytes = object.to_bytes();
		let tags_future = Self::count_account_tags(txn, subspace, account, object_bytes.as_ref());
		let (entries, tag_count) = futures::future::try_join(entries_future, tags_future).await?;
		let entries = match entries {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let tag_count = match tag_count {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let entry_count = entries.iter().filter(|value| value.is_some()).count();
		let count = u64::try_from(entry_count).unwrap() + tag_count;

		Ok(ControlFlow::Break(count))
	}

	async fn compute_account_process_reference_count(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		process: &tg::process::Id,
	) -> tg::Result<ControlFlow<u64, fdb::FdbError>> {
		let parents = crate::fdb::propagate!(
			Self::get_process_parents_with_transaction(txn, subspace, process).await
		);
		let keys = parents
			.into_iter()
			.map(|process| {
				let key = Key::Usage(crate::fdb::usage::Key::AccountProcess {
					account: account.clone(),
					process,
				});
				Self::pack(subspace, &key)
			})
			.collect::<Vec<_>>();
		let entries_future = async {
			let result = futures::future::try_join_all(
				keys.iter()
					.map(|key| async move { txn.get(key, false).await }),
			)
			.await;
			let entries = crate::fdb::retry!(result);

			Ok::<_, tg::Error>(ControlFlow::Break(entries))
		};
		let process_bytes = process.to_bytes();
		let tags_future = Self::count_account_tags(txn, subspace, account, process_bytes.as_ref());
		let (entries, tag_count) = futures::future::try_join(entries_future, tags_future).await?;
		let entries = match entries {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let tag_count = match tag_count {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let entry_count = entries.iter().filter(|value| value.is_some()).count();
		let count = u64::try_from(entry_count).unwrap() + tag_count;

		Ok(ControlFlow::Break(count))
	}

	async fn count_account_tags(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		target: &[u8],
	) -> tg::Result<ControlFlow<u64, fdb::FdbError>> {
		let tags = crate::fdb::propagate!(
			Self::get_target_tags_with_transaction(txn, subspace, target).await
		);
		let tags = {
			let result = futures::future::try_join_all(
				tags.iter()
					.map(|tag| Self::try_get_tag_with_transaction(txn, subspace, tag)),
			)
			.await;
			let results = result?;
			let mut values = Vec::with_capacity(results.len());
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};
		let count = tags
			.iter()
			.filter(|tag| tag.as_ref().and_then(|tag| tag.account.as_ref()) == Some(account))
			.count();
		let count = u64::try_from(count).unwrap();

		Ok(ControlFlow::Break(count))
	}

	#[allow(clippy::too_many_arguments)]
	async fn delete_account_object(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		object: &tg::object::Id,
		now: i64,
		partition_total: u64,
		usage_partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Key::Usage(crate::fdb::usage::Key::AccountObject {
			account: account.clone(),
			object: object.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		let key = Key::Usage(crate::fdb::usage::Key::ObjectAccount {
			account: account.clone(),
			object: object.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		let usage_partition = rand::random_range(0..usage_partition_total);
		Self::add_usage_delta(
			txn,
			subspace,
			account,
			now,
			crate::usage::DeltaKind::ObjectCount,
			-1,
			usage_partition,
		);
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Left(object.clone()),
			&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Clean(
				account.clone(),
			)),
			crate::fdb::update::Source::Put,
			partition_total,
		);
		let value = crate::fdb::propagate!(
			Self::try_get_object_with_transaction(txn, subspace, object).await
		)
		.ok_or_else(|| tg::error!(%object, "an object with a storage entry is missing"))?;
		let size = i64::try_from(value.metadata.node.size)
			.map_err(|_| tg::error!("the object size is too large"))?;
		Self::add_usage_delta(
			txn,
			subspace,
			account,
			now,
			crate::usage::DeltaKind::ObjectSize,
			-size,
			usage_partition,
		);
		let partition = Self::partition_for_id(object.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::fdb::clean::Key::Object {
			id: object.clone(),
			partition,
			touched_at: value.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(ControlFlow::Break(()))
	}

	#[allow(clippy::too_many_arguments)]
	async fn delete_account_process(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		process: &tg::process::Id,
		now: i64,
		partition_total: u64,
		usage_partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Key::Usage(crate::fdb::usage::Key::AccountProcess {
			account: account.clone(),
			process: process.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		let key = Key::Usage(crate::fdb::usage::Key::ProcessAccount {
			account: account.clone(),
			process: process.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		let usage_partition = rand::random_range(0..usage_partition_total);
		Self::add_usage_delta(
			txn,
			subspace,
			account,
			now,
			crate::usage::DeltaKind::ProcessCount,
			-1,
			usage_partition,
		);
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Right(process.clone()),
			&crate::fdb::update::Kind::Storage(crate::fdb::update::StorageKind::Clean(
				account.clone(),
			)),
			crate::fdb::update::Source::Put,
			partition_total,
		);
		let value = crate::fdb::propagate!(
			Self::try_get_process_with_transaction(txn, subspace, process).await
		)
		.ok_or_else(|| tg::error!(%process, "a process with a storage entry is missing"))?;
		let partition = Self::partition_for_id(process.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::fdb::clean::Key::Process {
			id: process.clone(),
			partition,
			touched_at: value.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(ControlFlow::Break(()))
	}

	pub(in crate::fdb) async fn schedule_account_object_for_cleaning(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		object: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let entry_key = Key::Usage(crate::fdb::usage::Key::AccountObject {
			account: account.clone(),
			object: object.clone(),
		});
		let result = txn.get(&Self::pack(subspace, &entry_key), false).await;
		let Some(value) = crate::fdb::retry!(result) else {
			return Ok(ControlFlow::Break(()));
		};
		let entry = crate::usage::storage::Entry::deserialize(&value)?;
		let partition = Self::partition_for_id(object.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::fdb::clean::Key::AccountObject {
			account: account.clone(),
			object: object.clone(),
			partition,
			touched_at: entry.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(ControlFlow::Break(()))
	}

	pub(in crate::fdb) async fn schedule_account_process_for_cleaning(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let entry_key = Key::Usage(crate::fdb::usage::Key::AccountProcess {
			account: account.clone(),
			process: process.clone(),
		});
		let result = txn.get(&Self::pack(subspace, &entry_key), false).await;
		let Some(value) = crate::fdb::retry!(result) else {
			return Ok(ControlFlow::Break(()));
		};
		let entry = crate::usage::storage::Entry::deserialize(&value)?;
		let partition = Self::partition_for_id(process.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::fdb::clean::Key::AccountProcess {
			account: account.clone(),
			partition,
			process: process.clone(),
			touched_at: entry.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(ControlFlow::Break(()))
	}
}
