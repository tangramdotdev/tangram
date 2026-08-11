use {
	crate::fdb::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	num_traits::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn touch_owner_object(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ObjectArg,
		time_to_touch: std::time::Duration,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Storage(crate::fdb::storage::Key::OwnerObject {
			object: arg.object.clone(),
			owner: arg.owner.clone(),
		});
		let key = Self::pack(subspace, &key);
		let Some(value) = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the owner object"))?
		else {
			return Ok(());
		};
		let mut association = crate::storage::Association::deserialize(&value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(association.touched_at) >= time_to_touch {
			association.touched_at = arg.touched_at;
			txn.set(&key, &association.serialize()?);
			Self::put_owner_object_clean_key(txn, subspace, arg, partition_total);
		}

		Ok(())
	}

	pub(crate) async fn touch_owner_process(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ProcessArg,
		time_to_touch: std::time::Duration,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Storage(crate::fdb::storage::Key::OwnerProcess {
			owner: arg.owner.clone(),
			process: arg.process.clone(),
		});
		let key = Self::pack(subspace, &key);
		let Some(value) = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the owner process"))?
		else {
			return Ok(());
		};
		let mut association = crate::storage::Association::deserialize(&value)?;
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if arg.touched_at.saturating_sub(association.touched_at) >= time_to_touch {
			association.touched_at = arg.touched_at;
			txn.set(&key, &association.serialize()?);
			Self::put_owner_process_clean_key(txn, subspace, arg, partition_total);
		}

		Ok(())
	}

	pub(crate) async fn enqueue_owned_object_from_parents(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		object: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let mut owners = BTreeSet::new();
		let parents = Self::get_object_parents_with_transaction(txn, subspace, object).await?;
		for parent in parents {
			owners.extend(Self::get_object_owners_with_transaction(txn, subspace, &parent).await?);
		}
		let processes = Self::get_object_processes_with_transaction(txn, subspace, object).await?;
		for (process, _) in processes {
			owners
				.extend(Self::get_process_owners_with_transaction(txn, subspace, &process).await?);
		}
		for owner in owners {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Left(object.clone()),
				&crate::fdb::update::Kind::Storage(owner),
				crate::fdb::update::Source::Put,
				partition_total,
			);
		}

		Ok(())
	}

	pub(crate) async fn enqueue_owned_process_from_parents(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let mut owners = BTreeSet::new();
		let parents = Self::get_process_parents_with_transaction(txn, subspace, process).await?;
		for parent in parents {
			owners.extend(Self::get_process_owners_with_transaction(txn, subspace, &parent).await?);
		}
		for owner in owners {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Right(process.clone()),
				&crate::fdb::update::Kind::Storage(owner),
				crate::fdb::update::Source::Put,
				partition_total,
			);
		}

		Ok(())
	}

	pub(crate) async fn enqueue_owned_process_relationships(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let owners = Self::get_process_owners_with_transaction(txn, subspace, process).await?;
		for owner in owners {
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Right(process.clone()),
				&crate::fdb::update::Kind::StorageRelationships(owner),
				crate::fdb::update::Source::Put,
				partition_total,
			);
		}

		Ok(())
	}

	async fn get_object_owners_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		object: &tg::object::Id,
	) -> tg::Result<Vec<crate::storage::Owner>> {
		let object_bytes = object.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::fdb::Kind::ObjectOwner.to_i32().unwrap(),
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
			.map_err(|error| tg::error!(!error, "failed to get the object owners"))?;
		let owners = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Storage(crate::fdb::storage::Key::ObjectOwner { owner, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(owner)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(owners)
	}

	async fn get_process_owners_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
	) -> tg::Result<Vec<crate::storage::Owner>> {
		let process_bytes = process.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::fdb::Kind::ProcessOwner.to_i32().unwrap(),
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
			.map_err(|error| tg::error!(!error, "failed to get the process owners"))?;
		let owners = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Storage(crate::fdb::storage::Key::ProcessOwner { owner, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(owner)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(owners)
	}

	pub(crate) async fn put_owner_object(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ObjectArg,
		partition_total: u64,
		storage_partition_total: u64,
		touch_existing: bool,
	) -> tg::Result<bool> {
		let association_key = Key::Storage(crate::fdb::storage::Key::OwnerObject {
			object: arg.object.clone(),
			owner: arg.owner.clone(),
		});
		let association_key = Self::pack(subspace, &association_key);
		if let Some(value) = txn
			.get(&association_key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the owner object"))?
		{
			let mut association = crate::storage::Association::deserialize(&value)?;
			if touch_existing && arg.touched_at > association.touched_at {
				association.touched_at = arg.touched_at;
				let value = association.serialize()?;
				txn.set(&association_key, &value);
				Self::put_owner_object_clean_key(txn, subspace, arg, partition_total);
			}
			return Ok(false);
		}

		let object = Self::try_get_object_with_transaction(txn, subspace, &arg.object).await?;
		let Some(object) = object else {
			if touch_existing {
				return Err(tg::error!(object = %arg.object, "cannot own a missing object"));
			}
			return Ok(false);
		};
		let association = crate::storage::Association {
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = association.serialize()?;
		txn.set(&association_key, &value);

		let reverse_key = Key::Storage(crate::fdb::storage::Key::ObjectOwner {
			object: arg.object.clone(),
			owner: arg.owner.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		txn.set(&reverse_key, &[]);
		Self::put_owner_object_clean_key(txn, subspace, arg, partition_total);

		Self::add_owner_storage(
			txn,
			subspace,
			&arg.owner,
			crate::storage::Kind::ObjectCount,
			1,
			storage_partition_total,
		);
		let size = i64::try_from(object.metadata.node.size)
			.map_err(|_| tg::error!(object = %arg.object, "the object size is too large"))?;
		Self::add_owner_storage(
			txn,
			subspace,
			&arg.owner,
			crate::storage::Kind::ObjectSize,
			size,
			storage_partition_total,
		);

		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Left(arg.object.clone()),
			&crate::fdb::update::Kind::StorageRelationships(arg.owner.clone()),
			crate::fdb::update::Source::Put,
			partition_total,
		);

		Ok(true)
	}

	pub(crate) async fn put_owner_process(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ProcessArg,
		partition_total: u64,
		storage_partition_total: u64,
		touch_existing: bool,
	) -> tg::Result<bool> {
		let association_key = Key::Storage(crate::fdb::storage::Key::OwnerProcess {
			owner: arg.owner.clone(),
			process: arg.process.clone(),
		});
		let association_key = Self::pack(subspace, &association_key);
		if let Some(value) = txn
			.get(&association_key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the owner process"))?
		{
			let mut association = crate::storage::Association::deserialize(&value)?;
			if touch_existing && arg.touched_at > association.touched_at {
				association.touched_at = arg.touched_at;
				let value = association.serialize()?;
				txn.set(&association_key, &value);
				Self::put_owner_process_clean_key(txn, subspace, arg, partition_total);
			}
			return Ok(false);
		}

		let process = Self::try_get_process_with_transaction(txn, subspace, &arg.process).await?;
		if process.is_none() {
			if touch_existing {
				return Err(tg::error!(process = %arg.process, "cannot own a missing process"));
			}
			return Ok(false);
		}
		let association = crate::storage::Association {
			reference_count: 0,
			touched_at: arg.touched_at,
		};
		let value = association.serialize()?;
		txn.set(&association_key, &value);

		let reverse_key = Key::Storage(crate::fdb::storage::Key::ProcessOwner {
			owner: arg.owner.clone(),
			process: arg.process.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		txn.set(&reverse_key, &[]);
		Self::put_owner_process_clean_key(txn, subspace, arg, partition_total);

		Self::add_owner_storage(
			txn,
			subspace,
			&arg.owner,
			crate::storage::Kind::ProcessCount,
			1,
			storage_partition_total,
		);

		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Right(arg.process.clone()),
			&crate::fdb::update::Kind::StorageRelationships(arg.owner.clone()),
			crate::fdb::update::Source::Put,
			partition_total,
		);

		Ok(true)
	}

	pub(crate) fn add_owner_storage(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		owner: &crate::storage::Owner,
		kind: crate::storage::Kind,
		delta: i64,
		storage_partition_total: u64,
	) {
		let partition = rand::random_range(0..storage_partition_total);
		let key = Key::Storage(crate::fdb::storage::Key::OwnerStorage {
			kind,
			owner: owner.clone(),
			partition,
		});
		let key = Self::pack(subspace, &key);
		txn.atomic_op(&key, &delta.to_le_bytes(), fdb::options::MutationType::Add);
	}

	fn put_owner_object_clean_key(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ObjectArg,
		partition_total: u64,
	) {
		let partition = Self::partition_for_id(arg.object.to_bytes().as_ref(), partition_total);
		let key = Key::Storage(crate::fdb::storage::Key::OwnerObjectClean {
			object: arg.object.clone(),
			owner: arg.owner.clone(),
			partition,
			touched_at: arg.touched_at,
		});
		let key = Self::pack(subspace, &key);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &[]);
	}

	fn put_owner_process_clean_key(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::storage::put::ProcessArg,
		partition_total: u64,
	) {
		let partition = Self::partition_for_id(arg.process.to_bytes().as_ref(), partition_total);
		let key = Key::Storage(crate::fdb::storage::Key::OwnerProcessClean {
			owner: arg.owner.clone(),
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
