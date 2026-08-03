use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn touch_owner_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ObjectArg,
	) -> tg::Result<()> {
		let key = Key::Storage(crate::lmdb::storage::Key::OwnerObject {
			object: arg.object.clone(),
			owner: arg.owner.clone(),
		});
		let key = Self::pack(subspace, &key);
		let Some(value) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the owner object"))?
		else {
			return Ok(());
		};
		let mut association = crate::storage::Association::deserialize(value)?;
		if arg.touched_at > association.touched_at {
			association.touched_at = arg.touched_at;
			db.put(transaction, &key, &association.serialize()?)
				.map_err(|error| tg::error!(!error, "failed to touch the owner object"))?;
			Self::put_owner_object_clean_key(db, subspace, transaction, arg)?;
		}

		Ok(())
	}

	pub(crate) fn touch_owner_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ProcessArg,
	) -> tg::Result<()> {
		let key = Key::Storage(crate::lmdb::storage::Key::OwnerProcess {
			owner: arg.owner.clone(),
			process: arg.process.clone(),
		});
		let key = Self::pack(subspace, &key);
		let Some(value) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the owner process"))?
		else {
			return Ok(());
		};
		let mut association = crate::storage::Association::deserialize(value)?;
		if arg.touched_at > association.touched_at {
			association.touched_at = arg.touched_at;
			db.put(transaction, &key, &association.serialize()?)
				.map_err(|error| tg::error!(!error, "failed to touch the owner process"))?;
			Self::put_owner_process_clean_key(db, subspace, transaction, arg)?;
		}

		Ok(())
	}

	pub(crate) fn enqueue_owned_object_from_parents(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		object: &tg::object::Id,
	) -> tg::Result<()> {
		let mut owners = BTreeSet::new();
		let parents = Self::get_object_parents_with_transaction(db, subspace, transaction, object)?;
		for parent in parents {
			owners.extend(Self::get_object_owners_with_transaction(
				db,
				subspace,
				transaction,
				&parent,
			)?);
		}
		let processes =
			Self::get_object_processes_with_transaction(db, subspace, transaction, object)?;
		for (process, _) in processes {
			owners.extend(Self::get_process_owners_with_transaction(
				db,
				subspace,
				transaction,
				&process,
			)?);
		}
		for owner in owners {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Left(object.clone()),
				crate::lmdb::update::Kind::Storage(owner),
				crate::lmdb::update::Source::Put,
				None,
			)?;
		}

		Ok(())
	}

	pub(crate) fn enqueue_owned_process_from_parents(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		let mut owners = BTreeSet::new();
		let parents =
			Self::get_process_parents_with_transaction(db, subspace, transaction, process)?;
		for parent in parents {
			owners.extend(Self::get_process_owners_with_transaction(
				db,
				subspace,
				transaction,
				&parent,
			)?);
		}
		for owner in owners {
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				tg::Either::Right(process.clone()),
				crate::lmdb::update::Kind::Storage(owner),
				crate::lmdb::update::Source::Put,
				None,
			)?;
		}

		Ok(())
	}

	pub(crate) fn enqueue_owned_process_relationships(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		let owners = Self::get_process_owners_with_transaction(db, subspace, transaction, process)?;
		if owners.is_empty() {
			return Ok(());
		}
		let children =
			Self::get_process_children_with_transaction(db, subspace, transaction, process)?;
		let objects =
			Self::get_process_objects_with_transaction(db, subspace, transaction, process)?;
		for owner in owners {
			let kind = crate::lmdb::update::Kind::Storage(owner);
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

	fn get_object_owners_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		object: &tg::object::Id,
	) -> tg::Result<Vec<crate::storage::Owner>> {
		let object_bytes = object.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::lmdb::Kind::ObjectOwner.to_i32().unwrap(),
				object_bytes.as_ref(),
			),
		);
		let owners = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the object owners"))?
			.map(|entry| {
				let (key, _) =
					entry.map_err(|error| tg::error!(!error, "failed to read an object owner"))?;
				let key = Self::unpack(subspace, key)?;
				let Key::Storage(crate::lmdb::storage::Key::ObjectOwner { owner, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(owner)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(owners)
	}

	fn get_process_owners_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		process: &tg::process::Id,
	) -> tg::Result<Vec<crate::storage::Owner>> {
		let process_bytes = process.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				crate::lmdb::Kind::ProcessOwner.to_i32().unwrap(),
				process_bytes.as_ref(),
			),
		);
		let owners = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the process owners"))?
			.map(|entry| {
				let (key, _) =
					entry.map_err(|error| tg::error!(!error, "failed to read a process owner"))?;
				let key = Self::unpack(subspace, key)?;
				let Key::Storage(crate::lmdb::storage::Key::ProcessOwner { owner, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(owner)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(owners)
	}

	pub(crate) fn put_owner_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ObjectArg,
		storage_partition_total: u64,
		touch_existing: bool,
	) -> tg::Result<bool> {
		let association_key = Key::Storage(crate::lmdb::storage::Key::OwnerObject {
			object: arg.object.clone(),
			owner: arg.owner.clone(),
		});
		let association_key = Self::pack(subspace, &association_key);
		if let Some(value) = db
			.get(transaction, &association_key)
			.map_err(|error| tg::error!(!error, "failed to get the owner object"))?
		{
			let mut association = crate::storage::Association::deserialize(value)?;
			if touch_existing && arg.touched_at > association.touched_at {
				association.touched_at = arg.touched_at;
				let value = association.serialize()?;
				db.put(transaction, &association_key, &value)
					.map_err(|error| tg::error!(!error, "failed to touch the owner object"))?;
				Self::put_owner_object_clean_key(db, subspace, transaction, arg)?;
			}
			return Ok(false);
		}

		let object = Self::try_get_object_with_transaction(db, subspace, transaction, &arg.object)?;
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
		db.put(transaction, &association_key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the owner object"))?;

		let reverse_key = Key::Storage(crate::lmdb::storage::Key::ObjectOwner {
			object: arg.object.clone(),
			owner: arg.owner.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		db.put(transaction, &reverse_key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the object owner"))?;
		Self::put_owner_object_clean_key(db, subspace, transaction, arg)?;

		Self::add_owner_storage(
			db,
			subspace,
			transaction,
			&arg.owner,
			crate::storage::Kind::ObjectCount,
			1,
			storage_partition_total,
		)?;
		let size = i64::try_from(object.metadata.node.size)
			.map_err(|_| tg::error!(object = %arg.object, "the object size is too large"))?;
		Self::add_owner_storage(
			db,
			subspace,
			transaction,
			&arg.owner,
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
				crate::lmdb::update::Kind::Storage(arg.owner.clone()),
				crate::lmdb::update::Source::Put,
				None,
			)?;
		}

		Ok(true)
	}

	pub(crate) fn put_owner_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ProcessArg,
		storage_partition_total: u64,
		touch_existing: bool,
	) -> tg::Result<bool> {
		let association_key = Key::Storage(crate::lmdb::storage::Key::OwnerProcess {
			owner: arg.owner.clone(),
			process: arg.process.clone(),
		});
		let association_key = Self::pack(subspace, &association_key);
		if let Some(value) = db
			.get(transaction, &association_key)
			.map_err(|error| tg::error!(!error, "failed to get the owner process"))?
		{
			let mut association = crate::storage::Association::deserialize(value)?;
			if touch_existing && arg.touched_at > association.touched_at {
				association.touched_at = arg.touched_at;
				let value = association.serialize()?;
				db.put(transaction, &association_key, &value)
					.map_err(|error| tg::error!(!error, "failed to touch the owner process"))?;
				Self::put_owner_process_clean_key(db, subspace, transaction, arg)?;
			}
			return Ok(false);
		}

		let process =
			Self::try_get_process_with_transaction(db, subspace, transaction, &arg.process)?;
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
		db.put(transaction, &association_key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the owner process"))?;

		let reverse_key = Key::Storage(crate::lmdb::storage::Key::ProcessOwner {
			owner: arg.owner.clone(),
			process: arg.process.clone(),
		});
		let reverse_key = Self::pack(subspace, &reverse_key);
		db.put(transaction, &reverse_key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the process owner"))?;
		Self::put_owner_process_clean_key(db, subspace, transaction, arg)?;

		Self::add_owner_storage(
			db,
			subspace,
			transaction,
			&arg.owner,
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
				crate::lmdb::update::Kind::Storage(arg.owner.clone()),
				crate::lmdb::update::Source::Put,
				None,
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
				crate::lmdb::update::Kind::Storage(arg.owner.clone()),
				crate::lmdb::update::Source::Put,
				None,
			)?;
		}

		Ok(true)
	}

	pub(crate) fn add_owner_storage(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		owner: &crate::storage::Owner,
		kind: crate::storage::Kind,
		delta: i64,
		storage_partition_total: u64,
	) -> tg::Result<()> {
		let partition = rand::random_range(0..storage_partition_total);
		let key = Key::Storage(crate::lmdb::storage::Key::OwnerStorage {
			kind,
			owner: owner.clone(),
			partition,
		});
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the owner storage value"))?
			.map(|value| {
				i64::from_le_bytes(
					value
						.try_into()
						.map_err(|_| tg::error!("invalid owner storage value"))?,
				)
				.checked_add(delta)
				.ok_or_else(|| tg::error!("the owner storage value overflowed"))
			})
			.transpose()?
			.unwrap_or(delta);
		db.put(transaction, &key, &value.to_le_bytes())
			.map_err(|error| tg::error!(!error, "failed to put the owner storage value"))?;

		Ok(())
	}

	fn put_owner_object_clean_key(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ObjectArg,
	) -> tg::Result<()> {
		let key = Key::Storage(crate::lmdb::storage::Key::OwnerObjectClean {
			object: arg.object.clone(),
			owner: arg.owner.clone(),
			touched_at: arg.touched_at,
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the owner object clean key"))?;

		Ok(())
	}

	fn put_owner_process_clean_key(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::storage::put::ProcessArg,
	) -> tg::Result<()> {
		let key = Key::Storage(crate::lmdb::storage::Key::OwnerProcessClean {
			owner: arg.owner.clone(),
			process: arg.process.clone(),
			touched_at: arg.touched_at,
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the owner process clean key"))?;

		Ok(())
	}
}
