use {
	crate::lmdb::{Db, Index, ItemKind, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

enum Candidate {
	Object {
		object: tg::object::Id,
		owner: crate::storage::Owner,
		touched_at: i64,
	},
	Process {
		owner: crate::storage::Owner,
		process: tg::process::Id,
		touched_at: i64,
	},
}

impl Index {
	pub(crate) fn schedule_object_owners_for_cleaning(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		object: &tg::object::Id,
	) -> tg::Result<()> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::ObjectOwner.to_i32().unwrap(),
				object.to_bytes().as_ref(),
			),
		);
		let owners = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the object owners"))?
			.map(|entry| {
				let (key, _) =
					entry.map_err(|error| tg::error!(!error, "failed to read an object owner"))?;
				let Key::Storage(crate::lmdb::storage::Key::ObjectOwner { owner, .. }) =
					Self::unpack(subspace, key)?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(owner)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for owner in owners {
			Self::schedule_owner_object_clean(db, subspace, transaction, &owner, object)?;
		}

		Ok(())
	}

	pub(crate) fn schedule_process_owners_for_cleaning(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::ProcessOwner.to_i32().unwrap(),
				process.to_bytes().as_ref(),
			),
		);
		let owners = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the process owners"))?
			.map(|entry| {
				let (key, _) =
					entry.map_err(|error| tg::error!(!error, "failed to read a process owner"))?;
				let Key::Storage(crate::lmdb::storage::Key::ProcessOwner { owner, .. }) =
					Self::unpack(subspace, key)?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(owner)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for owner in owners {
			Self::schedule_owner_process_clean(db, subspace, transaction, &owner, process)?;
		}

		Ok(())
	}

	pub(crate) fn clean_storage_associations(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		batch_size: usize,
		max_object_touched_at: i64,
		max_process_touched_at: i64,
		storage_partition_total: u64,
	) -> tg::Result<usize> {
		let mut candidates = Vec::new();
		for (kind, max_touched_at) in [
			(Kind::OwnerObjectClean, max_object_touched_at),
			(Kind::OwnerProcessClean, max_process_touched_at),
		] {
			let prefix = Self::pack(subspace, &(kind.to_i32().unwrap(),));
			let iter = db
				.prefix_iter(transaction, &prefix)
				.map_err(|error| tg::error!(!error, "failed to iterate storage clean keys"))?;
			for entry in iter {
				if candidates.len() >= batch_size {
					break;
				}
				let (key, _) = entry
					.map_err(|error| tg::error!(!error, "failed to read a storage clean key"))?;
				let key = Self::unpack(subspace, key)?;
				let candidate = match key {
					Key::Storage(crate::lmdb::storage::Key::OwnerObjectClean {
						object,
						owner,
						touched_at,
					}) => {
						if touched_at > max_touched_at {
							break;
						}
						Candidate::Object {
							object,
							owner,
							touched_at,
						}
					},
					Key::Storage(crate::lmdb::storage::Key::OwnerProcessClean {
						owner,
						process,
						touched_at,
					}) => {
						if touched_at > max_touched_at {
							break;
						}
						Candidate::Process {
							owner,
							process,
							touched_at,
						}
					},
					_ => return Err(tg::error!("unexpected key type")),
				};
				candidates.push(candidate);
			}
		}

		for candidate in &candidates {
			Self::clean_storage_association(
				db,
				subspace,
				transaction,
				candidate,
				storage_partition_total,
			)?;
		}

		Ok(candidates.len())
	}

	fn clean_storage_association(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		candidate: &Candidate,
		storage_partition_total: u64,
	) -> tg::Result<()> {
		let clean_key = match candidate {
			Candidate::Object {
				object,
				owner,
				touched_at,
			} => Key::Storage(crate::lmdb::storage::Key::OwnerObjectClean {
				object: object.clone(),
				owner: owner.clone(),
				touched_at: *touched_at,
			}),
			Candidate::Process {
				owner,
				process,
				touched_at,
			} => Key::Storage(crate::lmdb::storage::Key::OwnerProcessClean {
				owner: owner.clone(),
				process: process.clone(),
				touched_at: *touched_at,
			}),
		};
		let clean_key = Self::pack(subspace, &clean_key);
		let association_key = match candidate {
			Candidate::Object { object, owner, .. } => {
				Key::Storage(crate::lmdb::storage::Key::OwnerObject {
					object: object.clone(),
					owner: owner.clone(),
				})
			},
			Candidate::Process { owner, process, .. } => {
				Key::Storage(crate::lmdb::storage::Key::OwnerProcess {
					owner: owner.clone(),
					process: process.clone(),
				})
			},
		};
		let association_key = Self::pack(subspace, &association_key);
		let Some(value) = db
			.get(transaction, &association_key)
			.map_err(|error| tg::error!(!error, "failed to get a storage association"))?
		else {
			db.delete(transaction, &clean_key)
				.map_err(|error| tg::error!(!error, "failed to delete a storage clean key"))?;
			return Ok(());
		};
		let mut association = crate::storage::Association::deserialize(value)?;
		let touched_at = match candidate {
			Candidate::Object { touched_at, .. } | Candidate::Process { touched_at, .. } => {
				*touched_at
			},
		};
		if association.touched_at != touched_at {
			db.delete(transaction, &clean_key)
				.map_err(|error| tg::error!(!error, "failed to delete a storage clean key"))?;
			return Ok(());
		}

		let reference_count = match candidate {
			Candidate::Object { object, owner, .. } => Self::compute_owner_object_reference_count(
				db,
				subspace,
				transaction,
				owner,
				object,
			)?,
			Candidate::Process { owner, process, .. } => {
				Self::compute_owner_process_reference_count(
					db,
					subspace,
					transaction,
					owner,
					process,
				)?
			},
		};
		if reference_count > 0 {
			association.reference_count = reference_count;
			let value = association.serialize()?;
			db.put(transaction, &association_key, &value)
				.map_err(|error| tg::error!(!error, "failed to update a storage association"))?;
			db.delete(transaction, &clean_key)
				.map_err(|error| tg::error!(!error, "failed to delete a storage clean key"))?;
			return Ok(());
		}

		match candidate {
			Candidate::Object { object, owner, .. } => Self::delete_owner_object(
				db,
				subspace,
				transaction,
				owner,
				object,
				storage_partition_total,
			)?,
			Candidate::Process { owner, process, .. } => Self::delete_owner_process(
				db,
				subspace,
				transaction,
				owner,
				process,
				storage_partition_total,
			)?,
		}
		db.delete(transaction, &clean_key)
			.map_err(|error| tg::error!(!error, "failed to delete a storage clean key"))?;

		Ok(())
	}

	fn compute_owner_object_reference_count(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		owner: &crate::storage::Owner,
		object: &tg::object::Id,
	) -> tg::Result<u64> {
		let mut count = 0;
		for parent in Self::get_object_parents_with_transaction(db, subspace, transaction, object)?
		{
			let key = Key::Storage(crate::lmdb::storage::Key::OwnerObject {
				object: parent,
				owner: owner.clone(),
			});
			if db
				.get(transaction, &Self::pack(subspace, &key))
				.map_err(|error| tg::error!(!error, "failed to get an owner object"))?
				.is_some()
			{
				count += 1;
			}
		}
		for (process, _) in
			Self::get_object_processes_with_transaction(db, subspace, transaction, object)?
		{
			let key = Key::Storage(crate::lmdb::storage::Key::OwnerProcess {
				owner: owner.clone(),
				process,
			});
			if db
				.get(transaction, &Self::pack(subspace, &key))
				.map_err(|error| tg::error!(!error, "failed to get an owner process"))?
				.is_some()
			{
				count += 1;
			}
		}
		count += Self::count_owner_tags(db, subspace, transaction, owner, &object.to_bytes())?;

		Ok(count)
	}

	fn compute_owner_process_reference_count(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		owner: &crate::storage::Owner,
		process: &tg::process::Id,
	) -> tg::Result<u64> {
		let mut count = 0;
		for parent in
			Self::get_process_parents_with_transaction(db, subspace, transaction, process)?
		{
			let key = Key::Storage(crate::lmdb::storage::Key::OwnerProcess {
				owner: owner.clone(),
				process: parent,
			});
			if db
				.get(transaction, &Self::pack(subspace, &key))
				.map_err(|error| tg::error!(!error, "failed to get an owner process"))?
				.is_some()
			{
				count += 1;
			}
		}
		count += Self::count_owner_tags(db, subspace, transaction, owner, &process.to_bytes())?;

		Ok(count)
	}

	fn count_owner_tags(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		owner: &crate::storage::Owner,
		item: &[u8],
	) -> tg::Result<u64> {
		let tags = Self::get_item_tags_with_transaction(db, subspace, transaction, item)?;
		let mut count = 0;
		for tag in tags {
			let Some(tag) = Self::try_get_tag_with_transaction(db, subspace, transaction, &tag)?
			else {
				continue;
			};
			if tag.owner.as_ref() == Some(owner) {
				count += 1;
			}
		}

		Ok(count)
	}

	fn delete_owner_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		owner: &crate::storage::Owner,
		object: &tg::object::Id,
		storage_partition_total: u64,
	) -> tg::Result<()> {
		let children =
			Self::get_object_children_with_transaction(db, subspace, transaction, object)?;
		for child in children {
			Self::schedule_owner_object_clean(db, subspace, transaction, owner, &child)?;
		}
		let key = Key::Storage(crate::lmdb::storage::Key::OwnerObject {
			object: object.clone(),
			owner: owner.clone(),
		});
		db.delete(transaction, &Self::pack(subspace, &key))
			.map_err(|error| tg::error!(!error, "failed to delete the owner object"))?;
		let key = Key::Storage(crate::lmdb::storage::Key::ObjectOwner {
			object: object.clone(),
			owner: owner.clone(),
		});
		db.delete(transaction, &Self::pack(subspace, &key))
			.map_err(|error| tg::error!(!error, "failed to delete the object owner"))?;
		Self::add_owner_storage(
			db,
			subspace,
			transaction,
			owner,
			crate::storage::Kind::ObjectCount,
			-1,
			storage_partition_total,
		)?;
		let object_value =
			Self::try_get_object_with_transaction(db, subspace, transaction, object)?
				.ok_or_else(|| tg::error!(%object, "an owned object is missing"))?;
		let size = i64::try_from(object_value.metadata.node.size)
			.map_err(|_| tg::error!("the object size is too large"))?;
		Self::add_owner_storage(
			db,
			subspace,
			transaction,
			owner,
			crate::storage::Kind::ObjectSize,
			-size,
			storage_partition_total,
		)?;
		let key = Key::Clean(crate::lmdb::clean::Key::Clean {
			id: object.clone().into(),
			kind: ItemKind::Object,
			touched_at: object_value.touched_at,
		});
		db.put(transaction, &Self::pack(subspace, &key), &[])
			.map_err(|error| tg::error!(!error, "failed to schedule the object for cleaning"))?;

		Ok(())
	}

	fn delete_owner_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		owner: &crate::storage::Owner,
		process: &tg::process::Id,
		storage_partition_total: u64,
	) -> tg::Result<()> {
		let children =
			Self::get_process_children_with_transaction(db, subspace, transaction, process)?;
		for child in children {
			Self::schedule_owner_process_clean(db, subspace, transaction, owner, &child)?;
		}
		let objects =
			Self::get_process_objects_with_transaction(db, subspace, transaction, process)?;
		for (object, _) in objects {
			Self::schedule_owner_object_clean(db, subspace, transaction, owner, &object)?;
		}
		let key = Key::Storage(crate::lmdb::storage::Key::OwnerProcess {
			owner: owner.clone(),
			process: process.clone(),
		});
		db.delete(transaction, &Self::pack(subspace, &key))
			.map_err(|error| tg::error!(!error, "failed to delete the owner process"))?;
		let key = Key::Storage(crate::lmdb::storage::Key::ProcessOwner {
			owner: owner.clone(),
			process: process.clone(),
		});
		db.delete(transaction, &Self::pack(subspace, &key))
			.map_err(|error| tg::error!(!error, "failed to delete the process owner"))?;
		Self::add_owner_storage(
			db,
			subspace,
			transaction,
			owner,
			crate::storage::Kind::ProcessCount,
			-1,
			storage_partition_total,
		)?;
		let process_value =
			Self::try_get_process_with_transaction(db, subspace, transaction, process)?
				.ok_or_else(|| tg::error!(%process, "an owned process is missing"))?;
		let key = Key::Clean(crate::lmdb::clean::Key::Clean {
			id: process.clone().into(),
			kind: ItemKind::Process,
			touched_at: process_value.touched_at,
		});
		db.put(transaction, &Self::pack(subspace, &key), &[])
			.map_err(|error| tg::error!(!error, "failed to schedule the process for cleaning"))?;

		Ok(())
	}

	fn schedule_owner_object_clean(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		owner: &crate::storage::Owner,
		object: &tg::object::Id,
	) -> tg::Result<()> {
		let association_key = Key::Storage(crate::lmdb::storage::Key::OwnerObject {
			object: object.clone(),
			owner: owner.clone(),
		});
		let Some(value) = db
			.get(transaction, &Self::pack(subspace, &association_key))
			.map_err(|error| tg::error!(!error, "failed to get an owner object"))?
		else {
			return Ok(());
		};
		let association = crate::storage::Association::deserialize(value)?;
		let key = Key::Storage(crate::lmdb::storage::Key::OwnerObjectClean {
			object: object.clone(),
			owner: owner.clone(),
			touched_at: association.touched_at,
		});
		db.put(transaction, &Self::pack(subspace, &key), &[])
			.map_err(|error| {
				tg::error!(!error, "failed to schedule an owner object for cleaning")
			})?;

		Ok(())
	}

	fn schedule_owner_process_clean(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		owner: &crate::storage::Owner,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		let association_key = Key::Storage(crate::lmdb::storage::Key::OwnerProcess {
			owner: owner.clone(),
			process: process.clone(),
		});
		let Some(value) = db
			.get(transaction, &Self::pack(subspace, &association_key))
			.map_err(|error| tg::error!(!error, "failed to get an owner process"))?
		else {
			return Ok(());
		};
		let association = crate::storage::Association::deserialize(value)?;
		let key = Key::Storage(crate::lmdb::storage::Key::OwnerProcessClean {
			owner: owner.clone(),
			process: process.clone(),
			touched_at: association.touched_at,
		});
		db.put(transaction, &Self::pack(subspace, &key), &[])
			.map_err(|error| {
				tg::error!(!error, "failed to schedule an owner process for cleaning")
			})?;

		Ok(())
	}
}
