use {
	crate::fdb::{Index, ItemKind, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::StreamExt as _,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

enum Candidate {
	Object {
		object: tg::object::Id,
		owner: crate::storage::Owner,
		partition: u64,
		touched_at: i64,
	},
	Process {
		owner: crate::storage::Owner,
		partition: u64,
		process: tg::process::Id,
		touched_at: i64,
	},
}

impl Index {
	pub(crate) async fn schedule_object_owners_for_cleaning(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		object: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::ObjectOwner.to_i32().unwrap(),
				object.to_bytes().as_ref(),
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
				let Key::Storage(crate::fdb::storage::Key::ObjectOwner { owner, .. }) =
					Self::unpack(subspace, entry.key())?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(owner)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for owner in owners {
			Self::schedule_owner_object_clean(txn, subspace, &owner, object, partition_total)
				.await?;
		}

		Ok(())
	}

	pub(crate) async fn schedule_process_owners_for_cleaning(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::ProcessOwner.to_i32().unwrap(),
				process.to_bytes().as_ref(),
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
				let Key::Storage(crate::fdb::storage::Key::ProcessOwner { owner, .. }) =
					Self::unpack(subspace, entry.key())?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(owner)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for owner in owners {
			Self::schedule_owner_process_clean(txn, subspace, &owner, process, partition_total)
				.await?;
		}

		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn clean_storage_associations(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		batch_size: usize,
		max_object_touched_at: i64,
		max_process_touched_at: i64,
		partition_start: u64,
		partition_end: u64,
		partition_total: u64,
		storage_partition_total: u64,
	) -> tg::Result<usize> {
		let mut candidates = Vec::new();
		for (kind, max_touched_at) in [
			(Kind::OwnerObjectClean, max_object_touched_at),
			(Kind::OwnerProcessClean, max_process_touched_at),
		] {
			for partition in partition_start..partition_end {
				if candidates.len() >= batch_size {
					break;
				}
				let begin = Self::pack(subspace, &(kind.to_i32().unwrap(), partition, i64::MIN));
				let end = Self::pack(
					subspace,
					&(
						kind.to_i32().unwrap(),
						partition,
						max_touched_at.saturating_add(1),
					),
				);
				let range = fdb::RangeOption {
					begin: fdb::KeySelector::first_greater_or_equal(begin),
					end: fdb::KeySelector::first_greater_or_equal(end),
					mode: fdb::options::StreamingMode::Iterator,
					..Default::default()
				};
				let mut entries = txn.get_ranges_keyvalues(range, false);
				while candidates.len() < batch_size {
					let Some(entry) = entries.next().await.transpose().map_err(|error| {
						tg::error!(!error, "failed to read a storage clean key")
					})?
					else {
						break;
					};
					let key = Self::unpack(subspace, entry.key())?;
					let candidate = match key {
						Key::Storage(crate::fdb::storage::Key::OwnerObjectClean {
							object,
							owner,
							partition,
							touched_at,
						}) => Candidate::Object {
							object,
							owner,
							partition,
							touched_at,
						},
						Key::Storage(crate::fdb::storage::Key::OwnerProcessClean {
							owner,
							partition,
							process,
							touched_at,
						}) => Candidate::Process {
							owner,
							partition,
							process,
							touched_at,
						},
						_ => return Err(tg::error!("unexpected key type")),
					};
					candidates.push(candidate);
				}
			}
		}

		for candidate in &candidates {
			Self::clean_storage_association(
				txn,
				subspace,
				candidate,
				partition_total,
				storage_partition_total,
			)
			.await?;
		}

		Ok(candidates.len())
	}

	async fn clean_storage_association(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		candidate: &Candidate,
		partition_total: u64,
		storage_partition_total: u64,
	) -> tg::Result<()> {
		let (association_key, clean_key, touched_at) = match candidate {
			Candidate::Object {
				object,
				owner,
				partition,
				touched_at,
			} => (
				Key::Storage(crate::fdb::storage::Key::OwnerObject {
					object: object.clone(),
					owner: owner.clone(),
				}),
				Key::Storage(crate::fdb::storage::Key::OwnerObjectClean {
					object: object.clone(),
					owner: owner.clone(),
					partition: *partition,
					touched_at: *touched_at,
				}),
				*touched_at,
			),
			Candidate::Process {
				owner,
				partition,
				process,
				touched_at,
			} => (
				Key::Storage(crate::fdb::storage::Key::OwnerProcess {
					owner: owner.clone(),
					process: process.clone(),
				}),
				Key::Storage(crate::fdb::storage::Key::OwnerProcessClean {
					owner: owner.clone(),
					partition: *partition,
					process: process.clone(),
					touched_at: *touched_at,
				}),
				*touched_at,
			),
		};
		let association_key = Self::pack(subspace, &association_key);
		let clean_key = Self::pack(subspace, &clean_key);
		let Some(value) = txn
			.get(&association_key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get a storage association"))?
		else {
			txn.clear(&clean_key);
			return Ok(());
		};
		let mut association = crate::storage::Association::deserialize(&value)?;
		if association.touched_at != touched_at {
			txn.clear(&clean_key);
			return Ok(());
		}
		let reference_count = match candidate {
			Candidate::Object { object, owner, .. } => {
				Self::compute_owner_object_reference_count(txn, subspace, owner, object).await?
			},
			Candidate::Process { owner, process, .. } => {
				Self::compute_owner_process_reference_count(txn, subspace, owner, process).await?
			},
		};
		if reference_count > 0 {
			association.reference_count = reference_count;
			txn.set(&association_key, &association.serialize()?);
			txn.clear(&clean_key);
			return Ok(());
		}
		match candidate {
			Candidate::Object { object, owner, .. } => {
				Self::delete_owner_object(
					txn,
					subspace,
					owner,
					object,
					partition_total,
					storage_partition_total,
				)
				.await?;
			},
			Candidate::Process { owner, process, .. } => {
				Self::delete_owner_process(
					txn,
					subspace,
					owner,
					process,
					partition_total,
					storage_partition_total,
				)
				.await?;
			},
		}
		txn.clear(&clean_key);

		Ok(())
	}

	async fn compute_owner_object_reference_count(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		owner: &crate::storage::Owner,
		object: &tg::object::Id,
	) -> tg::Result<u64> {
		let mut count = 0;
		for parent in Self::get_object_parents_with_transaction(txn, subspace, object).await? {
			let key = Key::Storage(crate::fdb::storage::Key::OwnerObject {
				object: parent,
				owner: owner.clone(),
			});
			if txn
				.get(&Self::pack(subspace, &key), false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get an owner object"))?
				.is_some()
			{
				count += 1;
			}
		}
		for (process, _) in
			Self::get_object_processes_with_transaction(txn, subspace, object).await?
		{
			let key = Key::Storage(crate::fdb::storage::Key::OwnerProcess {
				owner: owner.clone(),
				process,
			});
			if txn
				.get(&Self::pack(subspace, &key), false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get an owner process"))?
				.is_some()
			{
				count += 1;
			}
		}
		count += Self::count_owner_tags(txn, subspace, owner, object.to_bytes().as_ref()).await?;

		Ok(count)
	}

	async fn compute_owner_process_reference_count(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		owner: &crate::storage::Owner,
		process: &tg::process::Id,
	) -> tg::Result<u64> {
		let mut count = 0;
		for parent in Self::get_process_parents_with_transaction(txn, subspace, process).await? {
			let key = Key::Storage(crate::fdb::storage::Key::OwnerProcess {
				owner: owner.clone(),
				process: parent,
			});
			if txn
				.get(&Self::pack(subspace, &key), false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get an owner process"))?
				.is_some()
			{
				count += 1;
			}
		}
		count += Self::count_owner_tags(txn, subspace, owner, process.to_bytes().as_ref()).await?;

		Ok(count)
	}

	async fn count_owner_tags(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		owner: &crate::storage::Owner,
		item: &[u8],
	) -> tg::Result<u64> {
		let tags = Self::get_item_tags_with_transaction(txn, subspace, item).await?;
		let mut count = 0;
		for tag in tags {
			let Some(tag) = Self::try_get_tag_with_transaction(txn, subspace, &tag).await? else {
				continue;
			};
			if tag.owner.as_ref() == Some(owner) {
				count += 1;
			}
		}

		Ok(count)
	}

	#[allow(clippy::too_many_arguments)]
	async fn delete_owner_object(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		owner: &crate::storage::Owner,
		object: &tg::object::Id,
		partition_total: u64,
		storage_partition_total: u64,
	) -> tg::Result<()> {
		let children = Self::get_object_children_with_transaction(txn, subspace, object).await?;
		for child in children {
			Self::schedule_owner_object_clean(txn, subspace, owner, &child, partition_total)
				.await?;
		}
		let key = Key::Storage(crate::fdb::storage::Key::OwnerObject {
			object: object.clone(),
			owner: owner.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		let key = Key::Storage(crate::fdb::storage::Key::ObjectOwner {
			object: object.clone(),
			owner: owner.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		Self::add_owner_storage(
			txn,
			subspace,
			owner,
			crate::storage::Kind::ObjectCount,
			-1,
			storage_partition_total,
		);
		let value = Self::try_get_object_with_transaction(txn, subspace, object)
			.await?
			.ok_or_else(|| tg::error!(%object, "an owned object is missing"))?;
		let size = i64::try_from(value.metadata.node.size)
			.map_err(|_| tg::error!("the object size is too large"))?;
		Self::add_owner_storage(
			txn,
			subspace,
			owner,
			crate::storage::Kind::ObjectSize,
			-size,
			storage_partition_total,
		);
		let partition = Self::partition_for_id(object.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::fdb::clean::Key::Clean {
			id: object.clone().into(),
			kind: ItemKind::Object,
			partition,
			touched_at: value.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn delete_owner_process(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		owner: &crate::storage::Owner,
		process: &tg::process::Id,
		partition_total: u64,
		storage_partition_total: u64,
	) -> tg::Result<()> {
		for child in Self::get_process_children_with_transaction(txn, subspace, process).await? {
			Self::schedule_owner_process_clean(txn, subspace, owner, &child, partition_total)
				.await?;
		}
		for (object, _) in
			Self::get_process_objects_with_transaction(txn, subspace, process).await?
		{
			Self::schedule_owner_object_clean(txn, subspace, owner, &object, partition_total)
				.await?;
		}
		let key = Key::Storage(crate::fdb::storage::Key::OwnerProcess {
			owner: owner.clone(),
			process: process.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		let key = Key::Storage(crate::fdb::storage::Key::ProcessOwner {
			owner: owner.clone(),
			process: process.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));
		Self::add_owner_storage(
			txn,
			subspace,
			owner,
			crate::storage::Kind::ProcessCount,
			-1,
			storage_partition_total,
		);
		let value = Self::try_get_process_with_transaction(txn, subspace, process)
			.await?
			.ok_or_else(|| tg::error!(%process, "an owned process is missing"))?;
		let partition = Self::partition_for_id(process.to_bytes().as_ref(), partition_total);
		let key = Key::Clean(crate::fdb::clean::Key::Clean {
			id: process.clone().into(),
			kind: ItemKind::Process,
			partition,
			touched_at: value.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(())
	}

	async fn schedule_owner_object_clean(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		owner: &crate::storage::Owner,
		object: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let association_key = Key::Storage(crate::fdb::storage::Key::OwnerObject {
			object: object.clone(),
			owner: owner.clone(),
		});
		let Some(value) = txn
			.get(&Self::pack(subspace, &association_key), false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get an owner object"))?
		else {
			return Ok(());
		};
		let association = crate::storage::Association::deserialize(&value)?;
		let partition = Self::partition_for_id(object.to_bytes().as_ref(), partition_total);
		let key = Key::Storage(crate::fdb::storage::Key::OwnerObjectClean {
			object: object.clone(),
			owner: owner.clone(),
			partition,
			touched_at: association.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(())
	}

	async fn schedule_owner_process_clean(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		owner: &crate::storage::Owner,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let association_key = Key::Storage(crate::fdb::storage::Key::OwnerProcess {
			owner: owner.clone(),
			process: process.clone(),
		});
		let Some(value) = txn
			.get(&Self::pack(subspace, &association_key), false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get an owner process"))?
		else {
			return Ok(());
		};
		let association = crate::storage::Association::deserialize(&value)?;
		let partition = Self::partition_for_id(process.to_bytes().as_ref(), partition_total);
		let key = Key::Storage(crate::fdb::storage::Key::OwnerProcessClean {
			owner: owner.clone(),
			partition,
			process: process.clone(),
			touched_at: association.touched_at,
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(())
	}
}
