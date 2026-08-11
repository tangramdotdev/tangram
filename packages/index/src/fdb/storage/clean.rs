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
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Left(object.clone()),
			&crate::fdb::update::Kind::StorageOwnersClean,
			crate::fdb::update::Source::Put,
			partition_total,
		);

		Ok(())
	}

	pub(crate) async fn schedule_process_owners_for_cleaning(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		process: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Right(process.clone()),
			&crate::fdb::update::Kind::StorageOwnersClean,
			crate::fdb::update::Source::Put,
			partition_total,
		);

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
		let (parents, processes) = futures::future::try_join(
			Self::get_object_parents_with_transaction(txn, subspace, object),
			Self::get_object_processes_with_transaction(txn, subspace, object),
		)
		.await?;
		let keys = parents
			.into_iter()
			.map(|object| {
				Key::Storage(crate::fdb::storage::Key::OwnerObject {
					object,
					owner: owner.clone(),
				})
			})
			.chain(processes.into_iter().map(|(process, _)| {
				Key::Storage(crate::fdb::storage::Key::OwnerProcess {
					owner: owner.clone(),
					process,
				})
			}))
			.map(|key| Self::pack(subspace, &key))
			.collect::<Vec<_>>();
		let associations_future =
			futures::future::try_join_all(keys.iter().map(|key| async move {
				txn.get(key, false)
					.await
					.map_err(|error| tg::error!(!error, "failed to get a storage association"))
			}));
		let object_bytes = object.to_bytes();
		let tags_future = Self::count_owner_tags(txn, subspace, owner, object_bytes.as_ref());
		let (associations, tag_count) =
			futures::future::try_join(associations_future, tags_future).await?;
		let association_count = associations.iter().filter(|value| value.is_some()).count();
		let count = u64::try_from(association_count).unwrap() + tag_count;

		Ok(count)
	}

	async fn compute_owner_process_reference_count(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		owner: &crate::storage::Owner,
		process: &tg::process::Id,
	) -> tg::Result<u64> {
		let parents = Self::get_process_parents_with_transaction(txn, subspace, process).await?;
		let keys = parents
			.into_iter()
			.map(|process| {
				let key = Key::Storage(crate::fdb::storage::Key::OwnerProcess {
					owner: owner.clone(),
					process,
				});
				Self::pack(subspace, &key)
			})
			.collect::<Vec<_>>();
		let associations_future =
			futures::future::try_join_all(keys.iter().map(|key| async move {
				txn.get(key, false)
					.await
					.map_err(|error| tg::error!(!error, "failed to get an owner process"))
			}));
		let process_bytes = process.to_bytes();
		let tags_future = Self::count_owner_tags(txn, subspace, owner, process_bytes.as_ref());
		let (associations, tag_count) =
			futures::future::try_join(associations_future, tags_future).await?;
		let association_count = associations.iter().filter(|value| value.is_some()).count();
		let count = u64::try_from(association_count).unwrap() + tag_count;

		Ok(count)
	}

	async fn count_owner_tags(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		owner: &crate::storage::Owner,
		target: &[u8],
	) -> tg::Result<u64> {
		let tags = Self::get_target_tags_with_transaction(txn, subspace, target).await?;
		let tags = futures::future::try_join_all(
			tags.iter()
				.map(|tag| Self::try_get_tag_with_transaction(txn, subspace, tag)),
		)
		.await?;
		let count = tags
			.iter()
			.filter(|tag| tag.as_ref().and_then(|tag| tag.owner.as_ref()) == Some(owner))
			.count();
		let count = u64::try_from(count).unwrap();

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
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Left(object.clone()),
			&crate::fdb::update::Kind::StorageClean(owner.clone()),
			crate::fdb::update::Source::Put,
			partition_total,
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
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&tg::Either::Right(process.clone()),
			&crate::fdb::update::Kind::StorageClean(owner.clone()),
			crate::fdb::update::Source::Put,
			partition_total,
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

	pub(in crate::fdb) async fn schedule_owner_object_clean(
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

	pub(in crate::fdb) async fn schedule_owner_process_clean(
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
