use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_object(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::object::put::Arg,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let id = &arg.id;
		if arg.checkout.is_some() {
			return Err(tg::error!(
				%id,
				"checkout pointers are not supported by the FDB index"
			));
		}
		let key = Key::Object(crate::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);

		// Put a complete object without reading, since it has nothing to merge.
		if arg.complete() {
			let object = tangram_index::object::Object {
				checkout: None,
				metadata: arg.metadata.clone(),
				put: arg.put,
				reference_count: 0,
				storage: arg.storage,
				touched_at: arg.touched_at,
			};
			Self::put_object_row(txn, subspace, arg, &key, &object, partition_totals)?;
			return Ok(ControlFlow::Break(()));
		}

		// Read the existing object.
		let result = txn.get(&key, false).await;
		let existing = crate::retry!(result)
			.and_then(|bytes| tangram_index::object::Object::deserialize(&bytes).ok());

		// Merge the existing object.
		let time_to_touch = i64::try_from(arg.time_to_touch.as_secs()).unwrap();
		let touch = existing.as_ref().is_none_or(|existing| {
			arg.touched_at.saturating_sub(existing.touched_at) >= time_to_touch
		});
		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			if touch {
				existing.touched_at.max(arg.touched_at)
			} else {
				existing.touched_at
			}
		});
		let checkout = existing
			.as_ref()
			.and_then(|existing| existing.checkout.clone());
		let mut storage = arg.storage;
		if let Some(existing) = &existing {
			storage.insert(existing.storage);
		}
		let mut metadata = arg.metadata.clone();
		if let Some(existing) = &existing {
			metadata.merge(&existing.metadata);
		}
		let put = existing
			.as_ref()
			.map_or(arg.put, |existing| existing.put.max(arg.put));
		let put_changed = existing.as_ref().is_none_or(|existing| existing.put != put);
		let changed = existing.as_ref().is_none_or(|existing| {
			existing.checkout != checkout
				|| existing.metadata != metadata
				|| existing.storage != storage
		});
		if !changed && !put_changed && !touch {
			return Ok(ControlFlow::Break(()));
		}

		// Put the object.
		let object = tangram_index::object::Object {
			checkout,
			metadata,
			put,
			reference_count: 0,
			storage,
			touched_at,
		};
		Self::put_object_row(txn, subspace, arg, &key, &object, partition_totals)?;

		Ok(ControlFlow::Break(()))
	}

	fn put_object_row(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::object::put::Arg,
		key: &[u8],
		object: &tangram_index::object::Object,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<()> {
		let id = &arg.id;

		// Put the object with a byte maximum, so that the object from the latest put wins without a read.
		let value = object.serialize()?;
		txn.atomic_op(key, &value, fdb::options::MutationType::ByteMax);

		// Put the children.
		let complete = object.complete();
		for child in &arg.children {
			let key = Key::Object(crate::object::Key::ObjectChild {
				object: id.clone(),
				child: child.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			let key = Key::Object(crate::object::Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			// Put a propagation from the child when the object is incomplete, so that the child's update enqueues an update on the object.
			if !complete {
				let key = Key::Update(crate::update::Key::Propagation {
					child: tg::Either::Left(child.clone()),
					parent: tg::Either::Left(id.clone()),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);
			}
		}

		// Put the clean key.
		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_totals.cleaning);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = crate::Key::Clean(crate::clean::Key::Object {
			id: id.clone(),
			partition,
			touched_at: object.touched_at,
		});
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		// Enqueue the update, which also resumes the permission and usage derivation that depend on the object.
		let source = if arg.account.is_some() {
			crate::update::StorageAndMetadataSource::PutWithAccount
		} else {
			crate::update::StorageAndMetadataSource::Put
		};
		Self::enqueue_update(
			txn,
			subspace,
			&tg::Either::Left(id.clone()),
			source,
			partition_totals.storage_and_metadata_update,
		);

		Ok(())
	}

	pub(crate) async fn put_objects_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::object::put::Arg],
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for object in args {
			crate::propagate!(Self::put_object(txn, subspace, object, partition_totals).await);

			// Put the account's storage entry with the object's put, so that it does not read the object or propagate to the children, whose entries the write puts.
			if let Some(account) = &object.account {
				let entry = tangram_index::usage::storage::put::ObjectArg {
					account: account.clone(),
					object: object.id.clone(),
					touched_at: object.touched_at,
				};
				crate::propagate!(
					Self::put_account_object(
						txn,
						subspace,
						&entry,
						partition_totals,
						None,
						None,
						Some(object),
					)
					.await
				);
			}
		}

		Ok(ControlFlow::Break(()))
	}
}
