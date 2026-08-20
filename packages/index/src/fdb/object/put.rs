use {
	crate::fdb::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_object(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::object::put::Arg,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let id = &arg.id;
		let key = Key::Object(crate::fdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);

		let existing = if arg.complete() {
			None
		} else {
			let result = txn.get(&key, false).await;
			crate::fdb::retry!(result)
				.and_then(|bytes| crate::object::Object::deserialize(&bytes).ok())
		};

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

		let checkout = arg.checkout.clone().or_else(|| {
			existing
				.as_ref()
				.and_then(|existing| existing.checkout.clone())
		});

		let storage = crate::object::Storage {
			subtree: arg.storage.subtree
				|| existing
					.as_ref()
					.is_some_and(|existing| existing.storage.subtree),
		};

		let mut metadata = arg.metadata.clone();
		if let Some(ref existing) = existing {
			metadata.merge(&existing.metadata);
		}
		let changed = existing.as_ref().is_none_or(|existing| {
			existing.checkout != checkout
				|| existing.metadata != metadata
				|| existing.storage != storage
		});
		if !changed && !touch {
			return Ok(ControlFlow::Break(()));
		}

		let value = crate::object::Object {
			checkout,
			metadata,
			reference_count: 0,
			storage,
			touched_at,
		}
		.serialize()?;

		if existing.is_none() {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
		}
		txn.set(&key, &value);

		for child in arg.children.iter().filter(|_| changed) {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object(crate::fdb::object::Key::ObjectChild {
				object: id.clone(),
				child: child.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object(crate::fdb::object::Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		if changed && let Some(checkout) = &arg.checkout {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object(crate::fdb::object::Key::ObjectCheckout {
				object: id.clone(),
				checkout: checkout.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object(crate::fdb::object::Key::CheckoutObject {
				checkout: checkout.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Object {
			id: id.clone(),
			partition,
			touched_at,
		});
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		if changed {
			Self::enqueue_update(
				txn,
				subspace,
				&tg::Either::Left(id.clone()),
				partition_total,
			);
			crate::fdb::propagate!(
				Self::enqueue_account_object_from_parents(
					txn,
					subspace,
					id,
					partition_total,
					touched_at,
				)
				.await
			);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn put_objects_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::object::put::Arg],
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for object in args {
			crate::fdb::propagate!(Self::put_object(txn, subspace, object, partition_total).await);
		}
		Ok(ControlFlow::Break(()))
	}
}
