use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::object::put::Arg,
	) -> tg::Result<()> {
		let id = &arg.id;
		let key = Key::Object(crate::lmdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);

		let existing = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
			.and_then(|bytes| crate::object::Object::deserialize(bytes).ok());
		let merge = !arg.complete();
		let merged = existing.as_ref().filter(|_| merge);

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

		let checkout = arg
			.checkout
			.clone()
			.or_else(|| merged.and_then(|existing| existing.checkout.clone()));
		let previous_checkout = existing
			.as_ref()
			.and_then(|existing| existing.checkout.clone());
		let checkout_changed = previous_checkout != checkout;

		let storage = crate::object::Storage {
			subtree: arg.storage.subtree || merged.is_some_and(|existing| existing.storage.subtree),
		};

		let mut metadata = arg.metadata.clone();
		if let Some(existing) = merged {
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
			return Ok(());
		}

		let value = crate::object::Object {
			checkout: checkout.clone(),
			metadata,
			put,
			reference_count: 0,
			storage,
			touched_at,
		}
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		for child in arg.children.iter().filter(|_| changed) {
			let key = Key::Object(crate::lmdb::object::Key::ObjectChild {
				object: id.clone(),
				child: child.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the object child"))?;

			let key = Key::Object(crate::lmdb::object::Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the child object"))?;
		}

		if checkout_changed && let Some(checkout) = &previous_checkout {
			let key = Key::Object(crate::lmdb::object::Key::ObjectCheckout {
				object: id.clone(),
				checkout: checkout.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the object checkout"))?;

			let key = Key::Object(crate::lmdb::object::Key::CheckoutObject {
				checkout: checkout.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the checkout object"))?;

			Self::decrement_checkout_reference_count(db, subspace, transaction, checkout)?;
		}

		if checkout_changed && let Some(checkout) = &checkout {
			let key = Key::Object(crate::lmdb::object::Key::ObjectCheckout {
				object: id.clone(),
				checkout: checkout.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the object checkout"))?;

			let key = Key::Object(crate::lmdb::object::Key::CheckoutObject {
				checkout: checkout.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the checkout object"))?;
		}

		let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Object {
			id: id.clone(),
			touched_at,
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;

		if changed {
			Self::enqueue_update(
				db,
				subspace,
				transaction,
				tg::Either::Left(id.clone()),
				crate::lmdb::update::Source::Put,
				None,
			)?;
			Self::enqueue_account_object_from_parents(db, subspace, transaction, id, touched_at)?;
		}

		Ok(())
	}

	pub(crate) fn put_objects_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::object::put::Arg],
	) -> tg::Result<()> {
		for object in args {
			Self::put_object(db, subspace, transaction, object)?;
		}
		Ok(())
	}
}
