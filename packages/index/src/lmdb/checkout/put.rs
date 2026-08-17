use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_checkout(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::checkout::put::Arg,
	) -> tg::Result<()> {
		let key = Key::Checkout(crate::lmdb::checkout::Key::Checkout(arg.id.clone()));
		let key = Self::pack(subspace, &key);

		let existing = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the checkout"))?
			.and_then(|bytes| crate::checkout::Checkout::deserialize(bytes).ok());

		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			existing.touched_at.max(arg.touched_at)
		});

		let value = crate::checkout::Checkout {
			reference_count: 0,
			touched_at,
		}
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the checkout"))?;

		for dependency in &arg.dependencies {
			let key = Key::Checkout(crate::lmdb::checkout::Key::CheckoutDependency {
				checkout: arg.id.clone(),
				dependency: dependency.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the checkout dependency"))?;

			let key = Key::Checkout(crate::lmdb::checkout::Key::DependencyCheckout {
				checkout: arg.id.clone(),
				dependency: dependency.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the dependency checkout"))?;
		}

		let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Checkout {
			id: arg.id.clone(),
			touched_at,
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;

		Ok(())
	}

	pub(crate) fn put_checkouts_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::checkout::put::Arg],
	) -> tg::Result<()> {
		for checkout in args {
			Self::put_checkout(db, subspace, transaction, checkout)?;
		}
		Ok(())
	}
}
