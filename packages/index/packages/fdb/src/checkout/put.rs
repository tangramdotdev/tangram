use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_checkout(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::checkout::put::Arg,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let id = &arg.id;

		// Delete the existing checkout, so that a replaced dependency does not remain.
		if arg.replace {
			crate::propagate!(Self::delete_checkout(txn, subspace, id, partition_total).await);
		}

		// Read the existing checkout.
		let key = Key::Checkout(crate::checkout::Key::Checkout(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let existing = crate::retry!(result)
			.and_then(|bytes| tangram_index::checkout::Checkout::deserialize(&bytes).ok());

		// Merge the existing checkout, keeping the larger put token and touch time.
		let put = existing
			.as_ref()
			.map_or(arg.put, |existing| existing.put.max(arg.put));
		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			existing.touched_at.max(arg.touched_at)
		});

		// Put the checkout.
		let value = tangram_index::checkout::Checkout {
			put,
			reference_count: 0,
			touched_at,
		}
		.serialize()?;
		txn.set(&key, &value);

		for dependency in &arg.dependencies {
			let key = Key::Checkout(crate::checkout::Key::CheckoutDependency {
				checkout: id.clone(),
				dependency: dependency.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			let key = Key::Checkout(crate::checkout::Key::DependencyCheckout {
				checkout: id.clone(),
				dependency: dependency.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		let key = crate::Key::Clean(crate::clean::Key::Checkout {
			id: arg.id.clone(),
			partition,
			touched_at,
		});
		let key = Self::pack(subspace, &key);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &[]);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn put_checkouts_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::checkout::put::Arg],
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for checkout in args {
			crate::propagate!(Self::put_checkout(txn, subspace, checkout, partition_total).await);
		}
		Ok(ControlFlow::Break(()))
	}
}
