use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_process_checkouts(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::process::checkout::put::Arg,
	) -> tg::Result<()> {
		for checkout in &arg.checkouts {
			let key = Key::Process(crate::process::Key::ProcessCheckout {
				checkout: checkout.clone(),
				process: arg.process.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the process checkout"))?;

			let key = Key::Process(crate::process::Key::CheckoutProcess {
				checkout: checkout.clone(),
				process: arg.process.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the checkout process"))?;
		}
		Ok(())
	}

	pub(crate) fn delete_process_checkouts(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		// Get the checkouts that the process retains.
		let id_bytes = id.to_bytes();
		let prefix = (Kind::ProcessCheckout.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the process checkout keys"))?;
		let mut checkouts = Vec::new();
		for result in iter {
			let (key, _) = result
				.map_err(|error| tg::error!(!error, "failed to read a process checkout key"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Process(crate::process::Key::ProcessCheckout { checkout, .. }) = key else {
				return Err(tg::error!("expected a process checkout key"));
			};
			checkouts.push(checkout);
		}

		// Delete the keys, and release each checkout so that cleaning considers it again.
		for checkout in checkouts {
			let key = Key::Process(crate::process::Key::ProcessCheckout {
				checkout: checkout.clone(),
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the process checkout"))?;

			let key = Key::Process(crate::process::Key::CheckoutProcess {
				checkout: checkout.clone(),
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the checkout process"))?;

			Self::decrement_checkout_reference_count(db, subspace, transaction, &checkout)?;
		}

		Ok(())
	}
}
