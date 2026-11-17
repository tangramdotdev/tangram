use {
	crate::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_process_checkouts(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::process::checkout::put::Arg,
	) {
		for checkout in &arg.checkouts {
			let key = Key::Process(crate::process::Key::ProcessCheckout {
				checkout: checkout.clone(),
				process: arg.process.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			let key = Key::Process(crate::process::Key::CheckoutProcess {
				checkout: checkout.clone(),
				process: arg.process.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
	}

	pub(crate) async fn delete_process_checkouts(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		// Get the checkouts that the process retains.
		let id_bytes = id.to_bytes();
		let prefix = (Kind::ProcessCheckout.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = fdbt::Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let checkouts = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::process::Key::ProcessCheckout { checkout, .. }) = key
				else {
					return Err(tg::error!("expected a process checkout key"));
				};
				Ok(checkout)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		// Delete the keys, and release each checkout so that cleaning considers it again.
		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);
		for checkout in checkouts {
			let key = Key::Process(crate::process::Key::CheckoutProcess {
				checkout: checkout.clone(),
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			crate::propagate!(
				Self::decrement_checkout_reference_count(txn, subspace, &checkout, partition_total)
					.await
			);
		}

		Ok(ControlFlow::Break(()))
	}
}
