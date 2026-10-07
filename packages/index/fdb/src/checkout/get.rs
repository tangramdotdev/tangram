use {
	crate::{Index, Key},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn try_get_checkouts_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		ids: &[tg::Id],
	) -> tg::Result<ControlFlow<Vec<Option<tangram_index::checkout::Checkout>>, fdb::FdbError>> {
		let entries = {
			let result = futures::future::try_join_all(
				ids.iter()
					.map(|id| Self::try_get_checkout_with_transaction(txn, subspace, id)),
			)
			.await;
			let results = result?;
			let mut values = Vec::with_capacity(results.len());
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};

		Ok(ControlFlow::Break(entries))
	}

	pub(crate) async fn try_get_checkout_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Id,
	) -> tg::Result<ControlFlow<Option<tangram_index::checkout::Checkout>, fdb::FdbError>> {
		let key = Key::Checkout(crate::checkout::Key::Checkout(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let entry = Some(tangram_index::checkout::Checkout::deserialize(&bytes)?);

		Ok(ControlFlow::Break(entry))
	}
}
