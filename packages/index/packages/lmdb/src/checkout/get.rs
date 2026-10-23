use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn try_get_checkouts_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<tangram_index::checkout::Checkout>>> {
		ids.iter()
			.map(|id| Self::try_get_checkout_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_checkout_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::Id,
	) -> tg::Result<Option<tangram_index::checkout::Checkout>> {
		let key = Key::Checkout(crate::checkout::Key::Checkout(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the checkout"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(tangram_index::checkout::Checkout::deserialize(bytes)?))
	}
}
