use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	std::time::Duration,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn touch_checkouts_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<tangram_index::checkout::Checkout>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		for id in ids {
			let key = Key::Checkout(crate::checkout::Key::Checkout(id.clone()));
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to get the checkout"))?;
			let existing = existing
				.map(tangram_index::checkout::Checkout::deserialize)
				.transpose()?;
			let Some(mut checkout) = existing else {
				outputs.push(None);
				continue;
			};
			if touched_at - checkout.touched_at < time_to_touch {
				outputs.push(Some(checkout));
				continue;
			}
			checkout.touched_at = checkout.touched_at.max(touched_at);
			let value = checkout.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, %id, "failed to put the checkout"))?;
			if checkout.reference_count == 0 {
				let key = crate::Key::Clean(crate::clean::Key::Checkout {
					id: id.clone(),
					touched_at: checkout.touched_at,
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;
			}
			outputs.push(Some(checkout));
		}
		Ok(outputs)
	}
}
