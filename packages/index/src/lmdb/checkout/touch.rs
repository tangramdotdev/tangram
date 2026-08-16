use {
	crate::lmdb::{Db, Index, Key, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	std::time::Duration,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn touch_checkouts(
		&self,
		ids: &[tg::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::checkout::Checkout>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = Request::TouchCheckouts(crate::lmdb::TouchCheckouts {
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		});
		let response = self.send_write_request(request).await?;
		let Response::Checkouts(checkouts) = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(checkouts)
	}

	pub(crate) fn touch_checkouts_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::checkout::Checkout>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		for id in ids {
			let key = Key::Checkout(crate::lmdb::checkout::Key::Checkout(id.clone()));
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to get the checkout"))?;
			let existing = existing
				.map(crate::checkout::Checkout::deserialize)
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
				let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Checkout {
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
