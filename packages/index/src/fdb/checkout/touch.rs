use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::future,
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn touch_checkouts(
		&self,
		ids: &[tg::artifact::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::checkout::Checkout>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = Request::TouchCheckouts(crate::fdb::TouchCheckouts {
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

	pub(crate) async fn touch_checkouts_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::artifact::Id],
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<Vec<Option<crate::checkout::Checkout>>, fdb::FdbError>> {
		let entries = {
			let result = future::try_join_all(ids.iter().map(|id| {
				let subspace = subspace.clone();
				async move {
					Self::touch_checkout_with_transaction(
						txn,
						&subspace,
						id,
						touched_at,
						time_to_touch,
						partition_total,
					)
					.await
				}
			}))
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

	async fn touch_checkout_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		id: &tg::artifact::Id,
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<Option<crate::checkout::Checkout>, fdb::FdbError>> {
		let key = Key::Checkout(crate::fdb::checkout::Key::Checkout(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let existing = crate::fdb::retry!(result);
		let existing = existing
			.as_ref()
			.map(|bytes| crate::checkout::Checkout::deserialize(bytes))
			.transpose()?;
		let Some(mut checkout) = existing else {
			return Ok(ControlFlow::Break(None));
		};
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if touched_at - checkout.touched_at < time_to_touch {
			return Ok(ControlFlow::Break(Some(checkout)));
		}

		let mut key_end = key.clone();
		key_end.push(0x00);
		let result = txn.add_conflict_range(&key, &key_end, fdb::options::ConflictRangeType::Read);
		crate::fdb::retry!(result);

		checkout.touched_at = checkout.touched_at.max(touched_at);
		let value = checkout
			.serialize()
			.map_err(|error| tg::error!(!error, "failed to serialize the cache entry"))?;
		txn.set(&key, &value);
		if checkout.reference_count == 0 {
			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Checkout {
				id: id.clone(),
				partition,
				touched_at: checkout.touched_at,
			});
			let key = Self::pack(subspace, &key);
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&key, &[]);
		}

		Ok(ControlFlow::Break(Some(checkout)))
	}
}
