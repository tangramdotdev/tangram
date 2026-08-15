use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::future,
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::object::Object>>> {
		self.touch_objects_with_account(ids, None, touched_at, time_to_touch)
			.await
	}

	pub async fn touch_objects_with_account(
		&self,
		ids: &[tg::object::Id],
		account: Option<&crate::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::object::Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = Request::TouchObjects(crate::fdb::TouchObjects {
			account: account.cloned(),
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		});
		let response = self.send_write_request(request).await?;
		let Response::Objects(objects) = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(objects)
	}

	pub(crate) async fn touch_objects_with_account_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::object::Id],
		account: Option<&crate::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<Vec<Option<crate::object::Object>>, fdb::FdbError>> {
		let objects = crate::fdb::propagate!(
			Self::touch_objects_with_transaction(
				txn,
				subspace,
				ids,
				touched_at,
				time_to_touch,
				partition_total,
			)
			.await
		);
		if let Some(account) = account {
			{
				let result = future::try_join_all(
					std::iter::zip(ids, &objects)
						.filter(|(_, object)| object.is_some())
						.map(|(id, _)| {
							let arg = crate::usage::storage::put::ObjectArg {
								account: account.clone(),
								object: id.clone(),
								touched_at,
							};
							async move {
								Self::touch_account_object(
									txn,
									subspace,
									&arg,
									time_to_touch,
									partition_total,
								)
								.await
							}
						}),
				)
				.await;
				let results = result?;
				for result in results {
					match result {
						ControlFlow::Break(()) => {},
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					}
				}
			};
		}

		Ok(ControlFlow::Break(objects))
	}

	pub(crate) async fn touch_objects_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<Vec<Option<crate::object::Object>>, fdb::FdbError>> {
		let objects = {
			let result = future::try_join_all(ids.iter().map(|id| {
				let subspace = subspace.clone();
				async move {
					Self::touch_object_with_transaction(
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

		Ok(ControlFlow::Break(objects))
	}

	async fn touch_object_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<Option<crate::object::Object>, fdb::FdbError>> {
		let key = Key::Object(crate::fdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let existing = crate::fdb::retry!(result);
		let existing = existing
			.as_ref()
			.map(|bytes| crate::object::Object::deserialize(bytes))
			.transpose()?;
		let Some(mut object) = existing else {
			return Ok(ControlFlow::Break(None));
		};
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if touched_at - object.touched_at < time_to_touch {
			return Ok(ControlFlow::Break(Some(object)));
		}

		let mut key_end = key.clone();
		key_end.push(0x00);
		let result = txn.add_conflict_range(&key, &key_end, fdb::options::ConflictRangeType::Read);
		crate::fdb::retry!(result);

		object.touched_at = object.touched_at.max(touched_at);
		let value = object
			.serialize()
			.map_err(|error| tg::error!(!error, "failed to serialize the object"))?;
		txn.set(&key, &value);
		if object.reference_count == 0 {
			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Object {
				id: id.clone(),
				partition,
				touched_at: object.touched_at,
			});
			let key = Self::pack(subspace, &key);
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&key, &[]);
		}

		Ok(ControlFlow::Break(Some(object)))
	}
}
