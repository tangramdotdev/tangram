use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::future,
	std::time::Duration,
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
	) -> tg::Result<Vec<Option<crate::object::Object>>> {
		let objects = Self::touch_objects_with_transaction(
			txn,
			subspace,
			ids,
			touched_at,
			time_to_touch,
			partition_total,
		)
		.await?;
		if let Some(account) = account {
			future::try_join_all(
				std::iter::zip(ids, &objects)
					.filter(|(_, object)| object.is_some())
					.map(|(id, _)| {
						let arg = crate::storage::put::ObjectArg {
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
			.await?;
		}

		Ok(objects)
	}

	pub(crate) async fn touch_objects_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Vec<Option<crate::object::Object>>> {
		future::try_join_all(ids.iter().map(|id| {
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
		.await
	}

	async fn touch_object_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Option<crate::object::Object>> {
		let key = Key::Object(crate::fdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
		let existing = existing
			.as_ref()
			.map(|bytes| crate::object::Object::deserialize(bytes))
			.transpose()?;
		let Some(mut object) = existing else {
			return Ok(None);
		};
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if touched_at - object.touched_at < time_to_touch {
			return Ok(Some(object));
		}

		let mut key_end = key.clone();
		key_end.push(0x00);
		txn.add_conflict_range(&key, &key_end, fdb::options::ConflictRangeType::Read)
			.map_err(|error| tg::error!(!error, "failed to add read conflict range"))?;

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

		Ok(Some(object))
	}
}
