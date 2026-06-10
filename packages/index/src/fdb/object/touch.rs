use {
	crate::fdb::{Index, ItemKind, Key, Request, Response},
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
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::TouchObjects(crate::fdb::TouchObjects {
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		});
		self.sender_high
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Objects(objects) = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(objects)
	}

	pub(crate) async fn task_touch_objects(
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
			let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
				partition,
				touched_at: object.touched_at,
				kind: ItemKind::Object,
				id: tg::Either::Left(id.clone()),
			});
			let key = Self::pack(subspace, &key);
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&key, &[]);
		}

		Ok(Some(object))
	}
}
