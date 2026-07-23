use {
	crate::lmdb::{Db, Index, ItemKind, Key, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
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
		let request = Request::TouchObjects(crate::lmdb::TouchObjects {
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

	pub(crate) fn touch_objects_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::object::Object>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		for id in ids {
			let key = Key::Object(crate::lmdb::object::Key::Object(id.clone()));
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
			let existing = existing
				.map(crate::object::Object::deserialize)
				.transpose()?;
			let Some(mut object) = existing else {
				outputs.push(None);
				continue;
			};
			if touched_at - object.touched_at < time_to_touch {
				outputs.push(Some(object));
				continue;
			}
			object.touched_at = object.touched_at.max(touched_at);
			let value = object.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;
			if object.reference_count == 0 {
				let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Clean {
					touched_at: object.touched_at,
					kind: ItemKind::Object,
					id: id.clone().into(),
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;
			}
			outputs.push(Some(object));
		}
		Ok(outputs)
	}
}
