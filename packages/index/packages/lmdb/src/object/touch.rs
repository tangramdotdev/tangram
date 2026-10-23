use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	std::time::Duration,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn touch_objects_with_account_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::object::Id],
		account: Option<&tangram_index::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<tangram_index::object::Object>>> {
		let objects = Self::touch_objects_with_transaction(
			db,
			subspace,
			transaction,
			ids,
			touched_at,
			time_to_touch,
		)?;
		if let Some(account) = account {
			for (id, object) in std::iter::zip(ids, &objects) {
				if object.is_none() {
					continue;
				}
				let entry = tangram_index::usage::storage::put::ObjectArg {
					account: account.clone(),
					object: id.clone(),
					touched_at,
				};
				Self::touch_account_object(db, subspace, transaction, &entry, time_to_touch)?;
			}
		}

		Ok(objects)
	}

	pub(crate) fn touch_objects_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<tangram_index::object::Object>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		for id in ids {
			let key = Key::Object(crate::object::Key::Object(id.clone()));
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
			let existing = existing
				.map(tangram_index::object::Object::deserialize)
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
				let key = crate::Key::Clean(crate::clean::Key::Object {
					id: id.clone(),
					touched_at: object.touched_at,
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
