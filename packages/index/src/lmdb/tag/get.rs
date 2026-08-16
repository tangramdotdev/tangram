use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_tags(
		&self,
		ids: &[tg::tag::Id],
	) -> tg::Result<Vec<Option<crate::tag::Tag>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetTags {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetTags(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_tags_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::tag::Id],
	) -> tg::Result<Vec<Option<crate::tag::Tag>>> {
		ids.iter()
			.map(|id| Self::try_get_tag_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_tag_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::tag::Id,
	) -> tg::Result<Option<crate::tag::Tag>> {
		let key = Key::Tag(crate::lmdb::tag::Key::Tag(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the tag"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::tag::Tag::deserialize(bytes)?))
	}

	pub(crate) fn get_target_tags_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		target: &[u8],
	) -> tg::Result<Vec<tg::tag::Id>> {
		let prefix = &(Kind::TargetTag.to_i32().unwrap(), target);
		let prefix = Self::pack(subspace, prefix);
		let mut tags = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the target tags"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read the target tag entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Tag(crate::lmdb::tag::Key::TargetTag { tag, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			tags.push(tag);
		}
		Ok(tags)
	}
}
