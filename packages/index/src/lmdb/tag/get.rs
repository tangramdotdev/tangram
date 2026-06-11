use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
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

	pub(crate) fn get_item_tags_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		item: &[u8],
	) -> tg::Result<Vec<tg::tag::Id>> {
		let prefix = &(Kind::ItemTag.to_i32().unwrap(), item);
		let prefix = Self::pack(subspace, prefix);
		let mut tags = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the item tags"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read the item tag entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Tag(crate::lmdb::tag::Key::ItemTag { tag, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			tags.push(tag);
		}
		Ok(tags)
	}
}
