use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn try_get_tags_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::tag::Id],
	) -> tg::Result<Vec<Option<tangram_index::tag::Tag>>> {
		ids.iter()
			.map(|id| Self::try_get_tag_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_tag_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::tag::Id,
	) -> tg::Result<Option<tangram_index::tag::Tag>> {
		let key = Key::Tag(crate::tag::Key::Tag(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the tag"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(tangram_index::tag::Tag::deserialize(bytes)?))
	}
}
