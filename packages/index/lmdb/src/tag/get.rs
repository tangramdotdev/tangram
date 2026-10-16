use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn get_target_tags_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
	) -> tg::Result<Vec<tg::tag::Id>> {
		let bytes = resource.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::TargetTag.to_i32().unwrap(), bytes.as_ref()),
		);
		let mut tags = Vec::new();
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the target tags"))?;
		for entry in entries {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read the target tag"))?;
			let Key::Tag(crate::tag::Key::TargetTag { tag, .. }) = Self::unpack(subspace, key)?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			tags.push(tag);
		}
		Ok(tags)
	}

	pub(crate) fn try_get_tag_target_creator_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		tag: &tg::tag::Id,
	) -> tg::Result<Option<tg::authorization::Subject>> {
		let key = Key::Tag(crate::tag::Key::TargetTag {
			target: resource.to_bytes().to_vec(),
			tag: tag.clone(),
		});
		let key = Self::pack(subspace, &key);
		let exists = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the target tag"))?
			.is_some();
		if !exists {
			return Ok(None);
		}
		let tag = Self::try_get_tag_with_transaction(db, subspace, transaction, tag)?;
		let creator = tag.map(|tag| tag.creator);
		Ok(creator)
	}

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
