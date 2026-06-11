use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn try_get_tag_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::tag::Id,
	) -> tg::Result<Option<crate::tag::Tag>> {
		let key = Key::Tag(crate::fdb::tag::Key::Tag(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the tag"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::tag::Tag::deserialize(&bytes)?))
	}

	pub(crate) async fn get_item_tags_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		item: &[u8],
	) -> tg::Result<Vec<tg::tag::Id>> {
		let key = (Kind::ItemTag.to_i32().unwrap(), item);
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the item tags"))?;

		let tags = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Tag(crate::fdb::tag::Key::ItemTag { tag, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(tag)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(tags)
	}
}
