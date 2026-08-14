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
	) -> crate::fdb::Result<Option<crate::tag::Tag>> {
		let key = Key::Tag(crate::fdb::tag::Key::Tag(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn.get(&key, false).await?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(
			crate::tag::Tag::deserialize(&bytes).map_err(crate::fdb::custom_error)?,
		))
	}

	pub(crate) async fn get_target_tags_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		target: &[u8],
	) -> crate::fdb::Result<Vec<tg::tag::Id>> {
		let key = (Kind::TargetTag.to_i32().unwrap(), target);
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn.get_range(&range, 1, false).await?;

		let tags = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Tag(crate::fdb::tag::Key::TargetTag { tag, .. }) = key else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				Ok(tag)
			})
			.collect::<crate::fdb::Result<Vec<_>>>()?;

		Ok(tags)
	}
}
