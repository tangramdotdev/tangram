use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn try_get_tag_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::tag::Id,
	) -> tg::Result<ControlFlow<Option<crate::tag::Tag>, fdb::FdbError>> {
		let key = Key::Tag(crate::fdb::tag::Key::Tag(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = crate::fdb::retry!(txn.get(&key, false).await);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let tag = Some(crate::tag::Tag::deserialize(&bytes)?);

		Ok(ControlFlow::Break(tag))
	}

	pub(crate) async fn get_target_tags_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		target: &[u8],
	) -> tg::Result<ControlFlow<Vec<tg::tag::Id>, fdb::FdbError>> {
		let key = (Kind::TargetTag.to_i32().unwrap(), target);
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = crate::fdb::retry!(txn.get_range(&range, 1, false).await);

		let tags = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Tag(crate::fdb::tag::Key::TargetTag { tag, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(tag)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(tags))
	}
}
