use {
	crate::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn get_target_tags_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
	) -> tg::Result<ControlFlow<Vec<tg::tag::Id>, fdb::FdbError>> {
		let bytes = resource.to_bytes();
		let prefix = Self::pack(subspace, &(Kind::TargetTag.to_i32().unwrap(), bytes.as_ref()));
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption::from(&range_subspace);
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let mut tags = Vec::new();
		for entry in entries {
			let Key::Tag(crate::tag::Key::TargetTag { tag, .. }) =
				Self::unpack(subspace, entry.key())?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			tags.push(tag);
		}

		Ok(ControlFlow::Break(tags))
	}

	pub(crate) async fn try_get_tag_target_creator_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		tag: &tg::tag::Id,
	) -> tg::Result<ControlFlow<Option<tg::authorization::Subject>, fdb::FdbError>> {
		let key = Key::Tag(crate::tag::Key::TargetTag {
			target: resource.to_bytes().to_vec(),
			tag: tag.clone(),
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let exists = crate::retry!(result).is_some();
		if !exists {
			return Ok(ControlFlow::Break(None));
		}
		let tag = crate::propagate!(Self::try_get_tag_with_transaction(txn, subspace, tag).await);
		let creator = tag.map(|tag| tag.creator);

		Ok(ControlFlow::Break(creator))
	}

	pub(crate) async fn try_get_tags_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		ids: &[tg::tag::Id],
	) -> tg::Result<ControlFlow<Vec<Option<tangram_index::tag::Tag>>, fdb::FdbError>> {
		let result = futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_tag_with_transaction(txn, subspace, id)),
		)
		.await;
		let results = result?;
		let mut tags = Vec::with_capacity(results.len());
		for result in results {
			let tag = match result {
				ControlFlow::Break(tag) => tag,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			tags.push(tag);
		}

		Ok(ControlFlow::Break(tags))
	}

	pub(crate) async fn try_get_tag_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::tag::Id,
	) -> tg::Result<ControlFlow<Option<tangram_index::tag::Tag>, fdb::FdbError>> {
		let key = Key::Tag(crate::tag::Key::Tag(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let tag = Some(tangram_index::tag::Tag::deserialize(&bytes)?);

		Ok(ControlFlow::Break(tag))
	}
}
