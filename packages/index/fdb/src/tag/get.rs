use {
	crate::{Index, Key},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
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
