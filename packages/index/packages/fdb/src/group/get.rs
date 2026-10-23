use {
	crate::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn try_get_groups_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::group::Id],
	) -> tg::Result<ControlFlow<Vec<Option<tangram_index::group::Group>>, fdb::FdbError>> {
		let groups = {
			let result = futures::future::try_join_all(
				ids.iter()
					.map(|id| Self::try_get_group_with_transaction(txn, subspace, id)),
			)
			.await;
			let results = result?;
			let mut values = Vec::with_capacity(results.len());
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};

		Ok(ControlFlow::Break(groups))
	}

	pub(crate) async fn try_get_group_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::group::Id,
	) -> tg::Result<ControlFlow<Option<tangram_index::group::Group>, fdb::FdbError>> {
		let key = Key::Group(crate::group::Key::Group(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let group = Some(tangram_index::group::Group::deserialize(&bytes)?);

		Ok(ControlFlow::Break(group))
	}

	pub(crate) async fn get_member_groups_and_organizations_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		member: &tg::Id,
	) -> tg::Result<ControlFlow<(Vec<tg::group::Id>, Vec<tg::organization::Id>), fdb::FdbError>> {
		let (groups, organizations) = futures::try_join!(
			Self::get_member_groups_with_transaction(txn, subspace, member),
			Self::get_member_organizations_with_transaction(txn, subspace, member),
		)?;
		let groups = match groups {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let organizations = match organizations {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};

		Ok(ControlFlow::Break((groups, organizations)))
	}

	pub(crate) async fn get_member_groups_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		member: &tg::Id,
	) -> tg::Result<ControlFlow<Vec<tg::group::Id>, fdb::FdbError>> {
		crate::page::collect(|after| async move {
			Self::get_member_groups_page_with_transaction(
				txn,
				subspace,
				member,
				after.as_deref(),
				crate::page::PAGE_SIZE,
			)
			.await
		})
		.await
	}

	pub(crate) async fn get_member_groups_page_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		member: &tg::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<ControlFlow<crate::page::Page<tg::group::Id>, fdb::FdbError>> {
		let bytes = member.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::MemberGroup.to_i32().unwrap(), bytes.as_ref()),
		);
		let (entries, cursor) = crate::propagate!(
			Self::get_entry_page_with_transaction(txn, subspace, &prefix, after, limit).await
		);
		let items = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Group(crate::group::Key::MemberGroup { group, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(group)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break((items, cursor)))
	}
}
