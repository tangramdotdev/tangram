use {
	crate::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn try_get_organizations_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::organization::Id],
	) -> tg::Result<
		ControlFlow<Vec<Option<tangram_index::organization::Organization>>, fdb::FdbError>,
	> {
		let organizations = {
			let result = futures::future::try_join_all(
				ids.iter()
					.map(|id| Self::try_get_organization_with_transaction(txn, subspace, id)),
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

		Ok(ControlFlow::Break(organizations))
	}

	pub(crate) async fn try_get_organization_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::organization::Id,
	) -> tg::Result<ControlFlow<Option<tangram_index::organization::Organization>, fdb::FdbError>>
	{
		let key = Key::Organization(crate::organization::Key::Organization(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let organization = Some(tangram_index::organization::Organization::deserialize(
			&bytes,
		)?);

		Ok(ControlFlow::Break(organization))
	}

	pub(crate) async fn get_member_organizations_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		member: &tg::Id,
	) -> tg::Result<ControlFlow<Vec<tg::organization::Id>, fdb::FdbError>> {
		crate::page::collect(|after| async move {
			Self::get_member_organizations_page_with_transaction(
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

	pub(crate) async fn get_member_organizations_page_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		member: &tg::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<ControlFlow<crate::page::Page<tg::organization::Id>, fdb::FdbError>> {
		let bytes = member.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::MemberOrganization.to_i32().unwrap(), bytes.as_ref()),
		);
		let (entries, cursor) = crate::propagate!(
			Self::get_entry_page_with_transaction(txn, subspace, &prefix, after, limit).await
		);
		let items = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Organization(crate::organization::Key::MemberOrganization {
					organization,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(organization)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break((items, cursor)))
	}
}
