use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn try_get_organizations_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::organization::Id],
	) -> tg::Result<Vec<Option<tangram_index::organization::Organization>>> {
		ids.iter()
			.map(|id| Self::try_get_organization_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_organization_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::organization::Id,
	) -> tg::Result<Option<tangram_index::organization::Organization>> {
		let key = Key::Organization(crate::organization::Key::Organization(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the organization"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(
			tangram_index::organization::Organization::deserialize(bytes)?,
		))
	}

	pub(crate) fn get_member_organizations_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		member: &tg::Id,
	) -> tg::Result<Vec<tg::organization::Id>> {
		crate::page::collect(|after| {
			Self::get_member_organizations_page_with_transaction(
				db,
				subspace,
				transaction,
				member,
				after,
				crate::page::PAGE_SIZE,
			)
		})
	}

	pub(crate) fn get_member_organizations_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		member: &tg::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<crate::page::Page<tg::organization::Id>> {
		let bytes = member.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::MemberOrganization.to_i32().unwrap(), bytes.as_ref()),
		);
		let (entries, cursor) = Self::get_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after,
			limit,
		)?;
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

		Ok((items, cursor))
	}
}
