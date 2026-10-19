use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn try_get_groups_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::group::Id],
	) -> tg::Result<Vec<Option<tangram_index::group::Group>>> {
		ids.iter()
			.map(|id| Self::try_get_group_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_group_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::group::Id,
	) -> tg::Result<Option<tangram_index::group::Group>> {
		let key = Key::Group(crate::group::Key::Group(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the group"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(tangram_index::group::Group::deserialize(bytes)?))
	}

	pub(crate) fn get_member_groups_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		member: &tg::Id,
	) -> tg::Result<Vec<tg::group::Id>> {
		crate::page::collect(|after| {
			Self::get_member_groups_page_with_transaction(
				db,
				subspace,
				transaction,
				member,
				after,
				crate::page::PAGE_SIZE,
			)
		})
	}

	pub(crate) fn get_member_groups_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		member: &tg::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<crate::page::Page<tg::group::Id>> {
		let bytes = member.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::MemberGroup.to_i32().unwrap(), bytes.as_ref()),
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
				let Key::Group(crate::group::Key::MemberGroup { group, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(group)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok((items, cursor))
	}
}
