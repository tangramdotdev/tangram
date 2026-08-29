use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_groups(
		&self,
		ids: &[tg::group::Id],
	) -> tg::Result<Vec<Option<crate::group::Group>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetGroups {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetGroups(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_groups_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::group::Id],
	) -> tg::Result<Vec<Option<crate::group::Group>>> {
		ids.iter()
			.map(|id| Self::try_get_group_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_group_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::group::Id,
	) -> tg::Result<Option<crate::group::Group>> {
		let key = Key::Group(crate::lmdb::group::Key::Group(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the group"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::group::Group::deserialize(bytes)?))
	}

	pub(crate) fn get_member_groups_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		member: &tg::Id,
	) -> tg::Result<Vec<tg::group::Id>> {
		let member_bytes = member.to_bytes();
		let prefix = &(Kind::MemberGroup.to_i32().unwrap(), member_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let mut groups = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the member groups"))?;
		for entry in iter {
			let (key, _) = entry
				.map_err(|error| tg::error!(!error, "failed to read the member group entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Group(crate::lmdb::group::Key::MemberGroup { group, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			groups.push(group);
		}
		Ok(groups)
	}
}
