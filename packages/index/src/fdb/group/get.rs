use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn try_get_group_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::group::Id,
	) -> tg::Result<Option<crate::group::Group>> {
		let key = Key::Group(crate::fdb::group::Key::Group(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the group"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::group::Group::deserialize(&bytes)?))
	}

	pub(crate) async fn get_group_members_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		group: &tg::group::Id,
	) -> tg::Result<Vec<tg::group::Member>> {
		let bytes = tg::Id::from(group.clone()).to_bytes();
		let key = (Kind::GroupMember.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the group members"))?;

		let members = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Group(crate::fdb::group::Key::GroupMember { member, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(member)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(members)
	}

	pub(crate) async fn get_member_groups_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		member: &tg::Id,
	) -> tg::Result<Vec<tg::group::Id>> {
		let bytes = member.to_bytes();
		let key = (Kind::MemberGroup.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the member groups"))?;

		let groups = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Group(crate::fdb::group::Key::MemberGroup { group, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(group)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(groups)
	}
}
