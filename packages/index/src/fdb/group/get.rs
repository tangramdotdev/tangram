use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
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

	pub(crate) async fn try_get_groups_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::group::Id],
	) -> crate::fdb::Result<Vec<Option<crate::group::Group>>> {
		futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_group_with_transaction(txn, subspace, id)),
		)
		.await
	}

	pub(crate) async fn try_get_group_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::group::Id,
	) -> crate::fdb::Result<Option<crate::group::Group>> {
		let key = Key::Group(crate::fdb::group::Key::Group(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn.get(&key, false).await?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(
			crate::group::Group::deserialize(&bytes).map_err(crate::fdb::custom_error)?,
		))
	}

	pub(crate) async fn get_group_members_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		group: &tg::group::Id,
	) -> crate::fdb::Result<Vec<tg::group::Member>> {
		let bytes = tg::Id::from(group.clone()).to_bytes();
		let key = (Kind::GroupMember.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn.get_range(&range, 1, false).await?;

		let members = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Group(crate::fdb::group::Key::GroupMember { member, .. }) = key else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				Ok(member)
			})
			.collect::<crate::fdb::Result<Vec<_>>>()?;

		Ok(members)
	}

	pub(crate) async fn get_member_groups_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		member: &tg::Id,
	) -> crate::fdb::Result<Vec<tg::group::Id>> {
		let bytes = member.to_bytes();
		let key = (Kind::MemberGroup.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn.get_range(&range, 1, false).await?;

		let groups = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Group(crate::fdb::group::Key::MemberGroup { group, .. }) = key else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				Ok(group)
			})
			.collect::<crate::fdb::Result<Vec<_>>>()?;

		Ok(groups)
	}
}
