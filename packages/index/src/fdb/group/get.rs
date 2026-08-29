use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
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
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::group::Id],
	) -> tg::Result<ControlFlow<Vec<Option<crate::group::Group>>, fdb::FdbError>> {
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
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		id: &tg::group::Id,
	) -> tg::Result<ControlFlow<Option<crate::group::Group>, fdb::FdbError>> {
		let key = Key::Group(crate::fdb::group::Key::Group(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::fdb::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let group = Some(crate::group::Group::deserialize(&bytes)?);

		Ok(ControlFlow::Break(group))
	}

	pub(crate) async fn get_member_groups_and_organizations_with_transaction(
		txn: &crate::fdb::Transaction,
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
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		member: &tg::Id,
	) -> tg::Result<ControlFlow<Vec<tg::group::Id>, fdb::FdbError>> {
		let bytes = member.to_bytes();
		let key = (Kind::MemberGroup.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);

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

		Ok(ControlFlow::Break(groups))
	}
}
