use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_organizations(
		&self,
		ids: &[tg::organization::Id],
	) -> tg::Result<Vec<Option<crate::organization::Organization>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetOrganizations {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetOrganizations(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_organizations_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::organization::Id],
	) -> crate::fdb::Result<Vec<Option<crate::organization::Organization>>> {
		futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_organization_with_transaction(txn, subspace, id)),
		)
		.await
	}

	pub(crate) async fn try_get_organization_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::organization::Id,
	) -> crate::fdb::Result<Option<crate::organization::Organization>> {
		let key = Key::Organization(crate::fdb::organization::Key::Organization(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn.get(&key, false).await?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(
			crate::organization::Organization::deserialize(&bytes)
				.map_err(crate::fdb::custom_error)?,
		))
	}

	pub(crate) async fn get_organization_members_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		organization: &tg::organization::Id,
	) -> crate::fdb::Result<Vec<tg::organization::Member>> {
		let bytes = tg::Id::from(organization.clone()).to_bytes();
		let key = (Kind::OrganizationMember.to_i32().unwrap(), bytes.as_ref());
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
				let Key::Organization(crate::fdb::organization::Key::OrganizationMember {
					member,
					..
				}) = key
				else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				Ok(member)
			})
			.collect::<crate::fdb::Result<Vec<_>>>()?;

		Ok(members)
	}

	pub(crate) async fn get_member_organizations_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		member: &tg::Id,
	) -> crate::fdb::Result<Vec<tg::organization::Id>> {
		let bytes = member.to_bytes();
		let key = (Kind::MemberOrganization.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn.get_range(&range, 1, false).await?;

		let organizations = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Organization(crate::fdb::organization::Key::MemberOrganization {
					organization,
					..
				}) = key
				else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				Ok(organization)
			})
			.collect::<crate::fdb::Result<Vec<_>>>()?;

		Ok(organizations)
	}
}
