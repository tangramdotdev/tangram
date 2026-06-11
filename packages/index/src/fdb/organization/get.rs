use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn get_organization_members_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		organization: &tg::organization::Id,
	) -> tg::Result<Vec<tg::organization::Member>> {
		let bytes = tg::Id::from(organization.clone()).to_bytes();
		let key = (Kind::OrganizationMember.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the organization members"))?;

		let members = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Organization(crate::fdb::organization::Key::OrganizationMember {
					member,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(member)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(members)
	}

	pub(crate) async fn get_member_organizations_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		member: &tg::Id,
	) -> tg::Result<Vec<tg::organization::Id>> {
		let bytes = member.to_bytes();
		let key = (Kind::MemberOrganization.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the member organizations"))?;

		let organizations = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Organization(crate::fdb::organization::Key::MemberOrganization {
					organization,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(organization)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(organizations)
	}
}
