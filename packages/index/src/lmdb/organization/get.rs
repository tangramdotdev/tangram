use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn get_organization_members_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		organization: &tg::organization::Id,
	) -> tg::Result<Vec<tg::organization::Member>> {
		let organization_bytes = tg::Id::from(organization.clone()).to_bytes();
		let prefix = &(
			Kind::OrganizationMember.to_i32().unwrap(),
			organization_bytes.as_ref(),
		);
		let prefix = Self::pack(subspace, prefix);
		let mut members = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the organization members"))?;
		for entry in iter {
			let (key, _) = entry.map_err(|error| {
				tg::error!(!error, "failed to read the organization member entry")
			})?;
			let key = Self::unpack(subspace, key)?;
			let Key::Organization(crate::lmdb::organization::Key::OrganizationMember {
				member,
				..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			members.push(member);
		}
		Ok(members)
	}

	pub(crate) fn get_member_organizations_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		member: &tg::Id,
	) -> tg::Result<Vec<tg::organization::Id>> {
		let member_bytes = member.to_bytes();
		let prefix = &(
			Kind::MemberOrganization.to_i32().unwrap(),
			member_bytes.as_ref(),
		);
		let prefix = Self::pack(subspace, prefix);
		let mut organizations = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the member organizations"))?;
		for entry in iter {
			let (key, _) = entry.map_err(|error| {
				tg::error!(!error, "failed to read the member organization entry")
			})?;
			let key = Self::unpack(subspace, key)?;
			let Key::Organization(crate::lmdb::organization::Key::MemberOrganization {
				organization,
				..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			organizations.push(organization);
		}
		Ok(organizations)
	}
}
