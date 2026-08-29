use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
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

	pub(crate) fn try_get_organizations_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::organization::Id],
	) -> tg::Result<Vec<Option<crate::organization::Organization>>> {
		ids.iter()
			.map(|id| Self::try_get_organization_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_organization_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::organization::Id,
	) -> tg::Result<Option<crate::organization::Organization>> {
		let key = Key::Organization(crate::lmdb::organization::Key::Organization(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the organization"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::organization::Organization::deserialize(bytes)?))
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
