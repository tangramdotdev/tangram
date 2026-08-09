use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn get_resource_grants_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
	) -> tg::Result<Vec<(tg::grant::Principal, tg::grant::Permission)>> {
		let resource_bytes = resource.to_bytes();
		let prefix = &(
			Kind::ResourceGrant.to_i32().unwrap(),
			resource_bytes.as_ref(),
		);
		let prefix = Self::pack(subspace, prefix);
		let mut grants = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the resource grants"))?;
		for entry in iter {
			let (key, _) = entry
				.map_err(|error| tg::error!(!error, "failed to read the resource grant entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Grant(crate::lmdb::grant::Key::ResourceGrant {
				principal,
				permission,
				..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			grants.push((principal, permission));
		}
		Ok(grants)
	}

	pub(crate) fn get_resource_grant_entries_for_principal_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		principal: &tg::grant::Principal,
	) -> tg::Result<Vec<crate::lmdb::grant::GrantEntry>> {
		let resource_bytes = resource.to_bytes();
		let prefix = &(
			Kind::ResourceGrant.to_i32().unwrap(),
			resource_bytes.as_ref(),
			principal.to_string(),
		);
		let prefix = Self::pack(subspace, prefix);
		let mut grants = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the resource grants"))?;
		for entry in iter {
			let (key, value) = entry
				.map_err(|error| tg::error!(!error, "failed to read the resource grant entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Grant(crate::lmdb::grant::Key::ResourceGrant {
				principal,
				permission,
				..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let value = crate::lmdb::grant::GrantValue::deserialize(value)?;
			grants.push(crate::lmdb::grant::GrantEntry {
				explicit: value.explicit,
				temporary: value.temporary,
				materialized: value.materialized,
				permission,
				principal,
			});
		}
		Ok(grants)
	}

	pub(crate) fn try_get_visibility_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		principal: &tg::grant::Principal,
	) -> tg::Result<bool> {
		let resource_bytes = resource.to_bytes();
		let prefix = &(
			Kind::Visibility.to_i32().unwrap(),
			resource_bytes.as_ref(),
			principal.to_string(),
		);
		let prefix = Self::pack(subspace, prefix);
		let mut iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the visibility entries"))?;
		let Some(entry) = iter.next() else {
			return Ok(false);
		};
		entry.map_err(|error| tg::error!(!error, "failed to read the visibility entry"))?;
		Ok(true)
	}
}
