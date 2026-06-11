use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn get_resource_grants_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
	) -> tg::Result<Vec<(tg::grant::Principal, tg::grant::Permission)>> {
		let bytes = resource.to_bytes();
		let key = (Kind::ResourceGrant.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the resource grants"))?;

		let grants = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Grant(crate::fdb::grant::Key::ResourceGrant {
					principal,
					permission,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((principal, permission))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(grants)
	}

	pub(crate) async fn try_get_visibility_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		principal: &tg::grant::Principal,
	) -> tg::Result<bool> {
		let bytes = resource.to_bytes();
		let key = (
			Kind::Visibility.to_i32().unwrap(),
			bytes.as_ref(),
			principal.to_string(),
		);
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			limit: Some(1),
			..fdb::RangeOption::from(&range_subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the visibility entries"))?;
		Ok(!entries.is_empty())
	}
}
