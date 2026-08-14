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
	) -> crate::fdb::Result<Vec<(tg::authorization::Subject, tg::authorization::Permission)>> {
		let bytes = resource.to_bytes();
		let key = (Kind::ResourceGrant.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn.get_range(&range, 1, false).await?;

		let grants = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Grant(crate::fdb::grant::Key::ResourceGrant {
					subject,
					permission,
					..
				}) = key
				else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				Ok((subject, permission))
			})
			.collect::<crate::fdb::Result<Vec<_>>>()?;

		Ok(grants)
	}

	pub(crate) async fn get_resource_grant_entries_for_subject_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> crate::fdb::Result<Vec<crate::fdb::grant::GrantEntry>> {
		let bytes = resource.to_bytes();
		let key = (
			Kind::ResourceGrant.to_i32().unwrap(),
			bytes.as_ref(),
			subject.to_string(),
		);
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn.get_range(&range, 1, false).await?;

		entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Grant(crate::fdb::grant::Key::ResourceGrant {
					subject,
					permission,
					..
				}) = key
				else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				let value = crate::fdb::grant::GrantValue::deserialize(entry.value())
					.map_err(crate::fdb::custom_error)?;
				Ok(crate::fdb::grant::GrantEntry {
					explicit: value.explicit,
					materialized: value.materialized,
					permission,
					subject,
					temporary: value.temporary,
				})
			})
			.collect()
	}

	pub(crate) async fn try_get_visibility_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> crate::fdb::Result<bool> {
		let bytes = resource.to_bytes();
		let key = (
			Kind::Visibility.to_i32().unwrap(),
			bytes.as_ref(),
			subject.to_string(),
		);
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			limit: Some(1),
			..fdb::RangeOption::from(&range_subspace)
		};
		let entries = txn.get_range(&range, 1, false).await?;
		Ok(!entries.is_empty())
	}
}
