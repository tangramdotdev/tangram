use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn get_resource_permission_entries_for_subject_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<ControlFlow<Vec<crate::fdb::permission::PermissionEntry>, fdb::FdbError>> {
		let bytes = resource.to_bytes();
		let key = (
			Kind::ResourcePermission.to_i32().unwrap(),
			bytes.as_ref(),
			subject.to_string(),
		);
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);

		let entries = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Permission(crate::fdb::permission::Key::ResourcePermission {
					creator,
					permission,
					subject,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				let value = crate::fdb::permission::PermissionValue::deserialize(entry.value())?;
				Ok(crate::fdb::permission::PermissionEntry {
					creator,
					grant: value.grant,
					direct: value.direct,
					materialized: value.materialized,
					permission,
					subject,
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(entries))
	}

	pub(crate) async fn try_get_visibility_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
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
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);

		Ok(ControlFlow::Break(!entries.is_empty()))
	}
}
