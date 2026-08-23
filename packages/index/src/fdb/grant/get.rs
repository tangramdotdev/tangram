use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn get_resource_grants_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
	) -> tg::Result<
		ControlFlow<
			Vec<(
				tg::authorization::Subject,
				tg::authorization::Permission,
				bool,
			)>,
			fdb::FdbError,
		>,
	> {
		let bytes = resource.to_bytes();
		let key = (Kind::ResourceGrant.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);

		let grants = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Grant(crate::fdb::grant::Key::ResourceGrant {
					creator,
					permission,
					subject,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				let value = crate::fdb::grant::GrantValue::deserialize(entry.value())?;
				let process_implicit = crate::fdb::grant::is_process_implicit(
					creator.as_ref(),
					value.implicit,
					&subject,
				);
				Ok((subject, permission, process_implicit))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(grants))
	}

	pub(crate) async fn get_resource_grant_entries_for_subject_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<ControlFlow<Vec<crate::fdb::grant::GrantEntry>, fdb::FdbError>> {
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

		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);

		let entries = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Grant(crate::fdb::grant::Key::ResourceGrant {
					creator,
					permission,
					subject,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				let value = crate::fdb::grant::GrantValue::deserialize(entry.value())?;
				Ok(crate::fdb::grant::GrantEntry {
					creator,
					explicit: value.explicit,
					implicit: value.implicit,
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
