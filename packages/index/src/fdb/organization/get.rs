use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
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
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::organization::Id],
	) -> tg::Result<ControlFlow<Vec<Option<crate::organization::Organization>>, fdb::FdbError>> {
		let organizations = {
			let result = futures::future::try_join_all(
				ids.iter()
					.map(|id| Self::try_get_organization_with_transaction(txn, subspace, id)),
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

		Ok(ControlFlow::Break(organizations))
	}

	pub(crate) async fn try_get_organization_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		id: &tg::organization::Id,
	) -> tg::Result<ControlFlow<Option<crate::organization::Organization>, fdb::FdbError>> {
		let key = Key::Organization(crate::fdb::organization::Key::Organization(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::fdb::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let organization = Some(crate::organization::Organization::deserialize(&bytes)?);

		Ok(ControlFlow::Break(organization))
	}

	pub(crate) async fn get_member_organizations_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		member: &tg::Id,
	) -> tg::Result<ControlFlow<Vec<tg::organization::Id>, fdb::FdbError>> {
		let bytes = member.to_bytes();
		let key = (Kind::MemberOrganization.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);

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

		Ok(ControlFlow::Break(organizations))
	}
}
