use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn list_sandboxes_for_creator(
		&self,
		creator: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		let request = crate::read::Request::ListSandboxesForCreator {
			creator: creator.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::ListSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn list_sandboxes_for_owner(
		&self,
		owner: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		let request = crate::read::Request::ListSandboxesForOwner {
			owner: owner.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::ListSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn list_sandboxes(
		&self,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		let response = self
			.send_read_request(crate::read::Request::ListSandboxes)
			.await?;
		let crate::read::Response::ListSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn list_sandboxes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		let prefix = Self::pack(subspace, &(Kind::Sandbox.to_i32().unwrap(),));
		let entries = txn
			.get_range(
				&fdb::RangeOption {
					mode: fdb::options::StreamingMode::WantAll,
					..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
				},
				1,
				false,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the sandboxes"))?;
		entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Sandbox(crate::fdb::sandbox::Key::Sandbox(id)) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				let sandbox = crate::sandbox::Sandbox::deserialize(entry.value())?;
				Ok((id, sandbox))
			})
			.collect()
	}

	pub(crate) async fn list_sandboxes_for_principal_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		principal: &tg::Principal,
		kind: Kind,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		let prefix = Self::pack(subspace, &(kind.to_i32().unwrap(), principal.to_string()));
		let entries = txn
			.get_range(
				&fdb::RangeOption {
					mode: fdb::options::StreamingMode::WantAll,
					..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
				},
				1,
				false,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the principal sandboxes"))?;
		let sandboxes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let sandbox =
					match key {
						Key::Sandbox(crate::fdb::sandbox::Key::CreatorSandbox {
							sandbox, ..
						}) if kind == Kind::CreatorSandbox => sandbox,
						Key::Sandbox(crate::fdb::sandbox::Key::OwnerSandbox {
							sandbox, ..
						}) if kind == Kind::OwnerSandbox => sandbox,
						_ => return Err(tg::error!("unexpected key type")),
					};
				Ok(sandbox)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		drop(entries);
		let mut output = Vec::with_capacity(sandboxes.len());
		for sandbox in sandboxes {
			let data = Self::try_get_sandbox_with_transaction(txn, subspace, &sandbox)
				.await?
				.ok_or_else(|| tg::error!(%sandbox, "failed to find the principal sandbox"))?;
			output.push((sandbox, data));
		}
		Ok(output)
	}
}
