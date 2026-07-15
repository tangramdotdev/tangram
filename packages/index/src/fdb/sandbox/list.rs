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
		self.list_sandboxes_for_principal(creator, Kind::CreatorSandbox)
			.await
	}

	pub async fn list_sandboxes_for_owner(
		&self,
		owner: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		self.list_sandboxes_for_principal(owner, Kind::OwnerSandbox)
			.await
	}

	pub async fn list_sandboxes(
		&self,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		let prefix = Self::pack(&self.subspace, &(Kind::Sandbox.to_i32().unwrap(),));
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
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
				let key = Self::unpack(&self.subspace, entry.key())?;
				let Key::Sandbox(crate::fdb::sandbox::Key::Sandbox(id)) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				let sandbox = crate::sandbox::Sandbox::deserialize(entry.value())?;
				Ok((id, sandbox))
			})
			.collect()
	}

	async fn list_sandboxes_for_principal(
		&self,
		principal: &tg::Principal,
		kind: Kind,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		let prefix = Self::pack(
			&self.subspace,
			&(kind.to_i32().unwrap(), principal.to_string()),
		);
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
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
				let key = Self::unpack(&self.subspace, entry.key())?;
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
			let data = Self::try_get_sandbox_with_transaction(&txn, &self.subspace, &sandbox)
				.await?
				.ok_or_else(|| tg::error!(%sandbox, "failed to find the principal sandbox"))?;
			output.push((sandbox, data));
		}
		Ok(output)
	}
}
