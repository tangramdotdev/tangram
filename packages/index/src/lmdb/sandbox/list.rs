use {
	crate::lmdb::{Index, Key, Kind},
	num::ToPrimitive as _,
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
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let prefix = Self::pack(&subspace, &(Kind::Sandbox.to_i32().unwrap(),));
				let iter = db
					.prefix_iter(&transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to get the sandboxes"))?;
				let mut output = Vec::new();
				for entry in iter {
					let (key, value) =
						entry.map_err(|error| tg::error!(!error, "failed to read a sandbox"))?;
					let key = Self::unpack(&subspace, key)?;
					let Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(id)) = key else {
						return Err(tg::error!("unexpected key type"));
					};
					let sandbox = crate::sandbox::Sandbox::deserialize(value)?;
					output.push((id, sandbox));
				}
				Ok(output)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	async fn list_sandboxes_for_principal(
		&self,
		principal: &tg::Principal,
		kind: Kind,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		let principal = principal.clone();
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let prefix =
					Self::pack(&subspace, &(kind.to_i32().unwrap(), principal.to_string()));
				let iter = db
					.prefix_iter(&transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to get the principal sandboxes"))?;
				let mut output = Vec::new();
				for entry in iter {
					let (key, _) = entry.map_err(|error| {
						tg::error!(!error, "failed to read a principal sandbox")
					})?;
					let key = Self::unpack(&subspace, key)?;
					let sandbox = match key {
						Key::Sandbox(crate::lmdb::sandbox::Key::CreatorSandbox {
							sandbox, ..
						}) if kind == Kind::CreatorSandbox => sandbox,
						Key::Sandbox(crate::lmdb::sandbox::Key::OwnerSandbox {
							sandbox, ..
						}) if kind == Kind::OwnerSandbox => sandbox,
						_ => return Err(tg::error!("unexpected key type")),
					};
					let data = Self::try_get_sandbox_with_transaction(
						&db,
						&subspace,
						&transaction,
						&sandbox,
					)?
					.ok_or_else(|| tg::error!(%sandbox, "failed to find the principal sandbox"))?;
					output.push((sandbox, data));
				}
				Ok(output)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}
}
