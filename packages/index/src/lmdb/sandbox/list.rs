use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
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

	pub(crate) fn list_sandboxes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		let prefix = Self::pack(subspace, &(Kind::Sandbox.to_i32().unwrap(),));
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the sandboxes"))?;
		let mut output = Vec::new();
		for entry in iter {
			let (key, value) =
				entry.map_err(|error| tg::error!(!error, "failed to read a sandbox"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(id)) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			let sandbox = crate::sandbox::Sandbox::deserialize(value)?;
			output.push((id, sandbox));
		}

		Ok(output)
	}

	pub(crate) fn list_sandboxes_for_principal_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		principal: &tg::Principal,
		kind: Kind,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		let prefix = Self::pack(subspace, &(kind.to_i32().unwrap(), principal.to_string()));
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the principal sandboxes"))?;
		let mut output = Vec::new();
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read a principal sandbox"))?;
			let key = Self::unpack(subspace, key)?;
			let sandbox = match key {
				Key::Sandbox(crate::lmdb::sandbox::Key::CreatorSandbox { sandbox, .. })
					if kind == Kind::CreatorSandbox =>
				{
					sandbox
				},
				Key::Sandbox(crate::lmdb::sandbox::Key::OwnerSandbox { sandbox, .. })
					if kind == Kind::OwnerSandbox =>
				{
					sandbox
				},
				_ => return Err(tg::error!("unexpected key type")),
			};
			let data = Self::try_get_sandbox_with_transaction(db, subspace, transaction, &sandbox)?
				.ok_or_else(|| tg::error!(%sandbox, "failed to find the principal sandbox"))?;
			output.push((sandbox, data));
		}

		Ok(output)
	}
}
