use {
	crate::lmdb::{Db, Index},
	foundationdb_tuple as fdbt, heed as lmdb,
	std::collections::{HashSet, VecDeque},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn visible(
		&self,
		ids: &[tg::Id],
		principal: &tg::Principal,
	) -> tg::Result<Vec<bool>> {
		if matches!(principal, tg::Principal::Root) {
			return Ok(vec![true; ids.len()]);
		}
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			let ids = ids.to_vec();
			let principal = principal.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let principals = Self::requester_principals_with_transaction(
					&db,
					&subspace,
					&transaction,
					&principal,
				)?;
				let mut output = Vec::with_capacity(ids.len());
				for id in &ids {
					let mut visible = false;
					for principal in &principals {
						if Self::try_get_visibility_with_transaction(
							&db,
							&subspace,
							&transaction,
							id,
							principal,
						)? {
							visible = true;
							break;
						}
					}
					output.push(visible);
				}
				Ok(output)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	fn requester_principals_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tg::grant::Principal>> {
		let mut principals = vec![tg::grant::Principal::Public];
		if !matches!(principal, tg::Principal::Anonymous) {
			principals.push(principal.try_to_grant_principal()?);
		}
		let id = match principal {
			tg::Principal::Group(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Organization(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::User(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Anonymous
			| tg::Principal::Process(_)
			| tg::Principal::Root
			| tg::Principal::Runner
			| tg::Principal::Sandbox(_) => None,
		};
		if let Some(id) = id {
			let mut queue = VecDeque::from([id.clone()]);
			let mut visited = HashSet::from([id]);
			while let Some(id) = queue.pop_front() {
				let groups =
					Self::get_member_groups_with_transaction(db, subspace, transaction, &id)?;
				for group in groups {
					let id = tg::Id::from(group.clone());
					if visited.insert(id.clone()) {
						principals.push(tg::grant::Principal::Group(group));
						queue.push_back(id);
					}
				}
				let organizations = Self::get_member_organizations_with_transaction(
					db,
					subspace,
					transaction,
					&id,
				)?;
				for organization in organizations {
					let id = tg::Id::from(organization.clone());
					if visited.insert(id.clone()) {
						principals.push(tg::grant::Principal::Organization(organization));
						queue.push_back(id);
					}
				}
			}
		}
		Ok(principals)
	}
}
