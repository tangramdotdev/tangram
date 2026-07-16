use {
	crate::fdb::Index,
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	std::collections::{HashSet, VecDeque},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn get_requester_principals(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tg::grant::Principal>> {
		let transaction = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		Self::requester_principals_with_transaction(&transaction, &self.subspace, principal).await
	}

	pub async fn visible(
		&self,
		ids: &[tg::Id],
		principal: &tg::Principal,
	) -> tg::Result<Vec<bool>> {
		if matches!(principal, tg::Principal::Root) {
			return Ok(vec![true; ids.len()]);
		}
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		let principals =
			Self::requester_principals_with_transaction(&txn, &self.subspace, principal).await?;
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let mut visible = false;
			for principal in &principals {
				if Self::try_get_visibility_with_transaction(&txn, &self.subspace, id, principal)
					.await?
				{
					visible = true;
					break;
				}
			}
			output.push(visible);
		}
		Ok(output)
	}

	async fn requester_principals_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
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
			| tg::Principal::Runner(_)
			| tg::Principal::Sandbox(_) => None,
		};
		if let Some(id) = id {
			let mut queue = VecDeque::from([id.clone()]);
			let mut visited = HashSet::from([id]);
			while let Some(id) = queue.pop_front() {
				let groups = Self::get_member_groups_with_transaction(txn, subspace, &id).await?;
				for group in groups {
					let id = tg::Id::from(group.clone());
					if visited.insert(id.clone()) {
						principals.push(tg::grant::Principal::Group(group));
						queue.push_back(id);
					}
				}
				let organizations =
					Self::get_member_organizations_with_transaction(txn, subspace, &id).await?;
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
