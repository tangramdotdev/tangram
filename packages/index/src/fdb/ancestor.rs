use {
	crate::fdb::Index, foundationdb as fdb, foundationdb_tuple::Subspace,
	std::collections::HashSet, tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_ancestors(&self, id: &tg::Id) -> tg::Result<Option<Vec<tg::Id>>> {
		let request = crate::read::Request::TryGetAncestors { id: id.clone() };
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetAncestors(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_ancestors_with_transaction(
		transaction: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Id,
	) -> tg::Result<Option<Vec<tg::Id>>> {
		let mut ancestors = Vec::new();
		let mut current = Some(id.clone());
		let mut visited = HashSet::<_, tg::id::BuildHasher>::default();
		while let Some(id) = current {
			if !visited.insert(id.clone()) {
				return Err(tg::error!(%id, "the owner hierarchy contains a cycle"));
			}
			current = match id.kind() {
				tg::id::Kind::Group => {
					let group = Self::try_get_group_with_transaction(
						transaction,
						subspace,
						&id.clone().try_into()?,
					)
					.await?;
					let Some(group) = group else {
						if ancestors.is_empty() {
							return Ok(None);
						}
						return Err(tg::error!(%id, "failed to find an ancestor"));
					};
					group.parent
				},
				tg::id::Kind::Organization => {
					let organization = Self::try_get_organization_with_transaction(
						transaction,
						subspace,
						&id.clone().try_into()?,
					)
					.await?;
					if organization.is_none() {
						if ancestors.is_empty() {
							return Ok(None);
						}
						return Err(tg::error!(%id, "failed to find an ancestor"));
					}
					None
				},
				tg::id::Kind::User => {
					let user = Self::try_get_user_with_transaction(
						transaction,
						subspace,
						&id.clone().try_into()?,
					)
					.await?;
					if user.is_none() {
						if ancestors.is_empty() {
							return Ok(None);
						}
						return Err(tg::error!(%id, "failed to find an ancestor"));
					}
					None
				},
				_ => return Err(tg::error!(%id, "invalid owner")),
			};
			ancestors.push(id);
		}

		Ok(Some(ancestors))
	}
}
