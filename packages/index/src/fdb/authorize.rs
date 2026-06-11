use {
	crate::fdb::Index,
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	std::collections::{HashSet, VecDeque},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn authorize(
		&self,
		resource: tg::grant::Resource,
		permission: tg::grant::Permission,
		principal: Option<&tg::Principal>,
	) -> tg::Result<Option<bool>> {
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		let Some(id) =
			Self::try_resolve_resource_with_transaction(&txn, &self.subspace, &resource).await?
		else {
			return Ok(None);
		};
		crate::authorize::validate(&id, permission)?;
		if matches!(principal, Some(tg::Principal::Root)) {
			return Ok(Some(true));
		}
		let authorized =
			Self::authorize_with_transaction(&txn, &self.subspace, id, permission, principal)
				.await?;
		Ok(Some(authorized))
	}

	/// Search for a grant that confers the permission on the resource to the principal. The search walks the resource side, and whenever a sufficiently permissioned grant is found, its principal either matches immediately or, for a group or organization principal, the search continues through membership edges looking for the requester.
	async fn authorize_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		resource: tg::Id,
		permission: tg::grant::Permission,
		principal: Option<&tg::Principal>,
	) -> tg::Result<bool> {
		let requester = principal.cloned().map(tg::grant::Principal::from);
		let requester_id = principal.and_then(|principal| match principal {
			tg::Principal::Group(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Organization(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Process(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Sandbox(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::User(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Root | tg::Principal::Runner => None,
		});
		let mut resource_queue = VecDeque::from([(resource, permission)]);
		let mut resource_visited = HashSet::new();
		let mut principal_queue: VecDeque<tg::Id> = VecDeque::new();
		let mut principal_visited = HashSet::new();
		loop {
			if let Some((id, needed)) = resource_queue.pop_front() {
				if !resource_visited.insert((id.clone(), needed)) {
					continue;
				}

				// Check the grants on this resource.
				let grants = Self::get_resource_grants_with_transaction(txn, subspace, &id).await?;
				for (granted_principal, granted_permission) in grants {
					if !granted_permission.implies(needed) {
						continue;
					}
					if granted_principal == tg::grant::Principal::Public {
						return Ok(true);
					}
					if requester.as_ref() == Some(&granted_principal) {
						return Ok(true);
					}
					if requester_id.is_some() {
						match granted_principal {
							tg::grant::Principal::Group(group) => {
								principal_queue.push_back(group.into());
							},
							tg::grant::Principal::Organization(organization) => {
								principal_queue.push_back(organization.into());
							},
							_ => {},
						}
					}
				}

				// Expand to the resources whose grants confer the needed permission.
				match needed {
					tg::grant::Permission::Object(_) => {
						let object = tg::object::Id::try_from(id.clone())?;
						let parents =
							Self::get_object_parents_with_transaction(txn, subspace, &object)
								.await?;
						for parent in parents {
							let needed = tg::grant::Permission::Object(
								tg::grant::permission::object::Permission::Subtree,
							);
							resource_queue.push_back((parent.into(), needed));
						}
						let processes =
							Self::get_object_processes_with_transaction(txn, subspace, &object)
								.await?;
						for (process, kind) in processes {
							let needed = tg::grant::Permission::Process(
								crate::authorize::node_permission(kind),
							);
							resource_queue.push_back((process.into(), needed));
						}
						Self::expand_item_tags_with_transaction(
							txn,
							subspace,
							&id,
							needed,
							&mut resource_queue,
						)
						.await?;
					},
					tg::grant::Permission::Process(process_permission) => {
						let process = tg::process::Id::try_from(id.clone())?;
						let parents =
							Self::get_process_parents_with_transaction(txn, subspace, &process)
								.await?;
						for parent in parents {
							let needed =
								tg::grant::Permission::Process(process_permission.subtree());
							resource_queue.push_back((parent.into(), needed));
						}
						Self::expand_item_tags_with_transaction(
							txn,
							subspace,
							&id,
							needed,
							&mut resource_queue,
						)
						.await?;
					},
					tg::grant::Permission::Admin
					| tg::grant::Permission::Read
					| tg::grant::Permission::Write => {
						let parent = match id.kind() {
							tg::id::Kind::Tag => Self::try_get_tag_with_transaction(
								txn,
								subspace,
								&id.clone().try_into()?,
							)
							.await?
							.and_then(|tag| tag.parent),
							tg::id::Kind::Group => Self::try_get_group_with_transaction(
								txn,
								subspace,
								&id.clone().try_into()?,
							)
							.await?
							.and_then(|group| group.parent),
							_ => None,
						};
						if let Some(parent) = parent {
							resource_queue.push_back((parent, needed));
						}
					},
				}

				continue;
			}

			if let Some(id) = principal_queue.pop_front() {
				if !principal_visited.insert(id.clone()) {
					continue;
				}
				let members: Vec<tg::Id> = match id.kind() {
					tg::id::Kind::Group => Self::get_group_members_with_transaction(
						txn,
						subspace,
						&id.clone().try_into()?,
					)
					.await?
					.into_iter()
					.map(tg::Id::from)
					.collect(),
					tg::id::Kind::Organization => Self::get_organization_members_with_transaction(
						txn,
						subspace,
						&id.clone().try_into()?,
					)
					.await?
					.into_iter()
					.map(tg::Id::from)
					.collect(),
					_ => Vec::new(),
				};
				for member in members {
					if requester_id.as_ref() == Some(&member) {
						return Ok(true);
					}
					if member.kind() == tg::id::Kind::Group {
						principal_queue.push_back(member);
					}
				}
				continue;
			}

			break;
		}
		Ok(false)
	}

	/// Expand to the tags on an item whose recorded permissions imply the needed permission.
	async fn expand_item_tags_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		item: &tg::Id,
		needed: tg::grant::Permission,
		resource_queue: &mut VecDeque<(tg::Id, tg::grant::Permission)>,
	) -> tg::Result<()> {
		let item_bytes = item.to_bytes();
		let tags = Self::get_item_tags_with_transaction(txn, subspace, item_bytes.as_ref()).await?;
		for tag in tags {
			let Some(value) = Self::try_get_tag_with_transaction(txn, subspace, &tag).await? else {
				continue;
			};
			if value
				.permissions
				.iter()
				.any(|permission| permission.implies(needed))
			{
				resource_queue.push_back((tag.into(), tg::grant::Permission::Read));
			}
		}
		Ok(())
	}
}
