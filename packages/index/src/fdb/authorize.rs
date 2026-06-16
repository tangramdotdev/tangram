use {
	crate::fdb::Index,
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	std::collections::{HashSet, VecDeque},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn authorize_batch(
		&self,
		args: &[crate::authorize::Arg],
		principal: Option<&tg::Principal>,
	) -> tg::Result<Vec<Option<crate::authorize::Output>>> {
		if args.is_empty() {
			return Ok(Vec::new());
		}
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		let mut outputs = Vec::with_capacity(args.len());
		for arg in args {
			let Some(id) =
				Self::try_resolve_resource_with_transaction(&txn, &self.subspace, &arg.resource)
					.await?
			else {
				outputs.push(None);
				continue;
			};
			arg.permissions.validate(&id)?;
			if matches!(principal, Some(tg::Principal::Root)) {
				outputs.push(Some(crate::authorize::Output {
					permissions: arg.permissions,
				}));
				continue;
			}
			if matches!(principal, Some(tg::Principal::Process(process)) if tg::Id::from(process.clone()) == id)
			{
				outputs.push(Some(crate::authorize::Output {
					permissions: arg.permissions,
				}));
				continue;
			}
			let permissions = Self::authorize_with_transaction(
				&txn,
				&self.subspace,
				&id,
				arg.permissions,
				principal,
			)
			.await?;
			outputs.push(Some(crate::authorize::Output { permissions }));
		}
		Ok(outputs)
	}

	/// Search for a grant that confers the permission on the resource to the principal. The search walks the resource side, and whenever a sufficiently permissioned grant is found, its principal either matches immediately or, for a group or organization principal, the search continues through membership edges looking for the requester.
	async fn authorize_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		permissions: crate::authorize::Permissions,
		principal: Option<&tg::Principal>,
	) -> tg::Result<crate::authorize::Permissions> {
		let requester = principal.cloned().map(tg::grant::Principal::from);
		let requester_id = principal.and_then(|principal| match principal {
			tg::Principal::Group(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Organization(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Process(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Sandbox(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::User(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Root | tg::Principal::Runner => None,
		});
		let mut resource_queue = permissions
			.entries()
			.map(|(permission, requested)| (resource.to_owned(), permission, requested))
			.collect::<VecDeque<_>>();
		let mut resource_visited = HashSet::new();
		let mut found = permissions.empty_like();
		let mut principal_queue: VecDeque<(tg::Id, crate::authorize::Permissions)> =
			VecDeque::new();
		let mut principal_visited = HashSet::new();
		loop {
			if found.contains(permissions) {
				break;
			}
			if let Some((id, needed, requested)) = resource_queue.pop_front() {
				if found.contains(requested) {
					continue;
				}
				if !resource_visited.insert((id.clone(), needed, requested)) {
					continue;
				}
				if let (Some(tg::Principal::Process(process)), tg::grant::Permission::Process(_)) =
					(principal, needed)
					&& tg::Id::from(process.clone()) == id
				{
					found.insert(requested);
					continue;
				}

				// Check the grants on this resource.
				let grants = Self::get_resource_grants_with_transaction(txn, subspace, &id).await?;
				for (granted_principal, granted_permission) in grants {
					if !granted_permission.implies(needed) {
						continue;
					}
					if granted_principal == tg::grant::Principal::Public {
						found.insert(requested);
						break;
					}
					if requester.as_ref() == Some(&granted_principal) {
						found.insert(requested);
						break;
					}
					if requester_id.is_some() {
						match granted_principal {
							tg::grant::Principal::Group(group) => {
								principal_queue.push_back((group.into(), requested));
							},
							tg::grant::Principal::Organization(organization) => {
								principal_queue.push_back((organization.into(), requested));
							},
							_ => {},
						}
					}
				}
				if found.contains(requested) {
					continue;
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
							resource_queue.push_back((parent.into(), needed, requested));
						}
						let processes =
							Self::get_object_processes_with_transaction(txn, subspace, &object)
								.await?;
						for (process, kind) in processes {
							let permission = match kind {
								crate::process::object::Kind::Command => {
									tg::grant::permission::process::Permission::NodeCommand
								},
								crate::process::object::Kind::Error => {
									tg::grant::permission::process::Permission::NodeError
								},
								crate::process::object::Kind::Log => {
									tg::grant::permission::process::Permission::NodeLog
								},
								crate::process::object::Kind::Output => {
									tg::grant::permission::process::Permission::NodeOutput
								},
							};
							let needed = tg::grant::Permission::Process(permission);
							resource_queue.push_back((process.into(), needed, requested));
						}
						Self::expand_item_tags_with_transaction(
							txn,
							subspace,
							&id,
							needed,
							requested,
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
								tg::grant::Permission::Process(process_permission.to_subtree());
							resource_queue.push_back((parent.into(), needed, requested));
						}
						Self::expand_item_tags_with_transaction(
							txn,
							subspace,
							&id,
							needed,
							requested,
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
							resource_queue.push_back((parent, needed, requested));
						}
					},
				}

				continue;
			}

			if let Some((id, requested)) = principal_queue.pop_front() {
				if found.contains(requested) {
					continue;
				}
				if !principal_visited.insert((id.clone(), requested)) {
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
						found.insert(requested);
						break;
					}
					if member.kind() == tg::id::Kind::Group {
						principal_queue.push_back((member, requested));
					}
				}
				continue;
			}

			break;
		}
		Ok(found)
	}

	/// Expand to the tags on an item whose recorded permissions imply the needed permission.
	async fn expand_item_tags_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		item: &tg::Id,
		needed: tg::grant::Permission,
		requested: crate::authorize::Permissions,
		resource_queue: &mut VecDeque<(
			tg::Id,
			tg::grant::Permission,
			crate::authorize::Permissions,
		)>,
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
				resource_queue.push_back((tag.into(), tg::grant::Permission::Read, requested));
			}
		}
		Ok(())
	}
}
