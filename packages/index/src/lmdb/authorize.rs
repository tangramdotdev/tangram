use {
	crate::lmdb::{Db, Index},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::collections::{HashMap, HashSet, VecDeque},
	tangram_client::prelude::*,
};

#[derive(Default)]
struct Cache {
	authorization: HashMap<(tg::Id, tg::grant::Permission), bool>,
	resource_parents: HashMap<tg::Id, Option<tg::Id>>,
	group_members: HashMap<tg::group::Id, Vec<tg::Id>>,
	item_tags: HashMap<(tg::Id, tg::grant::Permission), Vec<(tg::Id, tg::grant::Permission)>>,
	object_children: HashMap<tg::object::Id, Vec<tg::object::Id>>,
	object_parents: HashMap<tg::object::Id, Vec<tg::object::Id>>,
	object_processes: HashMap<tg::object::Id, Vec<(tg::process::Id, crate::process::object::Kind)>>,
	organization_members: HashMap<tg::organization::Id, Vec<tg::Id>>,
	principal_contains_requester: HashMap<tg::grant::Principal, bool>,
	process_parents: HashMap<tg::process::Id, Vec<tg::process::Id>>,
	resource_grants: HashMap<tg::Id, Vec<(tg::grant::Principal, tg::grant::Permission)>>,
}

struct Requester<'a> {
	principal: Option<&'a tg::Principal>,
	principal_: Option<tg::grant::Principal>,
	id: Option<tg::Id>,
}

struct AuthorizationFrame {
	resource: tg::Id,
	permission: tg::grant::Permission,
	dependencies: Option<Vec<(tg::Id, tg::grant::Permission)>>,
	subtree_unknown: bool,
}

struct SubtreeSearchBudget {
	max_depth: usize,
	remaining_objects: usize,
}

struct AuthorizationContext<'a, 'txn> {
	db: &'a Db,
	subspace: &'a fdbt::Subspace,
	transaction: &'a lmdb::RoTxn<'txn>,
	authorize: crate::lmdb::AuthorizeConfig,
	requester: &'a Requester<'a>,
	token: Option<(tg::grant::Body, tg::Id)>,
	cache: &'a mut Cache,
}

impl<'a> Requester<'a> {
	fn new(principal: Option<&'a tg::Principal>) -> Self {
		let principal_ = principal.cloned().map(tg::grant::Principal::from);
		let id = principal.and_then(|principal| match principal {
			tg::Principal::Group(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Organization(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Process(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Sandbox(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::User(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Root | tg::Principal::Runner => None,
		});
		Self {
			principal,
			principal_,
			id,
		}
	}
}

impl Index {
	pub async fn authorize_batch(
		&self,
		args: &[crate::authorize::Arg],
		principal: Option<&tg::Principal>,
	) -> tg::Result<Vec<Option<crate::authorize::Output>>> {
		if args.is_empty() {
			return Ok(Vec::new());
		}
		let config = self.config;
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			let args = args.to_owned();
			let principal = principal.cloned();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let requester = Requester::new(principal.as_ref());
				let mut cache = Cache::default();
				let mut outputs = Vec::with_capacity(args.len());
				for arg in args {
					let Some(id) = Self::try_resolve_resource_with_transaction(
						&db,
						&subspace,
						&transaction,
						&arg.resource,
					)?
					else {
						outputs.push(None);
						continue;
					};
					crate::authorize::validate(&id, arg.permissions)?;
					if matches!(principal, Some(tg::Principal::Root)) {
						outputs.push(Some(crate::authorize::Output {
							permissions: arg.permissions,
						}));
						continue;
					}
					if matches!(principal, Some(tg::Principal::Process(ref process)) if tg::Id::from(process.clone()) == id)
					{
						outputs.push(Some(crate::authorize::Output {
							permissions: arg.permissions,
						}));
						continue;
					}
					let token = if let Some(body) = arg.token {
						Self::try_resolve_resource_with_transaction(
							&db,
							&subspace,
							&transaction,
							&body.resource,
						)?
						.map(|resource| (body, resource))
					} else {
						None
					};
					let mut context = AuthorizationContext {
						db: &db,
						subspace: &subspace,
						transaction: &transaction,
						authorize: config,
						requester: &requester,
						token,
						cache: &mut cache,
					};
					let permissions =
						Self::authorize_with_transaction(&mut context, &id, arg.permissions)?;
					outputs.push(Some(crate::authorize::Output { permissions }));
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	fn authorize_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permissions: tg::grant::permission::Set,
	) -> tg::Result<tg::grant::permission::Set> {
		let mut found = permissions.empty_like();
		for permission in permissions.iter() {
			let authorized =
				Self::authorize_permission_with_transaction(context, resource, permission)?;
			if authorized {
				found.insert(tg::grant::permission::Set::from_permission(permission));
				if found.contains(permissions) {
					break;
				}
			}
		}
		Ok(found)
	}

	fn authorize_permission_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
	) -> tg::Result<bool> {
		let mut object_subtree_budget = SubtreeSearchBudget {
			max_depth: context.authorize.object_subtree.max_depth,
			remaining_objects: context.authorize.object_subtree.max_objects,
		};
		Self::authorize_permission_with_transaction_inner(
			context,
			resource,
			permission,
			&mut object_subtree_budget,
			&mut HashSet::new(),
			0,
		)
	}

	fn authorize_permission_with_transaction_inner(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
		object_subtree_budget: &mut SubtreeSearchBudget,
		unknown: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		depth: usize,
	) -> tg::Result<bool> {
		let root = (resource.clone(), permission);
		if !Self::is_authorized_by_token(context, resource, permission)
			&& let Some(authorized) = context.cache.authorization.get(&root)
		{
			return Ok(*authorized);
		}

		let mut token_authorized = HashSet::new();
		let mut pending = HashSet::from([root.clone()]);
		let mut queue = VecDeque::from([AuthorizationFrame {
			resource: resource.clone(),
			permission,
			dependencies: None,
			subtree_unknown: false,
		}]);

		while let Some(frame) = queue.pop_front() {
			let key = (frame.resource.clone(), frame.permission);
			if Self::is_authorized_by_token(context, &frame.resource, frame.permission) {
				pending.remove(&key);
				token_authorized.insert(key);
				continue;
			}
			if context.cache.authorization.contains_key(&key) || unknown.contains(&key) {
				pending.remove(&key);
				continue;
			}

			if let Some(dependencies) = frame.dependencies {
				let mut complete = true;
				let mut authorized = false;
				let mut has_unknown_dependency = false;
				for (dependency, dependency_permission) in &dependencies {
					let dependency_key = (dependency.clone(), *dependency_permission);
					if token_authorized.contains(&dependency_key) {
						authorized = true;
						break;
					}
					match context.cache.authorization.get(&dependency_key).copied() {
						Some(true) => {
							authorized = true;
							break;
						},
						Some(false) => {},
						None if unknown.contains(&dependency_key) => {
							has_unknown_dependency = true;
						},
						None => complete = false,
					}
				}
				if complete {
					if authorized || !(frame.subtree_unknown || has_unknown_dependency) {
						Self::finish_authorization(context.cache, &mut pending, key, authorized);
					} else {
						Self::finish_unknown_authorization(unknown, &mut pending, key);
					}
				} else {
					queue.push_back(AuthorizationFrame {
						resource: frame.resource,
						permission: frame.permission,
						dependencies: Some(dependencies),
						subtree_unknown: frame.subtree_unknown,
					});
				}
				continue;
			}

			let directly_authorized = Self::is_directly_authorized_with_transaction(
				context,
				&frame.resource,
				frame.permission,
			)?;
			if directly_authorized {
				Self::finish_authorization(context.cache, &mut pending, key, true);
				continue;
			}
			let subtree_unknown = match Self::authorize_with_object_subtree_search_with_transaction(
				context,
				&frame.resource,
				frame.permission,
				object_subtree_budget,
				unknown,
				depth,
			)? {
				Some(true) => {
					Self::finish_authorization(context.cache, &mut pending, key, true);
					continue;
				},
				Some(false) => false,
				None => true,
			};

			let dependencies = Self::get_authorization_dependencies_with_transaction(
				context.db,
				context.subspace,
				context.transaction,
				&frame.resource,
				frame.permission,
				context.cache,
			)?;
			let mut authorized = false;
			let mut has_unknown_dependency = false;
			let mut dependencies_to_push = Vec::new();
			for (dependency, dependency_permission) in &dependencies {
				let dependency_key = (dependency.clone(), *dependency_permission);
				if token_authorized.contains(&dependency_key) {
					authorized = true;
					break;
				}
				match context.cache.authorization.get(&dependency_key).copied() {
					Some(true) => {
						authorized = true;
						break;
					},
					Some(false) => {},
					None if unknown.contains(&dependency_key) => {
						has_unknown_dependency = true;
					},
					None => {
						has_unknown_dependency = true;
						if pending.insert(dependency_key) {
							dependencies_to_push.push((dependency.clone(), *dependency_permission));
						}
					},
				}
			}

			if authorized || !has_unknown_dependency {
				Self::finish_authorization(context.cache, &mut pending, key, authorized);
				continue;
			}
			if dependencies_to_push.is_empty() {
				Self::finish_unknown_authorization(unknown, &mut pending, key);
				continue;
			}

			queue.push_back(AuthorizationFrame {
				resource: frame.resource,
				permission: frame.permission,
				dependencies: Some(dependencies),
				subtree_unknown,
			});
			for (dependency, dependency_permission) in dependencies_to_push {
				queue.push_back(AuthorizationFrame {
					resource: dependency,
					permission: dependency_permission,
					dependencies: None,
					subtree_unknown: false,
				});
			}
		}

		Ok(token_authorized.contains(&root)
			|| context
				.cache
				.authorization
				.get(&root)
				.copied()
				.unwrap_or(false))
	}

	fn finish_authorization(
		cache: &mut Cache,
		pending: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		key: (tg::Id, tg::grant::Permission),
		authorized: bool,
	) {
		pending.remove(&key);
		cache.authorization.insert(key, authorized);
	}

	fn finish_unknown_authorization(
		unknown: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		pending: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		key: (tg::Id, tg::grant::Permission),
	) {
		pending.remove(&key);
		unknown.insert(key);
	}

	fn is_directly_authorized_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
	) -> tg::Result<bool> {
		if let (Some(tg::Principal::Process(process)), tg::grant::Permission::Process(_)) =
			(context.requester.principal, permission)
			&& tg::Id::from(process.clone()) == *resource
		{
			return Ok(true);
		}

		let grants = Self::get_cached_resource_grants_with_transaction(
			context.db,
			context.subspace,
			context.transaction,
			resource,
			context.cache,
		)?;
		for (granted_principal, granted_permission) in grants {
			if granted_permission.implies(permission)
				&& Self::principal_contains_requester_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&granted_principal,
					context.requester,
					context.cache,
				)? {
				return Ok(true);
			}
		}
		Ok(false)
	}

	fn is_authorized_by_token(
		context: &AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
	) -> bool {
		context
			.token
			.as_ref()
			.is_some_and(|(body, token_resource)| {
				token_resource == resource && body.grants(permission)
			})
	}

	fn authorize_with_object_subtree_search_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
		budget: &mut SubtreeSearchBudget,
		unknown: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		depth: usize,
	) -> tg::Result<Option<bool>> {
		if permission
			!= tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree)
		{
			return Ok(Some(false));
		}
		let node = tg::grant::Permission::Object(tg::grant::permission::object::Permission::Node);

		let mut stack = Vec::from([(tg::object::Id::try_from(resource.clone())?, depth)]);
		while let Some((object, depth)) = stack.pop() {
			if depth > budget.max_depth || budget.remaining_objects == 0 {
				return Ok(None);
			}
			budget.remaining_objects -= 1;

			let resource = tg::Id::from(object.clone());
			if !Self::authorize_permission_with_transaction_inner(
				context, &resource, node, budget, unknown, depth,
			)? {
				if unknown.contains(&(resource, node)) {
					return Ok(None);
				}
				return Ok(Some(false));
			}

			let children = Self::get_cached_object_children_limited_with_transaction(
				context.db,
				context.subspace,
				context.transaction,
				&object,
				budget.remaining_objects + 1,
				context.cache,
			)?;
			if children.len() > budget.remaining_objects {
				return Ok(None);
			}
			if !children.is_empty() && depth == budget.max_depth {
				return Ok(None);
			}
			for child in children {
				stack.push((child, depth + 1));
			}
		}

		Ok(Some(true))
	}

	fn get_authorization_dependencies_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::Id, tg::grant::Permission)>> {
		let mut dependencies = Vec::new();
		match permission {
			tg::grant::Permission::Object(_) => {
				let object = tg::object::Id::try_from(resource.clone())?;
				let object_parents = Self::get_cached_object_parents_with_transaction(
					db,
					subspace,
					transaction,
					&object,
					cache,
				)?;
				for parent in object_parents {
					let permission = tg::grant::Permission::Object(
						tg::grant::permission::object::Permission::Subtree,
					);
					dependencies.push((parent.into(), permission));
				}
				let processes = Self::get_cached_object_processes_with_transaction(
					db,
					subspace,
					transaction,
					&object,
					cache,
				)?;
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
					dependencies.push((process.into(), tg::grant::Permission::Process(permission)));
				}
				dependencies.extend(Self::get_cached_item_tags_with_transaction(
					db,
					subspace,
					transaction,
					resource,
					permission,
					cache,
				)?);
			},
			tg::grant::Permission::Process(process_permission) => {
				let process = tg::process::Id::try_from(resource.clone())?;
				let process_parents = Self::get_cached_process_parents_with_transaction(
					db,
					subspace,
					transaction,
					&process,
					cache,
				)?;
				for parent in process_parents {
					let permission =
						tg::grant::Permission::Process(process_permission.to_subtree());
					dependencies.push((parent.into(), permission));
				}
				dependencies.extend(Self::get_cached_item_tags_with_transaction(
					db,
					subspace,
					transaction,
					resource,
					permission,
					cache,
				)?);
			},
			tg::grant::Permission::Admin
			| tg::grant::Permission::Read
			| tg::grant::Permission::Write => {
				if let Some(parent) = Self::get_cached_resource_parent_with_transaction(
					db,
					subspace,
					transaction,
					resource,
					cache,
				)? {
					dependencies.push((parent, permission));
				}
			},
		}
		Ok(dependencies)
	}

	fn get_cached_object_children_limited_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		object: &tg::object::Id,
		limit: usize,
		cache: &mut Cache,
	) -> tg::Result<Vec<tg::object::Id>> {
		if let Some(children) = cache.object_children.get(object) {
			return Ok(children.iter().take(limit).cloned().collect());
		}
		let id_bytes = object.to_bytes();
		let prefix = &(
			crate::lmdb::Kind::ObjectChild.to_i32().unwrap(),
			id_bytes.as_ref(),
		);
		let prefix = Self::pack(subspace, prefix);
		let mut children = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get object children"))?;
		for entry in iter.take(limit) {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read object child entry"))?;
			let key = Self::unpack(subspace, key)?;
			let crate::lmdb::Key::Object(crate::lmdb::object::Key::ObjectChild { child, .. }) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			children.push(child);
		}
		if children.len() < limit {
			cache
				.object_children
				.insert(object.clone(), children.clone());
		}
		Ok(children)
	}

	fn principal_contains_requester_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		principal: &tg::grant::Principal,
		requester: &Requester<'_>,
		cache: &mut Cache,
	) -> tg::Result<bool> {
		if principal == &tg::grant::Principal::Public {
			return Ok(true);
		}
		if requester.principal_.as_ref() == Some(principal) {
			return Ok(true);
		}
		if requester.id.is_none() {
			return Ok(false);
		}
		if let Some(contains) = cache.principal_contains_requester.get(principal) {
			return Ok(*contains);
		}
		let contains = match principal {
			tg::grant::Principal::Group(group) => Self::group_contains_requester_with_transaction(
				db,
				subspace,
				transaction,
				group,
				requester,
				cache,
			)?,
			tg::grant::Principal::Organization(organization) => {
				Self::organization_contains_requester_with_transaction(
					db,
					subspace,
					transaction,
					organization,
					requester,
					cache,
				)?
			},
			_ => false,
		};
		cache
			.principal_contains_requester
			.insert(principal.clone(), contains);
		Ok(contains)
	}

	fn group_contains_requester_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		group: &tg::group::Id,
		requester: &Requester<'_>,
		cache: &mut Cache,
	) -> tg::Result<bool> {
		let principal = tg::grant::Principal::Group(group.clone());
		if let Some(contains) = cache.principal_contains_requester.get(&principal) {
			return Ok(*contains);
		}
		let root = group.clone();
		let mut visited = HashSet::new();
		let mut queue = VecDeque::from([group.clone()]);
		while let Some(group) = queue.pop_front() {
			if !visited.insert(group.clone()) {
				continue;
			}
			let principal = tg::grant::Principal::Group(group.clone());
			if let Some(contains) = cache.principal_contains_requester.get(&principal) {
				if *contains {
					cache
						.principal_contains_requester
						.insert(tg::grant::Principal::Group(root), true);
					return Ok(true);
				}
				continue;
			}
			let members = Self::get_cached_group_members_with_transaction(
				db,
				subspace,
				transaction,
				&group,
				cache,
			)?;
			for member in members {
				if requester.id.as_ref() == Some(&member) {
					cache
						.principal_contains_requester
						.insert(tg::grant::Principal::Group(root), true);
					return Ok(true);
				}
				if member.kind() == tg::id::Kind::Group {
					queue.push_back(tg::group::Id::try_from(member)?);
				}
			}
		}
		for group in visited {
			cache
				.principal_contains_requester
				.insert(tg::grant::Principal::Group(group), false);
		}
		Ok(false)
	}

	fn organization_contains_requester_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		organization: &tg::organization::Id,
		requester: &Requester<'_>,
		cache: &mut Cache,
	) -> tg::Result<bool> {
		let principal = tg::grant::Principal::Organization(organization.clone());
		if let Some(contains) = cache.principal_contains_requester.get(&principal) {
			return Ok(*contains);
		}
		let members = Self::get_cached_organization_members_with_transaction(
			db,
			subspace,
			transaction,
			organization,
			cache,
		)?;
		for member in members {
			if requester.id.as_ref() == Some(&member) {
				cache.principal_contains_requester.insert(
					tg::grant::Principal::Organization(organization.clone()),
					true,
				);
				return Ok(true);
			}
			if member.kind() == tg::id::Kind::Group {
				let group = tg::group::Id::try_from(member)?;
				if Self::group_contains_requester_with_transaction(
					db,
					subspace,
					transaction,
					&group,
					requester,
					cache,
				)? {
					cache.principal_contains_requester.insert(
						tg::grant::Principal::Organization(organization.clone()),
						true,
					);
					return Ok(true);
				}
			}
		}
		cache.principal_contains_requester.insert(
			tg::grant::Principal::Organization(organization.clone()),
			false,
		);
		Ok(false)
	}

	fn get_cached_resource_grants_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::grant::Principal, tg::grant::Permission)>> {
		if let Some(grants) = cache.resource_grants.get(resource) {
			return Ok(grants.clone());
		}
		let grants =
			Self::get_resource_grants_with_transaction(db, subspace, transaction, resource)?;
		cache
			.resource_grants
			.insert(resource.clone(), grants.clone());
		Ok(grants)
	}

	fn get_cached_resource_parent_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		cache: &mut Cache,
	) -> tg::Result<Option<tg::Id>> {
		if let Some(parent) = cache.resource_parents.get(resource) {
			return Ok(parent.clone());
		}
		let parent = match resource.kind() {
			tg::id::Kind::Tag => Self::try_get_tag_with_transaction(
				db,
				subspace,
				transaction,
				&resource.clone().try_into()?,
			)?
			.and_then(|tag| tag.parent),
			tg::id::Kind::Group => Self::try_get_group_with_transaction(
				db,
				subspace,
				transaction,
				&resource.clone().try_into()?,
			)?
			.and_then(|group| group.parent),
			_ => None,
		};
		cache
			.resource_parents
			.insert(resource.clone(), parent.clone());
		Ok(parent)
	}

	fn get_cached_item_tags_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		item: &tg::Id,
		permission: tg::grant::Permission,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::Id, tg::grant::Permission)>> {
		let key = (item.clone(), permission);
		if let Some(tags) = cache.item_tags.get(&key) {
			return Ok(tags.clone());
		}
		let item_bytes = item.to_bytes();
		let tags =
			Self::get_item_tags_with_transaction(db, subspace, transaction, item_bytes.as_ref())?;
		let mut parents = Vec::new();
		for tag in tags {
			let Some(value) = Self::try_get_tag_with_transaction(db, subspace, transaction, &tag)?
			else {
				continue;
			};
			if value
				.permissions
				.iter()
				.any(|tag_permission| tag_permission.implies(permission))
			{
				parents.push((tag.into(), tg::grant::Permission::Read));
			}
		}
		cache.item_tags.insert(key, parents.clone());
		Ok(parents)
	}

	fn get_cached_object_parents_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		object: &tg::object::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<tg::object::Id>> {
		if let Some(parents) = cache.object_parents.get(object) {
			return Ok(parents.clone());
		}
		let parents = Self::get_object_parents_with_transaction(db, subspace, transaction, object)?;
		cache.object_parents.insert(object.clone(), parents.clone());
		Ok(parents)
	}

	fn get_cached_object_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		object: &tg::object::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::object::Kind)>> {
		if let Some(processes) = cache.object_processes.get(object) {
			return Ok(processes.clone());
		}
		let processes =
			Self::get_object_processes_with_transaction(db, subspace, transaction, object)?;
		cache
			.object_processes
			.insert(object.clone(), processes.clone());
		Ok(processes)
	}

	fn get_cached_process_parents_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		process: &tg::process::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<tg::process::Id>> {
		if let Some(parents) = cache.process_parents.get(process) {
			return Ok(parents.clone());
		}
		let parents =
			Self::get_process_parents_with_transaction(db, subspace, transaction, process)?;
		cache
			.process_parents
			.insert(process.clone(), parents.clone());
		Ok(parents)
	}

	fn get_cached_group_members_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		group: &tg::group::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<tg::Id>> {
		if let Some(members) = cache.group_members.get(group) {
			return Ok(members.clone());
		}
		let members: Vec<tg::Id> =
			Self::get_group_members_with_transaction(db, subspace, transaction, group)?
				.into_iter()
				.map(tg::Id::from)
				.collect();
		cache.group_members.insert(group.clone(), members.clone());
		Ok(members)
	}

	fn get_cached_organization_members_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		organization: &tg::organization::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<tg::Id>> {
		if let Some(members) = cache.organization_members.get(organization) {
			return Ok(members.clone());
		}
		let members: Vec<tg::Id> = Self::get_organization_members_with_transaction(
			db,
			subspace,
			transaction,
			organization,
		)?
		.into_iter()
		.map(tg::Id::from)
		.collect();
		cache
			.organization_members
			.insert(organization.clone(), members.clone());
		Ok(members)
	}
}
