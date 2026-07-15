use {
	crate::lmdb::{Db, Index},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::collections::{HashMap, HashSet, VecDeque},
	tangram_client::prelude::*,
};

const PRECOMPUTE_REQUESTER_PRINCIPALS: bool = false;

#[derive(Default)]
struct Cache {
	group_members: HashMap<tg::group::Id, Vec<tg::Id>>,
	resource_parents: HashMap<tg::Id, Option<tg::Id>>,
	item_tags: HashMap<(tg::Id, tg::grant::Permission), Vec<(tg::Id, tg::grant::Permission)>>,
	object_children: HashMap<tg::object::Id, Vec<tg::object::Id>>,
	object_parents: HashMap<tg::object::Id, Vec<tg::object::Id>>,
	object_processes: HashMap<tg::object::Id, Vec<(tg::process::Id, crate::process::object::Kind)>>,
	organization_members: HashMap<tg::organization::Id, Vec<tg::Id>>,
	principal_contains_requester: HashMap<tg::grant::Principal, bool>,
	process_commands: HashMap<tg::process::Id, Option<tg::object::Id>>,
	process_parents: HashMap<tg::process::Id, Vec<tg::process::Id>>,
	process_sandboxes: HashMap<tg::process::Id, Option<tg::sandbox::Id>>,
	resource_grants: HashMap<tg::Id, Vec<(tg::grant::Principal, tg::grant::Permission)>>,
	sandbox_owners: HashMap<tg::sandbox::Id, Option<tg::Principal>>,
}

struct Requester<'a> {
	principal: &'a tg::Principal,
	principal_: tg::grant::Principal,
	id: Option<tg::Id>,
	principals: HashSet<tg::grant::Principal>,
}

struct AuthorizationNode {
	key: (tg::Id, tg::grant::Permission),
	dependents: Vec<usize>,
	authorized: bool,
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
	authorization: &'a mut HashMap<(tg::Id, tg::grant::Permission), bool>,
	cache: &'a mut Cache,
}

impl<'a> Requester<'a> {
	fn new(principal: &'a tg::Principal) -> Self {
		let principal_ = principal.to_grant_requester();
		let id = match principal {
			tg::Principal::Group(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Organization(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Process(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Sandbox(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::User(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Anonymous | tg::Principal::Root | tg::Principal::Runner(_) => None,
		};
		Self {
			principal,
			principal_: principal_.clone(),
			id,
			principals: HashSet::from([tg::grant::Principal::Public, principal_]),
		}
	}
}

impl Index {
	pub async fn authorize_batch(
		&self,
		args: &[crate::authorize::Arg],
		principal: &tg::Principal,
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
			let principal = principal.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let mut requester = Requester::new(&principal);
				if PRECOMPUTE_REQUESTER_PRINCIPALS {
					Self::load_requester_principals_with_transaction(
						&db,
						&subspace,
						&transaction,
						&mut requester,
					)?;
				}
				let mut cache = Cache::default();
				let mut authorization = HashMap::new();
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
					if crate::authorize::validate(&id, arg.permissions).is_err() {
						outputs.push(None);
						continue;
					}
					if matches!(principal, tg::Principal::Root) {
						outputs.push(Some(crate::authorize::Output {
							permissions: arg.permissions,
						}));
						continue;
					}
					if matches!(&principal, tg::Principal::Process(process) if tg::Id::from(process.clone()) == id)
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
					let mut token_authorization = HashMap::new();
					let authorization = if token.is_some() {
						&mut token_authorization
					} else {
						&mut authorization
					};
					let mut context = AuthorizationContext {
						db: &db,
						subspace: &subspace,
						transaction: &transaction,
						authorize: config,
						requester: &requester,
						token,
						authorization,
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

	fn load_requester_principals_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		requester: &mut Requester<'_>,
	) -> tg::Result<()> {
		let Some(id) = requester.id.clone() else {
			return Ok(());
		};
		if !matches!(id.kind(), tg::id::Kind::Group | tg::id::Kind::User) {
			return Ok(());
		}
		let mut visited = HashSet::new();
		let mut queue = VecDeque::from([id]);
		while let Some(member) = queue.pop_front() {
			if !visited.insert(member.clone()) {
				continue;
			}
			for group in
				Self::get_member_groups_with_transaction(db, subspace, transaction, &member)?
			{
				requester
					.principals
					.insert(tg::grant::Principal::Group(group.clone()));
				queue.push_back(group.into());
			}
			for organization in
				Self::get_member_organizations_with_transaction(db, subspace, transaction, &member)?
			{
				requester
					.principals
					.insert(tg::grant::Principal::Organization(organization));
			}
		}
		Ok(())
	}

	fn authorize_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permissions: tg::grant::permission::Set,
	) -> tg::Result<tg::grant::permission::Set> {
		let mut authorized = permissions.empty_like();
		for permission in permissions.iter() {
			let permission_authorized =
				Self::authorize_permission_with_transaction(context, resource, permission)?;
			if permission_authorized {
				authorized.insert(tg::grant::permission::Set::from_permission(permission));
				if authorized.contains(permissions) {
					break;
				}
			}
		}
		Ok(authorized)
	}

	fn authorize_permission_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
	) -> tg::Result<bool> {
		if Self::authorize_permission_ordinary_with_transaction(context, resource, permission)? {
			return Ok(true);
		}
		if permission
			!= tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree)
		{
			return Ok(false);
		}
		let mut object_subtree_budget = SubtreeSearchBudget {
			max_depth: context.authorize.object_subtree.max_depth,
			remaining_objects: context.authorize.object_subtree.max_objects,
		};
		Ok(Self::authorize_with_object_subtree_search_with_transaction(
			context,
			resource,
			&mut object_subtree_budget,
		)?
		.unwrap_or(false))
	}

	fn authorize_permission_ordinary_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
	) -> tg::Result<bool> {
		let root = (resource.clone(), permission);
		if let Some(authorized) = context.authorization.get(&root) {
			return Ok(*authorized);
		}

		let mut nodes = vec![AuthorizationNode {
			key: root.clone(),
			dependents: Vec::new(),
			authorized: false,
		}];

		let mut node_ids = HashMap::from([(root.clone(), 0)]);
		let mut queue = VecDeque::from([0]);
		while let Some(node_id) = queue.pop_front() {
			if nodes[node_id].authorized {
				continue;
			}
			let (resource, permission) = nodes[node_id].key.clone();
			if Self::is_authorized_by_token(context, &resource, permission)
				|| Self::is_directly_authorized_with_transaction(context, &resource, permission)?
			{
				Self::propagate_authorization(context.authorization, &mut nodes, node_id);
				if nodes[0].authorized {
					return Ok(true);
				}
				continue;
			}
			let dependencies = Self::get_authorization_dependencies_with_transaction(
				context.db,
				context.subspace,
				context.transaction,
				&resource,
				permission,
				context.requester,
				context.cache,
			)?;
			for (dependency, dependency_permission) in dependencies {
				let dependency_key = (dependency, dependency_permission);
				match context.authorization.get(&dependency_key).copied() {
					Some(true) => {
						Self::propagate_authorization(context.authorization, &mut nodes, node_id);
						break;
					},
					Some(false) => {},
					None => {
						let dependency_id =
							if let Some(dependency_id) = node_ids.get(&dependency_key) {
								*dependency_id
							} else {
								let dependency_id = nodes.len();
								node_ids.insert(dependency_key.clone(), dependency_id);
								nodes.push(AuthorizationNode {
									key: dependency_key,
									dependents: Vec::new(),
									authorized: false,
								});
								queue.push_back(dependency_id);
								dependency_id
							};
						nodes[dependency_id].dependents.push(node_id);
					},
				}
			}
			if nodes[0].authorized {
				return Ok(true);
			}
		}

		for node in nodes {
			context.authorization.entry(node.key).or_insert(false);
		}
		Ok(false)
	}

	fn propagate_authorization(
		authorization: &mut HashMap<(tg::Id, tg::grant::Permission), bool>,
		nodes: &mut [AuthorizationNode],
		node_id: usize,
	) {
		let mut stack = vec![node_id];
		while let Some(node_id) = stack.pop() {
			let node = &mut nodes[node_id];
			if node.authorized {
				continue;
			}
			node.authorized = true;
			authorization.insert(node.key.clone(), true);
			stack.extend(node.dependents.iter().copied());
		}
	}

	fn is_directly_authorized_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
	) -> tg::Result<bool> {
		if let (tg::Principal::Process(process), tg::grant::Permission::Process(_)) =
			(context.requester.principal, permission)
			&& tg::Id::from(process.clone()) == *resource
		{
			return Ok(true);
		}
		if let (tg::Principal::User(user), tg::grant::Permission::User(_)) =
			(context.requester.principal, permission)
			&& tg::Id::from(user.clone()) == *resource
		{
			return Ok(true);
		}
		if let (
			tg::Principal::Sandbox(sandbox),
			tg::grant::Permission::Sandbox(
				tg::grant::permission::sandbox::Permission::Read
				| tg::grant::permission::sandbox::Permission::Write,
			),
		) = (context.requester.principal, permission)
			&& tg::Id::from(sandbox.clone()) == *resource
		{
			return Ok(true);
		}
		if matches!(
			permission,
			tg::grant::Permission::Sandbox(
				tg::grant::permission::sandbox::Permission::Read
					| tg::grant::permission::sandbox::Permission::Write
			)
		) && resource.kind() == tg::id::Kind::Sandbox
		{
			let sandbox = tg::sandbox::Id::try_from(resource.clone())?;
			if let Some(owner) = Self::get_cached_sandbox_owner_with_transaction(
				context.db,
				context.subspace,
				context.transaction,
				&sandbox,
				context.cache,
			)? {
				let owner = owner.try_to_grant_principal()?;
				if Self::principal_contains_requester_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&owner,
					context.requester,
					context.cache,
				)? {
					return Ok(true);
				}
			}
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
		budget: &mut SubtreeSearchBudget,
	) -> tg::Result<Option<bool>> {
		let subtree =
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree);
		let node = tg::grant::Permission::Object(tg::grant::permission::object::Permission::Node);
		let root = tg::object::Id::try_from(resource.clone())?;
		let mut visited = HashSet::from([root.clone()]);
		let mut queue = VecDeque::from([(root, 0)]);
		while let Some((object, depth)) = queue.pop_front() {
			if budget.remaining_objects == 0 {
				return Ok(None);
			}
			budget.remaining_objects -= 1;

			let resource = tg::Id::from(object.clone());
			if Self::authorize_permission_ordinary_with_transaction(context, &resource, subtree)? {
				continue;
			}
			if !Self::authorize_permission_ordinary_with_transaction(context, &resource, node)? {
				return Ok(Some(false));
			}

			let limit = if depth == budget.max_depth {
				visited.len().saturating_add(1)
			} else {
				budget
					.remaining_objects
					.saturating_add(visited.len())
					.saturating_add(1)
			};
			let children = Self::get_cached_object_children_limited_with_transaction(
				context.db,
				context.subspace,
				context.transaction,
				&object,
				limit,
				context.cache,
			)?;
			for child in children {
				if visited.insert(child.clone()) {
					if depth == budget.max_depth {
						return Ok(None);
					}
					queue.push_back((child, depth + 1));
				}
			}
			if queue.len() > budget.remaining_objects {
				return Ok(None);
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
		requester: &Requester<'_>,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::Id, tg::grant::Permission)>> {
		let mut dependencies = Vec::new();
		match permission {
			tg::grant::Permission::Object(_) => {
				// Get the requester process and command.
				let requester_process = match requester.principal {
					tg::Principal::Process(process) => Some(process),
					_ => None,
				};
				let requester_command = match requester_process {
					None => None,
					Some(process) => Self::try_get_cached_process_command_with_transaction(
						db,
						subspace,
						transaction,
						process,
						cache,
					)?,
				};

				// Get the relationships.
				let object = tg::object::Id::try_from(resource.clone())?;
				let mut object_parents = Self::get_cached_object_parents_with_transaction(
					db,
					subspace,
					transaction,
					&object,
					cache,
				)?;
				let mut processes = Self::get_cached_object_processes_with_transaction(
					db,
					subspace,
					transaction,
					&object,
					cache,
				)?;

				// Add the requester command shortcut.
				if let (Some(process), Some(command)) = (requester_process, requester_command)
					&& let Some(position) =
						object_parents.iter().position(|parent| parent == &command)
				{
					object_parents.swap(0, position);
					let permission = tg::grant::Permission::Process(
						tg::grant::permission::process::Permission::NodeCommand,
					);
					dependencies.push((process.clone().into(), permission));
				}

				// Add the process relationships.
				if let Some(requester) = requester_process
					&& let Some(position) = processes
						.iter()
						.position(|(process, _)| process == requester)
				{
					processes.swap(0, position);
				}
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

				// Add the object parent relationships.
				for parent in object_parents {
					let permission = tg::grant::Permission::Object(
						tg::grant::permission::object::Permission::Subtree,
					);
					dependencies.push((parent.into(), permission));
				}

				// Add the tag relationships.
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
				// Get the relationships.
				let process = tg::process::Id::try_from(resource.clone())?;
				let sandbox = Self::get_cached_process_sandbox_with_transaction(
					db,
					subspace,
					transaction,
					&process,
					cache,
				)?;
				let process_parents = Self::get_cached_process_parents_with_transaction(
					db,
					subspace,
					transaction,
					&process,
					cache,
				)?;

				// Add the process parent relationships.
				for parent in process_parents {
					let permission =
						tg::grant::Permission::Process(process_permission.to_subtree());
					dependencies.push((parent.into(), permission));
				}

				// Add the sandbox relationship.
				if let Some(sandbox) = sandbox {
					let sandbox_permission = match process_permission {
						tg::grant::permission::process::Permission::Write => {
							tg::grant::permission::sandbox::Permission::Write
						},
						_ => tg::grant::permission::sandbox::Permission::Read,
					};
					dependencies.push((
						sandbox.into(),
						tg::grant::Permission::Sandbox(sandbox_permission),
					));
				}

				// Add the tag relationships.
				dependencies.extend(Self::get_cached_item_tags_with_transaction(
					db,
					subspace,
					transaction,
					resource,
					permission,
					cache,
				)?);
			},
			tg::grant::Permission::Group(_)
			| tg::grant::Permission::Organization(_)
			| tg::grant::Permission::Sandbox(_)
			| tg::grant::Permission::Tag(_)
			| tg::grant::Permission::User(_) => {
				if matches!(
					permission,
					tg::grant::Permission::Sandbox(
						tg::grant::permission::sandbox::Permission::Read
							| tg::grant::permission::sandbox::Permission::Write
					)
				) && resource.kind() == tg::id::Kind::Sandbox
				{
					let sandbox = tg::sandbox::Id::try_from(resource.clone())?;
					if let Some(owner) = Self::get_cached_sandbox_owner_with_transaction(
						db,
						subspace,
						transaction,
						&sandbox,
						cache,
					)? {
						let owner = match owner {
							tg::Principal::Group(id) => Some(tg::Id::from(id)),
							tg::Principal::Organization(id) => Some(tg::Id::from(id)),
							tg::Principal::Process(id) => Some(tg::Id::from(id)),
							tg::Principal::Anonymous
							| tg::Principal::Root
							| tg::Principal::Runner(_) => None,
							tg::Principal::Sandbox(id) => Some(tg::Id::from(id)),
							tg::Principal::User(id) => Some(tg::Id::from(id)),
						};
						if let Some(owner) = owner {
							dependencies.push((
								owner.clone(),
								Self::write_permission_for_resource(&owner)?,
							));
						}
					}
				}
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
		if PRECOMPUTE_REQUESTER_PRINCIPALS {
			return Ok(requester.principals.contains(principal));
		}
		if principal == &tg::grant::Principal::Public {
			return Ok(true);
		}
		if requester.principal_ == *principal {
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
				parents.push((
					tag.into(),
					tg::grant::Permission::Tag(tg::grant::permission::tag::Permission::Read),
				));
			}
		}
		cache.item_tags.insert(key, parents.clone());
		Ok(parents)
	}

	fn write_permission_for_resource(resource: &tg::Id) -> tg::Result<tg::grant::Permission> {
		match resource.kind() {
			tg::id::Kind::Group => Ok(tg::grant::Permission::Group(
				tg::grant::permission::group::Permission::Write,
			)),
			tg::id::Kind::Organization => Ok(tg::grant::Permission::Organization(
				tg::grant::permission::organization::Permission::Write,
			)),
			tg::id::Kind::Sandbox => Ok(tg::grant::Permission::Sandbox(
				tg::grant::permission::sandbox::Permission::Write,
			)),
			tg::id::Kind::Tag => Ok(tg::grant::Permission::Tag(
				tg::grant::permission::tag::Permission::Write,
			)),
			tg::id::Kind::User => Ok(tg::grant::Permission::User(
				tg::grant::permission::user::Permission::Write,
			)),
			_ => Err(tg::error!("invalid resource")),
		}
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

	fn try_get_cached_process_command_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		process: &tg::process::Id,
		cache: &mut Cache,
	) -> tg::Result<Option<tg::object::Id>> {
		if let Some(command) = cache.process_commands.get(process) {
			return Ok(command.clone());
		}
		let command = Self::try_get_process_with_transaction(db, subspace, transaction, process)?
			.and_then(|process| process.data)
			.map(|data| data.command.into());
		cache
			.process_commands
			.insert(process.clone(), command.clone());
		Ok(command)
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

	fn get_cached_process_sandbox_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		process: &tg::process::Id,
		cache: &mut Cache,
	) -> tg::Result<Option<tg::sandbox::Id>> {
		if let Some(sandbox) = cache.process_sandboxes.get(process) {
			return Ok(sandbox.clone());
		}
		let sandbox = Self::try_get_process_with_transaction(db, subspace, transaction, process)?
			.and_then(|process| process.sandbox);
		cache
			.process_sandboxes
			.insert(process.clone(), sandbox.clone());
		Ok(sandbox)
	}

	fn get_cached_sandbox_owner_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		sandbox: &tg::sandbox::Id,
		cache: &mut Cache,
	) -> tg::Result<Option<tg::Principal>> {
		if let Some(owner) = cache.sandbox_owners.get(sandbox) {
			return Ok(owner.clone());
		}
		let key = crate::lmdb::Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(sandbox.clone()));
		let key = Self::pack(subspace, &key);
		let owner = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the sandbox"))?
			.map(crate::sandbox::Sandbox::deserialize)
			.transpose()?
			.and_then(|sandbox| sandbox.data)
			.and_then(|data| data.owner);
		cache.sandbox_owners.insert(sandbox.clone(), owner.clone());
		Ok(owner)
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
