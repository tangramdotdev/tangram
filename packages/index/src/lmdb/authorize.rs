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
	target_tags: HashMap<
		(tg::Id, tg::authorization::Permission),
		Vec<(tg::Id, tg::authorization::Permission)>,
	>,
	object_children: HashMap<tg::object::Id, Vec<tg::object::Id>>,
	object_parents: HashMap<tg::object::Id, Vec<tg::object::Id>>,
	object_processes: HashMap<tg::object::Id, Vec<(tg::process::Id, crate::process::object::Kind)>>,
	organization_members: HashMap<tg::organization::Id, Vec<tg::Id>>,
	subject_contains_requester: HashMap<tg::authorization::Subject, bool>,
	process_children: HashMap<tg::process::Id, Vec<tg::process::Id>>,
	process_commands: HashMap<tg::process::Id, Option<tg::object::Id>>,
	process_objects: HashMap<tg::process::Id, Vec<(tg::object::Id, crate::process::object::Kind)>>,
	process_parents: HashMap<tg::process::Id, Vec<tg::process::Id>>,
	process_sandboxes: HashMap<tg::process::Id, Option<tg::sandbox::Id>>,
	resource_grants:
		HashMap<tg::Id, Vec<(tg::authorization::Subject, tg::authorization::Permission)>>,
	sandbox_owners: HashMap<tg::sandbox::Id, Option<tg::Principal>>,
}

struct Requester<'a> {
	principal: &'a tg::Principal,
	subject: tg::authorization::Subject,
	id: Option<tg::Id>,
	subjects: HashSet<tg::authorization::Subject>,
}

struct AuthorizationNode {
	key: (tg::Id, tg::authorization::Permission),
	dependents: Vec<usize>,
	authorized: bool,
}

struct SubtreeSearchBudget {
	max_depth: usize,
	remaining: usize,
}

struct AuthorizationContext<'a, 'txn> {
	db: &'a Db,
	subspace: &'a fdbt::Subspace,
	transaction: &'a lmdb::RoTxn<'txn>,
	authorize: crate::lmdb::AuthorizeConfig,
	requester: &'a Requester<'a>,
	token: Option<(tg::authorization::Body, tg::Id)>,
	authorization: &'a mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
	cache: &'a mut Cache,
}

impl<'a> Requester<'a> {
	fn new(principal: &'a tg::Principal) -> Self {
		let subject = principal.to_subject();
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
			subject: subject.clone(),
			id,
			subjects: HashSet::from([tg::authorization::Subject::Public, subject]),
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
		if matches!(principal, tg::Principal::Root) {
			let outputs = args
				.iter()
				.map(|arg| {
					Some(crate::authorize::Output {
						permissions: arg.permissions,
					})
				})
				.collect();
			return Ok(outputs);
		}
		let request = crate::read::Request::AuthorizeBatch {
			args: args.to_owned(),
			principal: principal.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::AuthorizeBatch(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn authorize_batch_with_transaction(
		authorize: crate::lmdb::AuthorizeConfig,
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		args: &[crate::authorize::Arg],
		principal: &tg::Principal,
	) -> tg::Result<Vec<Option<crate::authorize::Output>>> {
		if args.is_empty() {
			return Ok(Vec::new());
		}
		if matches!(principal, tg::Principal::Root) {
			let outputs = args
				.iter()
				.map(|arg| {
					Some(crate::authorize::Output {
						permissions: arg.permissions,
					})
				})
				.collect();
			return Ok(outputs);
		}
		let mut requester = Requester::new(principal);
		if PRECOMPUTE_REQUESTER_PRINCIPALS {
			Self::load_requester_subjects_with_transaction(
				db,
				subspace,
				transaction,
				&mut requester,
			)?;
		}
		let mut cache = Cache::default();
		let mut authorization = HashMap::new();
		let mut outputs = Vec::with_capacity(args.len());
		for arg in args {
			let Some((id, exact)) = Self::try_resolve_resource_with_transaction(
				db,
				subspace,
				transaction,
				&arg.resource,
			)?
			else {
				outputs.push(None);
				continue;
			};
			let permissions = if exact {
				arg.permissions
			} else {
				let Some(permissions) =
					crate::authorize::permissions_for_specifier_prefix(&id, arg.permissions)?
				else {
					outputs.push(None);
					continue;
				};
				permissions
			};
			if crate::authorize::validate(&id, permissions).is_err() {
				outputs.push(None);
				continue;
			}
			if matches!(principal, tg::Principal::Process(process) if tg::Id::from(process.clone()) == id)
			{
				outputs.push(Some(crate::authorize::Output {
					permissions: arg.permissions,
				}));
				continue;
			}
			let token = if let Some(body) = arg.token.clone() {
				let resource = body.resource.clone();
				Some((body, resource))
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
				authorization,
				authorize,
				cache: &mut cache,
				db,
				requester: &requester,
				subspace,
				token,
				transaction,
			};
			let authorized = Self::authorize_with_transaction(&mut context, &id, permissions)?;
			let permissions = if permissions == arg.permissions {
				authorized
			} else if authorized.contains(permissions) {
				arg.permissions
			} else {
				arg.permissions.empty_like()
			};
			outputs.push(Some(crate::authorize::Output { permissions }));
		}

		Ok(outputs)
	}

	fn load_requester_subjects_with_transaction(
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
					.subjects
					.insert(tg::authorization::Subject::Group(group.clone()));
				queue.push_back(group.into());
			}
			for organization in
				Self::get_member_organizations_with_transaction(db, subspace, transaction, &member)?
			{
				requester
					.subjects
					.insert(tg::authorization::Subject::Organization(organization));
			}
		}
		Ok(())
	}

	fn authorize_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permissions: tg::authorization::permission::Set,
	) -> tg::Result<tg::authorization::permission::Set> {
		let mut authorized = permissions.empty_like();
		for permission in permissions.iter() {
			let permission_authorized =
				Self::authorize_permission_with_transaction(context, resource, permission)?;
			if permission_authorized {
				authorized.insert(tg::authorization::permission::Set::from_permission(
					permission,
				));
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
		permission: tg::authorization::Permission,
	) -> tg::Result<bool> {
		if Self::authorize_permission_ordinary_with_transaction(context, resource, permission)? {
			return Ok(true);
		}
		match permission {
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			) => {
				let mut budget = SubtreeSearchBudget {
					max_depth: context.authorize.object_subtree.max_depth,
					remaining: context.authorize.object_subtree.max_objects,
				};
				Ok(Self::authorize_with_object_subtree_search_with_transaction(
					context,
					resource,
					&mut budget,
				)?
				.unwrap_or(false))
			},
			tg::authorization::Permission::Process(
				permission @ (tg::authorization::permission::process::Permission::NodeCommand
				| tg::authorization::permission::process::Permission::NodeError
				| tg::authorization::permission::process::Permission::NodeLog
				| tg::authorization::permission::process::Permission::NodeOutput),
			) => Self::authorize_process_node_with_transaction(context, resource, permission),
			tg::authorization::Permission::Process(
				permission @ (tg::authorization::permission::process::Permission::Subtree
				| tg::authorization::permission::process::Permission::SubtreeCommand
				| tg::authorization::permission::process::Permission::SubtreeError
				| tg::authorization::permission::process::Permission::SubtreeLog
				| tg::authorization::permission::process::Permission::SubtreeOutput),
			) => {
				let mut budget = SubtreeSearchBudget {
					max_depth: context.authorize.process_subtree.max_depth,
					remaining: context.authorize.process_subtree.max_processes,
				};
				Ok(
					Self::authorize_with_process_subtree_search_with_transaction(
						context,
						resource,
						permission,
						&mut budget,
					)?
					.unwrap_or(false),
				)
			},
			_ => Ok(false),
		}
	}

	fn authorize_permission_ordinary_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::authorization::Permission,
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
		authorization: &mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
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
		permission: tg::authorization::Permission,
	) -> tg::Result<bool> {
		if let (tg::Principal::Process(process), tg::authorization::Permission::Process(_)) =
			(context.requester.principal, permission)
			&& tg::Id::from(process.clone()) == *resource
		{
			return Ok(true);
		}
		if let (tg::Principal::User(user), tg::authorization::Permission::User(_)) =
			(context.requester.principal, permission)
			&& tg::Id::from(user.clone()) == *resource
		{
			return Ok(true);
		}
		if let (
			tg::Principal::Sandbox(sandbox),
			tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Read
				| tg::authorization::permission::sandbox::Permission::Write,
			),
		) = (context.requester.principal, permission)
			&& tg::Id::from(sandbox.clone()) == *resource
		{
			return Ok(true);
		}
		if matches!(
			permission,
			tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Read
					| tg::authorization::permission::sandbox::Permission::Write
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
				let owner = owner.try_to_subject()?;
				if Self::subject_contains_requester_with_transaction(
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
		for (granted_subject, granted_permission) in grants {
			if granted_permission.implies(permission)
				&& Self::subject_contains_requester_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&granted_subject,
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
		permission: tg::authorization::Permission,
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
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let root = tg::object::Id::try_from(resource.clone())?;
		let mut visited = HashSet::from([root.clone()]);
		let mut queue = VecDeque::from([(root, 0)]);
		while let Some((object, depth)) = queue.pop_front() {
			if budget.remaining == 0 {
				return Ok(None);
			}
			budget.remaining -= 1;

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
					.remaining
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
			if queue.len() > budget.remaining {
				return Ok(None);
			}
		}

		Ok(Some(true))
	}

	fn authorize_with_process_subtree_search_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::authorization::permission::process::Permission,
		budget: &mut SubtreeSearchBudget,
	) -> tg::Result<Option<bool>> {
		let node_permission = match permission {
			tg::authorization::permission::process::Permission::Subtree => {
				tg::authorization::permission::process::Permission::Node
			},
			tg::authorization::permission::process::Permission::SubtreeCommand => {
				tg::authorization::permission::process::Permission::NodeCommand
			},
			tg::authorization::permission::process::Permission::SubtreeError => {
				tg::authorization::permission::process::Permission::NodeError
			},
			tg::authorization::permission::process::Permission::SubtreeLog => {
				tg::authorization::permission::process::Permission::NodeLog
			},
			tg::authorization::permission::process::Permission::SubtreeOutput => {
				tg::authorization::permission::process::Permission::NodeOutput
			},
			_ => unreachable!(),
		};
		let subtree = tg::authorization::Permission::Process(permission);
		let root = tg::process::Id::try_from(resource.clone())?;
		let mut visited = HashSet::from([root.clone()]);
		let mut queue = VecDeque::from([(root, 0)]);
		while let Some((process, depth)) = queue.pop_front() {
			if budget.remaining == 0 {
				return Ok(None);
			}
			budget.remaining -= 1;

			let resource = tg::Id::from(process.clone());
			if Self::authorize_permission_ordinary_with_transaction(context, &resource, subtree)? {
				continue;
			}
			if !Self::authorize_process_node_with_transaction(context, &resource, node_permission)?
			{
				return Ok(Some(false));
			}

			let limit = if depth == budget.max_depth {
				visited.len().saturating_add(1)
			} else {
				budget
					.remaining
					.saturating_add(visited.len())
					.saturating_add(1)
			};
			let children = Self::get_cached_process_children_limited_with_transaction(
				context.db,
				context.subspace,
				context.transaction,
				&process,
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
			if queue.len() > budget.remaining {
				return Ok(None);
			}
		}

		Ok(Some(true))
	}

	fn authorize_process_node_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::authorization::permission::process::Permission,
	) -> tg::Result<bool> {
		let process_permission = tg::authorization::Permission::Process(permission);
		if Self::authorize_permission_ordinary_with_transaction(
			context,
			resource,
			process_permission,
		)? {
			return Ok(true);
		}
		let kind = match permission {
			tg::authorization::permission::process::Permission::NodeCommand => {
				crate::process::object::Kind::Command
			},
			tg::authorization::permission::process::Permission::NodeError => {
				crate::process::object::Kind::Error
			},
			tg::authorization::permission::process::Permission::NodeLog => {
				crate::process::object::Kind::Log
			},
			tg::authorization::permission::process::Permission::NodeOutput => {
				crate::process::object::Kind::Output
			},
			_ => return Ok(false),
		};
		let process = tg::process::Id::try_from(resource.clone())?;
		let objects = Self::get_cached_process_objects_with_transaction(
			context.db,
			context.subspace,
			context.transaction,
			&process,
			context.cache,
		)?;
		let objects = objects
			.into_iter()
			.filter_map(|(object, object_kind)| {
				let matches = match kind {
					crate::process::object::Kind::Command => object_kind.is_command(),
					crate::process::object::Kind::Error => object_kind.is_error(),
					crate::process::object::Kind::Log => object_kind.is_log(),
					crate::process::object::Kind::Output => object_kind.is_output(),
				};
				matches.then_some(object)
			})
			.collect::<Vec<_>>();
		let process = Self::try_get_process_with_transaction(
			context.db,
			context.subspace,
			context.transaction,
			&process,
		)?;
		let aspect_is_set = process.is_some_and(|process| match kind {
			crate::process::object::Kind::Command => true,
			crate::process::object::Kind::Error => process.set.error,
			crate::process::object::Kind::Log => process.set.log,
			crate::process::object::Kind::Output => process.set.output,
		});
		if !aspect_is_set || (kind.is_command() && objects.is_empty()) {
			return Ok(false);
		}
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		for object in objects {
			let resource = tg::Id::from(object);
			if Self::authorize_permission_ordinary_with_transaction(context, &resource, subtree)? {
				continue;
			}
			let mut budget = SubtreeSearchBudget {
				max_depth: context.authorize.object_subtree.max_depth,
				remaining: context.authorize.object_subtree.max_objects,
			};
			if !Self::authorize_with_object_subtree_search_with_transaction(
				context,
				&resource,
				&mut budget,
			)?
			.unwrap_or(false)
			{
				return Ok(false);
			}
		}

		Ok(true)
	}

	fn get_authorization_dependencies_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		permission: tg::authorization::Permission,
		requester: &Requester<'_>,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::Id, tg::authorization::Permission)>> {
		let mut dependencies = Vec::new();

		// Add the process subject grant relationships.
		let grants = Self::get_cached_resource_grants_with_transaction(
			db,
			subspace,
			transaction,
			resource,
			cache,
		)?;
		for (subject, granted_permission) in grants {
			if !granted_permission.implies(permission) {
				continue;
			}
			let tg::authorization::Subject::Process(process) = subject else {
				continue;
			};
			let permission = if permission.is_read() {
				tg::authorization::permission::process::Permission::Read
			} else {
				tg::authorization::permission::process::Permission::Write
			};
			let permission = tg::authorization::Permission::Process(permission);
			dependencies.push((process.into(), permission));
		}

		match permission {
			tg::authorization::Permission::Object(_) => {
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
					let permission = tg::authorization::Permission::Process(
						tg::authorization::permission::process::Permission::NodeCommand,
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
							tg::authorization::permission::process::Permission::NodeCommand
						},
						crate::process::object::Kind::Error => {
							tg::authorization::permission::process::Permission::NodeError
						},
						crate::process::object::Kind::Log => {
							tg::authorization::permission::process::Permission::NodeLog
						},
						crate::process::object::Kind::Output => {
							tg::authorization::permission::process::Permission::NodeOutput
						},
					};
					dependencies.push((
						process.into(),
						tg::authorization::Permission::Process(permission),
					));
				}

				// Add the object parent relationships.
				for parent in object_parents {
					let permission = tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					);
					dependencies.push((parent.into(), permission));
				}

				// Add the tag relationships.
				dependencies.extend(Self::get_cached_target_tags_with_transaction(
					db,
					subspace,
					transaction,
					resource,
					permission,
					cache,
				)?);
			},
			tg::authorization::Permission::Process(process_permission) => {
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
						tg::authorization::Permission::Process(process_permission.to_subtree());
					dependencies.push((parent.into(), permission));
				}

				// Add the sandbox relationship.
				if let Some(sandbox) = sandbox {
					let sandbox_permission = match process_permission {
						tg::authorization::permission::process::Permission::Write => {
							tg::authorization::permission::sandbox::Permission::Write
						},
						_ => tg::authorization::permission::sandbox::Permission::Read,
					};
					dependencies.push((
						sandbox.into(),
						tg::authorization::Permission::Sandbox(sandbox_permission),
					));
				}

				// Add the tag relationships.
				dependencies.extend(Self::get_cached_target_tags_with_transaction(
					db,
					subspace,
					transaction,
					resource,
					permission,
					cache,
				)?);
			},
			tg::authorization::Permission::Group(_)
			| tg::authorization::Permission::Organization(_)
			| tg::authorization::Permission::Sandbox(_)
			| tg::authorization::Permission::Tag(_)
			| tg::authorization::Permission::User(_) => {
				if matches!(
					permission,
					tg::authorization::Permission::Sandbox(
						tg::authorization::permission::sandbox::Permission::Read
							| tg::authorization::permission::sandbox::Permission::Write
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
								crate::authorize::write_permission_for_resource(&owner)?,
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
					let permission =
						crate::authorize::permission_for_named_parent(&parent, permission)?;
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

	fn get_cached_process_children_limited_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		process: &tg::process::Id,
		limit: usize,
		cache: &mut Cache,
	) -> tg::Result<Vec<tg::process::Id>> {
		if let Some(children) = cache.process_children.get(process) {
			return Ok(children.iter().take(limit).cloned().collect());
		}
		let process_bytes = process.to_bytes();
		let prefix = &(
			crate::lmdb::Kind::ProcessChild.to_i32().unwrap(),
			process_bytes.as_ref(),
		);
		let prefix = Self::pack(subspace, prefix);
		let mut children = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get process children"))?;
		for entry in iter.take(limit) {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read process child entry"))?;
			let key = Self::unpack(subspace, key)?;
			let crate::lmdb::Key::Process(crate::lmdb::process::Key::ProcessChild {
				child, ..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			children.push(child);
		}
		if children.len() < limit {
			cache
				.process_children
				.insert(process.clone(), children.clone());
		}

		Ok(children)
	}

	fn get_cached_process_objects_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		process: &tg::process::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::object::Id, crate::process::object::Kind)>> {
		if let Some(objects) = cache.process_objects.get(process) {
			return Ok(objects.clone());
		}
		let process_bytes = process.to_bytes();
		let prefix = &(
			crate::lmdb::Kind::ProcessObject.to_i32().unwrap(),
			process_bytes.as_ref(),
		);
		let prefix = Self::pack(subspace, prefix);
		let mut objects = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get process objects"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read process object entry"))?;
			let key = Self::unpack(subspace, key)?;
			let crate::lmdb::Key::Process(crate::lmdb::process::Key::ProcessObject {
				kind,
				object,
				..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			objects.push((object, kind));
		}
		cache
			.process_objects
			.insert(process.clone(), objects.clone());

		Ok(objects)
	}

	fn subject_contains_requester_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		subject: &tg::authorization::Subject,
		requester: &Requester<'_>,
		cache: &mut Cache,
	) -> tg::Result<bool> {
		if PRECOMPUTE_REQUESTER_PRINCIPALS {
			return Ok(requester.subjects.contains(subject));
		}
		if subject == &tg::authorization::Subject::Public {
			return Ok(true);
		}
		if requester.subject == *subject {
			return Ok(true);
		}
		if requester.id.is_none() {
			return Ok(false);
		}
		if let Some(contains) = cache.subject_contains_requester.get(subject) {
			return Ok(*contains);
		}
		let contains = match subject {
			tg::authorization::Subject::Group(group) => {
				Self::group_contains_requester_with_transaction(
					db,
					subspace,
					transaction,
					group,
					requester,
					cache,
				)?
			},
			tg::authorization::Subject::Organization(organization) => {
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
			.subject_contains_requester
			.insert(subject.clone(), contains);
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
		let subject = tg::authorization::Subject::Group(group.clone());
		if let Some(contains) = cache.subject_contains_requester.get(&subject) {
			return Ok(*contains);
		}
		let root = group.clone();
		let mut visited = HashSet::new();
		let mut queue = VecDeque::from([group.clone()]);
		while let Some(group) = queue.pop_front() {
			if !visited.insert(group.clone()) {
				continue;
			}
			let subject = tg::authorization::Subject::Group(group.clone());
			if let Some(contains) = cache.subject_contains_requester.get(&subject) {
				if *contains {
					cache
						.subject_contains_requester
						.insert(tg::authorization::Subject::Group(root), true);
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
						.subject_contains_requester
						.insert(tg::authorization::Subject::Group(root), true);
					return Ok(true);
				}
				if member.kind() == tg::id::Kind::Group {
					queue.push_back(tg::group::Id::try_from(member)?);
				}
			}
		}
		for group in visited {
			cache
				.subject_contains_requester
				.insert(tg::authorization::Subject::Group(group), false);
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
		let subject = tg::authorization::Subject::Organization(organization.clone());
		if let Some(contains) = cache.subject_contains_requester.get(&subject) {
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
				cache.subject_contains_requester.insert(
					tg::authorization::Subject::Organization(organization.clone()),
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
					cache.subject_contains_requester.insert(
						tg::authorization::Subject::Organization(organization.clone()),
						true,
					);
					return Ok(true);
				}
			}
		}
		cache.subject_contains_requester.insert(
			tg::authorization::Subject::Organization(organization.clone()),
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
	) -> tg::Result<Vec<(tg::authorization::Subject, tg::authorization::Permission)>> {
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

	fn get_cached_target_tags_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		node: &tg::Id,
		permission: tg::authorization::Permission,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::Id, tg::authorization::Permission)>> {
		let key = (node.clone(), permission);
		if let Some(tags) = cache.target_tags.get(&key) {
			return Ok(tags.clone());
		}
		let target_bytes = node.to_bytes();
		let tags = Self::get_target_tags_with_transaction(
			db,
			subspace,
			transaction,
			target_bytes.as_ref(),
		)?;
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
					tg::authorization::Permission::Tag(
						tg::authorization::permission::tag::Permission::Read,
					),
				));
			}
		}
		cache.target_tags.insert(key, parents.clone());
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
			.map(|data| data.command.node.into());
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
