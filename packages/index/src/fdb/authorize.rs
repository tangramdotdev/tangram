use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::{StreamExt as _, TryStreamExt as _, stream},
	num_traits::ToPrimitive as _,
	std::collections::{HashMap, HashSet, VecDeque},
	tangram_client::prelude::*,
};

const PRECOMPUTE_REQUESTER_PRINCIPALS: bool = false;

#[derive(Default)]
struct Cache {
	resource_parents: HashMap<tg::Id, Option<tg::Id>>,
	item_tags: HashMap<(tg::Id, tg::grant::Permission), Vec<(tg::Id, tg::grant::Permission)>>,
	object_children: HashMap<tg::object::Id, Vec<tg::object::Id>>,
	object_parents: HashMap<tg::object::Id, Vec<tg::object::Id>>,
	object_processes: HashMap<tg::object::Id, Vec<(tg::process::Id, crate::process::object::Kind)>>,
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
	membership_cache: std::sync::Mutex<RequesterMembershipCache>,
}

#[derive(Default)]
struct RequesterMembershipCache {
	group_members: HashMap<tg::group::Id, std::sync::Arc<[tg::Id]>>,
	organization_members: HashMap<tg::organization::Id, std::sync::Arc<[tg::Id]>>,
	principal_contains_requester: HashMap<tg::grant::Principal, bool>,
}

struct AuthorizationNode {
	key: (tg::Id, tg::grant::Permission),
	dependents: Vec<usize>,
	authorized: bool,
}

struct AuthorizationNodeEvaluation {
	node_id: usize,
	directly_authorized: bool,
	dependencies: Vec<(tg::Id, tg::grant::Permission)>,
	cache: Cache,
}

#[derive(Clone, Copy)]
struct AuthorizationContext<'a> {
	txn: &'a fdb::Transaction,
	subspace: &'a Subspace,
	config: crate::fdb::AuthorizeConfig,
	requester: &'a Requester<'a>,
	token: Option<(&'a tg::grant::Body, &'a tg::Id)>,
}

struct SubtreeSearchBudget {
	max_depth: usize,
	remaining_objects: usize,
}

impl Cache {
	fn merge(&mut self, other: Self) {
		self.resource_parents.extend(other.resource_parents);
		self.item_tags.extend(other.item_tags);
		self.object_children.extend(other.object_children);
		self.object_parents.extend(other.object_parents);
		self.object_processes.extend(other.object_processes);
		self.process_parents.extend(other.process_parents);
		self.process_sandboxes.extend(other.process_sandboxes);
		self.resource_grants.extend(other.resource_grants);
		self.sandbox_owners.extend(other.sandbox_owners);
	}

	fn clone_for_direct_authorization(
		&self,
		resource: &tg::Id,
		permission: tg::grant::Permission,
	) -> Self {
		let mut cache = Self::default();
		if let Some(grants) = self.resource_grants.get(resource) {
			cache
				.resource_grants
				.insert(resource.clone(), grants.clone());
		}
		if matches!(permission, tg::grant::Permission::Sandbox(_))
			&& let Ok(sandbox) = tg::sandbox::Id::try_from(resource.clone())
			&& let Some(owner) = self.sandbox_owners.get(&sandbox)
		{
			cache.sandbox_owners.insert(sandbox, owner.clone());
		}
		cache
	}

	fn clone_for_authorization_dependencies(
		&self,
		resource: &tg::Id,
		permission: tg::grant::Permission,
	) -> Self {
		let mut cache = Self::default();
		if let Some(parent) = self.resource_parents.get(resource) {
			cache
				.resource_parents
				.insert(resource.clone(), parent.clone());
		}
		if let Some(tags) = self.item_tags.get(&(resource.clone(), permission)) {
			cache
				.item_tags
				.insert((resource.clone(), permission), tags.clone());
		}
		match permission {
			tg::grant::Permission::Object(_) => {
				if let Ok(object) = tg::object::Id::try_from(resource.clone()) {
					if let Some(parents) = self.object_parents.get(&object) {
						cache.object_parents.insert(object.clone(), parents.clone());
					}
					if let Some(processes) = self.object_processes.get(&object) {
						cache.object_processes.insert(object, processes.clone());
					}
				}
			},
			tg::grant::Permission::Process(_) => {
				if let Ok(process) = tg::process::Id::try_from(resource.clone()) {
					if let Some(parents) = self.process_parents.get(&process) {
						cache
							.process_parents
							.insert(process.clone(), parents.clone());
					}
					if let Some(sandbox) = self.process_sandboxes.get(&process) {
						cache.process_sandboxes.insert(process, sandbox.clone());
					}
				}
			},
			tg::grant::Permission::Sandbox(_) => {
				if let Ok(sandbox) = tg::sandbox::Id::try_from(resource.clone())
					&& let Some(owner) = self.sandbox_owners.get(&sandbox)
				{
					cache.sandbox_owners.insert(sandbox, owner.clone());
				}
			},
			_ => {},
		}
		cache
	}

	fn clone_for_object_children(&self, object: &tg::object::Id) -> Self {
		let mut cache = Self::default();
		if let Some(children) = self.object_children.get(object) {
			cache
				.object_children
				.insert(object.clone(), children.clone());
		}
		cache
	}
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
			membership_cache: std::sync::Mutex::default(),
		}
	}

	fn membership_cache(&self) -> std::sync::MutexGuard<'_, RequesterMembershipCache> {
		self.membership_cache
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
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

	pub(crate) async fn authorize_batch_with_transaction(
		config: crate::fdb::AuthorizeConfig,
		txn: &fdb::Transaction,
		subspace: &Subspace,
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
			Self::load_requester_principals_with_transaction(
				txn,
				subspace,
				config.concurrency,
				&mut requester,
			)
			.await?;
		}
		let resource_requests = args
			.iter()
			.map(|arg| arg.resource.clone())
			.collect::<Vec<_>>();
		let resources = stream::iter(resource_requests)
			.map(|resource| async move {
				Self::try_resolve_resource_with_transaction(txn, subspace, &resource).await
			})
			.buffered(config.concurrency)
			.try_collect::<Vec<_>>()
			.await?;
		let token_requests = std::iter::zip(args, &resources)
			.enumerate()
			.filter_map(|(index, (arg, resource))| {
				let resource = resource.as_ref()?;
				if crate::authorize::validate(resource, arg.permissions).is_err()
					|| matches!(principal, tg::Principal::Process(process) if tg::Id::from(process.clone()) == *resource)
				{
					return None;
				}
				arg.token.clone().map(|body| (index, body))
			})
			.collect::<Vec<_>>();
		let resolved_tokens = stream::iter(token_requests)
			.map(|(index, body)| async move {
				let resource =
					Self::try_resolve_resource_with_transaction(txn, subspace, &body.resource)
						.await?;
				Ok::<_, tg::Error>((index, resource))
			})
			.buffered(config.concurrency)
			.try_collect::<Vec<_>>()
			.await?;
		let mut token_resources = vec![None; args.len()];
		for (index, resource) in resolved_tokens {
			token_resources[index] = resource;
		}
		let mut cache = Cache::default();
		let mut authorization = HashMap::new();
		let mut outputs = Vec::with_capacity(args.len());
		for (index, (arg, id)) in std::iter::zip(args, resources).enumerate() {
			let Some(id) = id else {
				outputs.push(None);
				continue;
			};
			if crate::authorize::validate(&id, arg.permissions).is_err() {
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
			let token = arg.token.as_ref().zip(token_resources[index].as_ref());
			let mut token_authorization = HashMap::new();
			let authorization = if token.is_some() {
				&mut token_authorization
			} else {
				&mut authorization
			};
			let context = AuthorizationContext {
				config,
				requester: &requester,
				subspace,
				token,
				txn,
			};
			let permissions = Self::authorize_with_transaction(
				context,
				&id,
				arg.permissions,
				authorization,
				&mut cache,
			)
			.await?;
			outputs.push(Some(crate::authorize::Output { permissions }));
		}

		Ok(outputs)
	}

	async fn load_requester_principals_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		concurrency: usize,
		requester: &mut Requester<'_>,
	) -> tg::Result<()> {
		let Some(id) = requester.id.clone() else {
			return Ok(());
		};
		if !matches!(id.kind(), tg::id::Kind::Group | tg::id::Kind::User) {
			return Ok(());
		}
		let mut visited = HashSet::new();
		let mut frontier = vec![id];
		while !frontier.is_empty() {
			let members = frontier
				.into_iter()
				.filter(|member| visited.insert(member.clone()))
				.collect::<Vec<_>>();
			let relations = stream::iter(members)
				.map(|member| async move {
					futures::try_join!(
						Self::get_member_groups_with_transaction(txn, subspace, &member),
						Self::get_member_organizations_with_transaction(txn, subspace, &member),
					)
				})
				.buffer_unordered(concurrency)
				.try_collect::<Vec<_>>()
				.await?;
			let mut next = Vec::new();
			for (groups, organizations) in relations {
				for group in groups {
					requester
						.principals
						.insert(tg::grant::Principal::Group(group.clone()));
					next.push(group.into());
				}
				for organization in organizations {
					requester
						.principals
						.insert(tg::grant::Principal::Organization(organization));
				}
			}
			frontier = next;
		}
		Ok(())
	}

	async fn authorize_with_transaction(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		permissions: tg::grant::permission::Set,
		authorization: &mut HashMap<(tg::Id, tg::grant::Permission), bool>,
		cache: &mut Cache,
	) -> tg::Result<tg::grant::permission::Set> {
		let roots = permissions
			.iter()
			.map(|permission| (resource.clone(), permission))
			.collect::<Vec<_>>();
		Self::authorize_permissions_ordinary_with_transaction(
			context,
			&roots,
			authorization,
			cache,
		)
		.await?;
		let mut authorized = permissions.empty_like();
		for permission in permissions.iter() {
			let key = (resource.clone(), permission);
			if authorization.get(&key).copied().unwrap_or(false) {
				authorized.insert(tg::grant::permission::Set::from_permission(permission));
				continue;
			}
			if permission
				== tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree)
				&& Self::authorize_with_object_subtree_search_with_transaction(
					context,
					resource,
					authorization,
					cache,
				)
				.await?
				.unwrap_or(false)
			{
				authorized.insert(tg::grant::permission::Set::from_permission(permission));
			}
		}
		Ok(authorized)
	}

	async fn authorize_permissions_ordinary_with_transaction(
		context: AuthorizationContext<'_>,
		roots: &[(tg::Id, tg::grant::Permission)],
		authorization: &mut HashMap<(tg::Id, tg::grant::Permission), bool>,
		cache: &mut Cache,
	) -> tg::Result<()> {
		let mut nodes = Vec::new();
		let mut node_ids = HashMap::new();
		let mut queue = VecDeque::new();
		for root in roots {
			if authorization.contains_key(root) || node_ids.contains_key(root) {
				continue;
			}
			let node_id = nodes.len();
			node_ids.insert(root.clone(), node_id);
			nodes.push(AuthorizationNode {
				key: root.clone(),
				dependents: Vec::new(),
				authorized: false,
			});
			queue.push_back(node_id);
		}
		while !queue.is_empty() {
			let mut requests = Vec::new();
			while requests.len() < context.config.concurrency
				&& let Some(node_id) = queue.pop_front()
			{
				if nodes[node_id].authorized {
					continue;
				}
				let (resource, permission) = nodes[node_id].key.clone();
				if Self::is_authorized_by_token(context, &resource, permission) {
					Self::propagate_authorization(authorization, &mut nodes, node_id);
					continue;
				}
				requests.push((
					node_id,
					resource.clone(),
					permission,
					cache.clone_for_direct_authorization(&resource, permission),
					cache.clone_for_authorization_dependencies(&resource, permission),
				));
			}
			if roots
				.iter()
				.all(|root| authorization.get(root).copied().unwrap_or(false))
			{
				return Ok(());
			}
			let dependency_concurrency = context.config.concurrency.div_ceil(requests.len().max(1));

			let evaluations = stream::iter(requests)
				.map(
					|(node_id, resource, permission, mut direct_cache, mut dependency_cache)| async move {
						let directly_authorized = Self::is_directly_authorized_with_transaction(
							context,
							&resource,
							permission,
							&mut direct_cache,
						);
						let dependencies = Self::get_authorization_dependencies_with_transaction(
							context.txn,
							context.subspace,
							dependency_concurrency,
							&resource,
							permission,
							&mut dependency_cache,
						);
						let (directly_authorized, dependencies) =
							futures::try_join!(directly_authorized, dependencies)?;
						direct_cache.merge(dependency_cache);
						Ok::<_, tg::Error>(AuthorizationNodeEvaluation {
							node_id,
							directly_authorized,
							dependencies,
							cache: direct_cache,
						})
					},
				)
				.buffer_unordered(context.config.concurrency)
				.try_collect::<Vec<_>>()
				.await?;

			for evaluation in evaluations {
				cache.merge(evaluation.cache);
				if nodes[evaluation.node_id].authorized {
					continue;
				}
				if evaluation.directly_authorized {
					Self::propagate_authorization(authorization, &mut nodes, evaluation.node_id);
					continue;
				}
				for (dependency, dependency_permission) in evaluation.dependencies {
					let dependency_key = (dependency, dependency_permission);
					match authorization.get(&dependency_key).copied() {
						Some(true) => {
							Self::propagate_authorization(
								authorization,
								&mut nodes,
								evaluation.node_id,
							);
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
							nodes[dependency_id].dependents.push(evaluation.node_id);
						},
					}
				}
			}
			if roots
				.iter()
				.all(|root| authorization.get(root).copied().unwrap_or(false))
			{
				return Ok(());
			}
		}

		for node in nodes {
			authorization.entry(node.key).or_insert(false);
		}
		Ok(())
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

	async fn is_directly_authorized_with_transaction(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
		cache: &mut Cache,
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
				context.txn,
				context.subspace,
				&sandbox,
				cache,
			)
			.await?
			{
				let owner = owner.try_to_grant_principal()?;
				if Self::principal_contains_requester_with_transaction(
					context.txn,
					context.subspace,
					&owner,
					context.requester,
				)
				.await?
				{
					return Ok(true);
				}
			}
		}

		let grants = Self::get_cached_resource_grants_with_transaction(
			context.txn,
			context.subspace,
			resource,
			cache,
		)
		.await?;
		for (granted_principal, granted_permission) in grants {
			if granted_permission.implies(permission)
				&& Self::principal_contains_requester_with_transaction(
					context.txn,
					context.subspace,
					&granted_principal,
					context.requester,
				)
				.await?
			{
				return Ok(true);
			}
		}
		Ok(false)
	}

	fn is_authorized_by_token(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
	) -> bool {
		context.token.is_some_and(|(body, token_resource)| {
			token_resource == resource && body.grants(permission)
		})
	}

	async fn authorize_with_object_subtree_search_with_transaction(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		authorization: &mut HashMap<(tg::Id, tg::grant::Permission), bool>,
		cache: &mut Cache,
	) -> tg::Result<Option<bool>> {
		let subtree =
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree);
		let node = tg::grant::Permission::Object(tg::grant::permission::object::Permission::Node);
		let mut budget = SubtreeSearchBudget {
			max_depth: context.config.object_subtree.max_depth,
			remaining_objects: context.config.object_subtree.max_objects,
		};
		let root = tg::object::Id::try_from(resource.clone())?;
		let mut visited = HashSet::from([root.clone()]);
		let mut frontier = vec![root];
		let mut depth = 0;
		while !frontier.is_empty() {
			if frontier.len() > budget.remaining_objects {
				return Ok(None);
			}
			budget.remaining_objects -= frontier.len();
			let subtree_roots = frontier
				.iter()
				.map(|object| (tg::Id::from(object.clone()), subtree))
				.collect::<Vec<_>>();
			Self::authorize_permissions_ordinary_with_transaction(
				context,
				&subtree_roots,
				authorization,
				cache,
			)
			.await?;
			let uncovered = frontier
				.into_iter()
				.filter(|object| {
					let key = (tg::Id::from(object.clone()), subtree);
					!authorization.get(&key).copied().unwrap_or(false)
				})
				.collect::<Vec<_>>();
			if uncovered.is_empty() {
				break;
			}

			let node_roots = uncovered
				.iter()
				.map(|object| (tg::Id::from(object.clone()), node))
				.collect::<Vec<_>>();
			Self::authorize_permissions_ordinary_with_transaction(
				context,
				&node_roots,
				authorization,
				cache,
			)
			.await?;
			if node_roots
				.iter()
				.any(|key| !authorization.get(key).copied().unwrap_or(false))
			{
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
			let requests = uncovered
				.into_iter()
				.map(|object| (object.clone(), cache.clone_for_object_children(&object)))
				.collect::<Vec<_>>();
			let children = stream::iter(requests)
				.map(|(object, mut cache)| async move {
					let children = Self::get_cached_object_children_limited_with_transaction(
						context.txn,
						context.subspace,
						&object,
						limit,
						&mut cache,
					)
					.await?;
					Ok::<_, tg::Error>((children, cache))
				})
				.buffer_unordered(context.config.concurrency)
				.try_collect::<Vec<_>>()
				.await?;
			let mut next = Vec::new();
			for (children, child_cache) in children {
				cache.merge(child_cache);
				for child in children {
					if visited.insert(child.clone()) {
						if depth == budget.max_depth {
							return Ok(None);
						}
						next.push(child);
					}
				}
			}
			if next.len() > budget.remaining_objects {
				return Ok(None);
			}
			frontier = next;
			depth += 1;
		}

		Ok(Some(true))
	}

	async fn get_authorization_dependencies_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		concurrency: usize,
		resource: &tg::Id,
		permission: tg::grant::Permission,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::Id, tg::grant::Permission)>> {
		let mut dependencies = Vec::new();
		match permission {
			tg::grant::Permission::Object(_) => {
				let object = tg::object::Id::try_from(resource.clone())?;
				let cached_parents = cache.object_parents.get(&object).cloned();
				let cached_processes = cache.object_processes.get(&object).cloned();
				let tag_key = (resource.clone(), permission);
				let cached_tags = cache.item_tags.get(&tag_key).cloned();
				let object_parents = async {
					if let Some(parents) = cached_parents {
						Ok(parents)
					} else {
						Self::get_object_parents_with_transaction(txn, subspace, &object).await
					}
				};
				let processes = async {
					if let Some(processes) = cached_processes {
						Ok(processes)
					} else {
						Self::get_object_processes_with_transaction(txn, subspace, &object).await
					}
				};
				let tags = async {
					if let Some(tags) = cached_tags {
						Ok(tags)
					} else {
						let mut tag_cache = Cache::default();
						Self::get_cached_item_tags_with_transaction(
							txn,
							subspace,
							concurrency,
							resource,
							permission,
							&mut tag_cache,
						)
						.await
					}
				};
				let (object_parents, processes, tags) =
					futures::try_join!(object_parents, processes, tags)?;
				cache
					.object_parents
					.insert(object.clone(), object_parents.clone());
				cache.object_processes.insert(object, processes.clone());
				cache.item_tags.insert(tag_key, tags.clone());
				for parent in object_parents {
					let permission = tg::grant::Permission::Object(
						tg::grant::permission::object::Permission::Subtree,
					);
					dependencies.push((parent.into(), permission));
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
				dependencies.extend(tags);
			},
			tg::grant::Permission::Process(process_permission) => {
				let process = tg::process::Id::try_from(resource.clone())?;
				let cached_sandbox = cache.process_sandboxes.get(&process).cloned();
				let cached_parents = cache.process_parents.get(&process).cloned();
				let tag_key = (resource.clone(), permission);
				let cached_tags = cache.item_tags.get(&tag_key).cloned();
				let sandbox = async {
					if let Some(sandbox) = cached_sandbox {
						Ok(sandbox)
					} else {
						Ok::<_, tg::Error>(
							Self::try_get_process_with_transaction(txn, subspace, &process)
								.await?
								.and_then(|process| process.sandbox),
						)
					}
				};
				let process_parents = async {
					if let Some(parents) = cached_parents {
						Ok(parents)
					} else {
						Self::get_process_parents_with_transaction(txn, subspace, &process).await
					}
				};
				let tags = async {
					if let Some(tags) = cached_tags {
						Ok(tags)
					} else {
						let mut tag_cache = Cache::default();
						Self::get_cached_item_tags_with_transaction(
							txn,
							subspace,
							concurrency,
							resource,
							permission,
							&mut tag_cache,
						)
						.await
					}
				};
				let (sandbox, process_parents, tags) =
					futures::try_join!(sandbox, process_parents, tags)?;
				cache
					.process_sandboxes
					.insert(process.clone(), sandbox.clone());
				cache
					.process_parents
					.insert(process, process_parents.clone());
				cache.item_tags.insert(tag_key, tags.clone());
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
				for parent in process_parents {
					let permission =
						tg::grant::Permission::Process(process_permission.to_subtree());
					dependencies.push((parent.into(), permission));
				}
				dependencies.extend(tags);
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
						txn, subspace, &sandbox, cache,
					)
					.await?
					{
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
					txn, subspace, resource, cache,
				)
				.await?
				{
					dependencies.push((parent, permission));
				}
			},
		}
		Ok(dependencies)
	}

	async fn principal_contains_requester_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		principal: &tg::grant::Principal,
		requester: &Requester<'_>,
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
		let contains = {
			let cache = requester.membership_cache();
			cache.principal_contains_requester.get(principal).copied()
		};
		if let Some(contains) = contains {
			return Ok(contains);
		}
		let contains = match principal {
			tg::grant::Principal::Group(group) => {
				Self::group_contains_requester_with_transaction(txn, subspace, group, requester)
					.await?
			},
			tg::grant::Principal::Organization(organization) => {
				Self::organization_contains_requester_with_transaction(
					txn,
					subspace,
					organization,
					requester,
				)
				.await?
			},
			_ => false,
		};
		requester
			.membership_cache()
			.principal_contains_requester
			.insert(principal.clone(), contains);
		Ok(contains)
	}

	async fn group_contains_requester_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		group: &tg::group::Id,
		requester: &Requester<'_>,
	) -> tg::Result<bool> {
		let principal = tg::grant::Principal::Group(group.clone());
		let contains = {
			let cache = requester.membership_cache();
			cache.principal_contains_requester.get(&principal).copied()
		};
		if let Some(contains) = contains {
			return Ok(contains);
		}
		let root = group.clone();
		let mut visited = HashSet::new();
		let mut queue = VecDeque::from([group.clone()]);
		while let Some(group) = queue.pop_front() {
			if !visited.insert(group.clone()) {
				continue;
			}
			let principal = tg::grant::Principal::Group(group.clone());
			let contains = {
				let cache = requester.membership_cache();
				cache.principal_contains_requester.get(&principal).copied()
			};
			if let Some(contains) = contains {
				if contains {
					requester
						.membership_cache()
						.principal_contains_requester
						.insert(tg::grant::Principal::Group(root), true);
					return Ok(true);
				}
				continue;
			}
			let members = {
				let cache = requester.membership_cache();
				cache.group_members.get(&group).cloned()
			};
			let members = if let Some(members) = members {
				members
			} else {
				let members: std::sync::Arc<[tg::Id]> =
					Self::get_group_members_with_transaction(txn, subspace, &group)
						.await?
						.into_iter()
						.map(tg::Id::from)
						.collect::<Vec<_>>()
						.into();
				requester
					.membership_cache()
					.group_members
					.insert(group.clone(), members.clone());
				members
			};
			for member in members.iter().cloned() {
				if requester.id.as_ref() == Some(&member) {
					requester
						.membership_cache()
						.principal_contains_requester
						.insert(tg::grant::Principal::Group(root), true);
					return Ok(true);
				}
				if member.kind() == tg::id::Kind::Group {
					queue.push_back(tg::group::Id::try_from(member)?);
				}
			}
		}
		let mut cache = requester.membership_cache();
		for group in visited {
			cache
				.principal_contains_requester
				.insert(tg::grant::Principal::Group(group), false);
		}
		Ok(false)
	}

	async fn organization_contains_requester_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		organization: &tg::organization::Id,
		requester: &Requester<'_>,
	) -> tg::Result<bool> {
		let principal = tg::grant::Principal::Organization(organization.clone());
		let contains = {
			let cache = requester.membership_cache();
			cache.principal_contains_requester.get(&principal).copied()
		};
		if let Some(contains) = contains {
			return Ok(contains);
		}
		let members = {
			let cache = requester.membership_cache();
			cache.organization_members.get(organization).cloned()
		};
		let members = if let Some(members) = members {
			members
		} else {
			let members: std::sync::Arc<[tg::Id]> =
				Self::get_organization_members_with_transaction(txn, subspace, organization)
					.await?
					.into_iter()
					.map(tg::Id::from)
					.collect::<Vec<_>>()
					.into();
			requester
				.membership_cache()
				.organization_members
				.insert(organization.clone(), members.clone());
			members
		};
		for member in members.iter().cloned() {
			if requester.id.as_ref() == Some(&member) {
				requester
					.membership_cache()
					.principal_contains_requester
					.insert(
						tg::grant::Principal::Organization(organization.clone()),
						true,
					);
				return Ok(true);
			}
			if member.kind() == tg::id::Kind::Group {
				let group = tg::group::Id::try_from(member)?;
				if Self::group_contains_requester_with_transaction(txn, subspace, &group, requester)
					.await?
				{
					requester
						.membership_cache()
						.principal_contains_requester
						.insert(
							tg::grant::Principal::Organization(organization.clone()),
							true,
						);
					return Ok(true);
				}
			}
		}
		requester
			.membership_cache()
			.principal_contains_requester
			.insert(
				tg::grant::Principal::Organization(organization.clone()),
				false,
			);
		Ok(false)
	}

	async fn get_cached_resource_grants_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::grant::Principal, tg::grant::Permission)>> {
		if let Some(grants) = cache.resource_grants.get(resource) {
			return Ok(grants.clone());
		}
		let grants = Self::get_resource_grants_with_transaction(txn, subspace, resource).await?;
		cache
			.resource_grants
			.insert(resource.clone(), grants.clone());
		Ok(grants)
	}

	async fn get_cached_resource_parent_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		cache: &mut Cache,
	) -> tg::Result<Option<tg::Id>> {
		if let Some(parent) = cache.resource_parents.get(resource) {
			return Ok(parent.clone());
		}
		let parent = match resource.kind() {
			tg::id::Kind::Tag => {
				Self::try_get_tag_with_transaction(txn, subspace, &resource.clone().try_into()?)
					.await?
					.and_then(|tag| tag.parent)
			},
			tg::id::Kind::Group => {
				Self::try_get_group_with_transaction(txn, subspace, &resource.clone().try_into()?)
					.await?
					.and_then(|group| group.parent)
			},
			_ => None,
		};
		cache
			.resource_parents
			.insert(resource.clone(), parent.clone());
		Ok(parent)
	}

	async fn get_cached_item_tags_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		concurrency: usize,
		item: &tg::Id,
		permission: tg::grant::Permission,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::Id, tg::grant::Permission)>> {
		let key = (item.clone(), permission);
		if let Some(tags) = cache.item_tags.get(&key) {
			return Ok(tags.clone());
		}
		let item_bytes = item.to_bytes();
		let tags = Self::get_item_tags_with_transaction(txn, subspace, item_bytes.as_ref()).await?;
		let tags = stream::iter(tags)
			.map(|tag| async move {
				let value = Self::try_get_tag_with_transaction(txn, subspace, &tag).await?;
				Ok::<_, tg::Error>((tag, value))
			})
			.buffered(concurrency)
			.try_collect::<Vec<_>>()
			.await?;
		let mut parents = Vec::new();
		for (tag, value) in tags {
			let Some(value) = value else {
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

	async fn get_cached_object_children_limited_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		object: &tg::object::Id,
		limit: usize,
		cache: &mut Cache,
	) -> tg::Result<Vec<tg::object::Id>> {
		if let Some(children) = cache.object_children.get(object) {
			return Ok(children.iter().take(limit).cloned().collect());
		}
		let bytes = object.to_bytes();
		let key = (Kind::ObjectChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			limit: Some(limit),
			..fdb::RangeOption::from(&range_subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get object children"))?;
		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Object(crate::fdb::object::Key::ObjectChild { child, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		if children.len() < limit {
			cache
				.object_children
				.insert(object.clone(), children.clone());
		}
		Ok(children)
	}

	async fn get_cached_sandbox_owner_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		sandbox: &tg::sandbox::Id,
		cache: &mut Cache,
	) -> tg::Result<Option<tg::Principal>> {
		if let Some(owner) = cache.sandbox_owners.get(sandbox) {
			return Ok(owner.clone());
		}
		let key = Key::Sandbox(crate::fdb::sandbox::Key::Sandbox(sandbox.clone()));
		let key = Self::pack(subspace, &key);
		let owner = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the sandbox"))?
			.map(|bytes| crate::sandbox::Sandbox::deserialize(&bytes))
			.transpose()?
			.and_then(|sandbox| sandbox.data)
			.and_then(|data| data.owner);
		cache.sandbox_owners.insert(sandbox.clone(), owner.clone());
		Ok(owner)
	}
}
