use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::{
		collections::{HashMap, HashSet, VecDeque},
		ops::ControlFlow,
		sync::Arc,
	},
	tangram_client::prelude::*,
	tokio::sync::Semaphore,
};

const PRECOMPUTE_REQUESTER_PRINCIPALS: bool = false;

#[derive(Default)]
struct Cache {
	resource_parents: HashMap<tg::Id, Option<tg::Id>>,
	target_tags: HashMap<
		(tg::Id, tg::authorization::Permission),
		Vec<(tg::Id, tg::authorization::Permission)>,
	>,
	object_children: HashMap<tg::object::Id, Vec<tg::object::Id>>,
	object_parents: HashMap<tg::object::Id, Vec<tg::object::Id>>,
	object_processes: HashMap<tg::object::Id, Vec<(tg::process::Id, crate::process::object::Kind)>>,
	process_children: HashMap<tg::process::Id, Vec<tg::process::Id>>,
	process_objects: HashMap<tg::process::Id, Vec<(tg::object::Id, crate::process::object::Kind)>>,
	process_parents: HashMap<tg::process::Id, Vec<tg::process::Id>>,
	process_sandboxes: HashMap<tg::process::Id, Option<tg::sandbox::Id>>,
	resource_grants: HashMap<
		tg::Id,
		Vec<(
			tg::authorization::Subject,
			tg::authorization::Permission,
			bool,
		)>,
	>,
	sandbox_owners: HashMap<tg::sandbox::Id, Option<tg::Principal>>,
}

struct Requester<'a> {
	principal: &'a tg::Principal,
	subject: tg::authorization::Subject,
	id: Option<tg::Id>,
	subjects: HashSet<tg::authorization::Subject>,
	membership_cache: std::sync::Mutex<RequesterMembershipCache>,
}

#[derive(Default)]
struct RequesterMembershipCache {
	group_members: HashMap<tg::group::Id, std::sync::Arc<[tg::Id]>>,
	organization_members: HashMap<tg::organization::Id, std::sync::Arc<[tg::Id]>>,
	subject_contains_requester: HashMap<tg::authorization::Subject, bool>,
}

struct AuthorizationNode {
	key: (tg::Id, tg::authorization::Permission),
	dependents: Vec<usize>,
	authorized: bool,
}

struct AuthorizationNodeEvaluation {
	node_id: usize,
	directly_authorized: bool,
	dependencies: Vec<(tg::Id, tg::authorization::Permission)>,
	cache: Cache,
}

#[derive(Clone, Copy)]
struct AuthorizationContext<'a> {
	txn: &'a crate::fdb::Transaction,
	subspace: &'a Subspace,
	config: crate::fdb::AuthorizeConfig,
	requester: &'a Requester<'a>,
	token: Option<(&'a tg::authorization::Body, &'a tg::Id)>,
}

struct SubtreeSearchBudget {
	max_depth: usize,
	remaining: usize,
}

impl Cache {
	fn merge(&mut self, other: Self) {
		self.resource_parents.extend(other.resource_parents);
		self.target_tags.extend(other.target_tags);
		self.object_children.extend(other.object_children);
		self.object_parents.extend(other.object_parents);
		self.object_processes.extend(other.object_processes);
		self.process_children.extend(other.process_children);
		self.process_objects.extend(other.process_objects);
		self.process_parents.extend(other.process_parents);
		self.process_sandboxes.extend(other.process_sandboxes);
		self.resource_grants.extend(other.resource_grants);
		self.sandbox_owners.extend(other.sandbox_owners);
	}

	fn clone_for_direct_authorization(
		&self,
		resource: &tg::Id,
		permission: tg::authorization::Permission,
	) -> Self {
		let mut cache = Self::default();
		if let Some(grants) = self.resource_grants.get(resource) {
			cache
				.resource_grants
				.insert(resource.clone(), grants.clone());
		}
		if matches!(permission, tg::authorization::Permission::Sandbox(_))
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
		permission: tg::authorization::Permission,
	) -> Self {
		let mut cache = Self::default();
		if let Some(grants) = self.resource_grants.get(resource) {
			cache
				.resource_grants
				.insert(resource.clone(), grants.clone());
		}
		if let Some(parent) = self.resource_parents.get(resource) {
			cache
				.resource_parents
				.insert(resource.clone(), parent.clone());
		}
		if let Some(tags) = self.target_tags.get(&(resource.clone(), permission)) {
			cache
				.target_tags
				.insert((resource.clone(), permission), tags.clone());
		}
		match permission {
			tg::authorization::Permission::Object(_) => {
				if let Ok(object) = tg::object::Id::try_from(resource.clone()) {
					if let Some(parents) = self.object_parents.get(&object) {
						cache.object_parents.insert(object.clone(), parents.clone());
					}
					if let Some(processes) = self.object_processes.get(&object) {
						cache.object_processes.insert(object, processes.clone());
					}
				}
			},
			tg::authorization::Permission::Process(_) => {
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
			tg::authorization::Permission::Sandbox(_) => {
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

	fn clone_for_process_children(&self, process: &tg::process::Id) -> Self {
		let mut cache = Self::default();
		if let Some(children) = self.process_children.get(process) {
			cache
				.process_children
				.insert(process.clone(), children.clone());
		}
		cache
	}
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
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		args: &[crate::authorize::Arg],
		principal: &tg::Principal,
	) -> tg::Result<ControlFlow<Vec<Option<crate::authorize::Output>>, fdb::FdbError>> {
		if args.is_empty() {
			return Ok(ControlFlow::Break(Vec::new()));
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
			return Ok(ControlFlow::Break(outputs));
		}
		let transaction = txn.with_read_semaphore(Arc::new(Semaphore::new(config.concurrency)));
		let txn = &transaction;
		let mut requester = Requester::new(principal);
		if PRECOMPUTE_REQUESTER_PRINCIPALS {
			crate::fdb::propagate!(
				Self::load_requester_subjects_with_transaction(txn, subspace, &mut requester).await
			);
		}
		let resource_requests = args
			.iter()
			.map(|arg| arg.resource.clone())
			.collect::<Vec<_>>();
		let resources = {
			let results = futures::future::try_join_all(resource_requests.into_iter().map(
				|resource| async move {
					Self::try_resolve_resource_with_transaction(txn, subspace, &resource).await
				},
			))
			.await?;
			let mut resources = Vec::with_capacity(results.len());
			for result in results {
				let resource = match result {
					ControlFlow::Break(resource) => resource,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				resources.push(resource);
			}

			resources
		};
		let permissions = std::iter::zip(args, &resources)
			.map(|(arg, resource)| {
				let Some((resource, exact)) = resource else {
					return Ok(None);
				};
				if *exact {
					return Ok(Some(arg.permissions));
				}
				crate::authorize::permissions_for_specifier_prefix(resource, arg.permissions)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let token_resources = args
			.iter()
			.map(|arg| arg.token.as_ref().map(|body| body.resource.clone()))
			.collect::<Vec<_>>();
		let mut cache = Cache::default();
		let mut authorization = HashMap::new();
		let mut ordinary_roots = Vec::new();
		for (index, (arg, resource)) in std::iter::zip(args, &resources).enumerate() {
			let Some((id, _)) = resource else {
				continue;
			};
			let Some(permissions) = permissions[index] else {
				continue;
			};
			if arg.token.is_some() || crate::authorize::validate(id, permissions).is_err() {
				continue;
			}
			if matches!(principal, tg::Principal::Process(process) if tg::Id::from(process.clone()) == *id)
			{
				continue;
			}
			ordinary_roots.extend(
				permissions
					.iter()
					.map(|permission| (id.clone(), permission)),
			);
		}
		let context = AuthorizationContext {
			config,
			requester: &requester,
			subspace,
			token: None,
			txn,
		};
		if !ordinary_roots.is_empty() {
			crate::fdb::propagate!(
				Self::authorize_permissions_ordinary_with_transaction(
					context,
					&ordinary_roots,
					&mut authorization,
					&mut cache,
				)
				.await
			);
		}
		let mut outputs = Vec::with_capacity(args.len());
		for (index, (arg, resource)) in std::iter::zip(args, resources).enumerate() {
			let Some((id, _)) = resource else {
				outputs.push(None);
				continue;
			};
			let Some(permissions) = permissions[index] else {
				outputs.push(None);
				continue;
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
			let authorized = crate::fdb::propagate!(
				Self::authorize_with_transaction(
					context,
					&id,
					permissions,
					authorization,
					&mut cache,
				)
				.await
			);
			let permissions = if permissions == arg.permissions {
				authorized
			} else if authorized.contains(permissions) {
				arg.permissions
			} else {
				arg.permissions.empty_like()
			};
			outputs.push(Some(crate::authorize::Output { permissions }));
		}

		Ok(ControlFlow::Break(outputs))
	}

	async fn load_requester_subjects_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		requester: &mut Requester<'_>,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let Some(id) = requester.id.clone() else {
			return Ok(ControlFlow::Break(()));
		};
		if !matches!(id.kind(), tg::id::Kind::Group | tg::id::Kind::User) {
			return Ok(ControlFlow::Break(()));
		}
		let mut visited = HashSet::new();
		let mut frontier = vec![id];
		while !frontier.is_empty() {
			let members = frontier
				.into_iter()
				.filter(|member| visited.insert(member.clone()))
				.collect::<Vec<_>>();
			let relations = {
				let results =
					futures::future::try_join_all(members.into_iter().map(|member| async move {
						Self::get_member_groups_and_organizations_with_transaction(
							txn, subspace, &member,
						)
						.await
					}))
					.await?;
				let mut relations = Vec::with_capacity(results.len());
				for result in results {
					let relation = match result {
						ControlFlow::Break(relation) => relation,
						ControlFlow::Continue(error) => {
							return Ok(ControlFlow::Continue(error));
						},
					};
					relations.push(relation);
				}

				relations
			};
			let mut next = Vec::new();
			for (groups, organizations) in relations {
				for group in groups {
					requester
						.subjects
						.insert(tg::authorization::Subject::Group(group.clone()));
					next.push(group.into());
				}
				for organization in organizations {
					requester
						.subjects
						.insert(tg::authorization::Subject::Organization(organization));
				}
			}
			frontier = next;
		}
		Ok(ControlFlow::Break(()))
	}

	async fn authorize_with_transaction(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		permissions: tg::authorization::permission::Set,
		authorization: &mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<tg::authorization::permission::Set, fdb::FdbError>> {
		let roots = permissions
			.iter()
			.map(|permission| (resource.clone(), permission))
			.collect::<Vec<_>>();
		crate::fdb::propagate!(
			Self::authorize_permissions_ordinary_with_transaction(
				context,
				&roots,
				authorization,
				cache,
			)
			.await
		);
		let mut authorized = permissions.empty_like();
		for permission in permissions.iter() {
			let key = (resource.clone(), permission);
			if authorization.get(&key).copied().unwrap_or(false) {
				authorized.insert(tg::authorization::permission::Set::from_permission(
					permission,
				));
				continue;
			}
			let permission_authorized = match permission {
				tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				) => crate::fdb::propagate!(Self::authorize_with_object_subtree_search_with_transaction(
					context,
					resource,
					authorization,
					cache,
				)
				.await)
				.unwrap_or(false),
				tg::authorization::Permission::Process(
					permission @ (tg::authorization::permission::process::Permission::NodeCommand
					| tg::authorization::permission::process::Permission::NodeError
					| tg::authorization::permission::process::Permission::NodeLog
					| tg::authorization::permission::process::Permission::NodeOutput),
				) => {
					crate::fdb::propagate!(Self::authorize_process_node_with_transaction(
						context,
						resource,
						permission,
						authorization,
						cache,
					)
					.await)
				},
				tg::authorization::Permission::Process(
					permission @ (tg::authorization::permission::process::Permission::Subtree
					| tg::authorization::permission::process::Permission::SubtreeCommand
					| tg::authorization::permission::process::Permission::SubtreeError
					| tg::authorization::permission::process::Permission::SubtreeLog
					| tg::authorization::permission::process::Permission::SubtreeOutput),
				) => crate::fdb::propagate!(Self::authorize_with_process_subtree_search_with_transaction(
					context,
					resource,
					permission,
					authorization,
					cache,
				)
				.await)
				.unwrap_or(false),
				_ => false,
			};
			if permission_authorized {
				authorized.insert(tg::authorization::permission::Set::from_permission(
					permission,
				));
			}
		}
		Ok(ControlFlow::Break(authorized))
	}

	async fn authorize_permissions_ordinary_with_transaction(
		context: AuthorizationContext<'_>,
		roots: &[(tg::Id, tg::authorization::Permission)],
		authorization: &mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
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
			while let Some(node_id) = queue.pop_front() {
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
				return Ok(ControlFlow::Break(()));
			}
			let evaluations = futures::future::try_join_all(requests.into_iter().map(
				|(node_id, resource, permission, mut direct_cache, mut dependency_cache)| async move {
					if !direct_cache.resource_grants.contains_key(&resource) {
						let grants = crate::fdb::propagate!(
							Self::get_resource_grants_with_transaction(
								context.txn,
								context.subspace,
								&resource,
							)
							.await
						);
						direct_cache
							.resource_grants
							.insert(resource.clone(), grants.clone());
						dependency_cache
							.resource_grants
							.insert(resource.clone(), grants);
					}
					let directly_authorized = Self::is_directly_authorized_with_transaction(
						context,
						&resource,
						permission,
						&mut direct_cache,
					);
					let dependencies = Self::get_authorization_dependencies_with_transaction(
						context.txn,
						context.subspace,
						&resource,
						permission,
						&mut dependency_cache,
					);
					let (directly_authorized, dependencies) =
						futures::try_join!(directly_authorized, dependencies)?;
					let directly_authorized = {
						let result = directly_authorized;
						match result {
							ControlFlow::Break(value) => value,
							ControlFlow::Continue(error) => {
								return Ok(ControlFlow::Continue(error));
							},
						}
					};
					let dependencies = {
						let result = dependencies;
						match result {
							ControlFlow::Break(value) => value,
							ControlFlow::Continue(error) => {
								return Ok(ControlFlow::Continue(error));
							},
						}
					};
					direct_cache.merge(dependency_cache);
					Ok::<_, tg::Error>(ControlFlow::Break(AuthorizationNodeEvaluation {
						node_id,
						directly_authorized,
						dependencies,
						cache: direct_cache,
					}))
				},
			))
			.await?;
			let evaluations = {
				let results = evaluations;
				let mut values = Vec::with_capacity(results.len());
				for result in results {
					let value = match result {
						ControlFlow::Break(value) => value,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					values.push(value);
				}
				values
			};

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
				return Ok(ControlFlow::Break(()));
			}
		}

		for node in nodes {
			authorization.entry(node.key).or_insert(false);
		}
		Ok(ControlFlow::Break(()))
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

	async fn is_directly_authorized_with_transaction(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		permission: tg::authorization::Permission,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		if let (tg::Principal::Process(process), tg::authorization::Permission::Process(_)) =
			(context.requester.principal, permission)
			&& tg::Id::from(process.clone()) == *resource
		{
			return Ok(ControlFlow::Break(true));
		}
		if let (tg::Principal::User(user), tg::authorization::Permission::User(_)) =
			(context.requester.principal, permission)
			&& tg::Id::from(user.clone()) == *resource
		{
			return Ok(ControlFlow::Break(true));
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
			return Ok(ControlFlow::Break(true));
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
			if let Some(owner) = crate::fdb::propagate!(
				Self::get_cached_sandbox_owner_with_transaction(
					context.txn,
					context.subspace,
					&sandbox,
					cache,
				)
				.await
			) {
				let owner = owner.try_to_subject()?;
				if crate::fdb::propagate!(
					Self::subject_contains_requester_with_transaction(
						context.txn,
						context.subspace,
						&owner,
						context.requester,
					)
					.await
				) {
					return Ok(ControlFlow::Break(true));
				}
			}
		}

		let grants = crate::fdb::propagate!(
			Self::get_cached_resource_grants_with_transaction(
				context.txn,
				context.subspace,
				resource,
				cache,
			)
			.await
		);
		for (granted_subject, granted_permission, _) in grants {
			if granted_permission.implies(permission)
				&& crate::fdb::propagate!(
					Self::subject_contains_requester_with_transaction(
						context.txn,
						context.subspace,
						&granted_subject,
						context.requester,
					)
					.await
				) {
				return Ok(ControlFlow::Break(true));
			}
		}
		Ok(ControlFlow::Break(false))
	}

	fn is_authorized_by_token(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		permission: tg::authorization::Permission,
	) -> bool {
		context.token.is_some_and(|(body, token_resource)| {
			token_resource == resource && body.grants(permission)
		})
	}

	async fn authorize_with_object_subtree_search_with_transaction(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		authorization: &mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<Option<bool>, fdb::FdbError>> {
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let mut budget = SubtreeSearchBudget {
			max_depth: context.config.object_subtree.max_depth,
			remaining: context.config.object_subtree.max_objects,
		};
		let root = tg::object::Id::try_from(resource.clone())?;
		let mut visited = HashSet::from([root.clone()]);
		let mut frontier = vec![root];
		let mut depth = 0;
		while !frontier.is_empty() {
			if frontier.len() > budget.remaining {
				return Ok(ControlFlow::Break(None));
			}
			budget.remaining -= frontier.len();
			let subtree_roots = frontier
				.iter()
				.map(|object| (tg::Id::from(object.clone()), subtree))
				.collect::<Vec<_>>();
			crate::fdb::propagate!(
				Self::authorize_permissions_ordinary_with_transaction(
					context,
					&subtree_roots,
					authorization,
					cache,
				)
				.await
			);
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
			crate::fdb::propagate!(
				Self::authorize_permissions_ordinary_with_transaction(
					context,
					&node_roots,
					authorization,
					cache,
				)
				.await
			);
			if node_roots
				.iter()
				.any(|key| !authorization.get(key).copied().unwrap_or(false))
			{
				return Ok(ControlFlow::Break(Some(false)));
			}

			let limit = if depth == budget.max_depth {
				visited.len().saturating_add(1)
			} else {
				budget
					.remaining
					.saturating_add(visited.len())
					.saturating_add(1)
			};
			let requests = uncovered
				.into_iter()
				.map(|object| (object.clone(), cache.clone_for_object_children(&object)))
				.collect::<Vec<_>>();
			let children = {
				let result = futures::future::try_join_all(requests.into_iter().map(
					|(object, mut cache)| async move {
						let children = crate::fdb::propagate!(
							Self::get_cached_object_children_limited_with_transaction(
								context.txn,
								context.subspace,
								&object,
								limit,
								&mut cache,
							)
							.await
						);
						Ok::<_, tg::Error>(ControlFlow::Break((children, cache)))
					},
				))
				.await;
				let results = result?;
				let mut values = Vec::with_capacity(results.len());
				for result in results {
					let value = match result {
						ControlFlow::Break(value) => value,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					values.push(value);
				}
				values
			};
			let mut next = Vec::new();
			for (children, child_cache) in children {
				cache.merge(child_cache);
				for child in children {
					if visited.insert(child.clone()) {
						if depth == budget.max_depth {
							return Ok(ControlFlow::Break(None));
						}
						next.push(child);
					}
				}
			}
			if next.len() > budget.remaining {
				return Ok(ControlFlow::Break(None));
			}
			frontier = next;
			depth += 1;
		}

		Ok(ControlFlow::Break(Some(true)))
	}

	async fn authorize_with_process_subtree_search_with_transaction(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		permission: tg::authorization::permission::process::Permission,
		authorization: &mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<Option<bool>, fdb::FdbError>> {
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
		let mut budget = SubtreeSearchBudget {
			max_depth: context.config.process_subtree.max_depth,
			remaining: context.config.process_subtree.max_processes,
		};
		let root = tg::process::Id::try_from(resource.clone())?;
		let mut visited = HashSet::from([root.clone()]);
		let mut frontier = vec![root];
		let mut depth = 0;
		while !frontier.is_empty() {
			if frontier.len() > budget.remaining {
				return Ok(ControlFlow::Break(None));
			}
			budget.remaining -= frontier.len();
			let subtree_roots = frontier
				.iter()
				.map(|process| (tg::Id::from(process.clone()), subtree))
				.collect::<Vec<_>>();
			crate::fdb::propagate!(
				Self::authorize_permissions_ordinary_with_transaction(
					context,
					&subtree_roots,
					authorization,
					cache,
				)
				.await
			);
			let uncovered = frontier
				.into_iter()
				.filter(|process| {
					let key = (tg::Id::from(process.clone()), subtree);
					!authorization.get(&key).copied().unwrap_or(false)
				})
				.collect::<Vec<_>>();
			if uncovered.is_empty() {
				break;
			}

			let node = tg::authorization::Permission::Process(node_permission);
			let node_roots = uncovered
				.iter()
				.map(|process| (tg::Id::from(process.clone()), node))
				.collect::<Vec<_>>();
			crate::fdb::propagate!(
				Self::authorize_permissions_ordinary_with_transaction(
					context,
					&node_roots,
					authorization,
					cache,
				)
				.await
			);
			for process in &uncovered {
				let resource = tg::Id::from(process.clone());
				let key = (resource.clone(), node);
				if authorization.get(&key).copied().unwrap_or(false) {
					continue;
				}
				if !crate::fdb::propagate!(
					Self::authorize_process_node_with_transaction(
						context,
						&resource,
						node_permission,
						authorization,
						cache,
					)
					.await
				) {
					return Ok(ControlFlow::Break(Some(false)));
				}
			}

			let limit = if depth == budget.max_depth {
				visited.len().saturating_add(1)
			} else {
				budget
					.remaining
					.saturating_add(visited.len())
					.saturating_add(1)
			};
			let requests = uncovered
				.into_iter()
				.map(|process| (process.clone(), cache.clone_for_process_children(&process)))
				.collect::<Vec<_>>();
			let children = {
				let result = futures::future::try_join_all(requests.into_iter().map(
					|(process, mut cache)| async move {
						let children = crate::fdb::propagate!(
							Self::get_cached_process_children_limited_with_transaction(
								context.txn,
								context.subspace,
								&process,
								limit,
								&mut cache,
							)
							.await
						);
						Ok::<_, tg::Error>(ControlFlow::Break((children, cache)))
					},
				))
				.await;
				let results = result?;
				let mut values = Vec::with_capacity(results.len());
				for result in results {
					let value = match result {
						ControlFlow::Break(value) => value,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					values.push(value);
				}
				values
			};
			let mut next = Vec::new();
			for (children, child_cache) in children {
				cache.merge(child_cache);
				for child in children {
					if visited.insert(child.clone()) {
						if depth == budget.max_depth {
							return Ok(ControlFlow::Break(None));
						}
						next.push(child);
					}
				}
			}
			if next.len() > budget.remaining {
				return Ok(ControlFlow::Break(None));
			}
			frontier = next;
			depth += 1;
		}

		Ok(ControlFlow::Break(Some(true)))
	}

	async fn authorize_process_node_with_transaction(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		permission: tg::authorization::permission::process::Permission,
		authorization: &mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let process_permission = tg::authorization::Permission::Process(permission);
		let root = (resource.clone(), process_permission);
		crate::fdb::propagate!(
			Self::authorize_permissions_ordinary_with_transaction(
				context,
				std::slice::from_ref(&root),
				authorization,
				cache,
			)
			.await
		);
		if authorization.get(&root).copied().unwrap_or(false) {
			return Ok(ControlFlow::Break(true));
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
			_ => return Ok(ControlFlow::Break(false)),
		};
		let process = tg::process::Id::try_from(resource.clone())?;
		let objects = crate::fdb::propagate!(
			Self::get_cached_process_objects_with_transaction(
				context.txn,
				context.subspace,
				&process,
				cache,
			)
			.await
		);
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
		let process = crate::fdb::propagate!(
			Self::try_get_process_with_transaction(context.txn, context.subspace, &process).await
		);
		let aspect_is_set = process.is_some_and(|process| match kind {
			crate::process::object::Kind::Command => true,
			crate::process::object::Kind::Error => process.set.error,
			crate::process::object::Kind::Log => process.set.log,
			crate::process::object::Kind::Output => process.set.output,
		});
		if !aspect_is_set || (kind.is_command() && objects.is_empty()) {
			return Ok(ControlFlow::Break(false));
		}
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let roots = objects
			.iter()
			.map(|object| (tg::Id::from(object.clone()), subtree))
			.collect::<Vec<_>>();
		crate::fdb::propagate!(
			Self::authorize_permissions_ordinary_with_transaction(
				context,
				&roots,
				authorization,
				cache,
			)
			.await
		);
		for (object, root) in std::iter::zip(objects, roots) {
			if authorization.get(&root).copied().unwrap_or(false) {
				continue;
			}
			let resource = object.into();
			if !crate::fdb::propagate!(
				Self::authorize_with_object_subtree_search_with_transaction(
					context,
					&resource,
					authorization,
					cache,
				)
				.await
			)
			.unwrap_or(false)
			{
				return Ok(ControlFlow::Break(false));
			}
		}

		Ok(ControlFlow::Break(true))
	}

	async fn get_authorization_dependencies_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		permission: tg::authorization::Permission,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::authorization::Permission)>, fdb::FdbError>> {
		let mut dependencies = Vec::new();

		// Add the non-expiring process implicit grant relationships.
		let grants = crate::fdb::propagate!(
			Self::get_cached_resource_grants_with_transaction(txn, subspace, resource, cache).await
		);
		for (subject, granted_permission, process_implicit) in &grants {
			if !process_implicit || !granted_permission.implies(permission) {
				continue;
			}
			let tg::authorization::Subject::Process(process) = subject else {
				continue;
			};
			let permission = tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Parent,
			);
			dependencies.push((process.clone().into(), permission));
		}

		match permission {
			tg::authorization::Permission::Object(object_permission) => {
				let object = tg::object::Id::try_from(resource.clone())?;
				let cached_parents = cache.object_parents.get(&object).cloned();
				let cached_processes = cache.object_processes.get(&object).cloned();
				let tag_key = (resource.clone(), permission);
				let cached_tags = cache.target_tags.get(&tag_key).cloned();
				let object_parents = async {
					if let Some(parents) = cached_parents {
						Ok(ControlFlow::Break(parents))
					} else {
						Self::get_object_parents_with_transaction(txn, subspace, &object).await
					}
				};
				let object_processes = async {
					if let Some(processes) = cached_processes {
						Ok(ControlFlow::Break(processes))
					} else {
						Self::get_object_processes_with_transaction(txn, subspace, &object).await
					}
				};
				let tags = async {
					if let Some(tags) = cached_tags {
						Ok(ControlFlow::Break(tags))
					} else {
						let mut tag_cache = Cache::default();
						Self::get_cached_target_tags_with_transaction(
							txn,
							subspace,
							resource,
							permission,
							&mut tag_cache,
						)
						.await
					}
				};
				let (object_parents, object_processes, tags) =
					futures::try_join!(object_parents, object_processes, tags)?;
				let object_parents = match object_parents {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let tags = match tags {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let object_processes = match object_processes {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				cache
					.object_parents
					.insert(object.clone(), object_parents.clone());
				cache
					.object_processes
					.insert(object, object_processes.clone());
				cache.target_tags.insert(tag_key, tags.clone());
				for (process, kind) in object_processes {
					let subject = tg::authorization::Subject::Process(process.clone());
					let granted = grants.iter().any(
						|(granted_subject, granted_permission, process_implicit)| {
							*process_implicit
								&& granted_subject == &subject
								&& granted_permission.implies(permission)
						},
					);
					if granted {
						let permission = tg::authorization::Permission::Process(
							crate::authorize::process_object_permission(kind, object_permission),
						);
						dependencies.push((process.into(), permission));
					}
				}
				for parent in object_parents {
					let permission = tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					);
					dependencies.push((parent.into(), permission));
				}
				dependencies.extend(tags);
			},
			tg::authorization::Permission::Process(process_permission) => {
				let process = tg::process::Id::try_from(resource.clone())?;
				let cached_sandbox = cache.process_sandboxes.get(&process).cloned();
				let cached_parents = cache.process_parents.get(&process).cloned();
				let tag_key = (resource.clone(), permission);
				let cached_tags = cache.target_tags.get(&tag_key).cloned();
				let sandbox = async {
					if let Some(sandbox) = cached_sandbox {
						Ok(ControlFlow::Break(sandbox))
					} else {
						let result =
							Self::try_get_process_with_transaction(txn, subspace, &process).await;
						let process = crate::fdb::propagate!(result);

						Ok(ControlFlow::Break(
							process.and_then(|process| process.sandbox),
						))
					}
				};
				let process_parents = async {
					if let Some(parents) = cached_parents {
						Ok(ControlFlow::Break(parents))
					} else {
						Self::get_process_parents_with_transaction(txn, subspace, &process).await
					}
				};
				let tags = async {
					if let Some(tags) = cached_tags {
						Ok(ControlFlow::Break(tags))
					} else {
						let mut tag_cache = Cache::default();
						Self::get_cached_target_tags_with_transaction(
							txn,
							subspace,
							resource,
							permission,
							&mut tag_cache,
						)
						.await
					}
				};
				let (sandbox, process_parents, tags) =
					futures::try_join!(sandbox, process_parents, tags)?;
				let sandbox = match sandbox {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let process_parents = match process_parents {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let tags = match tags {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				cache
					.process_sandboxes
					.insert(process.clone(), sandbox.clone());
				cache
					.process_parents
					.insert(process, process_parents.clone());
				cache.target_tags.insert(tag_key, tags.clone());
				if let Some(sandbox) = sandbox {
					let sandbox_permission = match process_permission {
						tg::authorization::permission::process::Permission::Parent => {
							tg::authorization::permission::sandbox::Permission::Write
						},
						_ => tg::authorization::permission::sandbox::Permission::Read,
					};
					dependencies.push((
						sandbox.into(),
						tg::authorization::Permission::Sandbox(sandbox_permission),
					));
				}
				for parent in process_parents {
					let permission =
						tg::authorization::Permission::Process(process_permission.to_subtree());
					dependencies.push((parent.into(), permission));
				}
				dependencies.extend(tags);
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
					if let Some(owner) = crate::fdb::propagate!(
						Self::get_cached_sandbox_owner_with_transaction(
							txn, subspace, &sandbox, cache,
						)
						.await
					) {
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
				if let Some(parent) = crate::fdb::propagate!(
					Self::get_cached_resource_parent_with_transaction(
						txn, subspace, resource, cache,
					)
					.await
				) {
					let permission =
						crate::authorize::permission_for_named_parent(&parent, permission)?;
					dependencies.push((parent, permission));
				}
			},
		}
		Ok(ControlFlow::Break(dependencies))
	}

	async fn subject_contains_requester_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		subject: &tg::authorization::Subject,
		requester: &Requester<'_>,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		if PRECOMPUTE_REQUESTER_PRINCIPALS {
			return Ok(ControlFlow::Break(requester.subjects.contains(subject)));
		}
		if subject == &tg::authorization::Subject::Public {
			return Ok(ControlFlow::Break(true));
		}
		if requester.subject == *subject {
			return Ok(ControlFlow::Break(true));
		}
		if requester.id.is_none() {
			return Ok(ControlFlow::Break(false));
		}
		let contains = {
			let cache = requester.membership_cache();
			cache.subject_contains_requester.get(subject).copied()
		};
		if let Some(contains) = contains {
			return Ok(ControlFlow::Break(contains));
		}
		let contains = match subject {
			tg::authorization::Subject::Group(group) => {
				crate::fdb::propagate!(
					Self::group_contains_requester_with_transaction(
						txn, subspace, group, requester,
					)
					.await
				)
			},
			tg::authorization::Subject::Organization(organization) => {
				crate::fdb::propagate!(
					Self::organization_contains_requester_with_transaction(
						txn,
						subspace,
						organization,
						requester,
					)
					.await
				)
			},
			_ => false,
		};
		requester
			.membership_cache()
			.subject_contains_requester
			.insert(subject.clone(), contains);
		Ok(ControlFlow::Break(contains))
	}

	async fn group_contains_requester_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		group: &tg::group::Id,
		requester: &Requester<'_>,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let subject = tg::authorization::Subject::Group(group.clone());
		let contains = {
			let cache = requester.membership_cache();
			cache.subject_contains_requester.get(&subject).copied()
		};
		if let Some(contains) = contains {
			return Ok(ControlFlow::Break(contains));
		}
		let root = group.clone();
		let mut visited = HashSet::new();
		let mut queue = VecDeque::from([group.clone()]);
		while let Some(group) = queue.pop_front() {
			if !visited.insert(group.clone()) {
				continue;
			}
			let subject = tg::authorization::Subject::Group(group.clone());
			let contains = {
				let cache = requester.membership_cache();
				cache.subject_contains_requester.get(&subject).copied()
			};
			if let Some(contains) = contains {
				if contains {
					requester
						.membership_cache()
						.subject_contains_requester
						.insert(tg::authorization::Subject::Group(root), true);
					return Ok(ControlFlow::Break(true));
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
				let members: std::sync::Arc<[tg::Id]> = crate::fdb::propagate!(
					Self::get_group_members_with_transaction(txn, subspace, &group).await
				)
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
						.subject_contains_requester
						.insert(tg::authorization::Subject::Group(root), true);
					return Ok(ControlFlow::Break(true));
				}
				if member.kind() == tg::id::Kind::Group {
					queue.push_back(tg::group::Id::try_from(member)?);
				}
			}
		}
		let mut cache = requester.membership_cache();
		for group in visited {
			cache
				.subject_contains_requester
				.insert(tg::authorization::Subject::Group(group), false);
		}
		Ok(ControlFlow::Break(false))
	}

	async fn organization_contains_requester_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		organization: &tg::organization::Id,
		requester: &Requester<'_>,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let subject = tg::authorization::Subject::Organization(organization.clone());
		let contains = {
			let cache = requester.membership_cache();
			cache.subject_contains_requester.get(&subject).copied()
		};
		if let Some(contains) = contains {
			return Ok(ControlFlow::Break(contains));
		}
		let members = {
			let cache = requester.membership_cache();
			cache.organization_members.get(organization).cloned()
		};
		let members = if let Some(members) = members {
			members
		} else {
			let members: std::sync::Arc<[tg::Id]> = crate::fdb::propagate!(
				Self::get_organization_members_with_transaction(txn, subspace, organization).await
			)
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
					.subject_contains_requester
					.insert(
						tg::authorization::Subject::Organization(organization.clone()),
						true,
					);
				return Ok(ControlFlow::Break(true));
			}
			if member.kind() == tg::id::Kind::Group {
				let group = tg::group::Id::try_from(member)?;
				if crate::fdb::propagate!(
					Self::group_contains_requester_with_transaction(
						txn, subspace, &group, requester,
					)
					.await
				) {
					requester
						.membership_cache()
						.subject_contains_requester
						.insert(
							tg::authorization::Subject::Organization(organization.clone()),
							true,
						);
					return Ok(ControlFlow::Break(true));
				}
			}
		}
		requester
			.membership_cache()
			.subject_contains_requester
			.insert(
				tg::authorization::Subject::Organization(organization.clone()),
				false,
			);
		Ok(ControlFlow::Break(false))
	}

	async fn get_cached_resource_grants_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		cache: &mut Cache,
	) -> tg::Result<
		ControlFlow<
			Vec<(
				tg::authorization::Subject,
				tg::authorization::Permission,
				bool,
			)>,
			fdb::FdbError,
		>,
	> {
		if let Some(grants) = cache.resource_grants.get(resource) {
			return Ok(ControlFlow::Break(grants.clone()));
		}
		let grants = crate::fdb::propagate!(
			Self::get_resource_grants_with_transaction(txn, subspace, resource).await
		);
		cache
			.resource_grants
			.insert(resource.clone(), grants.clone());
		Ok(ControlFlow::Break(grants))
	}

	async fn get_cached_resource_parent_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<Option<tg::Id>, fdb::FdbError>> {
		if let Some(parent) = cache.resource_parents.get(resource) {
			return Ok(ControlFlow::Break(parent.clone()));
		}
		let parent = match resource.kind() {
			tg::id::Kind::Tag => crate::fdb::propagate!(
				Self::try_get_tag_with_transaction(txn, subspace, &resource.clone().try_into()?,)
					.await
			)
			.and_then(|tag| tag.parent),
			tg::id::Kind::Group => crate::fdb::propagate!(
				Self::try_get_group_with_transaction(txn, subspace, &resource.clone().try_into()?,)
					.await
			)
			.and_then(|group| group.parent),
			_ => None,
		};
		cache
			.resource_parents
			.insert(resource.clone(), parent.clone());
		Ok(ControlFlow::Break(parent))
	}

	async fn get_cached_target_tags_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		node: &tg::Id,
		permission: tg::authorization::Permission,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::authorization::Permission)>, fdb::FdbError>> {
		let key = (node.clone(), permission);
		if let Some(tags) = cache.target_tags.get(&key) {
			return Ok(ControlFlow::Break(tags.clone()));
		}
		let target_bytes = node.to_bytes();
		let tags = crate::fdb::propagate!(
			Self::get_target_tags_with_transaction(txn, subspace, target_bytes.as_ref()).await
		);
		let tags = {
			let results = futures::future::try_join_all(tags.into_iter().map(|tag| async move {
				let value = crate::fdb::propagate!(
					Self::try_get_tag_with_transaction(txn, subspace, &tag).await
				);
				Ok::<_, tg::Error>(ControlFlow::Break((tag, value)))
			}))
			.await?;
			let mut tags = Vec::with_capacity(results.len());
			for result in results {
				let tag = match result {
					ControlFlow::Break(tag) => tag,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				tags.push(tag);
			}

			tags
		};
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
					tg::authorization::Permission::Tag(
						tg::authorization::permission::tag::Permission::Read,
					),
				));
			}
		}
		cache.target_tags.insert(key, parents.clone());
		Ok(ControlFlow::Break(parents))
	}

	async fn get_cached_object_children_limited_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		object: &tg::object::Id,
		limit: usize,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<Vec<tg::object::Id>, fdb::FdbError>> {
		if let Some(children) = cache.object_children.get(object) {
			return Ok(ControlFlow::Break(
				children.iter().take(limit).cloned().collect(),
			));
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
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);
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
		Ok(ControlFlow::Break(children))
	}

	async fn get_cached_process_children_limited_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		limit: usize,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<Vec<tg::process::Id>, fdb::FdbError>> {
		if let Some(children) = cache.process_children.get(process) {
			return Ok(ControlFlow::Break(
				children.iter().take(limit).cloned().collect(),
			));
		}
		let bytes = process.to_bytes();
		let key = (Kind::ProcessChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			limit: Some(limit),
			..fdb::RangeOption::from(&range_subspace)
		};
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);
		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::ProcessChild { child, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		if children.len() < limit {
			cache
				.process_children
				.insert(process.clone(), children.clone());
		}

		Ok(ControlFlow::Break(children))
	}

	async fn get_cached_process_objects_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<Vec<(tg::object::Id, crate::process::object::Kind)>, fdb::FdbError>>
	{
		if let Some(objects) = cache.process_objects.get(process) {
			return Ok(ControlFlow::Break(objects.clone()));
		}
		let bytes = process.to_bytes();
		let key = (Kind::ProcessObject.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);
		let objects = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::ProcessObject { kind, object, .. }) =
					key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((object, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		cache
			.process_objects
			.insert(process.clone(), objects.clone());

		Ok(ControlFlow::Break(objects))
	}

	async fn get_cached_sandbox_owner_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		sandbox: &tg::sandbox::Id,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<Option<tg::Principal>, fdb::FdbError>> {
		if let Some(owner) = cache.sandbox_owners.get(sandbox) {
			return Ok(ControlFlow::Break(owner.clone()));
		}
		let key = Key::Sandbox(crate::fdb::sandbox::Key::Sandbox(sandbox.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let owner = crate::fdb::retry!(result)
			.map(|bytes| crate::sandbox::Sandbox::deserialize(&bytes))
			.transpose()?
			.and_then(|sandbox| sandbox.data)
			.and_then(|data| data.owner);
		cache.sandbox_owners.insert(sandbox.clone(), owner.clone());
		Ok(ControlFlow::Break(owner))
	}
}
