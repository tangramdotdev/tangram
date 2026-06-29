use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future::BoxFuture, stream},
	num_traits::ToPrimitive as _,
	std::collections::{HashMap, HashSet, VecDeque},
	tangram_client::prelude::*,
};

#[derive(Clone, Default)]
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
	process_sandboxes: HashMap<tg::process::Id, Option<tg::sandbox::Id>>,
	resource_grants: HashMap<tg::Id, Vec<(tg::grant::Principal, tg::grant::Permission)>>,
	sandbox_owners: HashMap<tg::sandbox::Id, Option<tg::Principal>>,
}

struct Requester<'a> {
	principal: &'a tg::Principal,
	principal_: tg::grant::Principal,
	id: Option<tg::Id>,
}

struct AuthorizationFrame {
	resource: tg::Id,
	permission: tg::grant::Permission,
	dependencies: Option<Vec<(tg::Id, tg::grant::Permission)>>,
	subtree_unknown: bool,
}

struct AuthorizationFrameEvaluation {
	frame: AuthorizationFrame,
	directly_authorized: bool,
	subtree_authorized: Option<bool>,
	dependencies: Vec<(tg::Id, tg::grant::Permission)>,
	cache: Cache,
	unknown: HashSet<(tg::Id, tg::grant::Permission)>,
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

#[derive(Clone, Copy)]
enum SubtreeSearch {
	Enabled,
	Disabled,
}

impl Cache {
	fn merge(&mut self, other: Self) {
		for (key, authorized) in other.authorization {
			match self.authorization.get_mut(&key) {
				Some(existing) => *existing = *existing || authorized,
				None => {
					self.authorization.insert(key, authorized);
				},
			}
		}
		self.resource_parents.extend(other.resource_parents);
		self.group_members.extend(other.group_members);
		self.item_tags.extend(other.item_tags);
		self.object_children.extend(other.object_children);
		self.object_parents.extend(other.object_parents);
		self.object_processes.extend(other.object_processes);
		self.organization_members.extend(other.organization_members);
		self.principal_contains_requester
			.extend(other.principal_contains_requester);
		self.process_parents.extend(other.process_parents);
		self.process_sandboxes.extend(other.process_sandboxes);
		self.resource_grants.extend(other.resource_grants);
		self.sandbox_owners.extend(other.sandbox_owners);
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
			principal_,
			id,
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
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		let requester = Requester::new(principal);
		let mut cache = Cache::default();
		let mut outputs = Vec::with_capacity(args.len());
		for arg in args {
			let Some(id) =
				Self::try_resolve_resource_with_transaction(&txn, &self.subspace, &arg.resource)
					.await?
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
			if matches!(principal, tg::Principal::Process(process) if tg::Id::from(process.clone()) == id)
			{
				outputs.push(Some(crate::authorize::Output {
					permissions: arg.permissions,
				}));
				continue;
			}
			let token_resource = if let Some(body) = arg.token.as_ref() {
				Self::try_resolve_resource_with_transaction(&txn, &self.subspace, &body.resource)
					.await?
			} else {
				None
			};
			let token = arg.token.as_ref().zip(token_resource.as_ref());
			let context = AuthorizationContext {
				txn: &txn,
				subspace: &self.subspace,
				config: self.config,
				requester: &requester,
				token,
			};
			let permissions =
				Self::authorize_with_transaction(context, &id, arg.permissions, &mut cache).await?;
			outputs.push(Some(crate::authorize::Output { permissions }));
		}
		Ok(outputs)
	}

	async fn authorize_with_transaction(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		permissions: tg::grant::permission::Set,
		cache: &mut Cache,
	) -> tg::Result<tg::grant::permission::Set> {
		let mut found = permissions.empty_like();
		for permission in permissions.iter() {
			let authorized =
				Self::authorize_permission_with_transaction(context, resource, permission, cache)
					.await?;
			if authorized {
				found.insert(tg::grant::permission::Set::from_permission(permission));
				if found.contains(permissions) {
					break;
				}
			}
		}
		Ok(found)
	}

	async fn authorize_permission_with_transaction(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
		cache: &mut Cache,
	) -> tg::Result<bool> {
		Self::authorize_permission_with_transaction_inner(
			context,
			resource,
			permission,
			cache,
			&mut HashSet::new(),
			SubtreeSearch::Enabled,
		)
		.await
	}

	async fn authorize_permission_with_transaction_inner(
		context: AuthorizationContext<'_>,
		resource: &tg::Id,
		permission: tg::grant::Permission,
		cache: &mut Cache,
		unknown: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		subtree_search: SubtreeSearch,
	) -> tg::Result<bool> {
		let root = (resource.clone(), permission);
		if !Self::is_authorized_by_token(context, resource, permission)
			&& let Some(authorized) = cache.authorization.get(&root)
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
			if cache.authorization.contains_key(&key) || unknown.contains(&key) {
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
					match cache.authorization.get(&dependency_key).copied() {
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
						Self::finish_authorization(cache, &mut pending, key, authorized);
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

			let mut frames = vec![frame];
			while frames.len() < context.config.concurrency {
				let Some(frame) = queue.pop_front() else {
					break;
				};
				if frame.dependencies.is_some() {
					queue.push_front(frame);
					break;
				}
				let key = (frame.resource.clone(), frame.permission);
				if Self::is_authorized_by_token(context, &frame.resource, frame.permission) {
					pending.remove(&key);
					token_authorized.insert(key);
					continue;
				}
				if cache.authorization.contains_key(&key) {
					pending.remove(&key);
					continue;
				}
				frames.push(frame);
			}

			let evaluations = stream::iter(frames)
				.map(|frame| {
					let mut cache = cache.clone();
					let mut unknown = unknown.clone();
					async move {
						Self::evaluate_authorization_frame_with_transaction(
							context,
							frame,
							&mut cache,
							&mut unknown,
							subtree_search,
						)
						.await
					}
				})
				.buffered(context.config.concurrency)
				.try_collect::<Vec<_>>()
				.await?;

			for mut evaluation in evaluations {
				cache.merge(std::mem::take(&mut evaluation.cache));
				unknown.extend(std::mem::take(&mut evaluation.unknown));
				Self::apply_authorization_frame_evaluation(
					cache,
					unknown,
					&token_authorized,
					&mut pending,
					&mut queue,
					evaluation,
				);
			}
		}

		Ok(token_authorized.contains(&root)
			|| cache.authorization.get(&root).copied().unwrap_or(false))
	}

	async fn evaluate_authorization_frame_with_transaction(
		context: AuthorizationContext<'_>,
		frame: AuthorizationFrame,
		cache: &mut Cache,
		unknown: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		subtree_search: SubtreeSearch,
	) -> tg::Result<AuthorizationFrameEvaluation> {
		let directly_authorized = Self::is_directly_authorized_with_transaction(
			context,
			&frame.resource,
			frame.permission,
			cache,
		)
		.await?;
		let subtree_authorized = if directly_authorized {
			Some(false)
		} else {
			match subtree_search {
				SubtreeSearch::Enabled => {
					Self::authorize_with_object_subtree_search_with_transaction(
						context,
						&frame.resource,
						frame.permission,
						cache,
						unknown,
					)
					.await?
				},
				SubtreeSearch::Disabled => Some(false),
			}
		};
		let dependencies = if directly_authorized || subtree_authorized == Some(true) {
			Vec::new()
		} else {
			Self::get_authorization_dependencies_with_transaction(
				context.txn,
				context.subspace,
				context.config.concurrency,
				&frame.resource,
				frame.permission,
				cache,
			)
			.await?
		};
		Ok(AuthorizationFrameEvaluation {
			frame,
			directly_authorized,
			subtree_authorized,
			dependencies,
			cache: std::mem::take(cache),
			unknown: std::mem::take(unknown),
		})
	}

	fn apply_authorization_frame_evaluation(
		cache: &mut Cache,
		unknown: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		token_authorized: &HashSet<(tg::Id, tg::grant::Permission)>,
		pending: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		queue: &mut VecDeque<AuthorizationFrame>,
		evaluation: AuthorizationFrameEvaluation,
	) {
		let key = (
			evaluation.frame.resource.clone(),
			evaluation.frame.permission,
		);
		if evaluation.directly_authorized || evaluation.subtree_authorized == Some(true) {
			Self::finish_authorization(cache, pending, key, true);
			return;
		}

		let subtree_unknown = evaluation.subtree_authorized.is_none();
		let mut authorized = false;
		let mut has_unknown_dependency = false;
		let mut dependencies_to_push = Vec::new();
		for (dependency, dependency_permission) in &evaluation.dependencies {
			let dependency_key = (dependency.clone(), *dependency_permission);
			if token_authorized.contains(&dependency_key) {
				authorized = true;
				break;
			}
			match cache.authorization.get(&dependency_key).copied() {
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
			Self::finish_authorization(cache, pending, key, authorized);
			return;
		}
		if dependencies_to_push.is_empty() {
			Self::finish_unknown_authorization(unknown, pending, key);
			return;
		}

		queue.push_back(AuthorizationFrame {
			resource: evaluation.frame.resource,
			permission: evaluation.frame.permission,
			dependencies: Some(evaluation.dependencies),
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

	fn finish_authorization(
		cache: &mut Cache,
		pending: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		key: (tg::Id, tg::grant::Permission),
		authorized: bool,
	) {
		pending.remove(&key);
		match cache.authorization.get_mut(&key) {
			Some(existing) => *existing = *existing || authorized,
			None => {
				cache.authorization.insert(key, authorized);
			},
		}
	}

	fn finish_unknown_authorization(
		unknown: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		pending: &mut HashSet<(tg::Id, tg::grant::Permission)>,
		key: (tg::Id, tg::grant::Permission),
	) {
		pending.remove(&key);
		unknown.insert(key);
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
					cache,
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
					cache,
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

	fn authorize_with_object_subtree_search_with_transaction<'a>(
		context: AuthorizationContext<'a>,
		resource: &'a tg::Id,
		permission: tg::grant::Permission,
		cache: &'a mut Cache,
		unknown: &'a mut HashSet<(tg::Id, tg::grant::Permission)>,
	) -> BoxFuture<'a, tg::Result<Option<bool>>> {
		async move {
			if permission
				!= tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree)
			{
				return Ok(Some(false));
			}
			if Self::authorize_permission_with_transaction_inner(
				context,
				resource,
				permission,
				cache,
				unknown,
				SubtreeSearch::Disabled,
			)
			.await?
			{
				return Ok(Some(true));
			}
			let node =
				tg::grant::Permission::Object(tg::grant::permission::object::Permission::Node);
			let mut budget = SubtreeSearchBudget {
				max_depth: context.config.object_subtree.max_depth,
				remaining_objects: context.config.object_subtree.max_objects,
			};

			let mut stack = Vec::from([(tg::object::Id::try_from(resource.clone())?, 0)]);
			while let Some((object, depth)) = stack.pop() {
				if depth > budget.max_depth || budget.remaining_objects == 0 {
					return Ok(None);
				}
				budget.remaining_objects -= 1;

				let resource = tg::Id::from(object.clone());
				if !Self::authorize_permission_with_transaction_inner(
					context,
					&resource,
					node,
					cache,
					unknown,
					SubtreeSearch::Disabled,
				)
				.await?
				{
					if unknown.contains(&(resource, node)) {
						return Ok(None);
					}
					return Ok(Some(false));
				}

				let children = Self::get_cached_object_children_limited_with_transaction(
					context.txn,
					context.subspace,
					&object,
					budget.remaining_objects + 1,
					cache,
				)
				.await?;
				if children.len() > budget.remaining_objects {
					return Ok(None);
				}
				if children.is_empty() {
					continue;
				}
				if depth >= 1
					&& Self::authorize_permission_with_transaction_inner(
						context,
						&resource,
						permission,
						cache,
						unknown,
						SubtreeSearch::Disabled,
					)
					.await?
				{
					continue;
				}
				if depth == budget.max_depth {
					return Ok(None);
				}
				for child in children {
					stack.push((child, depth + 1));
				}
			}

			Ok(Some(true))
		}
		.boxed()
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
				let object_parents =
					Self::get_cached_object_parents_with_transaction(txn, subspace, &object, cache)
						.await?;
				for parent in object_parents {
					let permission = tg::grant::Permission::Object(
						tg::grant::permission::object::Permission::Subtree,
					);
					dependencies.push((parent.into(), permission));
				}
				let processes = Self::get_cached_object_processes_with_transaction(
					txn, subspace, &object, cache,
				)
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
					dependencies.push((process.into(), tg::grant::Permission::Process(permission)));
				}
				dependencies.extend(
					Self::get_cached_item_tags_with_transaction(
						txn,
						subspace,
						concurrency,
						resource,
						permission,
						cache,
					)
					.await?,
				);
			},
			tg::grant::Permission::Process(process_permission) => {
				let process = tg::process::Id::try_from(resource.clone())?;
				if let Some(sandbox) = Self::get_cached_process_sandbox_with_transaction(
					txn, subspace, &process, cache,
				)
				.await?
				{
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
				let process_parents = Self::get_cached_process_parents_with_transaction(
					txn, subspace, &process, cache,
				)
				.await?;
				for parent in process_parents {
					let permission =
						tg::grant::Permission::Process(process_permission.to_subtree());
					dependencies.push((parent.into(), permission));
				}
				dependencies.extend(
					Self::get_cached_item_tags_with_transaction(
						txn,
						subspace,
						concurrency,
						resource,
						permission,
						cache,
					)
					.await?,
				);
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
		cache: &mut Cache,
	) -> tg::Result<bool> {
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
			tg::grant::Principal::Group(group) => {
				Self::group_contains_requester_with_transaction(
					txn, subspace, group, requester, cache,
				)
				.await?
			},
			tg::grant::Principal::Organization(organization) => {
				Self::organization_contains_requester_with_transaction(
					txn,
					subspace,
					organization,
					requester,
					cache,
				)
				.await?
			},
			_ => false,
		};
		cache
			.principal_contains_requester
			.insert(principal.clone(), contains);
		Ok(contains)
	}

	async fn group_contains_requester_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
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
			let members =
				Self::get_cached_group_members_with_transaction(txn, subspace, &group, cache)
					.await?;
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

	async fn organization_contains_requester_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		organization: &tg::organization::Id,
		requester: &Requester<'_>,
		cache: &mut Cache,
	) -> tg::Result<bool> {
		let principal = tg::grant::Principal::Organization(organization.clone());
		if let Some(contains) = cache.principal_contains_requester.get(&principal) {
			return Ok(*contains);
		}
		let members = Self::get_cached_organization_members_with_transaction(
			txn,
			subspace,
			organization,
			cache,
		)
		.await?;
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
					txn, subspace, &group, requester, cache,
				)
				.await?
				{
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

	async fn get_cached_object_parents_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		object: &tg::object::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<tg::object::Id>> {
		if let Some(parents) = cache.object_parents.get(object) {
			return Ok(parents.clone());
		}
		let parents = Self::get_object_parents_with_transaction(txn, subspace, object).await?;
		cache.object_parents.insert(object.clone(), parents.clone());
		Ok(parents)
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

	async fn get_cached_object_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		object: &tg::object::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::object::Kind)>> {
		if let Some(processes) = cache.object_processes.get(object) {
			return Ok(processes.clone());
		}
		let processes = Self::get_object_processes_with_transaction(txn, subspace, object).await?;
		cache
			.object_processes
			.insert(object.clone(), processes.clone());
		Ok(processes)
	}

	async fn get_cached_process_parents_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<tg::process::Id>> {
		if let Some(parents) = cache.process_parents.get(process) {
			return Ok(parents.clone());
		}
		let parents = Self::get_process_parents_with_transaction(txn, subspace, process).await?;
		cache
			.process_parents
			.insert(process.clone(), parents.clone());
		Ok(parents)
	}

	async fn get_cached_process_sandbox_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		cache: &mut Cache,
	) -> tg::Result<Option<tg::sandbox::Id>> {
		if let Some(sandbox) = cache.process_sandboxes.get(process) {
			return Ok(sandbox.clone());
		}
		let sandbox = Self::try_get_process_with_transaction(txn, subspace, process)
			.await?
			.and_then(|process| process.sandbox);
		cache
			.process_sandboxes
			.insert(process.clone(), sandbox.clone());
		Ok(sandbox)
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
		let owner =
			txn.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the sandbox"))?
				.map(|bytes| {
					let string = std::str::from_utf8(&bytes)
						.map_err(|error| tg::error!(!error, "failed to parse the sandbox owner"))?;
					if string.is_empty() {
						Ok(None)
					} else {
						string.parse().map(Some).map_err(|error| {
							tg::error!(!error, "failed to parse the sandbox owner")
						})
					}
				})
				.transpose()?
				.flatten();
		cache.sandbox_owners.insert(sandbox.clone(), owner.clone());
		Ok(owner)
	}

	async fn get_cached_group_members_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		group: &tg::group::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<tg::Id>> {
		if let Some(members) = cache.group_members.get(group) {
			return Ok(members.clone());
		}
		let members: Vec<tg::Id> = Self::get_group_members_with_transaction(txn, subspace, group)
			.await?
			.into_iter()
			.map(tg::Id::from)
			.collect();
		cache.group_members.insert(group.clone(), members.clone());
		Ok(members)
	}

	async fn get_cached_organization_members_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		organization: &tg::organization::Id,
		cache: &mut Cache,
	) -> tg::Result<Vec<tg::Id>> {
		if let Some(members) = cache.organization_members.get(organization) {
			return Ok(members.clone());
		}
		let members: Vec<tg::Id> =
			Self::get_organization_members_with_transaction(txn, subspace, organization)
				.await?
				.into_iter()
				.map(tg::Id::from)
				.collect();
		cache
			.organization_members
			.insert(organization.clone(), members.clone());
		Ok(members)
	}
}
