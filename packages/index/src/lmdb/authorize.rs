use {
	crate::lmdb::{Db, Index},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::collections::{BTreeMap, HashMap, HashSet, VecDeque},
	tangram_client::prelude::*,
};

const PRECOMPUTE_REQUESTER_PRINCIPALS: bool = false;

#[derive(Default)]
struct Cache {
	authorization_dependencies: HashMap<
		(tg::Id, tg::authorization::Permission),
		Vec<(tg::Id, tg::authorization::Permission)>,
	>,
	direct_permissions: HashMap<tg::Id, HashSet<tg::authorization::Permission>>,
	group_members: HashMap<tg::group::Id, Vec<tg::Id>>,
	object_children: HashMap<tg::object::Id, Vec<tg::object::Id>>,
	object_processes: HashMap<tg::object::Id, Vec<(tg::process::Id, crate::process::object::Kind)>>,
	organization_members: HashMap<tg::organization::Id, Vec<tg::Id>>,
	process_children: HashMap<tg::process::Id, Vec<tg::process::Id>>,
	process_objects: HashMap<tg::process::Id, Vec<(tg::object::Id, crate::process::object::Kind)>>,
	process_sandboxes: HashMap<tg::process::Id, Option<tg::sandbox::Id>>,
	resource_grants: HashMap<
		tg::Id,
		Vec<(
			tg::authorization::Subject,
			tg::authorization::Permission,
			bool,
		)>,
	>,
	resource_parents: HashMap<tg::Id, Option<tg::Id>>,
	sandbox_owners: HashMap<tg::sandbox::Id, Option<tg::Principal>>,
	subject_contains_requester: HashMap<tg::authorization::Subject, bool>,
	target_tags: HashMap<
		(tg::Id, tg::authorization::Permission),
		Vec<(tg::Id, tg::authorization::Permission)>,
	>,
}

struct Requester<'a> {
	principal: &'a tg::Principal,
	subject: tg::authorization::Subject,
	id: Option<tg::Id>,
	subjects: HashSet<tg::authorization::Subject>,
}

enum AncestorTask {
	Node {
		depth: usize,
		key: (tg::Id, tg::authorization::Permission),
	},
	ObjectParents {
		after: Option<Vec<u8>>,
		dependent: (tg::Id, tg::authorization::Permission),
		depth: usize,
		object: tg::object::Id,
	},
	ProcessParents {
		after: Option<Vec<u8>>,
		dependent: (tg::Id, tg::authorization::Permission),
		depth: usize,
		permission: tg::authorization::permission::process::Permission,
		process: tg::process::Id,
	},
}

enum DescendantTask {
	Node {
		depth: usize,
		key: (tg::Id, tg::authorization::Permission),
	},
	ObjectChildren {
		after: Option<Vec<u8>>,
		depth: usize,
		object: tg::object::Id,
	},
	OwnerSandboxes {
		after: Option<Vec<u8>>,
		depth: usize,
		owner: tg::Principal,
	},
	ProcessChildren {
		after: Option<Vec<u8>>,
		depth: usize,
		permission: tg::authorization::permission::process::Permission,
		process: tg::process::Id,
	},
	ProcessGrants {
		after: Option<Vec<u8>>,
		depth: usize,
		process: tg::process::Id,
	},
	SandboxProcesses {
		after: Option<Vec<u8>>,
		depth: usize,
		permission: tg::authorization::permission::sandbox::Permission,
		sandbox: tg::sandbox::Id,
	},
	SubjectGrants {
		after: Option<Vec<u8>>,
		subject: tg::authorization::Subject,
	},
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SearchOutcome {
	Authorized,
	Denied,
	Exhausted,
	Pending,
}

struct AncestorSearch {
	budget: SearchBudget,
	incomplete: HashSet<(tg::Id, tg::authorization::Permission)>,
	queues: BTreeMap<usize, VecDeque<AncestorTask>>,
	unresolved: HashSet<(tg::Id, tg::authorization::Permission)>,
	visited: HashSet<(tg::Id, tg::authorization::Permission)>,
}

struct DescendantSearch {
	budget: SearchBudget,
	complete: bool,
	outcome: Option<SearchOutcome>,
	stack: Vec<DescendantTask>,
	target_permissions: HashMap<tg::Id, Vec<tg::authorization::Permission>>,
	unresolved: HashSet<(tg::Id, tg::authorization::Permission)>,
	visited: HashSet<(tg::Id, tg::authorization::Permission)>,
}

struct SearchBudget {
	config: crate::authorize::SearchConfig,
	edges: usize,
	nodes: usize,
}

struct SubtreeSearchBudget {
	max_depth: usize,
	remaining: usize,
}

impl SearchBudget {
	#[must_use]
	fn new(config: crate::authorize::SearchConfig) -> Self {
		Self {
			config,
			edges: 0,
			nodes: 0,
		}
	}

	#[must_use]
	fn with_root_total(mut config: crate::authorize::SearchConfig, root_total: usize) -> Self {
		config.max_edges = config.max_edges.saturating_mul(root_total);
		config.max_nodes = config.max_nodes.saturating_mul(root_total);

		Self::new(config)
	}

	fn add_edge(&mut self) -> bool {
		if self.edges == self.config.max_edges {
			return false;
		}
		self.edges += 1;
		true
	}

	fn add_node(&mut self, depth: usize) -> bool {
		if depth > self.config.max_depth || self.nodes == self.config.max_nodes {
			return false;
		}
		self.nodes += 1;
		true
	}
}

struct AuthorizationContext<'a, 'txn> {
	authorization: &'a mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
	authorize: crate::authorize::Config,
	cache: &'a mut Cache,
	db: &'a Db,
	dependents: &'a mut HashMap<
		(tg::Id, tg::authorization::Permission),
		HashSet<(tg::Id, tg::authorization::Permission)>,
	>,
	derived_authorization: &'a mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
	derived_exhausted_roots: &'a mut HashSet<(tg::Id, tg::authorization::Permission)>,
	exhausted_roots: &'a mut HashSet<(tg::Id, tg::authorization::Permission)>,
	requester: &'a Requester<'a>,
	subspace: &'a fdbt::Subspace,
	token: Option<(tg::authorization::Body, tg::Id)>,
	transaction: &'a lmdb::RoTxn<'txn>,
}

impl AncestorSearch {
	#[must_use]
	fn new(
		config: crate::authorize::SearchConfig,
		roots: &[(tg::Id, tg::authorization::Permission)],
	) -> Self {
		let mut budget = SearchBudget::with_root_total(config, roots.len());
		let mut incomplete = HashSet::new();
		let mut queues = BTreeMap::<_, VecDeque<_>>::new();
		let unresolved = roots.iter().cloned().collect();
		let mut visited = HashSet::new();
		for root in roots {
			if !budget.add_node(0) {
				incomplete.insert(root.clone());
				continue;
			}
			visited.insert(root.clone());
			queues.entry(0).or_default().push_back(AncestorTask::Node {
				depth: 0,
				key: root.clone(),
			});
		}

		Self {
			budget,
			incomplete,
			queues,
			unresolved,
			visited,
		}
	}
}

impl DescendantSearch {
	#[must_use]
	fn new(
		config: crate::authorize::SearchConfig,
		requester: &Requester<'_>,
		targets: Vec<(tg::Id, tg::authorization::Permission)>,
	) -> Self {
		let mut budget = SearchBudget::with_root_total(config, targets.len());
		let complete = !matches!(
			requester.principal,
			tg::Principal::Group(_) | tg::Principal::User(_)
		);
		let unresolved = targets.iter().cloned().collect();
		let mut target_permissions = HashMap::<_, Vec<_>>::new();
		for (resource, permission) in targets {
			target_permissions
				.entry(resource)
				.or_default()
				.push(permission);
		}
		if config.max_nodes == 0 {
			return Self {
				budget,
				complete,
				outcome: Some(SearchOutcome::Exhausted),
				stack: Vec::new(),
				target_permissions,
				unresolved,
				visited: HashSet::new(),
			};
		}
		let mut stack = Vec::new();
		let mut visited = HashSet::new();

		let public = tg::authorization::Subject::Public;
		stack.push(DescendantTask::SubjectGrants {
			after: None,
			subject: public.clone(),
		});
		if requester.subject != public {
			stack.push(DescendantTask::SubjectGrants {
				after: None,
				subject: requester.subject.clone(),
			});
		}

		let inherent = match requester.principal {
			tg::Principal::Process(process) => {
				crate::authorize::permissions_implied_by(tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Parent,
				))
				.into_iter()
				.map(|permission| (tg::Id::from(process.clone()), permission))
				.collect()
			},
			tg::Principal::Sandbox(sandbox) => vec![
				(
					tg::Id::from(sandbox.clone()),
					tg::authorization::Permission::Sandbox(
						tg::authorization::permission::sandbox::Permission::Read,
					),
				),
				(
					tg::Id::from(sandbox.clone()),
					tg::authorization::Permission::Sandbox(
						tg::authorization::permission::sandbox::Permission::Write,
					),
				),
			],
			tg::Principal::User(user) => {
				crate::authorize::permissions_implied_by(tg::authorization::Permission::User(
					tg::authorization::permission::user::Permission::Admin,
				))
				.into_iter()
				.map(|permission| (tg::Id::from(user.clone()), permission))
				.collect()
			},
			tg::Principal::Anonymous
			| tg::Principal::Group(_)
			| tg::Principal::Organization(_)
			| tg::Principal::Root
			| tg::Principal::Runner(_) => Vec::new(),
		};
		let mut outcome = None;
		for key in inherent {
			if !visited.insert(key.clone()) {
				continue;
			}
			if !budget.add_node(0) {
				outcome = Some(SearchOutcome::Exhausted);
				break;
			}
			stack.push(DescendantTask::Node { depth: 0, key });
		}

		Self {
			budget,
			complete,
			outcome,
			stack,
			target_permissions,
			unresolved,
			visited,
		}
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
		}
	}
}

impl Index {
	pub async fn authorize_batch(
		&self,
		args: &[crate::authorize::Arg],
		config: crate::authorize::Config,
		principal: &tg::Principal,
	) -> tg::Result<Vec<crate::authorize::Outcome>> {
		config.validate()?;
		args.iter().try_for_each(crate::authorize::Arg::validate)?;
		if args.is_empty() {
			return Ok(Vec::new());
		}
		if matches!(principal, tg::Principal::Root) {
			let outcomes = args
				.iter()
				.map(|arg| {
					crate::authorize::Outcome::Authorized(crate::authorize::Output {
						permissions: arg.requested,
					})
				})
				.collect();
			return Ok(outcomes);
		}
		let request = crate::read::Request::AuthorizeBatch {
			args: args.to_owned(),
			config,
			principal: principal.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::AuthorizeBatch(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn authorize_batch_with_transaction(
		config: crate::authorize::Config,
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		args: &[crate::authorize::Arg],
		principal: &tg::Principal,
	) -> tg::Result<Vec<crate::authorize::Outcome>> {
		args.iter().try_for_each(crate::authorize::Arg::validate)?;
		if args.is_empty() {
			return Ok(Vec::new());
		}
		if matches!(principal, tg::Principal::Root) {
			let outcomes = args
				.iter()
				.map(|arg| {
					crate::authorize::Outcome::Authorized(crate::authorize::Output {
						permissions: arg.requested,
					})
				})
				.collect();
			return Ok(outcomes);
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
		let resources = args
			.iter()
			.map(|arg| {
				Self::try_resolve_resource_with_transaction(
					db,
					subspace,
					transaction,
					&arg.resource,
				)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let requested = std::iter::zip(args, &resources)
			.map(|(arg, resource)| {
				let Some((resource, exact)) = resource else {
					return Ok(None);
				};
				if *exact {
					return Ok(Some(arg.requested));
				}
				crate::authorize::permissions_for_specifier_prefix(resource, arg.requested)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let required = std::iter::zip(args, &resources)
			.map(|(arg, resource)| {
				let Some((resource, exact)) = resource else {
					return Ok(None);
				};
				if *exact {
					return Ok(Some(arg.required));
				}
				crate::authorize::permissions_for_specifier_prefix(resource, arg.required)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let mut authorization = HashMap::new();
		let mut cache = Cache::default();
		let mut dependents = HashMap::new();
		let mut derived_authorization = HashMap::new();
		let mut derived_exhausted_roots = HashSet::new();
		let mut exhausted_roots = HashSet::new();
		let mut ordinary_roots = Vec::new();
		for (index, (arg, resource)) in std::iter::zip(args, &resources).enumerate() {
			let Some((id, _)) = resource else {
				continue;
			};
			let Some(requested) = requested[index] else {
				continue;
			};
			if arg.token.is_some() || crate::authorize::validate(id, requested).is_err() {
				continue;
			}
			if matches!(principal, tg::Principal::Process(process) if tg::Id::from(process.clone()) == *id)
			{
				continue;
			}
			ordinary_roots.extend(
				crate::authorize::permissions_in_search_order(requested)
					.into_iter()
					.map(|permission| (id.clone(), permission)),
			);
		}
		if !ordinary_roots.is_empty() {
			let mut context = AuthorizationContext {
				authorization: &mut authorization,
				authorize: config,
				cache: &mut cache,
				db,
				dependents: &mut dependents,
				derived_authorization: &mut derived_authorization,
				derived_exhausted_roots: &mut derived_exhausted_roots,
				exhausted_roots: &mut exhausted_roots,
				requester: &requester,
				subspace,
				token: None,
				transaction,
			};
			Self::authorize_permissions_ordinary_with_transaction(&mut context, &ordinary_roots)?;
		}
		let mut outcomes = Vec::with_capacity(args.len());
		for (index, (arg, resource)) in std::iter::zip(args, resources).enumerate() {
			let Some((id, _)) = resource else {
				outcomes.push(crate::authorize::Outcome::Denied(None));
				continue;
			};
			let Some(requested) = requested[index] else {
				outcomes.push(crate::authorize::Outcome::Denied(None));
				continue;
			};
			let Some(required) = required[index] else {
				outcomes.push(crate::authorize::Outcome::Denied(None));
				continue;
			};
			if crate::authorize::validate(&id, requested).is_err() {
				outcomes.push(crate::authorize::Outcome::Denied(None));
				continue;
			}
			if matches!(principal, tg::Principal::Process(process) if tg::Id::from(process.clone()) == id)
			{
				outcomes.push(crate::authorize::Outcome::Authorized(
					crate::authorize::Output {
						permissions: arg.requested,
					},
				));
				continue;
			}
			let token = if let Some(body) = arg.token.clone() {
				let resource = body.resource.clone();
				Some((body, resource))
			} else {
				None
			};
			let mut token_authorization = HashMap::new();
			let mut token_dependents = HashMap::new();
			let mut token_derived_authorization = HashMap::new();
			let mut token_derived_exhausted_roots = HashSet::new();
			let mut token_exhausted_roots = HashSet::new();
			let (
				authorization,
				dependents,
				derived_authorization,
				derived_exhausted_roots,
				exhausted_roots,
			) = if token.is_some() {
				(
					&mut token_authorization,
					&mut token_dependents,
					&mut token_derived_authorization,
					&mut token_derived_exhausted_roots,
					&mut token_exhausted_roots,
				)
			} else {
				(
					&mut authorization,
					&mut dependents,
					&mut derived_authorization,
					&mut derived_exhausted_roots,
					&mut exhausted_roots,
				)
			};
			let mut context = AuthorizationContext {
				authorization,
				authorize: config,
				cache: &mut cache,
				db,
				dependents,
				derived_authorization,
				derived_exhausted_roots,
				exhausted_roots,
				requester: &requester,
				subspace,
				token,
				transaction,
			};
			let (authorized, exhausted) =
				Self::authorize_with_transaction(&mut context, &id, requested)?;
			let required_exhausted = required.iter().any(|permission| {
				let permission = tg::authorization::permission::Set::from_permission(permission);
				!authorized.contains(permission) && exhausted.contains(permission)
			});
			if required_exhausted {
				outcomes.push(crate::authorize::Outcome::Exhausted);
				continue;
			}
			let permissions = if requested == arg.requested {
				authorized
			} else if authorized.contains(requested) {
				arg.requested
			} else {
				arg.requested.empty_like()
			};
			let output = crate::authorize::Output { permissions };
			let outcome = crate::authorize::Outcome::from_output(Some(output), arg.requested);
			outcomes.push(outcome);
		}

		Ok(outcomes)
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
	) -> tg::Result<(
		tg::authorization::permission::Set,
		tg::authorization::permission::Set,
	)> {
		let mut authorized = permissions.empty_like();
		let mut exhausted = permissions.empty_like();
		for permission in crate::authorize::permissions_in_search_order(permissions) {
			let permission_authorized =
				match Self::authorize_permission_with_transaction(context, resource, permission) {
					Ok(authorized) => authorized,
					Err(error) if crate::authorize::is_search_exhausted(&error) => {
						exhausted.insert(tg::authorization::permission::Set::from_permission(
							permission,
						));
						continue;
					},
					Err(error) => return Err(error),
				};
			if permission_authorized {
				crate::authorize::insert_implied_permissions(
					&mut authorized,
					permissions,
					permission,
				);
				if authorized.contains(permissions) {
					break;
				}
			}
		}

		Ok((authorized, exhausted))
	}

	fn authorize_permission_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		resource: &tg::Id,
		permission: tg::authorization::Permission,
	) -> tg::Result<bool> {
		if Self::authorize_permission_ordinary_with_transaction(context, resource, permission)? {
			return Ok(true);
		}
		let key = (resource.clone(), permission);
		if let Some(authorized) = context.derived_authorization.get(&key) {
			return Ok(*authorized);
		}
		if context.derived_exhausted_roots.contains(&key) {
			return Err(crate::authorize::search_exhausted_error(
				"the derived authorization search exhausted",
			));
		}
		let result = (|| match permission {
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			) => {
				let mut budget = SubtreeSearchBudget {
					max_depth: context.authorize.subtree.max_depth,
					remaining: context.authorize.subtree.max_objects,
				};
				let authorized = Self::authorize_with_object_subtree_search_with_transaction(
					context,
					resource,
					&mut budget,
				)?
				.ok_or_else(|| {
					crate::authorize::search_exhausted_error(
						"the subtree authorization search exhausted",
					)
				})?;

				Ok(authorized)
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
					max_depth: context.authorize.subtree.max_depth,
					remaining: context.authorize.subtree.max_processes,
				};
				let authorized = Self::authorize_with_process_subtree_search_with_transaction(
					context,
					resource,
					permission,
					&mut budget,
				)?
				.ok_or_else(|| {
					crate::authorize::search_exhausted_error(
						"the subtree authorization search exhausted",
					)
				})?;

				Ok(authorized)
			},
			_ => Ok(false),
		})();
		match result {
			Ok(authorized) => {
				context.derived_authorization.insert(key, authorized);
				Ok(authorized)
			},
			Err(error) if crate::authorize::is_search_exhausted(&error) => {
				context.derived_exhausted_roots.insert(key);
				Err(error)
			},
			Err(error) => Err(error),
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
		if context.exhausted_roots.contains(&root) {
			return Err(crate::authorize::search_exhausted_error(
				"the ancestor and descendant authorization searches exhausted",
			));
		}
		Self::authorize_permissions_ordinary_with_transaction(
			context,
			std::slice::from_ref(&root),
		)?;
		if let Some(authorized) = context.authorization.get(&root) {
			return Ok(*authorized);
		}
		if context.exhausted_roots.contains(&root) {
			return Err(crate::authorize::search_exhausted_error(
				"the ancestor and descendant authorization searches exhausted",
			));
		}

		Ok(false)
	}

	fn authorize_permissions_ordinary_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		roots: &[(tg::Id, tg::authorization::Permission)],
	) -> tg::Result<()> {
		// Collect the unique unresolved roots.
		let mut roots_ = Vec::new();
		let mut seen = HashSet::new();
		for root in roots {
			if context.authorization.contains_key(root)
				|| context.exhausted_roots.contains(root)
				|| !seen.insert(root.clone())
			{
				continue;
			}
			roots_.push(root.clone());
		}
		let roots = roots_;
		if roots.is_empty() {
			return Ok(());
		}

		// Search all of the roots with one shared ancestor graph.
		let mut ancestor = AncestorSearch::new(context.authorize.ancestor, &roots);
		while let SearchOutcome::Pending =
			Self::advance_ancestor_search_with_transaction(context, &mut ancestor)?
		{}

		// Collect the roots whose ancestor paths were incomplete.
		let mut descendant_roots = Vec::new();
		for root in roots {
			if context.authorization.contains_key(&root) {
				continue;
			}
			if !ancestor.incomplete.contains(&root) {
				context.authorization.insert(root, false);
				continue;
			}
			descendant_roots.push(root);
		}
		if descendant_roots.is_empty() {
			return Ok(());
		}

		// Search all of the remaining roots with one shared descendant graph.
		let mut descendant = DescendantSearch::new(
			context.authorize.descendant,
			context.requester,
			descendant_roots.clone(),
		);
		let outcome = loop {
			let outcome =
				Self::advance_descendant_search_with_transaction(context, &mut descendant)?;
			if outcome != SearchOutcome::Pending {
				break outcome;
			}
		};
		match outcome {
			SearchOutcome::Authorized => {},
			SearchOutcome::Denied => {
				for root in descendant_roots {
					context.authorization.entry(root).or_insert(false);
				}
			},
			SearchOutcome::Exhausted => {
				context.exhausted_roots.extend(
					descendant_roots
						.into_iter()
						.filter(|root| !context.authorization.contains_key(root)),
				);
			},
			SearchOutcome::Pending => unreachable!(),
		}

		Ok(())
	}

	fn advance_ancestor_search_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		search: &mut AncestorSearch,
	) -> tg::Result<SearchOutcome> {
		if search.unresolved.is_empty() {
			search.incomplete.clear();
			search.queues.clear();
			return Ok(SearchOutcome::Authorized);
		}
		let Some((depth, mut queue)) = search.queues.pop_first() else {
			let outcome = Self::finish_ancestor_search(context, search);
			return Ok(outcome);
		};
		let task = queue.pop_front().unwrap();
		if !queue.is_empty() {
			search.queues.insert(depth, queue);
		}
		match task {
			AncestorTask::Node { depth, key } => {
				if let Some(authorized) = context.authorization.get(&key).copied() {
					if authorized {
						Self::propagate_tracked_authorization(
							context.authorization,
							context.dependents,
							&key,
							&mut search.unresolved,
						);
						return Ok(SearchOutcome::Pending);
					}
					return Ok(SearchOutcome::Pending);
				}
				let (resource, permission) = key.clone();
				if Self::is_authorized_by_token(context, &resource, permission)
					|| Self::is_directly_authorized_with_transaction(
						context, &resource, permission,
					)? {
					Self::propagate_tracked_authorization(
						context.authorization,
						context.dependents,
						&key,
						&mut search.unresolved,
					);
					return Ok(SearchOutcome::Pending);
				}

				let dependencies = Self::get_authorization_dependencies_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&resource,
					permission,
					context.cache,
				)?;
				for dependency in dependencies {
					let dependency_depth = depth + 1;
					if !Self::add_ancestor_dependency(
						context,
						search,
						&key,
						dependency,
						dependency_depth,
					) || context.authorization.get(&key).copied() == Some(true)
					{
						return Ok(SearchOutcome::Pending);
					}
				}

				match permission {
					tg::authorization::Permission::Object(_) => {
						let object = tg::object::Id::try_from(resource)?;
						search.queues.entry(depth).or_default().push_back(
							AncestorTask::ObjectParents {
								after: None,
								dependent: key,
								depth,
								object,
							},
						);
					},
					tg::authorization::Permission::Process(permission) => {
						let process = tg::process::Id::try_from(resource)?;
						search.queues.entry(depth).or_default().push_back(
							AncestorTask::ProcessParents {
								after: None,
								dependent: key,
								depth,
								permission,
								process,
							},
						);
					},
					tg::authorization::Permission::Group(_)
					| tg::authorization::Permission::Organization(_)
					| tg::authorization::Permission::Sandbox(_)
					| tg::authorization::Permission::Tag(_)
					| tg::authorization::Permission::User(_) => {},
				}
			},
			AncestorTask::ObjectParents {
				after,
				dependent,
				depth,
				object,
			} => {
				if context.authorization.contains_key(&dependent) {
					return Ok(SearchOutcome::Pending);
				}
				let object_bytes = object.to_bytes();
				let prefix = Self::pack(
					context.subspace,
					&(
						crate::lmdb::Kind::ChildObject.to_i32().unwrap(),
						object_bytes.as_ref(),
					),
				);
				let (keys, after) = Self::get_key_page_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&prefix,
					after.as_deref(),
					search.budget.config.page_size,
				)?;
				for key in keys {
					let crate::lmdb::Key::Object(crate::lmdb::object::Key::ChildObject {
						object,
						..
					}) = key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					let key = (
						tg::Id::from(object),
						tg::authorization::Permission::Object(
							tg::authorization::permission::object::Permission::Subtree,
						),
					);
					let dependency_depth = depth + 1;
					if !Self::add_ancestor_dependency(
						context,
						search,
						&dependent,
						key,
						dependency_depth,
					) || context.authorization.get(&dependent).copied() == Some(true)
					{
						return Ok(SearchOutcome::Pending);
					}
				}
				if let Some(after) = after {
					search.queues.entry(depth).or_default().push_back(
						AncestorTask::ObjectParents {
							after: Some(after),
							dependent,
							depth,
							object,
						},
					);
				}
			},
			AncestorTask::ProcessParents {
				after,
				dependent,
				depth,
				permission,
				process,
			} => {
				if context.authorization.contains_key(&dependent) {
					return Ok(SearchOutcome::Pending);
				}
				let process_bytes = process.to_bytes();
				let prefix = Self::pack(
					context.subspace,
					&(
						crate::lmdb::Kind::ChildProcess.to_i32().unwrap(),
						process_bytes.as_ref(),
					),
				);
				let (keys, after) = Self::get_key_page_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&prefix,
					after.as_deref(),
					search.budget.config.page_size,
				)?;
				for key in keys {
					let crate::lmdb::Key::Process(crate::lmdb::process::Key::ChildProcess {
						parent,
						..
					}) = key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					let key = (
						tg::Id::from(parent),
						tg::authorization::Permission::Process(permission.to_subtree()),
					);
					let dependency_depth = depth + 1;
					if !Self::add_ancestor_dependency(
						context,
						search,
						&dependent,
						key,
						dependency_depth,
					) || context.authorization.get(&dependent).copied() == Some(true)
					{
						return Ok(SearchOutcome::Pending);
					}
				}
				if let Some(after) = after {
					search.queues.entry(depth).or_default().push_back(
						AncestorTask::ProcessParents {
							after: Some(after),
							dependent,
							depth,
							permission,
							process,
						},
					);
				}
			},
		}

		Ok(SearchOutcome::Pending)
	}

	fn finish_ancestor_search(
		context: &mut AuthorizationContext<'_, '_>,
		search: &mut AncestorSearch,
	) -> SearchOutcome {
		// Propagate incomplete paths to every unresolved dependent.
		let mut incomplete = HashSet::new();
		let mut stack = std::mem::take(&mut search.incomplete)
			.into_iter()
			.collect::<Vec<_>>();
		while let Some(key) = stack.pop() {
			if context.authorization.contains_key(&key) || !incomplete.insert(key.clone()) {
				continue;
			}
			if let Some(dependents) = context.dependents.get(&key) {
				stack.extend(dependents.iter().cloned());
			}
		}
		search.incomplete = incomplete;

		// Preserve complete negative proofs for later roots in the request.
		for key in search.visited.iter().cloned() {
			if !search.incomplete.contains(&key) {
				context.authorization.entry(key).or_insert(false);
			}
		}
		search
			.unresolved
			.retain(|root| context.authorization.get(root).copied() != Some(true));

		if search.unresolved.is_empty() {
			SearchOutcome::Authorized
		} else if search.unresolved.iter().any(|root| {
			context.authorization.get(root).is_none() && search.incomplete.contains(root)
		}) {
			SearchOutcome::Exhausted
		} else {
			SearchOutcome::Denied
		}
	}

	fn add_ancestor_dependency(
		context: &mut AuthorizationContext<'_, '_>,
		search: &mut AncestorSearch,
		dependent: &(tg::Id, tg::authorization::Permission),
		dependency: (tg::Id, tg::authorization::Permission),
		depth: usize,
	) -> bool {
		// Record each proof edge once for the entire request-local graph.
		let edge_known = context
			.dependents
			.get(&dependency)
			.is_some_and(|dependents| dependents.contains(dependent));
		if !edge_known {
			if !search.budget.add_edge() {
				search.incomplete.insert(dependent.clone());
				return false;
			}
			context
				.dependents
				.entry(dependency.clone())
				.or_default()
				.insert(dependent.clone());
		}

		// Reuse a completed proof or an evaluation already queued by another root.
		match context.authorization.get(&dependency).copied() {
			Some(true) => {
				Self::propagate_tracked_authorization(
					context.authorization,
					context.dependents,
					&dependency,
					&mut search.unresolved,
				);
				return true;
			},
			Some(false) => return true,
			None => {},
		}
		if search.visited.contains(&dependency) {
			return true;
		}
		if depth > search.budget.config.max_depth {
			search.incomplete.insert(dependent.clone());
			return false;
		}
		if !search.budget.add_node(depth) {
			search.incomplete.insert(dependency);
			return false;
		}
		search.visited.insert(dependency.clone());
		search
			.queues
			.entry(depth)
			.or_default()
			.push_back(AncestorTask::Node {
				depth,
				key: dependency,
			});

		true
	}

	fn propagate_ancestor_authorization(
		authorization: &mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
		dependents: &HashMap<
			(tg::Id, tg::authorization::Permission),
			HashSet<(tg::Id, tg::authorization::Permission)>,
		>,
		key: &(tg::Id, tg::authorization::Permission),
	) {
		Self::propagate_authorization(authorization, dependents, key, None);
	}

	fn propagate_tracked_authorization(
		authorization: &mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
		dependents: &HashMap<
			(tg::Id, tg::authorization::Permission),
			HashSet<(tg::Id, tg::authorization::Permission)>,
		>,
		key: &(tg::Id, tg::authorization::Permission),
		unresolved: &mut HashSet<(tg::Id, tg::authorization::Permission)>,
	) {
		Self::propagate_authorization(authorization, dependents, key, Some(unresolved));
	}

	fn propagate_authorization(
		authorization: &mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
		dependents: &HashMap<
			(tg::Id, tg::authorization::Permission),
			HashSet<(tg::Id, tg::authorization::Permission)>,
		>,
		key: &(tg::Id, tg::authorization::Permission),
		mut unresolved: Option<&mut HashSet<(tg::Id, tg::authorization::Permission)>>,
	) {
		let mut stack = vec![key.clone()];
		let mut visited = HashSet::new();
		while let Some(key) = stack.pop() {
			if !visited.insert(key.clone()) {
				continue;
			}
			authorization.insert(key.clone(), true);
			if let Some(unresolved) = &mut unresolved {
				unresolved.remove(&key);
			}
			stack.extend(
				crate::authorize::permissions_implied_by(key.1)
					.into_iter()
					.map(|permission| (key.0.clone(), permission)),
			);
			if let Some(dependents) = dependents.get(&key) {
				stack.extend(dependents.iter().cloned());
			}
		}
	}

	fn advance_descendant_search_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		search: &mut DescendantSearch,
	) -> tg::Result<SearchOutcome> {
		if search.unresolved.is_empty() {
			return Ok(SearchOutcome::Authorized);
		}
		if let Some(outcome) = search.outcome.take() {
			return Ok(outcome);
		}
		let budget = &mut search.budget;
		let stack = &mut search.stack;
		let visited = &mut search.visited;
		let Some(task) = stack.pop() else {
			let outcome = if search.unresolved.is_empty() {
				SearchOutcome::Authorized
			} else if search.complete {
				SearchOutcome::Denied
			} else {
				SearchOutcome::Exhausted
			};

			return Ok(outcome);
		};
		match task {
			DescendantTask::Node { depth, key } => {
				if let Some(target_permissions) = search.target_permissions.get(&key.0) {
					for &target_permission in target_permissions {
						let target = (key.0.clone(), target_permission);
						if !key.1.implies(target_permission) || !search.unresolved.contains(&target)
						{
							continue;
						}
						Self::propagate_tracked_authorization(
							context.authorization,
							context.dependents,
							&target,
							&mut search.unresolved,
						);
					}
				}
				if search.unresolved.is_empty() {
					return Ok(SearchOutcome::Authorized);
				}
				let (resource, permission) = key;
				let implied = if matches!(permission, tg::authorization::Permission::Object(_)) {
					Vec::new()
				} else {
					crate::authorize::permissions_implied_by(permission)
				};
				for implied_permission in implied.into_iter().rev() {
					if implied_permission == permission {
						continue;
					}
					let key = (resource.clone(), implied_permission);
					let depth = depth + 1;
					if !budget.add_edge() {
						return Ok(SearchOutcome::Exhausted);
					}
					if visited.contains(&key) {
						continue;
					}
					if depth > budget.config.max_depth {
						return Ok(SearchOutcome::Exhausted);
					}
					if !budget.add_node(depth) {
						return Ok(SearchOutcome::Exhausted);
					}
					visited.insert(key.clone());
					stack.push(DescendantTask::Node { depth, key });
				}
				if crate::authorize::write_permission_for_resource(&resource).ok()
					== Some(permission)
					&& let Some(owner) = Self::principal_for_resource(&resource)?
				{
					stack.push(DescendantTask::OwnerSandboxes {
						after: None,
						depth,
						owner,
					});
				}

				match permission {
					tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					) => {
						let object = tg::object::Id::try_from(resource)?;
						stack.push(DescendantTask::ObjectChildren {
							after: None,
							depth,
							object,
						});
					},
					tg::authorization::Permission::Process(permission) => {
						let process = tg::process::Id::try_from(resource.clone())?;
						let parent = (
							resource,
							tg::authorization::Permission::Process(
								tg::authorization::permission::process::Permission::Parent,
							),
						);
						if permission != tg::authorization::permission::process::Permission::Parent
							&& !visited.contains(&parent)
						{
							search.complete = false;
						}
						let traverses_children = matches!(
							permission,
							tg::authorization::permission::process::Permission::Parent
								| tg::authorization::permission::process::Permission::Subtree
								| tg::authorization::permission::process::Permission::SubtreeCommand
								| tg::authorization::permission::process::Permission::SubtreeError
								| tg::authorization::permission::process::Permission::SubtreeLog
								| tg::authorization::permission::process::Permission::SubtreeOutput
						);
						if traverses_children {
							stack.push(DescendantTask::ProcessChildren {
								after: None,
								depth,
								permission,
								process: process.clone(),
							});
						}
						if permission == tg::authorization::permission::process::Permission::Parent
						{
							stack.push(DescendantTask::ProcessGrants {
								after: None,
								depth,
								process,
							});
						}
					},
					tg::authorization::Permission::Sandbox(permission) => {
						let sandbox = tg::sandbox::Id::try_from(resource)?;
						stack.push(DescendantTask::SandboxProcesses {
							after: None,
							depth,
							permission,
							sandbox,
						});
					},
					tg::authorization::Permission::Group(_)
					| tg::authorization::Permission::Organization(_)
					| tg::authorization::Permission::Tag(_)
					| tg::authorization::Permission::User(_) => search.complete = false,
					tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Node,
					) => {},
				}
			},
			DescendantTask::ObjectChildren {
				after,
				depth,
				object,
			} => {
				let object_bytes = object.to_bytes();
				let prefix = Self::pack(
					context.subspace,
					&(
						crate::lmdb::Kind::ObjectChild.to_i32().unwrap(),
						object_bytes.as_ref(),
					),
				);
				let (keys, after) = Self::get_key_page_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&prefix,
					after.as_deref(),
					budget.config.page_size,
				)?;
				if let Some(after) = after {
					stack.push(DescendantTask::ObjectChildren {
						after: Some(after),
						depth,
						object,
					});
				}
				for key in keys.into_iter().rev() {
					let crate::lmdb::Key::Object(crate::lmdb::object::Key::ObjectChild {
						child,
						..
					}) = key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					let resource = tg::Id::from(child.clone());
					let subtree = tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					);
					let mut permissions = vec![subtree];
					if let Some(target_permissions) = search.target_permissions.get(&resource) {
						for &permission in target_permissions {
							let target = (resource.clone(), permission);
							if permission != subtree && search.unresolved.contains(&target) {
								permissions.push(permission);
							}
						}
					}
					for permission in permissions {
						let key = (resource.clone(), permission);
						let depth = depth + 1;
						if !budget.add_edge() {
							return Ok(SearchOutcome::Exhausted);
						}
						if visited.contains(&key) {
							continue;
						}
						if depth > budget.config.max_depth {
							return Ok(SearchOutcome::Exhausted);
						}
						if !budget.add_node(depth) {
							return Ok(SearchOutcome::Exhausted);
						}
						visited.insert(key.clone());
						stack.push(DescendantTask::Node { depth, key });
					}
				}
			},
			DescendantTask::OwnerSandboxes {
				after,
				depth,
				owner,
			} => {
				let prefix = Self::pack(
					context.subspace,
					&(
						crate::lmdb::Kind::OwnerSandbox.to_i32().unwrap(),
						owner.to_string(),
					),
				);
				let (keys, after) = Self::get_key_page_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&prefix,
					after.as_deref(),
					budget.config.page_size,
				)?;
				if let Some(after) = after {
					stack.push(DescendantTask::OwnerSandboxes {
						after: Some(after),
						depth,
						owner,
					});
				}
				for key in keys.into_iter().rev() {
					let crate::lmdb::Key::Sandbox(crate::lmdb::sandbox::Key::OwnerSandbox {
						sandbox,
						..
					}) = key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					for permission in [
						tg::authorization::permission::sandbox::Permission::Write,
						tg::authorization::permission::sandbox::Permission::Read,
					] {
						let key = (
							tg::Id::from(sandbox.clone()),
							tg::authorization::Permission::Sandbox(permission),
						);
						let depth = depth + 1;
						if !budget.add_edge() {
							return Ok(SearchOutcome::Exhausted);
						}
						if visited.contains(&key) {
							continue;
						}
						if depth > budget.config.max_depth {
							return Ok(SearchOutcome::Exhausted);
						}
						if !budget.add_node(depth) {
							return Ok(SearchOutcome::Exhausted);
						}
						visited.insert(key.clone());
						stack.push(DescendantTask::Node { depth, key });
					}
				}
			},
			DescendantTask::ProcessChildren {
				after,
				depth,
				permission,
				process,
			} => {
				let process_bytes = process.to_bytes();
				let prefix = Self::pack(
					context.subspace,
					&(
						crate::lmdb::Kind::ProcessChild.to_i32().unwrap(),
						process_bytes.as_ref(),
					),
				);
				let (keys, after) = Self::get_key_page_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&prefix,
					after.as_deref(),
					budget.config.page_size,
				)?;
				if let Some(after) = after {
					stack.push(DescendantTask::ProcessChildren {
						after: Some(after),
						depth,
						permission,
						process,
					});
				}
				for key in keys.into_iter().rev() {
					let crate::lmdb::Key::Process(crate::lmdb::process::Key::ProcessChild {
						child,
						..
					}) = key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					let permissions = match permission {
						tg::authorization::permission::process::Permission::Parent => {
							vec![permission]
						},
						permission => {
							vec![permission, Self::process_node_permission(permission)]
						},
					};
					for permission in permissions {
						let key = (
							tg::Id::from(child.clone()),
							tg::authorization::Permission::Process(permission),
						);
						let depth = depth + 1;
						if !budget.add_edge() {
							return Ok(SearchOutcome::Exhausted);
						}
						if visited.contains(&key) {
							continue;
						}
						if depth > budget.config.max_depth {
							return Ok(SearchOutcome::Exhausted);
						}
						if !budget.add_node(depth) {
							return Ok(SearchOutcome::Exhausted);
						}
						visited.insert(key.clone());
						stack.push(DescendantTask::Node { depth, key });
					}
				}
			},
			DescendantTask::ProcessGrants {
				after,
				depth,
				process,
			} => {
				let subject = tg::authorization::Subject::Process(process.clone());
				let prefix = Self::pack(
					context.subspace,
					&(
						crate::lmdb::Kind::SubjectGrant.to_i32().unwrap(),
						subject.to_string(),
					),
				);
				let (entries, after) = Self::get_entry_page_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&prefix,
					after.as_deref(),
					budget.config.page_size,
				)?;
				if let Some(after) = after {
					stack.push(DescendantTask::ProcessGrants {
						after: Some(after),
						depth,
						process,
					});
				}
				for (key, value) in entries.into_iter().rev() {
					let crate::lmdb::Key::Grant(crate::lmdb::grant::Key::SubjectGrant {
						creator,
						permission,
						resource,
						..
					}) = key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					let value = crate::lmdb::grant::GrantValue::deserialize(&value)?;
					if !crate::lmdb::grant::is_process_implicit(
						creator.as_ref(),
						value.implicit,
						&subject,
					) {
						continue;
					}
					for permission in crate::authorize::permissions_implied_by(permission) {
						let key = (resource.clone(), permission);
						let depth = depth + 1;
						if !budget.add_edge() {
							return Ok(SearchOutcome::Exhausted);
						}
						if visited.contains(&key) {
							continue;
						}
						if depth > budget.config.max_depth {
							return Ok(SearchOutcome::Exhausted);
						}
						if !budget.add_node(depth) {
							return Ok(SearchOutcome::Exhausted);
						}
						visited.insert(key.clone());
						stack.push(DescendantTask::Node { depth, key });
					}
				}
			},
			DescendantTask::SandboxProcesses {
				after,
				depth,
				permission,
				sandbox,
			} => {
				let sandbox_bytes = sandbox.to_bytes();
				let prefix = Self::pack(
					context.subspace,
					&(
						crate::lmdb::Kind::SandboxProcess.to_i32().unwrap(),
						sandbox_bytes.as_ref(),
					),
				);
				let (keys, after) = Self::get_key_page_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&prefix,
					after.as_deref(),
					budget.config.page_size,
				)?;
				if let Some(after) = after {
					stack.push(DescendantTask::SandboxProcesses {
						after: Some(after),
						depth,
						permission,
						sandbox,
					});
				}
				for key in keys.into_iter().rev() {
					let crate::lmdb::Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcess {
						process,
						..
					}) = key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					let permissions = match permission {
						tg::authorization::permission::sandbox::Permission::Read => vec![
							tg::authorization::permission::process::Permission::Node,
							tg::authorization::permission::process::Permission::NodeCommand,
							tg::authorization::permission::process::Permission::NodeError,
							tg::authorization::permission::process::Permission::NodeLog,
							tg::authorization::permission::process::Permission::NodeOutput,
							tg::authorization::permission::process::Permission::Subtree,
							tg::authorization::permission::process::Permission::SubtreeCommand,
							tg::authorization::permission::process::Permission::SubtreeError,
							tg::authorization::permission::process::Permission::SubtreeLog,
							tg::authorization::permission::process::Permission::SubtreeOutput,
						],
						tg::authorization::permission::sandbox::Permission::Write => {
							vec![tg::authorization::permission::process::Permission::Parent]
						},
					};
					for permission in permissions {
						let key = (
							tg::Id::from(process.clone()),
							tg::authorization::Permission::Process(permission),
						);
						let depth = depth + 1;
						if !budget.add_edge() {
							return Ok(SearchOutcome::Exhausted);
						}
						if visited.contains(&key) {
							continue;
						}
						if depth > budget.config.max_depth {
							return Ok(SearchOutcome::Exhausted);
						}
						if !budget.add_node(depth) {
							return Ok(SearchOutcome::Exhausted);
						}
						visited.insert(key.clone());
						stack.push(DescendantTask::Node { depth, key });
					}
				}
			},
			DescendantTask::SubjectGrants { after, subject } => {
				let prefix = Self::pack(
					context.subspace,
					&(
						crate::lmdb::Kind::SubjectGrant.to_i32().unwrap(),
						subject.to_string(),
					),
				);
				let (entries, after) = Self::get_entry_page_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&prefix,
					after.as_deref(),
					budget.config.page_size,
				)?;
				if let Some(after) = after {
					stack.push(DescendantTask::SubjectGrants {
						after: Some(after),
						subject,
					});
				}
				for (key, _) in entries.into_iter().rev() {
					let crate::lmdb::Key::Grant(crate::lmdb::grant::Key::SubjectGrant {
						permission,
						resource,
						..
					}) = key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					if !budget.add_edge() {
						return Ok(SearchOutcome::Exhausted);
					}
					let key = (resource, permission);
					if !visited.insert(key.clone()) {
						continue;
					}
					if !budget.add_node(0) {
						return Ok(SearchOutcome::Exhausted);
					}
					stack.push(DescendantTask::Node { depth: 0, key });
				}
			},
		}

		Ok(SearchOutcome::Pending)
	}

	#[must_use]
	fn process_node_permission(
		permission: tg::authorization::permission::process::Permission,
	) -> tg::authorization::permission::process::Permission {
		match permission {
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
		}
	}

	fn principal_for_resource(resource: &tg::Id) -> tg::Result<Option<tg::Principal>> {
		let principal = match resource.kind() {
			tg::id::Kind::Group => Some(tg::Principal::Group(resource.clone().try_into()?)),
			tg::id::Kind::Organization => {
				Some(tg::Principal::Organization(resource.clone().try_into()?))
			},
			tg::id::Kind::Process => Some(tg::Principal::Process(resource.clone().try_into()?)),
			tg::id::Kind::Sandbox => Some(tg::Principal::Sandbox(resource.clone().try_into()?)),
			tg::id::Kind::User => Some(tg::Principal::User(resource.clone().try_into()?)),
			_ => None,
		};

		Ok(principal)
	}

	fn get_key_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		prefix: &[u8],
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<(Vec<crate::lmdb::Key>, Option<Vec<u8>>)> {
		let (entries, after) =
			Self::get_entry_page_with_transaction(db, subspace, transaction, prefix, after, limit)?;
		let keys = entries.into_iter().map(|(key, _)| key).collect();

		Ok((keys, after))
	}

	fn get_entry_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		prefix: &[u8],
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<(Vec<(crate::lmdb::Key, Vec<u8>)>, Option<Vec<u8>>)> {
		let start = if let Some(after) = after {
			let mut start = after.to_vec();
			start.push(0);
			start
		} else {
			prefix.to_vec()
		};
		let range = (
			std::ops::Bound::Included(start.as_slice()),
			std::ops::Bound::Unbounded,
		);
		let iter = db
			.range(transaction, &range)
			.map_err(|error| tg::error!(!error, "failed to page authorization relationships"))?;
		let mut entries = Vec::new();
		let mut last = None;
		for entry in iter.take(limit) {
			let (key, value) = entry.map_err(|error| {
				tg::error!(!error, "failed to read an authorization relationship")
			})?;
			if !key.starts_with(prefix) {
				break;
			}
			last = Some(key.to_vec());
			let key = Self::unpack(subspace, key)?;
			entries.push((key, value.to_vec()));
		}
		let after = (entries.len() == limit).then_some(last).flatten();

		Ok((entries, after))
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

		let direct_permissions =
			if let Some(permissions) = context.cache.direct_permissions.get(resource).cloned() {
				permissions
			} else {
				let grants = Self::get_cached_resource_grants_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					resource,
					context.cache,
				)?;
				let mut permissions = HashSet::new();
				for (granted_subject, granted_permission, _) in grants {
					if Self::subject_contains_requester_with_transaction(
						context.db,
						context.subspace,
						context.transaction,
						&granted_subject,
						context.requester,
						context.cache,
					)? {
						permissions.insert(granted_permission);
					}
				}
				context
					.cache
					.direct_permissions
					.insert(resource.clone(), permissions.clone());
				permissions
			};
		for permission in direct_permissions {
			let key = (resource.clone(), permission);
			Self::propagate_ancestor_authorization(context.authorization, context.dependents, &key);
		}
		let key = (resource.clone(), permission);
		let authorized = context.authorization.get(&key).copied().unwrap_or(false);

		Ok(authorized)
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
		let mut dependents = HashMap::<_, HashSet<_>>::new();
		let mut visited = HashSet::from([root.clone()]);
		let mut queue = VecDeque::from([(root, 0)]);
		while let Some((object, depth)) = queue.pop_front() {
			if budget.remaining == 0 {
				return Ok(None);
			}
			budget.remaining -= 1;

			let resource = tg::Id::from(object.clone());
			let key = (resource.clone(), subtree);
			match context.derived_authorization.get(&key).copied() {
				Some(true) => continue,
				Some(false) => {
					Self::propagate_derived_denial(
						context.derived_authorization,
						&dependents,
						&key,
					);
					return Ok(Some(false));
				},
				None => {},
			}
			if context.derived_exhausted_roots.contains(&key) {
				return Ok(None);
			}
			if Self::authorize_permission_ordinary_with_transaction(context, &resource, subtree)? {
				context.derived_authorization.insert(key, true);
				continue;
			}
			if !Self::authorize_permission_ordinary_with_transaction(context, &resource, node)? {
				Self::propagate_derived_denial(context.derived_authorization, &dependents, &key);
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
				let child_key = (tg::Id::from(child.clone()), subtree);
				dependents.entry(child_key).or_default().insert(key.clone());
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
		context
			.derived_authorization
			.extend(visited.into_iter().map(|object| {
				let key = (tg::Id::from(object), subtree);
				(key, true)
			}));

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
		let mut dependents = HashMap::<_, HashSet<_>>::new();
		let mut visited = HashSet::from([root.clone()]);
		let mut queue = VecDeque::from([(root, 0)]);
		while let Some((process, depth)) = queue.pop_front() {
			if budget.remaining == 0 {
				return Ok(None);
			}
			budget.remaining -= 1;

			let resource = tg::Id::from(process.clone());
			let key = (resource.clone(), subtree);
			match context.derived_authorization.get(&key).copied() {
				Some(true) => continue,
				Some(false) => {
					Self::propagate_derived_denial(
						context.derived_authorization,
						&dependents,
						&key,
					);
					return Ok(Some(false));
				},
				None => {},
			}
			if context.derived_exhausted_roots.contains(&key) {
				return Ok(None);
			}
			if Self::authorize_permission_ordinary_with_transaction(context, &resource, subtree)? {
				context.derived_authorization.insert(key, true);
				continue;
			}
			if !Self::authorize_process_node_with_transaction(context, &resource, node_permission)?
			{
				Self::propagate_derived_denial(context.derived_authorization, &dependents, &key);
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
				let child_key = (tg::Id::from(child.clone()), subtree);
				dependents.entry(child_key).or_default().insert(key.clone());
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
		context
			.derived_authorization
			.extend(visited.into_iter().map(|process| {
				let key = (tg::Id::from(process), subtree);
				(key, true)
			}));

		Ok(Some(true))
	}

	fn propagate_derived_denial(
		authorization: &mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
		dependents: &HashMap<
			(tg::Id, tg::authorization::Permission),
			HashSet<(tg::Id, tg::authorization::Permission)>,
		>,
		key: &(tg::Id, tg::authorization::Permission),
	) {
		let mut stack = vec![key.clone()];
		let mut visited = HashSet::new();
		while let Some(key) = stack.pop() {
			if !visited.insert(key.clone()) {
				continue;
			}
			authorization.insert(key.clone(), false);
			if let Some(dependents) = dependents.get(&key) {
				stack.extend(dependents.iter().cloned());
			}
		}
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
		if context.authorize.subtree.max_objects == 0 {
			return Err(crate::authorize::search_exhausted_error(
				"the subtree authorization search exhausted",
			));
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
				max_depth: context.authorize.subtree.max_depth,
				remaining: context.authorize.subtree.max_objects,
			};
			let authorized = Self::authorize_with_object_subtree_search_with_transaction(
				context,
				&resource,
				&mut budget,
			)?
			.ok_or_else(|| {
				crate::authorize::search_exhausted_error(
					"the subtree authorization search exhausted",
				)
			})?;
			if !authorized {
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
		cache: &mut Cache,
	) -> tg::Result<Vec<(tg::Id, tg::authorization::Permission)>> {
		let key = (resource.clone(), permission);
		if let Some(dependencies) = cache.authorization_dependencies.get(&key).cloned() {
			return Ok(dependencies);
		}
		let mut dependencies = Vec::new();

		// Add the process implicit grant relationships.
		let grants = Self::get_cached_resource_grants_with_transaction(
			db,
			subspace,
			transaction,
			resource,
			cache,
		)?;
		let mut implicit_processes = HashSet::new();
		for (subject, granted_permission, process_implicit) in &grants {
			if !process_implicit || !granted_permission.implies(permission) {
				continue;
			}
			let tg::authorization::Subject::Process(process) = subject else {
				continue;
			};
			if !implicit_processes.insert(process.clone()) {
				continue;
			}
			let permission = tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Parent,
			);
			dependencies.push((process.clone().into(), permission));
		}

		match permission {
			tg::authorization::Permission::Object(object_permission) => {
				// Get the relationships.
				let object = tg::object::Id::try_from(resource.clone())?;
				let object_processes = Self::get_cached_object_processes_with_transaction(
					db,
					subspace,
					transaction,
					&object,
					cache,
				)?;

				// Add the process object relationships bounded by implicit grants.
				for (process, kind) in object_processes {
					if implicit_processes.contains(&process) {
						let permission = tg::authorization::Permission::Process(
							crate::authorize::process_object_permission(kind, object_permission),
						);
						dependencies.push((process.into(), permission));
					}
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

				// Add the sandbox relationship.
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
		cache
			.authorization_dependencies
			.insert(key, dependencies.clone());

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
	) -> tg::Result<
		Vec<(
			tg::authorization::Subject,
			tg::authorization::Permission,
			bool,
		)>,
	> {
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
