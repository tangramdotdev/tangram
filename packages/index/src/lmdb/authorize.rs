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
	object_processes: HashMap<tg::object::Id, Vec<(tg::process::Id, crate::process::object::Kind)>>,
	organization_members: HashMap<tg::organization::Id, Vec<tg::Id>>,
	subject_contains_requester: HashMap<tg::authorization::Subject, bool>,
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
	sandbox_owners: HashMap<tg::sandbox::Id, Option<tg::Principal>>,
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
		depth: usize,
		object: tg::object::Id,
	},
	ProcessParents {
		after: Option<Vec<u8>>,
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
	outcome: Option<SearchOutcome>,
	stack: Vec<AncestorTask>,
	visited: HashSet<(tg::Id, tg::authorization::Permission)>,
}

struct DescendantSearch {
	budget: SearchBudget,
	complete: bool,
	outcome: Option<SearchOutcome>,
	stack: Vec<DescendantTask>,
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
	db: &'a Db,
	subspace: &'a fdbt::Subspace,
	transaction: &'a lmdb::RoTxn<'txn>,
	authorize: crate::lmdb::AuthorizeConfig,
	requester: &'a Requester<'a>,
	token: Option<(tg::authorization::Body, tg::Id)>,
	authorization: &'a mut HashMap<(tg::Id, tg::authorization::Permission), bool>,
	cache: &'a mut Cache,
}

impl AncestorSearch {
	#[must_use]
	fn new(
		config: crate::authorize::SearchConfig,
		root: &(tg::Id, tg::authorization::Permission),
	) -> Self {
		let mut budget = SearchBudget::new(config);
		let outcome = (!budget.add_node(0)).then_some(SearchOutcome::Exhausted);
		let stack = vec![AncestorTask::Node {
			depth: 0,
			key: root.clone(),
		}];
		let visited = HashSet::from([root.clone()]);

		Self {
			budget,
			outcome,
			stack,
			visited,
		}
	}
}

impl DescendantSearch {
	#[must_use]
	fn new(config: crate::authorize::SearchConfig, requester: &Requester<'_>) -> Self {
		let mut budget = SearchBudget::new(config);
		let complete = !matches!(
			requester.principal,
			tg::Principal::Group(_) | tg::Principal::User(_)
		);
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
					max_depth: context.authorize.subtree.max_depth,
					remaining: context.authorize.subtree.max_objects,
				};
				let authorized = Self::authorize_with_object_subtree_search_with_transaction(
					context,
					resource,
					&mut budget,
				)?
				.ok_or_else(|| tg::error!("the subtree authorization search exhausted"))?;

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
				.ok_or_else(|| tg::error!("the subtree authorization search exhausted"))?;

				Ok(authorized)
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

		let mut ancestor = AncestorSearch::new(context.authorize.ancestor, &root);
		let mut ancestor_exhausted = false;
		let mut descendant = DescendantSearch::new(context.authorize.descendant, context.requester);
		let mut descendant_exhausted = false;
		loop {
			if !ancestor_exhausted {
				let outcome =
					Self::advance_ancestor_search_with_transaction(context, &mut ancestor)?;
				match outcome {
					SearchOutcome::Authorized => {
						context.authorization.insert(root, true);
						return Ok(true);
					},
					SearchOutcome::Denied => {
						context.authorization.insert(root, false);
						return Ok(false);
					},
					SearchOutcome::Exhausted => ancestor_exhausted = true,
					SearchOutcome::Pending => {},
				}
			}
			if !descendant_exhausted {
				let outcome = Self::advance_descendant_search_with_transaction(
					context,
					&mut descendant,
					&root,
				)?;
				match outcome {
					SearchOutcome::Authorized => {
						context.authorization.insert(root, true);
						return Ok(true);
					},
					SearchOutcome::Denied => {
						context.authorization.insert(root, false);
						return Ok(false);
					},
					SearchOutcome::Exhausted => descendant_exhausted = true,
					SearchOutcome::Pending => {},
				}
			}
			if ancestor_exhausted && descendant_exhausted {
				return Err(tg::error!(
					"the ancestor and descendant authorization searches exhausted"
				));
			}
		}
	}

	fn advance_ancestor_search_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		search: &mut AncestorSearch,
	) -> tg::Result<SearchOutcome> {
		if let Some(outcome) = search.outcome.take() {
			return Ok(outcome);
		}
		let budget = &mut search.budget;
		let stack = &mut search.stack;
		let visited = &mut search.visited;
		let Some(task) = stack.pop() else {
			for key in visited.iter().cloned() {
				context.authorization.entry(key).or_insert(false);
			}

			return Ok(SearchOutcome::Denied);
		};
		match task {
			AncestorTask::Node { depth, key } => {
				if let Some(authorized) = context.authorization.get(&key) {
					if *authorized {
						return Ok(SearchOutcome::Authorized);
					}
					return Ok(SearchOutcome::Pending);
				}
				let (resource, permission) = key.clone();
				if Self::is_authorized_by_token(context, &resource, permission)
					|| Self::is_directly_authorized_with_transaction(
						context, &resource, permission,
					)? {
					context.authorization.insert(key, true);
					return Ok(SearchOutcome::Authorized);
				}

				let dependencies = Self::get_authorization_dependencies_with_transaction(
					context.db,
					context.subspace,
					context.transaction,
					&resource,
					permission,
					context.cache,
				)?;
				for key in dependencies.into_iter().rev() {
					let depth = depth + 1;
					if !budget.add_edge() {
						return Ok(SearchOutcome::Exhausted);
					}
					if visited.contains(&key)
						|| context.authorization.get(&key).copied() == Some(false)
					{
						continue;
					}
					if depth > budget.config.max_depth {
						return Ok(SearchOutcome::Exhausted);
					}
					if context.authorization.get(&key).copied() == Some(true) {
						return Ok(SearchOutcome::Authorized);
					}
					if !budget.add_node(depth) {
						return Ok(SearchOutcome::Exhausted);
					}
					visited.insert(key.clone());
					stack.push(AncestorTask::Node { depth, key });
				}

				match permission {
					tg::authorization::Permission::Object(_) => {
						let object = tg::object::Id::try_from(resource)?;
						stack.push(AncestorTask::ObjectParents {
							after: None,
							depth,
							object,
						});
					},
					tg::authorization::Permission::Process(permission) => {
						let process = tg::process::Id::try_from(resource)?;
						stack.push(AncestorTask::ProcessParents {
							after: None,
							depth,
							permission,
							process,
						});
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
				depth,
				object,
			} => {
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
					budget.config.page_size,
				)?;
				if let Some(after) = after {
					stack.push(AncestorTask::ObjectParents {
						after: Some(after),
						depth,
						object,
					});
				}
				for key in keys.into_iter().rev() {
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
					let depth = depth + 1;
					if !budget.add_edge() {
						return Ok(SearchOutcome::Exhausted);
					}
					if visited.contains(&key)
						|| context.authorization.get(&key).copied() == Some(false)
					{
						continue;
					}
					if depth > budget.config.max_depth {
						return Ok(SearchOutcome::Exhausted);
					}
					if context.authorization.get(&key).copied() == Some(true) {
						return Ok(SearchOutcome::Authorized);
					}
					if !budget.add_node(depth) {
						return Ok(SearchOutcome::Exhausted);
					}
					visited.insert(key.clone());
					stack.push(AncestorTask::Node { depth, key });
				}
			},
			AncestorTask::ProcessParents {
				after,
				depth,
				permission,
				process,
			} => {
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
					budget.config.page_size,
				)?;
				if let Some(after) = after {
					stack.push(AncestorTask::ProcessParents {
						after: Some(after),
						depth,
						permission,
						process,
					});
				}
				for key in keys.into_iter().rev() {
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
					let depth = depth + 1;
					if !budget.add_edge() {
						return Ok(SearchOutcome::Exhausted);
					}
					if visited.contains(&key)
						|| context.authorization.get(&key).copied() == Some(false)
					{
						continue;
					}
					if depth > budget.config.max_depth {
						return Ok(SearchOutcome::Exhausted);
					}
					if context.authorization.get(&key).copied() == Some(true) {
						return Ok(SearchOutcome::Authorized);
					}
					if !budget.add_node(depth) {
						return Ok(SearchOutcome::Exhausted);
					}
					visited.insert(key.clone());
					stack.push(AncestorTask::Node { depth, key });
				}
			},
		}

		Ok(SearchOutcome::Pending)
	}

	fn advance_descendant_search_with_transaction(
		context: &mut AuthorizationContext<'_, '_>,
		search: &mut DescendantSearch,
		target: &(tg::Id, tg::authorization::Permission),
	) -> tg::Result<SearchOutcome> {
		if let Some(outcome) = search.outcome.take() {
			return Ok(outcome);
		}
		let budget = &mut search.budget;
		let stack = &mut search.stack;
		let visited = &mut search.visited;
		let Some(task) = stack.pop() else {
			let outcome = if search.complete {
				SearchOutcome::Denied
			} else {
				SearchOutcome::Exhausted
			};

			return Ok(outcome);
		};
		match task {
			DescendantTask::Node { depth, key } => {
				if key.0 == target.0 && key.1.implies(target.1) {
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
					let permissions = if tg::Id::from(child.clone()) == target.0 {
						vec![target.1]
					} else {
						vec![tg::authorization::Permission::Object(
							tg::authorization::permission::object::Permission::Subtree,
						)]
					};
					for permission in permissions {
						let key = (tg::Id::from(child.clone()), permission);
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

		let grants = Self::get_cached_resource_grants_with_transaction(
			context.db,
			context.subspace,
			context.transaction,
			resource,
			context.cache,
		)?;
		for (granted_subject, granted_permission, _) in grants {
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
				max_depth: context.authorize.subtree.max_depth,
				remaining: context.authorize.subtree.max_objects,
			};
			let authorized = Self::authorize_with_object_subtree_search_with_transaction(
				context,
				&resource,
				&mut budget,
			)?
			.ok_or_else(|| tg::error!("the subtree authorization search exhausted"))?;
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
		let mut dependencies = Vec::new();

		// Add the process implicit grant relationships.
		let grants = Self::get_cached_resource_grants_with_transaction(
			db,
			subspace,
			transaction,
			resource,
			cache,
		)?;
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
