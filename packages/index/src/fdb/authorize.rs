use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::{
		collections::{HashMap, HashSet, VecDeque},
		ops::ControlFlow,
		pin::pin,
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
	object_processes: HashMap<tg::object::Id, Vec<(tg::process::Id, crate::process::object::Kind)>>,
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
	membership_cache: std::sync::Mutex<RequesterMembershipCache>,
}

#[derive(Default)]
struct RequesterMembershipCache {
	group_members: HashMap<tg::group::Id, std::sync::Arc<[tg::Id]>>,
	organization_members: HashMap<tg::organization::Id, std::sync::Arc<[tg::Id]>>,
	subject_contains_requester: HashMap<tg::authorization::Subject, bool>,
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
}

struct RootSearchOutput {
	authorization: HashMap<(tg::Id, tg::authorization::Permission), bool>,
	cache: Cache,
	outcome: SearchOutcome,
	root: (tg::Id, tg::authorization::Permission),
}

struct SearchAuthorization<'a> {
	base: &'a HashMap<(tg::Id, tg::authorization::Permission), bool>,
	values: HashMap<(tg::Id, tg::authorization::Permission), bool>,
}

struct SearchBudget {
	config: crate::authorize::SearchConfig,
	edges: usize,
	nodes: usize,
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

impl<'a> SearchAuthorization<'a> {
	#[must_use]
	fn new(base: &'a HashMap<(tg::Id, tg::authorization::Permission), bool>) -> Self {
		Self {
			base,
			values: HashMap::new(),
		}
	}

	fn insert(&mut self, key: (tg::Id, tg::authorization::Permission), value: bool) {
		self.values.insert(key, value);
	}

	#[must_use]
	fn into_values(self) -> HashMap<(tg::Id, tg::authorization::Permission), bool> {
		self.values
	}

	#[must_use]
	fn value(&self, key: &(tg::Id, tg::authorization::Permission)) -> Option<bool> {
		self.values.get(key).or_else(|| self.base.get(key)).copied()
	}
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

impl Cache {
	fn merge(&mut self, other: Self) {
		self.resource_parents.extend(other.resource_parents);
		self.target_tags.extend(other.target_tags);
		self.object_children.extend(other.object_children);
		self.object_processes.extend(other.object_processes);
		self.process_children.extend(other.process_children);
		self.process_objects.extend(other.process_objects);
		self.process_sandboxes.extend(other.process_sandboxes);
		self.resource_grants.extend(other.resource_grants);
		self.sandbox_owners.extend(other.sandbox_owners);
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
				) => {
					crate::fdb::propagate!(
						Self::authorize_with_object_subtree_search_with_transaction(
							context,
							resource,
							authorization,
							cache,
						)
						.await
					)
					.ok_or_else(|| tg::error!("the subtree authorization search exhausted"))?
				},
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
				) => {
					crate::fdb::propagate!(
						Self::authorize_with_process_subtree_search_with_transaction(
							context,
							resource,
							permission,
							authorization,
							cache,
						)
						.await
					)
					.ok_or_else(|| tg::error!("the subtree authorization search exhausted"))?
				},
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
		// Collect the unique unresolved roots.
		let mut seen = HashSet::new();
		let roots = roots
			.iter()
			.filter(|root| authorization.get(*root).is_none() && seen.insert((*root).clone()))
			.cloned()
			.collect::<Vec<_>>();

		// Search the roots concurrently.
		let outputs = {
			let base = &*authorization;
			let results = futures::future::try_join_all(roots.into_iter().map(|root| async move {
				let mut authorization = SearchAuthorization::new(base);
				let mut cache = Cache::default();
				let outcome = crate::fdb::propagate!(
					Self::authorize_permission_ordinary_with_transaction(
						context,
						&root,
						&mut authorization,
						&mut cache,
					)
					.await
				);
				let output = RootSearchOutput {
					authorization: authorization.into_values(),
					cache,
					outcome,
					root,
				};

				Ok::<_, tg::Error>(ControlFlow::Break(output))
			}))
			.await?;
			let mut outputs = Vec::with_capacity(results.len());
			for result in results {
				let output = match result {
					ControlFlow::Break(output) => output,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				outputs.push(output);
			}

			outputs
		};

		// Merge the search results.
		for output in outputs {
			let authorized = match output.outcome {
				SearchOutcome::Authorized => true,
				SearchOutcome::Denied => false,
				SearchOutcome::Exhausted => {
					return Err(tg::error!(
						"the ancestor and descendant authorization searches exhausted"
					));
				},
			};
			for (key, authorized) in output.authorization {
				authorization
					.entry(key)
					.and_modify(|value| *value |= authorized)
					.or_insert(authorized);
			}
			authorization
				.entry(output.root)
				.and_modify(|value| *value |= authorized)
				.or_insert(authorized);
			cache.merge(output.cache);
		}

		Ok(ControlFlow::Break(()))
	}

	async fn authorize_permission_ordinary_with_transaction(
		context: AuthorizationContext<'_>,
		root: &(tg::Id, tg::authorization::Permission),
		authorization: &mut SearchAuthorization<'_>,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<SearchOutcome, fdb::FdbError>> {
		// Prefer the typically smaller ancestor search while both searches make progress.
		let ancestor = Self::authorize_permission_ancestor_with_transaction(
			context,
			root,
			authorization,
			cache,
		);
		let descendant = Self::authorize_permission_descendant_with_transaction(context, root);
		let mut ancestor = pin!(ancestor);
		let mut descendant = pin!(descendant);
		let (ancestor_finished, result) = tokio::select! {
			biased;
			result = &mut ancestor => (true, result),
			result = &mut descendant => (false, result),
		};
		let outcome = crate::fdb::propagate!(result);
		let outcome = if outcome != SearchOutcome::Exhausted {
			outcome
		} else if ancestor_finished {
			crate::fdb::propagate!(descendant.await)
		} else {
			crate::fdb::propagate!(ancestor.await)
		};

		Ok(ControlFlow::Break(outcome))
	}

	async fn authorize_permission_ancestor_with_transaction(
		context: AuthorizationContext<'_>,
		root: &(tg::Id, tg::authorization::Permission),
		authorization: &mut SearchAuthorization<'_>,
		cache: &mut Cache,
	) -> tg::Result<ControlFlow<SearchOutcome, fdb::FdbError>> {
		let mut budget = SearchBudget::new(context.config.ancestor);
		if !budget.add_node(0) {
			return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
		}
		let mut queue = VecDeque::from([AncestorTask::Node {
			depth: 0,
			key: root.clone(),
		}]);
		let mut visited = HashSet::from([root.clone()]);
		while let Some(task) = queue.pop_front() {
			match task {
				AncestorTask::Node { depth, key } => {
					if let Some(authorized) = authorization.value(&key) {
						if authorized {
							return Ok(ControlFlow::Break(SearchOutcome::Authorized));
						}
						continue;
					}
					let (resource, permission) = key.clone();
					if Self::is_authorized_by_token(context, &resource, permission)
						|| crate::fdb::propagate!(
							Self::is_directly_authorized_with_transaction(
								context, &resource, permission, cache,
							)
							.await
						) {
						authorization.insert(key, true);
						return Ok(ControlFlow::Break(SearchOutcome::Authorized));
					}

					let dependencies = crate::fdb::propagate!(
						Self::get_authorization_dependencies_with_transaction(
							context.txn,
							context.subspace,
							&resource,
							permission,
							cache,
						)
						.await
					);
					for key in dependencies {
						let depth = depth + 1;
						if !budget.add_edge() {
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						if visited.contains(&key) || authorization.value(&key) == Some(false) {
							continue;
						}
						if depth > budget.config.max_depth {
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						if authorization.value(&key) == Some(true) {
							return Ok(ControlFlow::Break(SearchOutcome::Authorized));
						}
						if !budget.add_node(depth) {
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						visited.insert(key.clone());
						queue.push_back(AncestorTask::Node { depth, key });
					}

					match permission {
						tg::authorization::Permission::Object(_) => {
							let object = tg::object::Id::try_from(resource)?;
							queue.push_back(AncestorTask::ObjectParents {
								after: None,
								depth,
								object,
							});
						},
						tg::authorization::Permission::Process(permission) => {
							let process = tg::process::Id::try_from(resource)?;
							queue.push_back(AncestorTask::ProcessParents {
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
							crate::fdb::Kind::ChildObject.to_i32().unwrap(),
							object_bytes.as_ref(),
						),
					);
					let (keys, after) = crate::fdb::propagate!(
						Self::get_key_page_with_transaction(
							context.txn,
							context.subspace,
							&prefix,
							after.as_deref(),
							budget.config.page_size,
						)
						.await
					);
					if let Some(after) = after {
						queue.push_back(AncestorTask::ObjectParents {
							after: Some(after),
							depth,
							object,
						});
					}
					for key in keys {
						let crate::fdb::Key::Object(crate::fdb::object::Key::ChildObject {
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
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						if visited.contains(&key) || authorization.value(&key) == Some(false) {
							continue;
						}
						if depth > budget.config.max_depth {
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						if authorization.value(&key) == Some(true) {
							return Ok(ControlFlow::Break(SearchOutcome::Authorized));
						}
						if !budget.add_node(depth) {
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						visited.insert(key.clone());
						queue.push_back(AncestorTask::Node { depth, key });
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
							crate::fdb::Kind::ChildProcess.to_i32().unwrap(),
							process_bytes.as_ref(),
						),
					);
					let (keys, after) = crate::fdb::propagate!(
						Self::get_key_page_with_transaction(
							context.txn,
							context.subspace,
							&prefix,
							after.as_deref(),
							budget.config.page_size,
						)
						.await
					);
					if let Some(after) = after {
						queue.push_back(AncestorTask::ProcessParents {
							after: Some(after),
							depth,
							permission,
							process,
						});
					}
					for key in keys {
						let crate::fdb::Key::Process(crate::fdb::process::Key::ChildProcess {
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
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						if visited.contains(&key) || authorization.value(&key) == Some(false) {
							continue;
						}
						if depth > budget.config.max_depth {
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						if authorization.value(&key) == Some(true) {
							return Ok(ControlFlow::Break(SearchOutcome::Authorized));
						}
						if !budget.add_node(depth) {
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						visited.insert(key.clone());
						queue.push_back(AncestorTask::Node { depth, key });
					}
				},
			}
		}

		for key in visited {
			if authorization.value(&key).is_none() {
				authorization.insert(key, false);
			}
		}
		Ok(ControlFlow::Break(SearchOutcome::Denied))
	}

	async fn authorize_permission_descendant_with_transaction(
		context: AuthorizationContext<'_>,
		target: &(tg::Id, tg::authorization::Permission),
	) -> tg::Result<ControlFlow<SearchOutcome, fdb::FdbError>> {
		if context.token.is_none()
			&& matches!(target.1, tg::authorization::Permission::Object(_))
			&& let tg::Principal::Process(process) = context.requester.principal
		{
			if context.config.descendant.max_nodes == 0 {
				return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
			}
			let has_edges = crate::fdb::propagate!(
				Self::process_descendant_has_edges_with_transaction(context, process).await
			);
			if !has_edges {
				return Ok(ControlFlow::Break(SearchOutcome::Denied));
			}
		}

		let mut budget = SearchBudget::new(context.config.descendant);
		let mut complete = matches!(context.requester.principal, tg::Principal::Anonymous);
		let mut queue = VecDeque::new();
		let mut visited = HashSet::new();

		let public = tg::authorization::Subject::Public;
		queue.push_back(DescendantTask::SubjectGrants {
			after: None,
			subject: public.clone(),
		});
		if context.requester.subject != public {
			queue.push_back(DescendantTask::SubjectGrants {
				after: None,
				subject: context.requester.subject.clone(),
			});
		}

		let inherent = match context.requester.principal {
			tg::Principal::User(user) => {
				crate::authorize::permissions_implied_by(tg::authorization::Permission::User(
					tg::authorization::permission::user::Permission::Admin,
				))
				.into_iter()
				.map(|permission| (tg::Id::from(user.clone()), permission))
				.collect()
			},
			_ => Vec::new(),
		};
		for key in inherent {
			if !visited.insert(key.clone()) {
				continue;
			}
			if !budget.add_node(0) {
				return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
			}
			queue.push_back(DescendantTask::Node { depth: 0, key });
		}

		while let Some(task) = queue.pop_front() {
			match task {
				DescendantTask::Node { depth, key } => {
					if key.0 == target.0 && key.1.implies(target.1) {
						return Ok(ControlFlow::Break(SearchOutcome::Authorized));
					}
					let (resource, permission) = key;
					let implied = if matches!(permission, tg::authorization::Permission::Object(_))
					{
						Vec::new()
					} else {
						crate::authorize::permissions_implied_by(permission)
					};
					for implied_permission in implied {
						if implied_permission == permission {
							continue;
						}
						let key = (resource.clone(), implied_permission);
						let depth = depth + 1;
						if !budget.add_edge() {
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						if visited.contains(&key) {
							continue;
						}
						if depth > budget.config.max_depth || !budget.add_node(depth) {
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						visited.insert(key.clone());
						queue.push_back(DescendantTask::Node { depth, key });
					}

					match permission {
						tg::authorization::Permission::Object(
							tg::authorization::permission::object::Permission::Subtree,
						) => {
							let object = tg::object::Id::try_from(resource)?;
							queue.push_back(DescendantTask::ObjectChildren {
								after: None,
								depth,
								object,
							});
						},
						tg::authorization::Permission::Object(
							tg::authorization::permission::object::Permission::Node,
						) => {},
						tg::authorization::Permission::Group(_)
						| tg::authorization::Permission::Organization(_)
						| tg::authorization::Permission::Process(_)
						| tg::authorization::Permission::Sandbox(_)
						| tg::authorization::Permission::Tag(_)
						| tg::authorization::Permission::User(_) => complete = false,
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
							crate::fdb::Kind::ObjectChild.to_i32().unwrap(),
							object_bytes.as_ref(),
						),
					);
					let (keys, after) = crate::fdb::propagate!(
						Self::get_key_page_with_transaction(
							context.txn,
							context.subspace,
							&prefix,
							after.as_deref(),
							budget.config.page_size,
						)
						.await
					);
					if let Some(after) = after {
						queue.push_back(DescendantTask::ObjectChildren {
							after: Some(after),
							depth,
							object,
						});
					}
					for key in keys {
						let crate::fdb::Key::Object(crate::fdb::object::Key::ObjectChild {
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
								return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
							}
							if visited.contains(&key) {
								continue;
							}
							if depth > budget.config.max_depth || !budget.add_node(depth) {
								return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
							}
							visited.insert(key.clone());
							queue.push_back(DescendantTask::Node { depth, key });
						}
					}
				},
				DescendantTask::SubjectGrants { after, subject } => {
					let prefix = Self::pack(
						context.subspace,
						&(
							crate::fdb::Kind::SubjectGrant.to_i32().unwrap(),
							subject.to_string(),
						),
					);
					let (keys, after) = crate::fdb::propagate!(
						Self::get_key_page_with_transaction(
							context.txn,
							context.subspace,
							&prefix,
							after.as_deref(),
							budget.config.page_size,
						)
						.await
					);
					if let Some(after) = after {
						queue.push_back(DescendantTask::SubjectGrants {
							after: Some(after),
							subject,
						});
					}
					for key in keys {
						let crate::fdb::Key::Grant(crate::fdb::grant::Key::SubjectGrant {
							permission,
							resource,
							..
						}) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};
						if !budget.add_edge() {
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						let key = (resource, permission);
						if !visited.insert(key.clone()) {
							continue;
						}
						if !budget.add_node(0) {
							return Ok(ControlFlow::Break(SearchOutcome::Exhausted));
						}
						queue.push_back(DescendantTask::Node { depth: 0, key });
					}
				},
			}
		}

		let outcome = if complete {
			SearchOutcome::Denied
		} else {
			SearchOutcome::Exhausted
		};

		Ok(ControlFlow::Break(outcome))
	}

	async fn process_descendant_has_edges_with_transaction(
		context: AuthorizationContext<'_>,
		process: &tg::process::Id,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let process_subject = tg::authorization::Subject::Process(process.clone());
		let public = tg::authorization::Subject::Public;
		let process_bytes = process.to_bytes();
		let prefixes = [
			Self::pack(
				context.subspace,
				&(
					crate::fdb::Kind::OwnerSandbox.to_i32().unwrap(),
					context.requester.principal.to_string(),
				),
			),
			Self::pack(
				context.subspace,
				&(
					crate::fdb::Kind::ProcessChild.to_i32().unwrap(),
					process_bytes.as_ref(),
				),
			),
			Self::pack(
				context.subspace,
				&(
					crate::fdb::Kind::SubjectGrant.to_i32().unwrap(),
					process_subject.to_string(),
				),
			),
			Self::pack(
				context.subspace,
				&(
					crate::fdb::Kind::SubjectGrant.to_i32().unwrap(),
					public.to_string(),
				),
			),
		];
		for prefix in prefixes {
			let (keys, _) = crate::fdb::propagate!(
				Self::get_key_page_with_transaction(
					context.txn,
					context.subspace,
					&prefix,
					None,
					1,
				)
				.await
			);
			if !keys.is_empty() {
				return Ok(ControlFlow::Break(true));
			}
		}

		Ok(ControlFlow::Break(false))
	}

	async fn get_key_page_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		prefix: &[u8],
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<ControlFlow<(Vec<crate::fdb::Key>, Option<Vec<u8>>), fdb::FdbError>> {
		let range_subspace = Subspace::from_bytes(prefix.to_vec());
		let mut range = fdb::RangeOption {
			limit: Some(limit),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		if let Some(after) = after {
			let mut begin = after.to_vec();
			begin.push(0);
			range.begin = fdb::KeySelector::first_greater_or_equal(begin);
		}
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);
		let keys = entries
			.iter()
			.map(|entry| Self::unpack(subspace, entry.key()))
			.collect::<tg::Result<Vec<_>>>()?;
		let after = (keys.len() == limit)
			.then(|| entries.last().map(|entry| entry.key().to_vec()))
			.flatten();

		Ok(ControlFlow::Break((keys, after)))
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
			max_depth: context.config.subtree.max_depth,
			remaining: context.config.subtree.max_objects,
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
			max_depth: context.config.subtree.max_depth,
			remaining: context.config.subtree.max_processes,
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
			let authorized = crate::fdb::propagate!(
				Self::authorize_with_object_subtree_search_with_transaction(
					context,
					&resource,
					authorization,
					cache,
				)
				.await
			)
			.ok_or_else(|| tg::error!("the subtree authorization search exhausted"))?;
			if !authorized {
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

		// Add the process implicit grant relationships.
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
				let cached_processes = cache.object_processes.get(&object).cloned();
				let tag_key = (resource.clone(), permission);
				let cached_tags = cache.target_tags.get(&tag_key).cloned();
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
				let (object_processes, tags) = futures::try_join!(object_processes, tags)?;
				let tags = match tags {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let object_processes = match object_processes {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
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
				dependencies.extend(tags);
			},
			tg::authorization::Permission::Process(process_permission) => {
				let process = tg::process::Id::try_from(resource.clone())?;
				let cached_sandbox = cache.process_sandboxes.get(&process).cloned();
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
				let (sandbox, tags) = futures::try_join!(sandbox, tags)?;
				let sandbox = match sandbox {
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
