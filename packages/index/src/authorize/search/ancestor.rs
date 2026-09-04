use {
	super::{
		AncestorCandidate, AncestorChecks, AncestorNodeFacts, AncestorNodeRead, Budget, Key,
		Outcome, Read, ReadOutput, State,
	},
	std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
	tangram_client::prelude::*,
};

enum AncestorTask {
	Checks(AncestorChecks),
	GroupMembers {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		group: tg::group::Id,
	},
	Node {
		depth: usize,
		key: Key,
	},
	NodeRead {
		depth: usize,
		key: Key,
		read: AncestorNodeRead,
	},
	ObjectParents {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		object: tg::object::Id,
	},
	OrganizationMembers {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		organization: tg::organization::Id,
	},
	ProcessParents {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		permission: tg::authorization::permission::process::Permission,
		process: tg::process::Id,
	},
	Subject {
		dependent: Key,
		depth: usize,
		subject: tg::authorization::Subject,
	},
}

struct PendingAncestorNode {
	facts: AncestorNodeFacts,
	remaining: usize,
}

struct MembershipPage {
	container: tg::authorization::Subject,
	continuation: Option<AncestorTask>,
	members: Vec<tg::Id>,
}

pub(super) struct Search {
	authorization_revision: usize,
	budget: Budget,
	dormant: HashMap<Key, Vec<AncestorTask>>,
	pub(super) incomplete: HashSet<Key>,
	// Reference counting prunes acyclic stale branches; cycles remain live conservatively.
	live_references: HashMap<Key, usize>,
	node_checks_started: HashSet<Key>,
	pending_nodes: HashMap<tg::Id, PendingAncestorNode>,
	principal: tg::Principal,
	queues: BTreeMap<usize, VecDeque<AncestorTask>>,
	token: Option<(tg::authorization::Body, tg::Id)>,
	unresolved: HashSet<Key>,
	visited: HashSet<Key>,
	visited_subjects: HashSet<(tg::authorization::Subject, Key)>,
}

impl AncestorTask {
	#[must_use]
	fn dependent(&self) -> &Key {
		match self {
			Self::Checks(checks) => &checks.dependent,
			Self::GroupMembers { dependent: key, .. }
			| Self::Node { key, .. }
			| Self::NodeRead { key, .. }
			| Self::ObjectParents { dependent: key, .. }
			| Self::OrganizationMembers { dependent: key, .. }
			| Self::ProcessParents { dependent: key, .. }
			| Self::Subject { dependent: key, .. } => key,
		}
	}

	#[must_use]
	fn depth(&self) -> usize {
		match self {
			Self::Checks(checks) => checks.depth,
			Self::GroupMembers { depth, .. }
			| Self::Node { depth, .. }
			| Self::NodeRead { depth, .. }
			| Self::ObjectParents { depth, .. }
			| Self::OrganizationMembers { depth, .. }
			| Self::ProcessParents { depth, .. }
			| Self::Subject { depth, .. } => *depth,
		}
	}
}

impl Search {
	#[must_use]
	pub(super) fn new(
		config: crate::authorize::SearchConfig,
		principal: &tg::Principal,
		roots: &[Key],
		token: Option<(tg::authorization::Body, tg::Id)>,
		state: &State,
	) -> Self {
		let authorization_revision = state.authorization_revision();
		let mut budget = Budget::with_root_total(config, roots.len());
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

		let mut search = Self {
			authorization_revision,
			budget,
			dormant: HashMap::new(),
			incomplete,
			live_references: HashMap::new(),
			node_checks_started: HashSet::new(),
			pending_nodes: HashMap::new(),
			principal: principal.clone(),
			queues,
			token,
			unresolved,
			visited,
			visited_subjects: HashSet::new(),
		};
		for root in roots {
			search.add_live_reference(state, root.clone());
		}

		search
	}

	pub(super) fn take_reads(&mut self, state: &mut State, limit: usize) -> tg::Result<Vec<Read>> {
		assert!(limit > 0);
		let authorized = state.authorization_changes_since(&mut self.authorization_revision);
		self.remove_authorized(state, authorized);
		let mut deferred = Vec::new();
		let mut reads = Vec::new();
		while reads.len() < limit && !self.unresolved.is_empty() {
			let Some((depth, mut queue)) = self.queues.pop_first() else {
				break;
			};
			let task = queue.pop_front().unwrap();
			if !queue.is_empty() {
				self.queues.insert(depth, queue);
			}
			let node_read_is_pending = matches!(
				&task,
				AncestorTask::NodeRead { key, .. }
					if self.pending_nodes.contains_key(&key.0)
			);
			if !node_read_is_pending && !self.is_live(task.dependent()) {
				self.suspend(task);
				continue;
			}
			match task {
				AncestorTask::Checks(checks) => reads.push(Read::AncestorChecks(checks)),
				AncestorTask::GroupMembers {
					after,
					dependent,
					depth,
					group,
				} => {
					let limit = self.budget.config.page_size;
					reads.push(Read::GroupMembers {
						after,
						dependent,
						depth,
						group,
						limit,
					});
				},
				AncestorTask::Node { depth, key } => {
					match state.ancestor_or_descendant(&key) {
						Outcome::Authorized | Outcome::Denied => continue,
						Outcome::Exhausted => unreachable!(),
						Outcome::Pending => {},
					}
					if self.node_checks_started.insert(key.clone()) {
						self.queue_node_checks(depth, &key)?;
						continue;
					}
					match state.ancestor_or_descendant(&key) {
						Outcome::Authorized | Outcome::Denied => {},
						Outcome::Exhausted => unreachable!(),
						Outcome::Pending => {
							if state.ancestor_node_is_complete(&key) {
								for dependency in state.authorization_dependencies(&key) {
									let dependency_depth = depth + 1;
									self.add_dependency(state, &key, dependency, dependency_depth);
									if state.is_authorized(&key) {
										break;
									}
								}
								if !state.is_authorized(&key) {
									self.queue_parents(state, depth, &key)?;
								}
							} else if let Some(facts) = state.ancestor_facts(&key.0) {
								self.expand_node(state, depth, &key, &facts)?;
							} else if self.pending_nodes.contains_key(&key.0) {
								deferred.push(AncestorTask::Node { depth, key });
							} else {
								self.queue_node_reads(depth, &key);
							}
						},
					}
				},
				AncestorTask::NodeRead { depth, key, read } => {
					reads.push(Read::AncestorNode { depth, key, read });
				},
				AncestorTask::ObjectParents {
					after,
					dependent,
					depth,
					object,
				} => {
					if state.is_authorized(&dependent) {
						continue;
					}
					let limit = self.budget.config.page_size;
					reads.push(Read::ObjectParents {
						after,
						dependent,
						depth,
						limit,
						object,
					});
				},
				AncestorTask::OrganizationMembers {
					after,
					dependent,
					depth,
					organization,
				} => {
					let limit = self.budget.config.page_size;
					reads.push(Read::OrganizationMembers {
						after,
						dependent,
						depth,
						limit,
						organization,
					});
				},
				AncestorTask::ProcessParents {
					after,
					dependent,
					depth,
					permission,
					process,
				} => {
					if state.is_authorized(&dependent) {
						continue;
					}
					let limit = self.budget.config.page_size;
					reads.push(Read::ProcessParents {
						after,
						dependent,
						depth,
						limit,
						permission,
						process,
					});
				},
				AncestorTask::Subject {
					dependent,
					depth,
					subject,
				} => {
					self.expand_subject(state, dependent, depth, subject);
				},
			}
		}
		for task in deferred.into_iter().rev() {
			let depth = task.depth();
			self.queues.entry(depth).or_default().push_front(task);
		}

		Ok(reads)
	}

	pub(super) fn apply(
		&mut self,
		state: &mut State,
		read: Read,
		output: ReadOutput,
	) -> tg::Result<()> {
		match read {
			Read::AncestorChecks(checks) => {
				let values = output.into_bools()?;
				self.apply_checks(state, checks, values);
			},
			Read::AncestorNode { depth, key, read } => {
				self.apply_node_read(state, depth, &key, read, output)?;
			},
			Read::GroupMembers {
				dependent,
				depth,
				group,
				..
			} => {
				let (after, members) = output.into_ids()?;
				let continuation = after.map(|after| AncestorTask::GroupMembers {
					after: Some(after),
					dependent: dependent.clone(),
					depth,
					group: group.clone(),
				});
				let container = tg::authorization::Subject::Group(group);
				let page = MembershipPage {
					container,
					continuation,
					members,
				};
				self.apply_members(state, &dependent, depth, page)?;
			},
			Read::ObjectParents {
				dependent,
				depth,
				object,
				..
			} => {
				let (after, parents) = output.into_ids()?;
				if state.is_authorized(&dependent) {
					return Ok(());
				}
				for parent in parents {
					let permission = tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					);
					let dependency = (parent, permission);
					let dependency_depth = depth + 1;
					if !self.add_dependency(state, &dependent, dependency, dependency_depth) {
						return Ok(());
					}
					if state.is_authorized(&dependent) {
						return Ok(());
					}
				}
				if let Some(after) = after.clone() {
					state.set_ancestor_cursor(&dependent, &after);
					self.queues
						.entry(depth)
						.or_default()
						.push_back(AncestorTask::ObjectParents {
							after: Some(after),
							dependent,
							depth,
							object,
						});
				} else {
					state.complete_ancestor_parents(&dependent);
				}
			},
			Read::OrganizationMembers {
				dependent,
				depth,
				organization,
				..
			} => {
				let (after, members) = output.into_ids()?;
				let continuation = after.map(|after| AncestorTask::OrganizationMembers {
					after: Some(after),
					dependent: dependent.clone(),
					depth,
					organization: organization.clone(),
				});
				let container = tg::authorization::Subject::Organization(organization);
				let page = MembershipPage {
					container,
					continuation,
					members,
				};
				self.apply_members(state, &dependent, depth, page)?;
			},
			Read::ProcessParents {
				dependent,
				depth,
				permission,
				process,
				..
			} => {
				let (after, parents) = output.into_ids()?;
				if state.is_authorized(&dependent) {
					return Ok(());
				}
				for parent in parents {
					let permission =
						tg::authorization::Permission::Process(permission.to_subtree());
					let dependency = (parent, permission);
					let dependency_depth = depth + 1;
					if !self.add_dependency(state, &dependent, dependency, dependency_depth) {
						return Ok(());
					}
					if state.is_authorized(&dependent) {
						return Ok(());
					}
				}
				if let Some(after) = after.clone() {
					state.set_ancestor_cursor(&dependent, &after);
					self.queues
						.entry(depth)
						.or_default()
						.push_back(AncestorTask::ProcessParents {
							after: Some(after),
							dependent,
							depth,
							permission,
							process,
						});
				} else {
					state.complete_ancestor_parents(&dependent);
				}
			},
			Read::DescendantChecks(_)
			| Read::Member { .. }
			| Read::ObjectChildren { .. }
			| Read::OwnerSandboxes { .. }
			| Read::Process { .. }
			| Read::ProcessChildren { .. }
			| Read::ProcessObjectChildren { .. }
			| Read::ProcessObjects { .. }
			| Read::Resolve { .. }
			| Read::SandboxProcesses { .. }
			| Read::SubjectGrants { .. }
			| Read::SubtreeObjectChildren { .. }
			| Read::SubtreeProcessChildren { .. } => {
				return Err(tg::error!(
					"received a descendant read for an ancestor search"
				));
			},
		}

		Ok(())
	}

	fn apply_checks(&mut self, state: &mut State, checks: AncestorChecks, values: Vec<bool>) {
		debug_assert_eq!(checks.candidates.len(), values.len());
		for (candidate, value) in std::iter::zip(checks.candidates, values) {
			if !value {
				continue;
			}
			for _ in 1..candidate.edges {
				if !self.budget.add_edge() {
					self.incomplete.insert(checks.dependent);

					return;
				}
			}
			debug_assert!(self.source_authorizes(&candidate.dependency));
			state.authorize_ancestor_or_descendant(candidate.dependency.clone());
			if !self.add_dependency(
				state,
				&checks.dependent,
				candidate.dependency,
				checks.depth + 1,
			) || state.is_authorized(&checks.dependent)
			{
				return;
			}
		}
		self.queues
			.entry(checks.depth)
			.or_default()
			.push_back(AncestorTask::Node {
				depth: checks.depth,
				key: checks.dependent,
			});
	}

	fn queue_checks(&mut self, depth: usize, dependent: &Key, candidates: Vec<AncestorCandidate>) {
		if candidates.is_empty() {
			self.queues
				.entry(depth)
				.or_default()
				.push_back(AncestorTask::Node {
					depth,
					key: dependent.clone(),
				});
			return;
		}
		let checks = AncestorChecks {
			candidates,
			dependent: dependent.clone(),
			depth,
		};
		self.queues
			.entry(depth)
			.or_default()
			.push_back(AncestorTask::Checks(checks));
	}

	fn queue_node_checks(&mut self, depth: usize, key: &Key) -> tg::Result<()> {
		let mut candidates = Vec::new();
		match key.1 {
			tg::authorization::Permission::Object(permission) => {
				let object = tg::object::Id::try_from(key.0.clone())?;
				let parent_permission = tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				);
				if let Some((body, resource)) = &self.token
					&& body.grants(parent_permission)
					&& let Ok(parent) = tg::object::Id::try_from(resource.clone())
					&& parent != object
				{
					let dependency = (tg::Id::from(parent.clone()), parent_permission);
					let check = crate::authorize::Check::ObjectChild {
						child: object.clone(),
						parent,
					};
					candidates.push(ancestor_candidate(dependency, 1, [check]));
				}
				let grant_permissions = match permission {
					tg::authorization::permission::object::Permission::Node => vec![
						tg::authorization::permission::object::Permission::Subtree,
						tg::authorization::permission::object::Permission::Node,
					],
					tg::authorization::permission::object::Permission::Subtree => {
						vec![tg::authorization::permission::object::Permission::Subtree]
					},
				};
				for process in self.process_sources() {
					for kind in [
						crate::process::object::Kind::Command,
						crate::process::object::Kind::Error,
						crate::process::object::Kind::Log,
						crate::process::object::Kind::Output,
					] {
						let dependency_permission = tg::authorization::Permission::Process(
							crate::authorize::process_object_permission(kind, permission),
						);
						let dependency = (tg::Id::from(process.clone()), dependency_permission);
						if !self.source_authorizes(&dependency) {
							continue;
						}
						for grant_permission in &grant_permissions {
							let relationship = crate::authorize::Check::ProcessObject {
								kind,
								object: object.clone(),
								process: process.clone(),
							};
							let grant = crate::authorize::Check::ProcessObjectGrant {
								object: object.clone(),
								permission: *grant_permission,
								process: process.clone(),
							};
							candidates.push(ancestor_candidate(
								dependency.clone(),
								2,
								[relationship, grant],
							));
						}
					}
				}
			},
			tg::authorization::Permission::Process(permission) => {
				let process = tg::process::Id::try_from(key.0.clone())?;
				let dependency_permission =
					tg::authorization::Permission::Process(permission.to_subtree());
				for parent in self.process_sources() {
					if parent == process {
						continue;
					}
					let dependency = (tg::Id::from(parent.clone()), dependency_permission);
					if !self.source_authorizes(&dependency) {
						continue;
					}
					let check = crate::authorize::Check::ProcessChild {
						child: process.clone(),
						parent,
					};
					candidates.push(ancestor_candidate(dependency, 1, [check]));
				}
			},
			tg::authorization::Permission::Group(_)
			| tg::authorization::Permission::Organization(_)
			| tg::authorization::Permission::Sandbox(_)
			| tg::authorization::Permission::Tag(_)
			| tg::authorization::Permission::User(_) => {},
		}
		self.queue_checks(depth, key, candidates);

		Ok(())
	}

	fn apply_members(
		&mut self,
		state: &mut State,
		dependent: &Key,
		depth: usize,
		page: MembershipPage,
	) -> tg::Result<()> {
		if state.is_authorized(dependent) {
			return Ok(());
		}
		let next_depth = depth + 1;
		for member in page.members {
			let member = subject_for_member(member)?;
			let edge_known = state.has_membership_dependency(&member, &page.container);
			if !edge_known && !self.budget.add_edge() {
				self.incomplete.insert(dependent.clone());

				return Ok(());
			}
			state.add_membership_dependency(&member, page.container.clone());
			if state.is_authorized(dependent) {
				return Ok(());
			}
			self.queue_subject(state, dependent, next_depth, member);
		}
		if let Some(continuation) = page.continuation {
			self.queues
				.entry(depth)
				.or_default()
				.push_back(continuation);
		}

		Ok(())
	}

	fn apply_node_read(
		&mut self,
		state: &mut State,
		depth: usize,
		key: &Key,
		read: AncestorNodeRead,
		output: ReadOutput,
	) -> tg::Result<()> {
		let resource = key.0.clone();
		let pending = self
			.pending_nodes
			.get_mut(&resource)
			.ok_or_else(|| tg::error!("received a fact for an inactive ancestor node"))?;
		let mut grants_for_search = Vec::new();
		let mut next = Vec::new();
		match read {
			AncestorNodeRead::Group { .. } => {
				pending.facts.parent = output.into_group()?.and_then(|group| group.parent);
			},
			AncestorNodeRead::ObjectProcesses { object, .. } => {
				let (after, processes) = output.into_object_processes()?;
				pending.facts.object_processes.extend(processes);
				if let Some(after) = after {
					let limit = self.budget.config.page_size;
					next.push(AncestorNodeRead::ObjectProcesses {
						after: Some(after),
						limit,
						object,
					});
				}
			},
			AncestorNodeRead::Process { .. } => {
				pending.facts.process_sandbox =
					output.into_process()?.and_then(|process| process.sandbox);
			},
			AncestorNodeRead::ResourceGrants { resource, .. } => {
				let (after, grants) = output.into_grants()?;
				grants_for_search.clone_from(&grants);
				pending.facts.grants.extend(grants);
				if let Some(after) = after {
					let limit = self.budget.config.page_size;
					next.push(AncestorNodeRead::ResourceGrants {
						after: Some(after),
						limit,
						resource,
					});
				}
			},
			AncestorNodeRead::SandboxOwner { .. } => {
				pending.facts.sandbox_owner = output.into_sandbox_owner()?;
			},
			AncestorNodeRead::Tag { .. } => {
				pending.facts.parent = output.into_tag()?.and_then(|tag| tag.parent);
			},
			AncestorNodeRead::TargetTag { tag } => {
				if let Some(value) = output.into_tag()? {
					pending.facts.tags.push((tag, value.permissions));
				}
			},
			AncestorNodeRead::TargetTags { target, .. } => {
				let (after, tags) = output.into_tags()?;
				next.extend(
					tags.into_iter()
						.map(|tag| AncestorNodeRead::TargetTag { tag }),
				);
				if let Some(after) = after {
					let limit = self.budget.config.page_size;
					next.push(AncestorNodeRead::TargetTags {
						after: Some(after),
						limit,
						target,
					});
				}
			},
		}
		pending.remaining = pending
			.remaining
			.checked_sub(1)
			.ok_or_else(|| tg::error!("received an extra fact for an ancestor node"))?
			.saturating_add(next.len());
		let complete = pending.remaining == 0;
		for read in next {
			self.queues
				.entry(depth)
				.or_default()
				.push_back(AncestorTask::NodeRead {
					depth,
					key: key.clone(),
					read,
				});
		}
		for grant in &grants_for_search {
			if !self.add_grant(state, key, grant, depth) {
				break;
			}
		}
		if complete {
			let pending = self.pending_nodes.remove(&resource).unwrap();
			let facts = state.set_ancestor_facts(resource, pending.facts);
			self.expand_node(state, depth, key, &facts)?;
		}

		Ok(())
	}

	fn queue_node_reads(&mut self, depth: usize, key: &Key) {
		let limit = self.budget.config.page_size;
		let resource = key.0.clone();
		let mut reads = vec![AncestorNodeRead::ResourceGrants {
			after: None,
			limit,
			resource: resource.clone(),
		}];
		if let Ok(group) = tg::group::Id::try_from(resource.clone()) {
			reads.push(AncestorNodeRead::Group { group });
		} else if let Ok(object) = tg::object::Id::try_from(resource.clone()) {
			reads.push(AncestorNodeRead::ObjectProcesses {
				after: None,
				limit,
				object,
			});
			reads.push(AncestorNodeRead::TargetTags {
				after: None,
				limit,
				target: resource.clone(),
			});
		} else if let Ok(process) = tg::process::Id::try_from(resource.clone()) {
			reads.push(AncestorNodeRead::Process { process });
			reads.push(AncestorNodeRead::TargetTags {
				after: None,
				limit,
				target: resource.clone(),
			});
		} else if let Ok(sandbox) = tg::sandbox::Id::try_from(resource.clone()) {
			reads.push(AncestorNodeRead::SandboxOwner { sandbox });
		} else if let Ok(tag) = tg::tag::Id::try_from(resource.clone()) {
			reads.push(AncestorNodeRead::Tag { tag });
		}
		let pending = PendingAncestorNode {
			facts: AncestorNodeFacts::default(),
			remaining: reads.len(),
		};
		self.pending_nodes.insert(resource, pending);
		for read in reads.into_iter().rev() {
			self.queues
				.entry(depth)
				.or_default()
				.push_front(AncestorTask::NodeRead {
					depth,
					key: key.clone(),
					read,
				});
		}
	}

	pub(super) fn finish(&mut self, state: &mut State) {
		if self.unresolved.is_empty() {
			self.incomplete.clear();
			self.queues.clear();
			return;
		}

		// Propagate incomplete paths to every unresolved dependent.
		let mut incomplete = HashSet::new();
		let mut stack = std::mem::take(&mut self.incomplete)
			.into_iter()
			.collect::<Vec<_>>();
		while let Some(key) = stack.pop() {
			if state.is_authorized(&key) || !incomplete.insert(key.clone()) {
				continue;
			}
			stack.extend(state.authorization_dependents(&key));
		}
		self.incomplete = incomplete;

		// Preserve complete negative proofs for later roots in the request.
		for key in &self.visited {
			if !self.incomplete.contains(key) && !self.is_deferred(key) {
				state.deny_ancestor_or_descendant(key);
			}
		}
		let authorized = state.authorization_changes_since(&mut self.authorization_revision);
		self.remove_authorized(state, authorized);
	}

	fn expand_node(
		&mut self,
		state: &mut State,
		depth: usize,
		key: &Key,
		facts: &AncestorNodeFacts,
	) -> tg::Result<()> {
		// Apply the direct proofs.
		for grant in &facts.grants {
			if !self.add_grant(state, key, grant, depth) {
				return Ok(());
			}
		}
		let (resource, permission) = key;
		let principal_is_resource = match (&self.principal, permission) {
			(tg::Principal::Process(process), tg::authorization::Permission::Process(_)) => {
				tg::Id::from(process.clone()) == *resource
			},
			(
				tg::Principal::Sandbox(sandbox),
				tg::authorization::Permission::Sandbox(
					tg::authorization::permission::sandbox::Permission::Read
					| tg::authorization::permission::sandbox::Permission::Write,
				),
			) => tg::Id::from(sandbox.clone()) == *resource,
			(tg::Principal::User(user), tg::authorization::Permission::User(_)) => {
				tg::Id::from(user.clone()) == *resource
			},
			_ => false,
		};
		let token_grants = self.token.as_ref().is_some_and(|(body, token_resource)| {
			token_resource == resource && body.grants(*permission)
		});
		if matches!(
			permission,
			tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Read
					| tg::authorization::permission::sandbox::Permission::Write
			)
		) && let Some(owner) = &facts.sandbox_owner
			&& let Ok(subject) = owner.try_to_subject()
			&& !self.add_subject_dependency(state, key, key.clone(), subject, depth)
		{
			return Ok(());
		}
		if principal_is_resource || token_grants {
			state.authorize_ancestor_or_descendant(key.clone());
		}
		if state.is_authorized(key) {
			return Ok(());
		}

		// Construct the authorization dependencies from the facts.
		let mut dependencies = Vec::new();
		let mut implicit_processes = HashSet::new();
		for grant in &facts.grants {
			if !grant.is_process_implicit() || !grant.permission.implies(*permission) {
				continue;
			}
			let tg::authorization::Subject::Process(process) = &grant.subject else {
				continue;
			};
			implicit_processes.insert(process.clone());
		}
		match permission {
			tg::authorization::Permission::Object(object_permission) => {
				for (process, kind) in &facts.object_processes {
					if implicit_processes.contains(process) {
						let permission = tg::authorization::Permission::Process(
							crate::authorize::process_object_permission(*kind, *object_permission),
						);
						dependencies.push((tg::Id::from(process.clone()), permission));
					}
				}
				dependencies.extend(Self::tag_dependencies(facts, *permission));
			},
			tg::authorization::Permission::Process(process_permission) => {
				if let Some(sandbox) = &facts.process_sandbox {
					let permission = match process_permission {
						tg::authorization::permission::process::Permission::Parent => {
							tg::authorization::permission::sandbox::Permission::Write
						},
						_ => tg::authorization::permission::sandbox::Permission::Read,
					};
					let permission = tg::authorization::Permission::Sandbox(permission);
					dependencies.push((tg::Id::from(sandbox.clone()), permission));
				}
				dependencies.extend(Self::tag_dependencies(facts, *permission));
			},
			tg::authorization::Permission::Group(_)
			| tg::authorization::Permission::Organization(_)
			| tg::authorization::Permission::Sandbox(_)
			| tg::authorization::Permission::Tag(_)
			| tg::authorization::Permission::User(_) => {
				if let Some(owner) = &facts.sandbox_owner {
					let owner = match owner {
						tg::Principal::Group(id) => Some(tg::Id::from(id.clone())),
						tg::Principal::Organization(id) => Some(tg::Id::from(id.clone())),
						tg::Principal::Process(id) => Some(tg::Id::from(id.clone())),
						tg::Principal::Sandbox(id) => Some(tg::Id::from(id.clone())),
						tg::Principal::User(id) => Some(tg::Id::from(id.clone())),
						tg::Principal::Anonymous
						| tg::Principal::Root
						| tg::Principal::Runner(_) => None,
					};
					if let Some(owner) = owner {
						let permission = crate::authorize::write_permission_for_resource(&owner)?;
						dependencies.push((owner, permission));
					}
				}
				if let Some(parent) = &facts.parent {
					let permission =
						crate::authorize::permission_for_named_parent(parent, *permission)?;
					dependencies.push((parent.clone(), permission));
				}
			},
		}
		for dependency in dependencies {
			let dependency_depth = depth + 1;
			if !self.add_dependency(state, key, dependency, dependency_depth) {
				return Ok(());
			}
			if state.is_authorized(key) {
				return Ok(());
			}
		}
		state.complete_ancestor_node(key);
		self.queue_parents(state, depth, key)?;

		Ok(())
	}

	fn add_grant(
		&mut self,
		state: &mut State,
		dependent: &Key,
		grant: &super::Grant,
		depth: usize,
	) -> bool {
		if !grant.permission.implies(dependent.1) {
			return true;
		}
		let source = (grant.resource.clone(), grant.permission);
		let subject = grant.subject.clone();
		if !self.add_subject_dependency(state, dependent, source, subject.clone(), depth) {
			return false;
		}
		if state.process_parent_delegation()
			&& let tg::authorization::Subject::Process(process) = subject
		{
			let permission = tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Parent,
			);
			let dependency = (tg::Id::from(process), permission);
			if dependency != *dependent
				&& !self.add_dependency(state, dependent, dependency, depth + 1)
			{
				return false;
			}
		}

		true
	}

	fn add_subject_dependency(
		&mut self,
		state: &mut State,
		dependent: &Key,
		source: Key,
		subject: tg::authorization::Subject,
		depth: usize,
	) -> bool {
		let direct =
			subject == tg::authorization::Subject::Public || state.is_subject_authorized(&subject);
		let edge_known = state.has_subject_dependency(&subject, &source);
		if !direct && !edge_known && !self.budget.add_edge() {
			self.incomplete.insert(dependent.clone());

			return false;
		}
		if subject == tg::authorization::Subject::Public {
			state.authorize_subject(subject.clone());
		}
		state.add_subject_dependency(&subject, source);
		if !state.is_authorized(dependent) {
			self.queue_subject(state, dependent, depth, subject);
		}

		true
	}

	fn queue_subject(
		&mut self,
		state: &State,
		dependent: &Key,
		depth: usize,
		subject: tg::authorization::Subject,
	) {
		if state.is_subject_authorized(&subject)
			|| !matches!(
				subject,
				tg::authorization::Subject::Group(_) | tg::authorization::Subject::Organization(_)
			) || !self
			.visited_subjects
			.insert((subject.clone(), dependent.clone()))
		{
			return;
		}
		if !self.budget.add_node(depth) {
			self.incomplete.insert(dependent.clone());

			return;
		}
		self.queues
			.entry(depth)
			.or_default()
			.push_back(AncestorTask::Subject {
				dependent: dependent.clone(),
				depth,
				subject,
			});
	}

	fn expand_subject(
		&mut self,
		state: &State,
		dependent: Key,
		depth: usize,
		subject: tg::authorization::Subject,
	) {
		if state.is_authorized(&dependent) {
			return;
		}
		let task = match subject {
			tg::authorization::Subject::Group(group) => AncestorTask::GroupMembers {
				after: None,
				dependent,
				depth,
				group,
			},
			tg::authorization::Subject::Organization(organization) => {
				AncestorTask::OrganizationMembers {
					after: None,
					dependent,
					depth,
					organization,
				}
			},
			tg::authorization::Subject::Process(_)
			| tg::authorization::Subject::Public
			| tg::authorization::Subject::Root
			| tg::authorization::Subject::Runner(_)
			| tg::authorization::Subject::Sandbox(_)
			| tg::authorization::Subject::User(_) => return,
		};
		self.queues.entry(depth).or_default().push_back(task);
	}

	fn tag_dependencies(
		facts: &AncestorNodeFacts,
		permission: tg::authorization::Permission,
	) -> Vec<Key> {
		facts
			.tags
			.iter()
			.filter(|(_, permissions)| {
				permissions
					.iter()
					.any(|tag_permission| tag_permission.implies(permission))
			})
			.map(|(tag, _)| {
				let permission = tg::authorization::Permission::Tag(
					tg::authorization::permission::tag::Permission::Read,
				);

				(tg::Id::from(tag.clone()), permission)
			})
			.collect()
	}

	fn queue_parents(&mut self, state: &mut State, depth: usize, key: &Key) -> tg::Result<()> {
		if state.ancestor_parents_are_complete(key) {
			return Ok(());
		}
		let after = state.ancestor_cursor(key);
		let task = match key.1 {
			tg::authorization::Permission::Object(_) => {
				let object = tg::object::Id::try_from(key.0.clone())?;

				Some(AncestorTask::ObjectParents {
					after,
					dependent: key.clone(),
					depth,
					object,
				})
			},
			tg::authorization::Permission::Process(permission) => {
				let process = tg::process::Id::try_from(key.0.clone())?;

				Some(AncestorTask::ProcessParents {
					after,
					dependent: key.clone(),
					depth,
					permission,
					process,
				})
			},
			tg::authorization::Permission::Group(_)
			| tg::authorization::Permission::Organization(_)
			| tg::authorization::Permission::Sandbox(_)
			| tg::authorization::Permission::Tag(_)
			| tg::authorization::Permission::User(_) => None,
		};
		let Some(task) = task else {
			state.complete_ancestor_parents(key);

			return Ok(());
		};
		self.queues.entry(depth).or_default().push_back(task);

		Ok(())
	}

	fn process_sources(&self) -> BTreeSet<tg::process::Id> {
		let mut processes = BTreeSet::new();
		if let tg::Principal::Process(process) = &self.principal {
			processes.insert(process.clone());
		}
		if let Some((body, resource)) = &self.token
			&& body
				.permissions
				.iter()
				.any(|permission| matches!(permission, tg::authorization::Permission::Process(_)))
			&& let Ok(process) = tg::process::Id::try_from(resource.clone())
		{
			processes.insert(process);
		}

		processes
	}

	fn source_authorizes(&self, key: &Key) -> bool {
		if let tg::authorization::Permission::Process(_) = key.1
			&& let Ok(process) = tg::process::Id::try_from(key.0.clone())
			&& matches!(&self.principal, tg::Principal::Process(principal) if principal == &process)
		{
			return true;
		}
		self.token
			.as_ref()
			.is_some_and(|(body, resource)| resource == &key.0 && body.grants(key.1))
	}

	fn add_dependency(
		&mut self,
		state: &mut State,
		dependent: &Key,
		dependency: Key,
		depth: usize,
	) -> bool {
		let edge_known = state.has_authorization_dependency(&dependency, dependent);
		if !edge_known {
			if !self.budget.add_edge() {
				self.incomplete.insert(dependent.clone());
				return false;
			}
			let inserted = state.add_authorization_dependency(&dependency, dependent.clone());
			debug_assert!(inserted);
			self.add_live_dependency(state, dependent, &dependency);
		}

		match state.ancestor_or_descendant(&dependency) {
			Outcome::Authorized | Outcome::Denied => return true,
			Outcome::Exhausted => unreachable!(),
			Outcome::Pending => {},
		}
		if self.visited.contains(&dependency) {
			return true;
		}
		if depth > self.budget.config.max_depth {
			self.incomplete.insert(dependent.clone());
			return true;
		}
		if !self.budget.add_node(depth) {
			self.incomplete.insert(dependency);
			return true;
		}
		self.visited.insert(dependency.clone());
		self.queues
			.entry(depth)
			.or_default()
			.push_back(AncestorTask::Node {
				depth,
				key: dependency,
			});

		true
	}

	fn add_live_dependency(&mut self, state: &State, dependent: &Key, dependency: &Key) {
		if self.is_live(dependent) {
			self.add_live_reference(state, dependency.clone());
		}
	}

	fn add_live_reference(&mut self, state: &State, key: Key) {
		let mut stack = vec![key];
		while let Some(key) = stack.pop() {
			let count = self.live_references.entry(key.clone()).or_default();
			*count = count.saturating_add(1);
			if *count > 1 {
				continue;
			}
			if let Some(tasks) = self.dormant.remove(&key) {
				for task in tasks {
					let depth = task.depth();
					self.queues.entry(depth).or_default().push_back(task);
				}
			}
			stack.extend(state.authorization_dependencies(&key));
		}
	}

	#[must_use]
	fn is_deferred(&self, key: &Key) -> bool {
		self.dormant.contains_key(key)
	}

	#[must_use]
	fn is_live(&self, key: &Key) -> bool {
		self.live_references.contains_key(key)
	}

	fn remove_authorized(&mut self, state: &State, authorized: Vec<Key>) {
		for key in authorized {
			if self.unresolved.remove(&key) {
				self.remove_live_reference(state, &key);
			}
		}
	}

	fn remove_live_reference(&mut self, state: &State, key: &Key) {
		let mut stack = vec![key.clone()];
		while let Some(key) = stack.pop() {
			let Some(count) = self.live_references.get_mut(&key) else {
				continue;
			};
			if *count > 1 {
				*count -= 1;
				continue;
			}
			self.live_references.remove(&key);
			stack.extend(state.authorization_dependencies(&key));
		}
	}

	fn suspend(&mut self, task: AncestorTask) {
		let key = task.dependent().clone();
		self.dormant.entry(key).or_default().push(task);
	}
}

fn ancestor_candidate(
	dependency: Key,
	edges: usize,
	checks: impl IntoIterator<Item = crate::authorize::Check>,
) -> AncestorCandidate {
	let checks = checks.into_iter().collect();

	AncestorCandidate {
		checks,
		dependency,
		edges,
	}
}

fn subject_for_member(member: tg::Id) -> tg::Result<tg::authorization::Subject> {
	match member.kind() {
		tg::id::Kind::Group => Ok(tg::authorization::Subject::Group(member.try_into()?)),
		tg::id::Kind::User => Ok(tg::authorization::Subject::User(member.try_into()?)),
		_ => Err(tg::error!("invalid authorization membership subject")),
	}
}
