use {
	super::{
		AncestorCandidate, AncestorChecks, AncestorNodeFacts, AncestorNodeRead, Budget,
		DelegationRead, Key, Outcome, Read, ReadOutput, State,
	},
	std::{
		collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
		sync::Arc,
	},
	tangram_client::prelude::*,
};

enum AncestorTask {
	Checks(AncestorChecks),
	Delegation {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		read: DelegationRead,
		resource: tg::Id,
	},
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
	StorageChildren {
		after: Option<Vec<u8>>,
		depth: usize,
		key: Key,
		objects: bool,
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
	verification_revision: usize,
	budget: Budget,
	fact_budget: usize,
	incomplete_nodes: HashSet<tg::Id>,
	delegation_search_started: bool,
	dormant: HashMap<Key, Vec<AncestorTask>>,
	pub(super) incomplete: HashSet<Key>,
	// Reference counting prunes acyclic stale branches; cycles remain live conservatively.
	live_references: HashMap<Key, usize>,
	node_checks_started: HashSet<Key>,
	storage_children_started: HashSet<Key>,
	pending_nodes: HashMap<tg::Id, PendingAncestorNode>,
	principal: tg::Principal,
	queues: BTreeMap<usize, VecDeque<AncestorTask>>,
	token_subject: Option<tg::authorization::Subject>,
	tokens: Vec<tg::authorization::Body>,
	unresolved: HashSet<Key>,
	visited: HashSet<Key>,
	visited_delegations: HashSet<(Key, tg::Id)>,
	visited_subjects: HashSet<(tg::authorization::Subject, Key)>,
}

impl AncestorTask {
	#[must_use]
	fn dependent(&self) -> &Key {
		match self {
			Self::Checks(checks) => &checks.dependent,
			Self::Delegation { dependent: key, .. }
			| Self::GroupMembers { dependent: key, .. }
			| Self::Node { key, .. }
			| Self::NodeRead { key, .. }
			| Self::ObjectParents { dependent: key, .. }
			| Self::OrganizationMembers { dependent: key, .. }
			| Self::ProcessParents { dependent: key, .. }
			| Self::StorageChildren { key, .. }
			| Self::Subject { dependent: key, .. } => key,
		}
	}

	#[must_use]
	fn depth(&self) -> usize {
		match self {
			Self::Checks(checks) => checks.depth,
			Self::Delegation { depth, .. }
			| Self::GroupMembers { depth, .. }
			| Self::Node { depth, .. }
			| Self::NodeRead { depth, .. }
			| Self::ObjectParents { depth, .. }
			| Self::OrganizationMembers { depth, .. }
			| Self::ProcessParents { depth, .. }
			| Self::StorageChildren { depth, .. }
			| Self::Subject { depth, .. } => *depth,
		}
	}
}

impl Search {
	#[must_use]
	pub(super) fn new(
		config: crate::verify::SearchConfig,
		principal: &tg::Principal,
		roots: &[Key],
		tokens: Vec<tg::authorization::Body>,
		state: &State,
	) -> Self {
		let verification_revision = state.verification_revision();
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
			verification_revision,
			fact_budget: budget.config.max_edges,
			incomplete_nodes: HashSet::new(),
			budget,
			delegation_search_started: false,
			dormant: HashMap::new(),
			incomplete,
			live_references: HashMap::new(),
			node_checks_started: HashSet::new(),
			storage_children_started: HashSet::new(),
			pending_nodes: HashMap::new(),
			principal: principal.clone(),
			queues,
			token_subject: state.token_subject().cloned(),
			tokens,
			unresolved,
			visited,
			visited_delegations: HashSet::new(),
			visited_subjects: HashSet::new(),
		};
		for root in roots {
			search.add_live_reference(state, root.clone());
		}

		search
	}

	pub(super) fn take_reads(&mut self, state: &mut State, limit: usize) -> tg::Result<Vec<Read>> {
		assert!(limit > 0);
		let verified = state.verification_changes_since(&mut self.verification_revision);
		self.remove_verified(state, verified);
		let mut deferred = Vec::new();
		let mut reads = Vec::new();
		while reads.len() < limit && !self.unresolved.is_empty() {
			let Some((depth, mut queue)) = self.queues.pop_first() else {
				if !self.delegation_search_started {
					self.delegation_search_started = true;
					if state.has_graph_scopes() {
						let roots = self.unresolved.iter().cloned().collect::<Vec<_>>();
						for key in roots {
							self.visited_delegations
								.insert((key.clone(), key.0.clone()));
							self.queue_delegation_parents(&key, &key.0, 0);
						}
						continue;
					}
				}
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
				AncestorTask::StorageChildren {
					after,
					depth,
					key,
					objects,
				} => reads.push(Read::StorageChildren {
					after,
					depth,
					key,
					limit: self.budget.config.page_size,
					objects,
				}),
				AncestorTask::Checks(checks) => reads.push(Read::AncestorChecks(checks)),
				AncestorTask::Delegation {
					after,
					dependent,
					depth,
					read,
					resource,
				} => {
					let limit = self.budget.config.page_size;
					reads.push(Read::Delegation {
						after,
						dependent,
						depth,
						limit,
						read,
						resource,
					});
				},
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
					if state.storage_children.contains(&key)
						&& self.storage_children_started.insert(key.clone())
					{
						if key.1 == key.1.subtree() {
							self.queues.entry(depth).or_default().push_back(
								AncestorTask::StorageChildren {
									after: None,
									depth,
									key: key.clone(),
									objects: false,
								},
							);
						}
						if matches!(key.1, tg::authorization::Permission::Process(permission) if !matches!(permission, tg::authorization::permission::process::Permission::Node | tg::authorization::permission::process::Permission::Subtree))
						{
							self.queues.entry(depth).or_default().push_back(
								AncestorTask::StorageChildren {
									after: None,
									depth,
									key: key.clone(),
									objects: true,
								},
							);
						}
					}

					match state.search_outcome(&key) {
						Outcome::Verified | Outcome::Denied => continue,
						Outcome::Exhausted => unreachable!(),
						Outcome::Pending => {},
					}
					if self.node_checks_started.insert(key.clone()) {
						self.queue_node_checks(depth, &key)?;
						continue;
					}
					match state.search_outcome(&key) {
						Outcome::Verified | Outcome::Denied => {},
						Outcome::Exhausted => unreachable!(),
						Outcome::Pending => {
							if state.ancestor_node_is_complete(&key) {
								for dependency in state.verification_dependencies(&key) {
									let dependency_depth = depth + 1;
									self.add_dependency(state, &key, dependency, dependency_depth);
									if state.search_verified(&key) {
										break;
									}
								}
								if !state.search_verified(&key) {
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
					if state.search_verified(&dependent) {
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
					if state.search_verified(&dependent) {
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
			Read::StorageChildren {
				after: _,
				depth,
				key,
				limit: _,
				objects,
			} => {
				let (after, children) = match output {
					ReadOutput::ProcessObjects { after, objects } => {
						let children = objects
							.into_iter()
							.filter(|(_, kind)| {
								key.1.implies(tg::authorization::Permission::Process(
									crate::verify::process_object_permission(*kind),
								))
							})
							.map(|(object, _)| {
								(
									object.into(),
									tg::authorization::Permission::Object(
										tg::authorization::permission::object::Permission::Subtree,
									),
									key.2.clone(),
								)
							})
							.collect::<Vec<_>>();
						(after, children)
					},
					output => {
						let (after, ids) = output.into_ids()?;
						(
							after,
							ids.into_iter()
								.map(|id| (id, key.1, key.2.clone()))
								.collect(),
						)
					},
				};
				for child in children {
					if !self.budget.add_edge() {
						self.incomplete.insert(key.clone());
						return Ok(());
					}
					state.storage.insert(child.clone());
					state.storage_children.insert(child.clone());
					state.add_sync_dependency(&child, &key);
					self.add_live_dependency(state, &key, &child);
					self.queue_dependency(state, &key, child, depth + 1);
				}
				if let Some(after) = after {
					self.queues.entry(depth).or_default().push_back(
						AncestorTask::StorageChildren {
							after: Some(after),
							depth,
							key,
							objects,
						},
					);
				}
			},

			Read::AncestorChecks(checks) => {
				let values = output.into_bools()?;
				self.apply_checks(state, checks, values);
			},
			Read::Delegation {
				dependent,
				depth,
				read,
				resource,
				..
			} => {
				let after = match read {
					DelegationRead::Delegations => {
						let ReadOutput::Delegations { after, delegations } = output else {
							return Err(tg::error!("expected delegation facts"));
						};
						self.fact_budget = self
							.fact_budget
							.saturating_sub(delegations.len().saturating_add(1));
						self.apply_delegations(state, depth, &dependent, &delegations);
						if self.fact_budget == 0 && after.is_some() {
							self.incomplete.insert(dependent.clone());
							None
						} else {
							after
						}
					},
					DelegationRead::ObjectParents | DelegationRead::ProcessParents => {
						let (after, parents) = output.into_ids()?;
						for parent in parents {
							self.queue_delegation_root(state, &dependent, &parent, depth + 1);
						}
						after
					},
					DelegationRead::ObjectProcesses => {
						let (after, processes) = output.into_object_processes()?;
						for (process, kind) in processes {
							if matches!(dependent.1, tg::authorization::Permission::Object(_)) {
								let guard = (
									process.clone().into(),
									tg::authorization::Permission::Process(
										crate::verify::process_object_permission(kind),
									),
									dependent.2.clone(),
								);
								let source = (
									dependent.0.clone(),
									dependent.1,
									Some(tg::authorization::Subject::Process(process.clone())),
								);
								if source != dependent
									&& !self.add_conjunction(
										state,
										&dependent,
										guard,
										source.clone(),
										depth + 1,
									) {
									return Ok(());
								}
								self.queue_delegation_root(state, &source, &source.0, depth + 1);
							}
							self.queue_delegation_root(
								state,
								&dependent,
								&process.into(),
								depth + 1,
							);
						}
						after
					},
				};
				if let Some(after) = after {
					self.queues
						.entry(depth)
						.or_default()
						.push_back(AncestorTask::Delegation {
							after: Some(after),
							dependent,
							depth,
							read,
							resource,
						});
				}
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
				if state.search_verified(&dependent) {
					return Ok(());
				}
				for parent in parents {
					let permission = tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					);
					let dependency = (parent, permission, dependent.2.clone());
					let dependency_depth = depth + 1;
					if !self.add_dependency(state, &dependent, dependency, dependency_depth) {
						return Ok(());
					}
					if state.search_verified(&dependent) {
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
				if state.search_verified(&dependent) {
					return Ok(());
				}
				for parent in parents {
					let permission =
						tg::authorization::Permission::Process(permission.to_subtree());
					let dependency = (parent, permission, dependent.2.clone());
					let dependency_depth = depth + 1;
					if !self.add_dependency(state, &dependent, dependency, dependency_depth) {
						return Ok(());
					}
					if state.search_verified(&dependent) {
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
			| Read::ProcessObjectDescendants { .. }
			| Read::ProcessObjects { .. }
			| Read::Resolve { .. }
			| Read::SubjectPermissions { .. }
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
			debug_assert!(self.source_verifys(&candidate.dependency));
			let expires_at = self.source_expiration(&candidate.dependency).unwrap();
			state.verify_with_expiration(candidate.dependency.clone(), expires_at);
			if !self.add_dependency(
				state,
				&checks.dependent,
				candidate.dependency,
				checks.depth + 1,
			) || state.search_verified(&checks.dependent)
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
		if key.2 != self.token_subject {
			self.queue_checks(depth, key, Vec::new());
			return Ok(());
		}
		let mut candidates = Vec::new();
		match key.1 {
			tg::authorization::Permission::Object(permission) => {
				let object = tg::object::Id::try_from(key.0.clone())?;
				let parent_permission = tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				);
				for body in &self.tokens {
					let Ok(parent) = tg::object::Id::try_from(body.resource.clone()) else {
						continue;
					};
					if !body.authorizes(parent_permission) || parent == object {
						continue;
					}
					let dependency = (
						tg::Id::from(parent.clone()),
						parent_permission,
						self.token_subject.clone(),
					);
					let check = crate::verify::Check::ObjectChild {
						child: object.clone(),
						parent,
					};
					candidates.push(ancestor_candidate(dependency, 1, [check]));
				}
				let covering_permissions = match permission {
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
							crate::verify::process_object_permission(kind),
						);
						let dependency =
							(tg::Id::from(process.clone()), dependency_permission, None);
						if !self.source_verifys(&dependency) {
							continue;
						}
						for covering_permission in &covering_permissions {
							let relationship = crate::verify::Check::ProcessObject {
								kind,
								object: object.clone(),
								process: process.clone(),
							};
							let permission = crate::verify::Check::ProcessObjectPermission {
								object: object.clone(),
								permission: *covering_permission,
								process: process.clone(),
							};
							candidates.push(ancestor_candidate(
								dependency.clone(),
								2,
								[relationship, permission],
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
					let dependency = (tg::Id::from(parent.clone()), dependency_permission, None);
					if !self.source_verifys(&dependency) {
						continue;
					}
					let check = crate::verify::Check::ProcessChild {
						child: process.clone(),
						parent,
					};
					candidates.push(ancestor_candidate(dependency, 1, [check]));
				}
			},
			tg::authorization::Permission::Group(_)
			| tg::authorization::Permission::Organization(_)
			| tg::authorization::Permission::Sandbox(_)
			| tg::authorization::Permission::Sync(_)
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
		if state.search_verified(dependent) {
			return Ok(());
		}
		let next_depth = depth + 1;
		for member in page.members {
			let member = subject_for_member(member)?;
			let edge_known =
				state.has_membership_dependency(&member, &page.container, dependent.2.as_ref());
			if !edge_known && !self.budget.add_edge() {
				self.incomplete.insert(dependent.clone());

				return Ok(());
			}
			state.add_membership_dependency(&member, page.container.clone(), dependent.2.as_ref());
			if state.search_verified(dependent) {
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
		let count = match &output {
			ReadOutput::Delegations { delegations, .. } => delegations.len(),
			ReadOutput::Permissions { permissions, .. } => permissions.len(),
			ReadOutput::ObjectProcesses { processes, .. } => processes.len(),
			_ => 1,
		};
		self.fact_budget = self.fact_budget.saturating_sub(count.saturating_add(1));
		let resource = key.0.clone();
		let pending = self
			.pending_nodes
			.get_mut(&resource)
			.ok_or_else(|| tg::error!("received a fact for an inactive ancestor node"))?;
		let mut permissions_for_search = Vec::new();
		let mut next = Vec::new();
		match read {
			AncestorNodeRead::Delegations { resource, .. } => {
				let ReadOutput::Delegations { after, delegations } = output else {
					return Err(tg::error!("expected delegation facts"));
				};
				pending.facts.delegations.extend(delegations);
				if let Some(after) = after {
					next.push(AncestorNodeRead::Delegations {
						after: Some(after),
						limit: self.budget.config.page_size,
						resource,
					});
				}
			},
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
			AncestorNodeRead::ResourcePermissions { resource, .. } => {
				let (after, permissions) = output.into_permissions()?;
				permissions_for_search.clone_from(&permissions);
				pending.facts.permissions.extend(permissions);
				if let Some(after) = after {
					let limit = self.budget.config.page_size;
					next.push(AncestorNodeRead::ResourcePermissions {
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
		}
		if self.fact_budget == 0 && !next.is_empty() {
			self.incomplete.insert(key.clone());
			self.incomplete_nodes.insert(resource.clone());
			next.clear();
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
		for permission in &permissions_for_search {
			if !self.add_permission(state, key, permission, depth) {
				break;
			}
		}
		if complete {
			let pending = self.pending_nodes.remove(&resource).unwrap();
			let facts = if self.incomplete_nodes.contains(&resource) {
				Arc::new(pending.facts)
			} else {
				state.set_ancestor_facts(resource, pending.facts)
			};
			self.expand_node(state, depth, key, &facts)?;
		}

		Ok(())
	}

	fn queue_node_reads(&mut self, depth: usize, key: &Key) {
		let limit = self.budget.config.page_size;
		let resource = key.0.clone();
		let mut reads = vec![AncestorNodeRead::ResourcePermissions {
			after: None,
			limit,
			resource: resource.clone(),
		}];
		if let Ok(group) = tg::group::Id::try_from(resource.clone()) {
			reads.push(AncestorNodeRead::Group { group });
		} else if let Ok(object) = tg::object::Id::try_from(resource.clone()) {
			reads.push(AncestorNodeRead::Delegations {
				after: None,
				limit,
				resource: object.clone().into(),
			});
			reads.push(AncestorNodeRead::ObjectProcesses {
				after: None,
				limit,
				object,
			});
		} else if resource.kind() == tg::id::Kind::Process {
			reads.push(AncestorNodeRead::Delegations {
				after: None,
				limit,
				resource: resource.clone(),
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
			if state.search_verified(&key) || !incomplete.insert(key.clone()) {
				continue;
			}
			stack.extend(state.verification_dependents(&key));
			stack.extend(
				state
					.sync_dependents
					.get(&key)
					.into_iter()
					.flatten()
					.cloned(),
			);
		}
		self.incomplete = incomplete;

		// Preserve complete negative proofs for later roots in the request.
		for key in &self.visited {
			if !self.incomplete.contains(key) && !self.is_deferred(key) {
				state.deny_ancestor_or_descendant(key);
			}
		}
		let verified = state.verification_changes_since(&mut self.verification_revision);
		self.remove_verified(state, verified);
	}

	fn expand_node(
		&mut self,
		state: &mut State,
		depth: usize,
		key: &Key,
		facts: &AncestorNodeFacts,
	) -> tg::Result<()> {
		// Apply the direct proofs.
		for permission in &facts.permissions {
			if !self.add_permission(state, key, permission, depth) {
				return Ok(());
			}
		}
		let (resource, permission, _) = key;
		let scoped_principal = key
			.2
			.as_ref()
			.and_then(|subject| subject.try_to_principal().ok());
		let principal = scoped_principal.as_ref().unwrap_or(&self.principal);
		let principal_is_resource = (key.2.is_none() || scoped_principal.is_some())
			&& match (principal, permission) {
				(tg::Principal::Process(process), tg::authorization::Permission::Process(_)) => {
					tg::Id::from(process.clone()) == *resource
				},
				(
					tg::Principal::Sandbox(sandbox),
					tg::authorization::Permission::Sandbox(
						tg::authorization::permission::sandbox::Permission::Node
						| tg::authorization::permission::sandbox::Permission::Parent,
					),
				) => tg::Id::from(sandbox.clone()) == *resource,
				(tg::Principal::User(user), tg::authorization::Permission::User(_)) => {
					tg::Id::from(user.clone()) == *resource
				},
				_ => false,
			};
		let token_permissions = key.2 == self.token_subject
			&& self
				.tokens
				.iter()
				.any(|body| &body.resource == resource && body.authorizes(*permission));
		if matches!(
			permission,
			tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Node
					| tg::authorization::permission::sandbox::Permission::Parent
			)
		) && let Some(owner) = &facts.sandbox_owner
			&& let Ok(subject) = owner.try_to_subject()
			&& !self.add_subject_dependency(state, key, key.clone(), subject, depth)
		{
			return Ok(());
		}
		if (key.2.is_none() && matches!(self.principal, tg::Principal::Root))
			|| principal_is_resource
			|| scoped_principal
				.as_ref()
				.is_some_and(|principal| matches!(principal, tg::Principal::Root))
		{
			state.verify_ancestor_or_descendant(key.clone());
		} else if token_permissions {
			let expires_at = self.source_expiration(key).unwrap();
			state.verify_with_expiration(key.clone(), expires_at);
		}
		if state.search_verified(key) {
			return Ok(());
		}

		self.apply_delegations(state, depth, key, &facts.delegations);

		// Construct the verification dependencies from the facts.
		let mut dependencies = Vec::new();
		let mut direct_processes = HashSet::new();
		for entry in &facts.permissions {
			if !entry.is_process_direct() || !entry.permission.implies(*permission) {
				continue;
			}
			let tg::authorization::Subject::Process(process) = &entry.subject else {
				continue;
			};
			direct_processes.insert(process.clone());
		}
		match permission {
			tg::authorization::Permission::Object(_) => {
				for (process, kind) in &facts.object_processes {
					let permission = tg::authorization::Permission::Process(
						crate::verify::process_object_permission(*kind),
					);
					let process_key = (tg::Id::from(process.clone()), permission, key.2.clone());
					if direct_processes.contains(process) {
						dependencies.push(process_key);
					} else {
						let source = (
							key.0.clone(),
							key.1,
							Some(tg::authorization::Subject::Process(process.clone())),
						);
						if !self.add_conjunction(state, key, process_key, source, depth + 1) {
							return Ok(());
						}
					}
				}
			},
			tg::authorization::Permission::Process(_) => {},
			tg::authorization::Permission::Group(_)
			| tg::authorization::Permission::Organization(_)
			| tg::authorization::Permission::Sandbox(_)
			| tg::authorization::Permission::Sync(_)
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
						let permission = crate::verify::write_permission_for_resource(&owner)?;
						dependencies.push((owner, permission, None));
					}
				}
				if let Some(parent) = &facts.parent {
					let permission =
						crate::verify::permission_for_named_parent(parent, *permission)?;
					dependencies.push((parent.clone(), permission, None));
				}
			},
		}
		for dependency in dependencies {
			let dependency_depth = depth + 1;
			if !self.add_dependency(state, key, dependency, dependency_depth) {
				return Ok(());
			}
			if state.search_verified(key) {
				return Ok(());
			}
		}
		if self.incomplete_nodes.contains(&key.0) {
			self.incomplete.insert(key.clone());
		} else {
			state.complete_ancestor_node(key);
		}
		self.queue_parents(state, depth, key)?;

		Ok(())
	}

	fn apply_delegations(
		&mut self,
		state: &mut State,
		depth: usize,
		key: &Key,
		delegations: &[crate::delegation::put::Arg],
	) {
		for delegation in delegations {
			if !(state.is_subject_verified(&delegation.subject, key.2.as_ref())
				|| (key.2.is_none() && matches!(self.principal, tg::Principal::Root)))
			{
				if key.1.is_read_like()
					&& let tg::authorization::Subject::Tag(tag) = &delegation.subject
				{
					let read = (
						tag.clone().into(),
						tg::authorization::Permission::Tag(
							tg::authorization::permission::tag::Permission::Read,
						),
						key.2.clone(),
					);
					let recipient = (key.0.clone(), key.1, Some(delegation.subject.clone()));
					if !self.add_conjunction(state, key, read, recipient, depth + 1) {
						return;
					}
				}
				// A process parent can use the recipient's permissions only if the recipient proves them.
				if state.process_parent_delegation()
					&& let tg::authorization::Subject::Process(process) = &delegation.subject
				{
					let parent = (
						process.clone().into(),
						tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::Parent,
						),
						key.2.clone(),
					);
					let recipient = (key.0.clone(), key.1, Some(delegation.subject.clone()));
					if !self.add_conjunction(state, key, parent, recipient, depth + 1) {
						return;
					}
				}
				continue;
			}
			let dependency = (key.0.clone(), key.1, Some(delegation.source.clone()));
			state
				.verification_expirations
				.entry((dependency.clone(), key.clone()))
				.and_modify(|expires_at| *expires_at = (*expires_at).max(delegation.expires_at))
				.or_insert(delegation.expires_at);
			if !self.add_dependency(state, key, dependency.clone(), depth + 1) {
				return;
			}
			if self.delegation_search_started
				&& !state.search_verified(&dependency)
				&& self
					.visited_delegations
					.insert((dependency.clone(), dependency.0.clone()))
			{
				self.queue_delegation_parents(&dependency, &dependency.0, depth + 1);
			}
		}
	}

	fn queue_delegation_root(
		&mut self,
		state: &State,
		dependent: &Key,
		resource: &tg::Id,
		depth: usize,
	) {
		if !self
			.visited_delegations
			.insert((dependent.clone(), resource.clone()))
		{
			return;
		}
		if !self.budget.add_edge() || !self.budget.add_node(depth) {
			self.incomplete.insert(dependent.clone());
			return;
		}
		if state.search_verified(dependent) {
			return;
		}
		self.queues
			.entry(depth)
			.or_default()
			.push_back(AncestorTask::Delegation {
				after: None,
				dependent: dependent.clone(),
				depth,
				read: DelegationRead::Delegations,
				resource: resource.clone(),
			});
		self.queue_delegation_parents(dependent, resource, depth);
	}

	fn queue_delegation_parents(&mut self, dependent: &Key, resource: &tg::Id, depth: usize) {
		let reads = if tg::object::Id::try_from(resource.clone()).is_ok() {
			&[
				DelegationRead::ObjectParents,
				DelegationRead::ObjectProcesses,
			][..]
		} else if resource.kind() == tg::id::Kind::Process {
			&[DelegationRead::ProcessParents][..]
		} else {
			&[][..]
		};
		for read in reads {
			self.queues
				.entry(depth)
				.or_default()
				.push_back(AncestorTask::Delegation {
					after: None,
					dependent: dependent.clone(),
					depth,
					read: *read,
					resource: resource.clone(),
				});
		}
	}

	fn add_permission(
		&mut self,
		state: &mut State,
		dependent: &Key,
		permission: &super::Permission,
		depth: usize,
	) -> bool {
		if !permission.permission.implies(dependent.1) {
			return true;
		}
		if state.is_subject_verified(&permission.subject, dependent.2.as_ref()) {
			state.verify_ancestor_or_descendant(dependent.clone());
			return true;
		}
		if let tg::authorization::Subject::Tag(tag) = &permission.subject {
			if !permission.permission.is_read_like() {
				return true;
			}
			let read = tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Read,
			);
			return self.add_dependency(
				state,
				dependent,
				(tag.clone().into(), read, dependent.2.clone()),
				depth + 1,
			);
		}
		if let tg::authorization::Subject::Sync(sync) = &permission.subject {
			if !permission.permission.is_read_like() {
				return true;
			}
			let permission = tg::authorization::Permission::Sync(
				tg::authorization::permission::sync::Permission::Read,
			);
			let access = (sync.clone().into(), permission, dependent.2.clone());
			state.register_sync_read(dependent, sync, &access);
			return self.add_dependency(state, dependent, access, depth + 1);
		}
		let source = (
			permission.resource.clone(),
			permission.permission,
			dependent.2.clone(),
		);
		let subject = permission.subject.clone();
		if !self.add_subject_dependency(state, dependent, source, subject.clone(), depth) {
			return false;
		}
		if state.process_parent_delegation()
			&& let tg::authorization::Subject::Process(process) = subject
		{
			let permission = tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Parent,
			);
			let dependency = (tg::Id::from(process), permission, dependent.2.clone());
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
		let direct = subject == tg::authorization::Subject::Public
			|| state.is_subject_verified(&subject, dependent.2.as_ref());
		let edge_known = state.has_subject_dependency(&subject, &source);
		if !direct && !edge_known && !self.budget.add_edge() {
			self.incomplete.insert(dependent.clone());

			return false;
		}
		if subject == tg::authorization::Subject::Public {
			state.verify_subject(subject.clone(), dependent.2.as_ref());
		}
		state.add_subject_dependency(&subject, source);
		if !state.search_verified(dependent) {
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
		if state.is_subject_verified(&subject, dependent.2.as_ref())
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
		if state.search_verified(&dependent) {
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
			| tg::authorization::Subject::Sync(_)
			| tg::authorization::Subject::Tag(_)
			| tg::authorization::Subject::User(_) => return,
		};
		self.queues.entry(depth).or_default().push_back(task);
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
			| tg::authorization::Permission::Sync(_)
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
		for body in &self.tokens {
			if let Ok(process) = tg::process::Id::try_from(body.resource.clone()) {
				processes.insert(process);
			}
		}

		processes
	}

	fn source_verifys(&self, key: &Key) -> bool {
		self.source_expiration(key).is_some()
	}

	fn source_expiration(&self, key: &Key) -> Option<i64> {
		if key.2 != self.token_subject {
			return None;
		}
		if key.2.is_none()
			&& let tg::authorization::Permission::Process(_) = key.1
			&& let Ok(process) = tg::process::Id::try_from(key.0.clone())
			&& matches!(&self.principal, tg::Principal::Process(principal) if principal == &process)
		{
			return Some(i64::MAX);
		}
		self.tokens
			.iter()
			.filter(|body| body.resource == key.0 && body.authorizes(key.1))
			.map(|body| body.expires_at)
			.max()
	}

	fn add_dependency(
		&mut self,
		state: &mut State,
		dependent: &Key,
		mut dependency: Key,
		depth: usize,
	) -> bool {
		if dependency.2.is_none() {
			dependency.2.clone_from(&dependent.2);
		}
		state.inherit_storage(dependent, &dependency);
		state.register_sync_scope(&dependency);
		let edge_known = state.has_verification_dependency(&dependency, dependent);
		if !edge_known {
			if !self.budget.add_edge() {
				self.incomplete.insert(dependent.clone());
				return false;
			}
			let inserted = state.add_verification_dependency(&dependency, dependent.clone());
			debug_assert!(inserted);
			self.add_live_dependency(state, dependent, &dependency);
		}

		self.queue_dependency(state, dependent, dependency, depth)
	}

	fn add_conjunction(
		&mut self,
		state: &mut State,
		dependent: &Key,
		first: Key,
		second: Key,
		depth: usize,
	) -> bool {
		state.sync_access.insert(first.clone());
		state.inherit_storage(dependent, &second);
		state.register_sync_scope(&first);
		state.register_sync_scope(&second);
		if !state.has_verification_conjunction(&first, &second, dependent) {
			if !self.budget.add_edge() || !self.budget.add_edge() {
				self.incomplete.insert(dependent.clone());
				return false;
			}
			state.add_verification_conjunction(&first, &second, dependent);
			self.add_live_dependency(state, dependent, &first);
			self.add_live_dependency(state, dependent, &second);
		}

		self.queue_dependency(state, dependent, first, depth)
			&& self.queue_dependency(state, dependent, second, depth)
	}

	fn queue_dependency(
		&mut self,
		state: &State,
		dependent: &Key,
		dependency: Key,
		depth: usize,
	) -> bool {
		match state.search_outcome(&dependency) {
			Outcome::Verified | Outcome::Denied => return true,
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
			stack.extend(state.verification_dependencies(&key));
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

	fn remove_verified(&mut self, state: &State, verified: Vec<Key>) {
		for key in verified {
			if state.storage.contains(&key) {
				continue;
			}
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
			stack.extend(state.verification_dependencies(&key));
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
	checks: impl IntoIterator<Item = crate::verify::Check>,
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
		_ => Err(tg::error!("invalid verification membership subject")),
	}
}
