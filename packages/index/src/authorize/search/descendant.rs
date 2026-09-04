use {
	super::{
		Budget, DescendantCandidate, DescendantChecks, DescendantFallback, Key, MemberRead,
		Outcome, Read, ReadOutput, State,
	},
	std::collections::{BTreeMap, HashSet, VecDeque},
	tangram_client::prelude::*,
};

enum DescendantTask {
	Checks(DescendantChecks),
	Member {
		depth: usize,
		member: tg::Id,
		read: MemberRead,
	},
	Node {
		depth: usize,
		key: Key,
	},
	NodeImplications {
		depth: usize,
		key: Key,
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
	ProcessObjectChildren {
		after: Option<Vec<u8>>,
		depth: usize,
		permission: tg::authorization::permission::process::Permission,
		process: tg::process::Id,
	},
	SandboxProcesses {
		after: Option<Vec<u8>>,
		depth: usize,
		permission: tg::authorization::permission::sandbox::Permission,
		sandbox: tg::sandbox::Id,
	},
	Subject {
		depth: usize,
		subject: tg::authorization::Subject,
	},
	SubjectGrants {
		after: Option<Vec<u8>>,
		depth: usize,
		subject: tg::authorization::Subject,
	},
}

pub(super) struct Search {
	authorization_revision: usize,
	budget: Budget,
	complete: bool,
	pub(super) exhausted: bool,
	queues: BTreeMap<usize, VecDeque<DescendantTask>>,
	pub(super) unresolved: HashSet<Key>,
	visited: HashSet<Key>,
	visited_subjects: HashSet<tg::authorization::Subject>,
}

impl Search {
	#[must_use]
	pub(super) fn new(
		config: crate::authorize::SearchConfig,
		principal: &tg::Principal,
		state: &State,
		targets: Vec<Key>,
		token: Option<(&tg::authorization::Body, &tg::Id)>,
	) -> Self {
		let authorization_revision = state.authorization_revision();
		let budget = Budget::with_root_total(config, targets.len());
		// Public grants are checked only by the ancestor search, so a descendant search cannot deny.
		let complete = false;
		let unresolved = targets.into_iter().collect();
		if config.max_nodes == 0 {
			return Self {
				authorization_revision,
				budget,
				complete,
				exhausted: true,
				queues: BTreeMap::new(),
				unresolved,
				visited: HashSet::new(),
				visited_subjects: HashSet::new(),
			};
		}

		let mut queues = BTreeMap::<_, VecDeque<_>>::new();
		let visited = HashSet::new();
		if let Ok(subject) = principal.try_to_subject() {
			queues
				.entry(0)
				.or_default()
				.push_back(DescendantTask::Subject { depth: 0, subject });
		}

		let mut sources = inherent_sources(principal);
		if let Some((body, resource)) = token {
			sources.extend(
				body.permissions
					.iter()
					.map(|permission| (resource.clone(), *permission)),
			);
		}
		let mut sources_seen = HashSet::new();
		for key in sources {
			if !sources_seen.insert(key.clone()) {
				continue;
			}
			queues
				.entry(0)
				.or_default()
				.push_front(DescendantTask::Node { depth: 0, key });
		}

		Self {
			authorization_revision,
			budget,
			complete,
			exhausted: false,
			queues,
			unresolved,
			visited,
			visited_subjects: HashSet::new(),
		}
	}

	pub(super) fn add_targets(
		&mut self,
		config: crate::authorize::SearchConfig,
		targets: Vec<Key>,
	) {
		let added = targets
			.into_iter()
			.filter(|target| self.unresolved.insert(target.clone()))
			.collect::<Vec<_>>();
		self.budget.add_root_total(config, added.len());
		if !added.is_empty() && (config.max_edges > 0 || config.max_nodes > 0) {
			self.exhausted = false;
		}
	}

	pub(super) fn take_reads(&mut self, state: &mut State, limit: usize) -> Vec<Read> {
		assert!(limit > 0);
		for key in state.authorization_changes_since(&mut self.authorization_revision) {
			self.unresolved.remove(&key);
		}
		let mut reads = Vec::new();
		while reads.len() < limit && !self.unresolved.is_empty() {
			if self.exhausted {
				break;
			}
			let Some((priority, mut queue)) = self.queues.pop_first() else {
				break;
			};
			let task = queue.pop_front().unwrap();
			if !queue.is_empty() {
				self.queues.insert(priority, queue);
			}
			match task {
				DescendantTask::Checks(checks) => reads.push(Read::DescendantChecks(checks)),
				DescendantTask::Member {
					depth,
					member,
					read,
				} => {
					let limit = self.budget.config.page_size;
					reads.push(Read::Member {
						depth,
						limit,
						member,
						read,
					});
				},
				DescendantTask::Node { depth, key } => self.expand_node(state, depth, key),
				DescendantTask::NodeImplications { depth, key } => {
					self.expand_node_implications(depth, key);
				},
				DescendantTask::ObjectChildren {
					after,
					depth,
					object,
				} => {
					let limit = self.budget.config.page_size;
					reads.push(Read::ObjectChildren {
						after,
						depth,
						limit,
						object,
					});
				},
				DescendantTask::OwnerSandboxes {
					after,
					depth,
					owner,
				} => {
					let limit = self.budget.config.page_size;
					reads.push(Read::OwnerSandboxes {
						after,
						depth,
						limit,
						owner,
					});
				},
				DescendantTask::ProcessChildren {
					after,
					depth,
					permission,
					process,
				} => {
					let limit = self.budget.config.page_size;
					reads.push(Read::ProcessChildren {
						after,
						depth,
						limit,
						permission,
						process,
					});
				},
				DescendantTask::ProcessObjectChildren {
					after,
					depth,
					permission,
					process,
				} => {
					let limit = self.budget.config.page_size;
					reads.push(Read::ProcessObjectChildren {
						after,
						depth,
						limit,
						permission,
						process,
					});
				},
				DescendantTask::SandboxProcesses {
					after,
					depth,
					permission,
					sandbox,
				} => {
					let limit = self.budget.config.page_size;
					reads.push(Read::SandboxProcesses {
						after,
						depth,
						limit,
						permission,
						sandbox,
					});
				},
				DescendantTask::Subject { depth, subject } => {
					self.expand_subject(depth, subject);
				},
				DescendantTask::SubjectGrants {
					after,
					depth,
					subject,
				} => {
					let limit = self.budget.config.page_size;
					reads.push(Read::SubjectGrants {
						after,
						depth,
						limit,
						subject,
					});
				},
			}
		}

		reads
	}

	pub(super) fn apply(
		&mut self,
		state: &mut State,
		read: Read,
		output: ReadOutput,
	) -> tg::Result<()> {
		if self.exhausted {
			self.requeue_read(read)?;

			return Ok(());
		}
		let retry = read.clone();
		let (depth, next_depth, continuation, neighbors) = match read {
			Read::DescendantChecks(checks) => {
				let values = output.into_bools()?;
				self.apply_checks(state, checks, values);

				return Ok(());
			},
			Read::Member {
				depth,
				member,
				read,
				..
			} => {
				return self.apply_member(state, retry, depth, member, &read, output);
			},
			Read::ObjectChildren { depth, object, .. } => {
				let (after, children) = output.into_ids()?;
				let continuation = after.map(|after| DescendantTask::ObjectChildren {
					after: Some(after),
					depth,
					object,
				});
				let permission = tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				);
				let neighbors = children
					.into_iter()
					.map(|child| (child, permission))
					.collect();

				(depth, depth + 1, continuation, neighbors)
			},
			Read::OwnerSandboxes { depth, owner, .. } => {
				let (after, sandboxes) = output.into_ids()?;
				let continuation = after.map(|after| DescendantTask::OwnerSandboxes {
					after: Some(after),
					depth,
					owner,
				});
				let mut neighbors = Vec::with_capacity(2 * sandboxes.len());
				for sandbox in sandboxes {
					for permission in [
						tg::authorization::permission::sandbox::Permission::Write,
						tg::authorization::permission::sandbox::Permission::Read,
					] {
						neighbors.push((
							sandbox.clone(),
							tg::authorization::Permission::Sandbox(permission),
						));
					}
				}

				(depth, depth + 1, continuation, neighbors)
			},
			Read::ProcessChildren {
				depth,
				permission,
				process,
				..
			} => {
				let (after, children) = output.into_ids()?;
				let continuation = after.map(|after| DescendantTask::ProcessChildren {
					after: Some(after),
					depth,
					permission,
					process,
				});
				let permissions = process_child_permissions(permission);
				let neighbors = children
					.into_iter()
					.flat_map(|child| {
						permissions.iter().map(move |permission| {
							(
								child.clone(),
								tg::authorization::Permission::Process(*permission),
							)
						})
					})
					.collect();

				(depth, depth + 1, continuation, neighbors)
			},
			Read::ProcessObjectChildren {
				depth,
				permission,
				process,
				..
			} => {
				let (after, objects) = output.into_process_objects()?;
				let candidates = objects
					.into_iter()
					.filter_map(|(object, kind)| {
						Self::process_object_candidate(&process, permission, object, kind, true)
					})
					.collect();
				let fallback = after.map_or(DescendantFallback::None, |after| {
					DescendantFallback::ProcessObjects {
						after: Some(after),
						permission,
						process,
					}
				});
				self.queue_candidates(depth, candidates, fallback);

				return Ok(());
			},
			Read::SandboxProcesses {
				depth,
				permission,
				sandbox,
				..
			} => {
				let (after, processes) = output.into_ids()?;
				let continuation = after.map(|after| DescendantTask::SandboxProcesses {
					after: Some(after),
					depth,
					permission,
					sandbox,
				});
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
				let neighbors = processes
					.into_iter()
					.flat_map(|process| {
						permissions.iter().map(move |permission| {
							(
								process.clone(),
								tg::authorization::Permission::Process(*permission),
							)
						})
					})
					.collect();

				(depth, depth + 1, continuation, neighbors)
			},
			Read::SubjectGrants { depth, subject, .. } => {
				let (after, grants) = output.into_grants()?;
				let continuation = after.map(|after| DescendantTask::SubjectGrants {
					after: Some(after),
					depth,
					subject,
				});
				let neighbors = grants
					.into_iter()
					.map(|grant| (grant.resource, grant.permission))
					.collect();

				(depth, depth, continuation, neighbors)
			},
			Read::AncestorChecks(_)
			| Read::AncestorNode { .. }
			| Read::GroupMembers { .. }
			| Read::ObjectParents { .. }
			| Read::OrganizationMembers { .. }
			| Read::Process { .. }
			| Read::ProcessObjects { .. }
			| Read::ProcessParents { .. }
			| Read::Resolve { .. }
			| Read::SubtreeObjectChildren { .. }
			| Read::SubtreeProcessChildren { .. } => {
				return Err(tg::error!(
					"received a non-descendant read for a descendant search"
				));
			},
		};
		for key in neighbors {
			if self.visited.contains(&key) {
				continue;
			}
			if !self.budget.add(1, 1, next_depth) {
				self.exhausted = true;
				self.requeue_read(retry)?;

				return Ok(());
			}
			self.visited.insert(key.clone());
			self.queues
				.entry(next_depth)
				.or_default()
				.push_back(DescendantTask::Node {
					depth: next_depth,
					key,
				});
		}
		if let Some(continuation) = continuation {
			self.queues
				.entry(depth)
				.or_default()
				.push_back(continuation);
		}

		Ok(())
	}

	fn apply_checks(&mut self, state: &mut State, checks: DescendantChecks, values: Vec<bool>) {
		debug_assert_eq!(checks.candidates.len(), values.len());
		for (candidate, value) in std::iter::zip(&checks.candidates, values) {
			if !value {
				continue;
			}
			let neighbor = candidate.neighbor.clone();
			if !self.visited.contains(&neighbor) {
				if !self.budget.add(candidate.edges, 1, checks.depth + 1) {
					self.exhausted = true;
					self.queues
						.entry(checks.depth)
						.or_default()
						.push_front(DescendantTask::Checks(checks));

					return;
				}
				self.visited.insert(neighbor.clone());
				self.queues
					.entry(checks.depth + 1)
					.or_default()
					.push_back(DescendantTask::Node {
						depth: checks.depth + 1,
						key: neighbor.clone(),
					});
			}
			state.authorize_ancestor_or_descendant(neighbor);
			for key in state.authorization_changes_since(&mut self.authorization_revision) {
				self.unresolved.remove(&key);
			}
			if self.unresolved.is_empty() {
				return;
			}
		}
		self.queue_fallback(checks.depth, checks.fallback);
	}

	fn queue_candidates(
		&mut self,
		depth: usize,
		candidates: Vec<DescendantCandidate>,
		fallback: DescendantFallback,
	) {
		if candidates.len() > self.budget.config.page_size {
			self.queue_fallback(depth, fallback);

			return;
		}
		let candidates = candidates
			.into_iter()
			.filter(|candidate| {
				self.unresolved.iter().any(|target| {
					target.0 == candidate.neighbor.0 && candidate.neighbor.1.implies(target.1)
				})
			})
			.collect::<Vec<_>>();
		if candidates.is_empty() {
			self.queue_fallback(depth, fallback);

			return;
		}
		let checks = DescendantChecks {
			candidates,
			depth,
			fallback,
		};
		self.queues
			.entry(depth)
			.or_default()
			.push_back(DescendantTask::Checks(checks));
	}

	fn queue_fallback(&mut self, depth: usize, fallback: DescendantFallback) {
		let task = match fallback {
			DescendantFallback::None => return,
			DescendantFallback::ObjectChildren { object } => DescendantTask::ObjectChildren {
				after: None,
				depth,
				object,
			},
			DescendantFallback::ProcessChildren {
				permission,
				process,
			} => DescendantTask::ProcessChildren {
				after: None,
				depth,
				permission,
				process,
			},
			DescendantFallback::ProcessObjects {
				after,
				permission,
				process,
			} => DescendantTask::ProcessObjectChildren {
				after,
				depth,
				permission,
				process,
			},
		};
		self.queues.entry(depth).or_default().push_back(task);
	}

	fn apply_member(
		&mut self,
		state: &mut State,
		retry: Read,
		depth: usize,
		member: tg::Id,
		read: &MemberRead,
		output: ReadOutput,
	) -> tg::Result<()> {
		let member_subject = subject_for_member(member.clone())?;
		let (continuation, containers) = match read {
			MemberRead::Groups { .. } => {
				let (after, groups) = output.into_member_groups()?;
				let continuation = after.map(|after| DescendantTask::Member {
					depth,
					member,
					read: MemberRead::Groups { after: Some(after) },
				});
				let containers = groups
					.into_iter()
					.map(tg::authorization::Subject::Group)
					.collect::<Vec<_>>();

				(continuation, containers)
			},
			MemberRead::Organizations { .. } => {
				let (after, organizations) = output.into_member_organizations()?;
				let continuation = after.map(|after| DescendantTask::Member {
					depth,
					member,
					read: MemberRead::Organizations { after: Some(after) },
				});
				let containers = organizations
					.into_iter()
					.map(tg::authorization::Subject::Organization)
					.collect::<Vec<_>>();

				(continuation, containers)
			},
		};
		let next_depth = depth + 1;
		for container in containers {
			let edge_known = state.has_membership_dependency(&member_subject, &container);
			if !edge_known && !self.budget.add_edge() {
				self.exhausted = true;
				self.requeue_read(retry)?;

				return Ok(());
			}
			state.add_membership_dependency(&member_subject, container.clone());
			if self.visited_subjects.contains(&container) {
				continue;
			}
			if !self.budget.add_node(next_depth) {
				self.exhausted = true;
				self.requeue_read(retry)?;

				return Ok(());
			}
			self.visited_subjects.insert(container.clone());
			self.queues
				.entry(next_depth)
				.or_default()
				.push_back(DescendantTask::Subject {
					depth: next_depth,
					subject: container,
				});
		}
		if let Some(continuation) = continuation {
			self.queues
				.entry(depth)
				.or_default()
				.push_back(continuation);
		}

		Ok(())
	}

	#[must_use]
	pub(super) fn finish(&mut self, state: &mut State) -> Outcome {
		if self.unresolved.is_empty() {
			return Outcome::Authorized;
		}
		if !self.complete || self.exhausted {
			return Outcome::Exhausted;
		}
		for key in std::mem::take(&mut self.unresolved) {
			state.deny_ancestor_or_descendant(&key);
		}

		Outcome::Denied
	}

	pub(super) fn reset_visited_if_complete(&mut self) {
		if self.queues.is_empty() && !self.exhausted {
			self.visited.clear();
		}
	}

	fn expand_node(&mut self, state: &mut State, depth: usize, key: Key) {
		if !self.visited.contains(&key) {
			if !self.budget.add_node(depth) {
				self.exhausted = true;
				self.queues
					.entry(depth)
					.or_default()
					.push_front(DescendantTask::Node { depth, key });

				return;
			}
			self.visited.insert(key.clone());
		}
		state.authorize_ancestor_or_descendant(key.clone());
		for key in state.authorization_changes_since(&mut self.authorization_revision) {
			self.unresolved.remove(&key);
		}
		if self.exhausted {
			return;
		}
		let (resource, permission) = key.clone();
		if !matches!(permission, tg::authorization::Permission::Object(_)) {
			self.queues
				.entry(depth + 1)
				.or_default()
				.push_back(DescendantTask::NodeImplications {
					depth: depth + 1,
					key: key.clone(),
				});
		}
		if crate::authorize::write_permission_for_resource(&resource).ok() == Some(permission)
			&& let Some(owner) = principal_for_resource(&resource)
		{
			self.queues
				.entry(depth)
				.or_default()
				.push_back(DescendantTask::OwnerSandboxes {
					after: None,
					depth,
					owner,
				});
		}

		match permission {
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			) => {
				let Ok(object) = tg::object::Id::try_from(resource) else {
					self.exhausted = true;
					return;
				};
				let fallback = DescendantFallback::ObjectChildren {
					object: object.clone(),
				};
				if self.unresolved.len() <= self.budget.config.page_size {
					let targets = self.unresolved.iter().cloned().collect::<Vec<_>>();
					self.queue_object_checks(depth, &object, &targets, fallback);
				} else {
					self.queue_fallback(depth, fallback);
				}
			},
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			) => {},
			tg::authorization::Permission::Process(permission) => {
				let Ok(process) = tg::process::Id::try_from(resource.clone()) else {
					self.exhausted = true;
					return;
				};
				let parent = (
					resource,
					tg::authorization::Permission::Process(
						tg::authorization::permission::process::Permission::Parent,
					),
				);
				if permission != tg::authorization::permission::process::Permission::Parent
					&& !self.visited.contains(&parent)
				{
					self.complete = false;
				}
				let traverses_children = process_traverses_children(permission);
				if traverses_children {
					let fallback = DescendantFallback::ProcessChildren {
						permission,
						process: process.clone(),
					};
					if self.unresolved.len() <= self.budget.config.page_size {
						let targets = self.unresolved.iter().cloned().collect::<Vec<_>>();
						self.queue_process_checks(depth, &process, permission, &targets, fallback);
					} else {
						self.queue_fallback(depth, fallback);
					}
				}
				if !process_object_permissions(permission).is_empty() {
					let fallback = DescendantFallback::ProcessObjects {
						after: None,
						permission,
						process: process.clone(),
					};
					if self.unresolved.len() <= self.budget.config.page_size {
						let targets = self.unresolved.iter().cloned().collect::<Vec<_>>();
						self.queue_process_object_checks(
							depth, &process, permission, &targets, fallback,
						);
					} else {
						self.queue_fallback(depth, fallback);
					}
				}
			},
			tg::authorization::Permission::Sandbox(permission) => {
				let Ok(sandbox) = tg::sandbox::Id::try_from(resource) else {
					self.exhausted = true;
					return;
				};
				self.queues
					.entry(depth)
					.or_default()
					.push_back(DescendantTask::SandboxProcesses {
						after: None,
						depth,
						permission,
						sandbox,
					});
			},
			tg::authorization::Permission::Group(_)
			| tg::authorization::Permission::Organization(_)
			| tg::authorization::Permission::Tag(_)
			| tg::authorization::Permission::User(_) => self.complete = false,
		}
	}

	fn expand_node_implications(&mut self, depth: usize, key: Key) {
		let implied = crate::authorize::permissions_implied_by(key.1)
			.into_iter()
			.rev()
			.filter(|permission| *permission != key.1)
			.map(|permission| (key.0.clone(), permission))
			.filter(|key| !self.visited.contains(key))
			.collect::<Vec<_>>();
		if !self.budget.add(implied.len(), implied.len(), depth) {
			self.exhausted = true;
			self.queues
				.entry(depth)
				.or_default()
				.push_front(DescendantTask::NodeImplications { depth, key });

			return;
		}
		for key in implied {
			self.visited.insert(key.clone());
			self.queues
				.entry(depth)
				.or_default()
				.push_back(DescendantTask::Node { depth, key });
		}
	}

	fn queue_object_checks(
		&mut self,
		depth: usize,
		object: &tg::object::Id,
		targets: &[Key],
		fallback: DescendantFallback,
	) {
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let candidates = targets
			.iter()
			.filter(|target| matches!(target.1, tg::authorization::Permission::Object(_)))
			.filter_map(|target| tg::object::Id::try_from(target.0.clone()).ok())
			.collect::<std::collections::BTreeSet<_>>()
			.into_iter()
			.filter(|child| child != object)
			.map(|child| {
				let check = crate::authorize::Check::ObjectChild {
					child: child.clone(),
					parent: object.clone(),
				};
				DescendantCandidate {
					edges: 1,
					neighbor: (tg::Id::from(child), permission),
					proofs: vec![vec![check]],
				}
			})
			.collect();
		self.queue_candidates(depth, candidates, fallback);
	}

	fn queue_process_checks(
		&mut self,
		depth: usize,
		process: &tg::process::Id,
		permission: tg::authorization::permission::process::Permission,
		targets: &[Key],
		fallback: DescendantFallback,
	) {
		let mut candidates = Vec::new();
		let targets = targets
			.iter()
			.filter_map(|target| {
				let tg::authorization::Permission::Process(permission) = target.1 else {
					return None;
				};
				let process = tg::process::Id::try_from(target.0.clone()).ok()?;

				Some((process, permission))
			})
			.collect::<std::collections::BTreeSet<_>>();
		for (child, target) in targets {
			if &child == process {
				continue;
			}
			for permission in process_child_permissions(permission) {
				if !permission.implies(target) {
					continue;
				}
				let check = crate::authorize::Check::ProcessChild {
					child: child.clone(),
					parent: process.clone(),
				};
				let permission = tg::authorization::Permission::Process(permission);
				candidates.push(DescendantCandidate {
					edges: 1,
					neighbor: (tg::Id::from(child.clone()), permission),
					proofs: vec![vec![check]],
				});
			}
		}
		self.queue_candidates(depth, candidates, fallback);
	}

	fn queue_process_object_checks(
		&mut self,
		depth: usize,
		process: &tg::process::Id,
		permission: tg::authorization::permission::process::Permission,
		targets: &[Key],
		fallback: DescendantFallback,
	) {
		let mut candidates = Vec::new();
		for target in targets {
			let tg::authorization::Permission::Object(target_permission) = target.1 else {
				continue;
			};
			let Ok(object) = tg::object::Id::try_from(target.0.clone()) else {
				continue;
			};
			for (kind, object_permission) in process_object_permissions(permission) {
				if !object_permission.implies(target_permission) {
					continue;
				}
				let candidate = Self::process_object_candidate(
					process,
					permission,
					object.clone(),
					kind,
					false,
				)
				.unwrap();
				candidates.push(candidate);
			}
		}
		self.queue_candidates(depth, candidates, fallback);
	}

	fn process_object_candidate(
		process: &tg::process::Id,
		permission: tg::authorization::permission::process::Permission,
		object: tg::object::Id,
		kind: crate::process::object::Kind,
		relationship_is_known: bool,
	) -> Option<DescendantCandidate> {
		let object_permission = process_object_permissions(permission)
			.into_iter()
			.find_map(|(candidate, permission)| (candidate == kind).then_some(permission))?;
		let grant_permissions = match object_permission {
			tg::authorization::permission::object::Permission::Node => vec![
				tg::authorization::permission::object::Permission::Subtree,
				tg::authorization::permission::object::Permission::Node,
			],
			tg::authorization::permission::object::Permission::Subtree => {
				vec![tg::authorization::permission::object::Permission::Subtree]
			},
		};
		let proofs = grant_permissions
			.into_iter()
			.map(|grant_permission| {
				let mut proof = Vec::new();
				if !relationship_is_known {
					proof.push(crate::authorize::Check::ProcessObject {
						kind,
						object: object.clone(),
						process: process.clone(),
					});
				}
				proof.push(crate::authorize::Check::ProcessObjectGrant {
					object: object.clone(),
					permission: grant_permission,
					process: process.clone(),
				});

				proof
			})
			.collect();
		let permission = tg::authorization::Permission::Object(object_permission);
		let candidate = DescendantCandidate {
			edges: 2,
			neighbor: (tg::Id::from(object), permission),
			proofs,
		};

		Some(candidate)
	}

	fn expand_subject(&mut self, depth: usize, subject: tg::authorization::Subject) {
		if !self.visited_subjects.contains(&subject) {
			if !self.budget.add_node(depth) {
				self.exhausted = true;
				self.queues
					.entry(depth)
					.or_default()
					.push_front(DescendantTask::Subject { depth, subject });

				return;
			}
			self.visited_subjects.insert(subject.clone());
		}
		self.queues
			.entry(depth)
			.or_default()
			.push_back(DescendantTask::SubjectGrants {
				after: None,
				depth,
				subject: subject.clone(),
			});
		let member = match subject {
			tg::authorization::Subject::Group(group) => Some(tg::Id::from(group)),
			tg::authorization::Subject::User(user) => Some(tg::Id::from(user)),
			tg::authorization::Subject::Organization(_)
			| tg::authorization::Subject::Process(_)
			| tg::authorization::Subject::Public
			| tg::authorization::Subject::Root
			| tg::authorization::Subject::Runner(_)
			| tg::authorization::Subject::Sandbox(_) => None,
		};
		let Some(member) = member else {
			return;
		};
		for read in [
			MemberRead::Groups { after: None },
			MemberRead::Organizations { after: None },
		] {
			self.queues
				.entry(depth)
				.or_default()
				.push_back(DescendantTask::Member {
					depth,
					member: member.clone(),
					read,
				});
		}
	}

	fn requeue_read(&mut self, read: Read) -> tg::Result<()> {
		let (depth, task) = match read {
			Read::DescendantChecks(checks) => (checks.depth, DescendantTask::Checks(checks)),
			Read::Member {
				depth,
				member,
				read,
				..
			} => (
				depth,
				DescendantTask::Member {
					depth,
					member,
					read,
				},
			),
			Read::ObjectChildren {
				after,
				depth,
				object,
				..
			} => (
				depth,
				DescendantTask::ObjectChildren {
					after,
					depth,
					object,
				},
			),
			Read::OwnerSandboxes {
				after,
				depth,
				owner,
				..
			} => (
				depth,
				DescendantTask::OwnerSandboxes {
					after,
					depth,
					owner,
				},
			),
			Read::ProcessChildren {
				after,
				depth,
				permission,
				process,
				..
			} => (
				depth,
				DescendantTask::ProcessChildren {
					after,
					depth,
					permission,
					process,
				},
			),
			Read::ProcessObjectChildren {
				after,
				depth,
				permission,
				process,
				..
			} => (
				depth,
				DescendantTask::ProcessObjectChildren {
					after,
					depth,
					permission,
					process,
				},
			),
			Read::SandboxProcesses {
				after,
				depth,
				permission,
				sandbox,
				..
			} => (
				depth,
				DescendantTask::SandboxProcesses {
					after,
					depth,
					permission,
					sandbox,
				},
			),
			Read::SubjectGrants {
				after,
				depth,
				subject,
				..
			} => (
				depth,
				DescendantTask::SubjectGrants {
					after,
					depth,
					subject,
				},
			),
			Read::AncestorChecks(_)
			| Read::AncestorNode { .. }
			| Read::GroupMembers { .. }
			| Read::ObjectParents { .. }
			| Read::OrganizationMembers { .. }
			| Read::Process { .. }
			| Read::ProcessObjects { .. }
			| Read::ProcessParents { .. }
			| Read::Resolve { .. }
			| Read::SubtreeObjectChildren { .. }
			| Read::SubtreeProcessChildren { .. } => {
				return Err(tg::error!(
					"received a non-descendant read for a descendant search"
				));
			},
		};
		self.queues.entry(depth).or_default().push_front(task);

		Ok(())
	}
}

fn process_child_permissions(
	permission: tg::authorization::permission::process::Permission,
) -> Vec<tg::authorization::permission::process::Permission> {
	if permission == tg::authorization::permission::process::Permission::Parent {
		vec![permission]
	} else {
		vec![permission, super::process_node_permission(permission)]
	}
}

fn process_object_permissions(
	permission: tg::authorization::permission::process::Permission,
) -> Vec<(
	crate::process::object::Kind,
	tg::authorization::permission::object::Permission,
)> {
	[
		crate::process::object::Kind::Command,
		crate::process::object::Kind::Error,
		crate::process::object::Kind::Log,
		crate::process::object::Kind::Output,
	]
	.into_iter()
	.filter_map(|kind| {
		let subtree = tg::authorization::permission::object::Permission::Subtree;
		let needed = crate::authorize::process_object_permission(kind, subtree);
		if permission.implies(needed) {
			return Some((kind, subtree));
		}
		let node = tg::authorization::permission::object::Permission::Node;
		let needed = crate::authorize::process_object_permission(kind, node);

		permission.implies(needed).then_some((kind, node))
	})
	.collect()
}

fn process_traverses_children(
	permission: tg::authorization::permission::process::Permission,
) -> bool {
	matches!(
		permission,
		tg::authorization::permission::process::Permission::Parent
			| tg::authorization::permission::process::Permission::Subtree
			| tg::authorization::permission::process::Permission::SubtreeCommand
			| tg::authorization::permission::process::Permission::SubtreeError
			| tg::authorization::permission::process::Permission::SubtreeLog
			| tg::authorization::permission::process::Permission::SubtreeOutput
	)
}

fn subject_for_member(member: tg::Id) -> tg::Result<tg::authorization::Subject> {
	match member.kind() {
		tg::id::Kind::Group => Ok(tg::authorization::Subject::Group(member.try_into()?)),
		tg::id::Kind::User => Ok(tg::authorization::Subject::User(member.try_into()?)),
		_ => Err(tg::error!("invalid authorization membership subject")),
	}
}

fn inherent_sources(principal: &tg::Principal) -> Vec<Key> {
	match principal {
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
	}
}

#[must_use]
fn principal_for_resource(resource: &tg::Id) -> Option<tg::Principal> {
	match resource.kind() {
		tg::id::Kind::Group => tg::group::Id::try_from(resource.clone())
			.ok()
			.map(tg::Principal::Group),
		tg::id::Kind::Organization => tg::organization::Id::try_from(resource.clone())
			.ok()
			.map(tg::Principal::Organization),
		tg::id::Kind::Process => tg::process::Id::try_from(resource.clone())
			.ok()
			.map(tg::Principal::Process),
		tg::id::Kind::Sandbox => tg::sandbox::Id::try_from(resource.clone())
			.ok()
			.map(tg::Principal::Sandbox),
		tg::id::Kind::User => tg::user::Id::try_from(resource.clone())
			.ok()
			.map(tg::Principal::User),
		_ => None,
	}
}
