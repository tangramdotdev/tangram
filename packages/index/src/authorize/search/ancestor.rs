use {
	super::{AncestorNodeFacts, Budget, Key, Outcome, Read, ReadOutput, State},
	std::collections::{BTreeMap, HashMap, HashSet, VecDeque},
	tangram_client::prelude::*,
};

enum AncestorTask {
	Node {
		depth: usize,
		key: Key,
	},
	ObjectParents {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		object: tg::object::Id,
	},
	ProcessParents {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		permission: tg::authorization::permission::process::Permission,
		process: tg::process::Id,
	},
}

pub(super) struct Search {
	authorization_revision: usize,
	budget: Budget,
	dormant: HashMap<Key, Vec<AncestorTask>>,
	pub(super) incomplete: HashSet<Key>,
	// Reference counting prunes acyclic stale branches; cycles remain live conservatively.
	live_references: HashMap<Key, usize>,
	principal: tg::Principal,
	queues: BTreeMap<usize, VecDeque<AncestorTask>>,
	subjects: HashSet<tg::authorization::Subject>,
	token: Option<(tg::authorization::Body, tg::Id)>,
	unresolved: HashSet<Key>,
	visited: HashSet<Key>,
}

impl AncestorTask {
	#[must_use]
	fn dependent(&self) -> &Key {
		match self {
			Self::Node { key, .. }
			| Self::ObjectParents { dependent: key, .. }
			| Self::ProcessParents { dependent: key, .. } => key,
		}
	}

	#[must_use]
	fn depth(&self) -> usize {
		match self {
			Self::Node { depth, .. }
			| Self::ObjectParents { depth, .. }
			| Self::ProcessParents { depth, .. } => *depth,
		}
	}
}

impl Search {
	#[must_use]
	pub(super) fn new(
		config: crate::authorize::SearchConfig,
		principal: &tg::Principal,
		roots: &[Key],
		subjects: &[tg::authorization::Subject],
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
			principal: principal.clone(),
			queues,
			subjects: subjects.iter().cloned().collect(),
			token,
			unresolved,
			visited,
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
		let mut resources = HashSet::new();
		while reads.len() < limit && !self.unresolved.is_empty() {
			let Some((depth, mut queue)) = self.queues.pop_first() else {
				break;
			};
			let task = queue.pop_front().unwrap();
			if !queue.is_empty() {
				self.queues.insert(depth, queue);
			}
			if !self.is_live(task.dependent()) {
				self.suspend(task);
				continue;
			}
			match task {
				AncestorTask::Node { depth, key } => match state.ancestor_or_descendant(&key) {
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
						} else if resources.insert(key.0.clone()) {
							// One node read discovers every direct grant on the resource.
							reads.push(Read::AncestorNode { depth, key });
						} else {
							deferred.push(AncestorTask::Node { depth, key });
						}
					},
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
			Read::AncestorNode { depth, key } => {
				let facts = output.into_ancestor_node()?;
				let facts = state.set_ancestor_facts(key.0.clone(), facts);
				self.expand_node(state, depth, &key, &facts)?;
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
			Read::Member { .. }
			| Read::ObjectChildren { .. }
			| Read::OwnerSandboxes { .. }
			| Read::Process { .. }
			| Read::ProcessChildren { .. }
			| Read::ProcessGrants { .. }
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
			if self.subjects.contains(&grant.subject) {
				let key = (grant.resource.clone(), grant.permission);
				state.authorize_ancestor_or_descendant(key);
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
		let owner_grants = if matches!(
			permission,
			tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Read
					| tg::authorization::permission::sandbox::Permission::Write
			)
		) {
			facts
				.sandbox_owner
				.as_ref()
				.map(tg::Principal::try_to_subject)
				.transpose()?
				.is_some_and(|owner| self.subjects.contains(&owner))
		} else {
			false
		};
		if principal_is_resource || token_grants || owner_grants {
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
			if !implicit_processes.insert(process.clone()) {
				continue;
			}
			let permission = tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Parent,
			);
			dependencies.push((tg::Id::from(process.clone()), permission));
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
