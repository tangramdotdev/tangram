use {
	super::{Budget, Key, Outcome, Read, ReadOutput, State},
	std::collections::{BTreeMap, HashSet, VecDeque},
	tangram_client::prelude::*,
};

enum DescendantTask {
	Node {
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

pub(super) struct Search {
	authorization_revision: usize,
	budget: Budget,
	complete: bool,
	pub(super) exhausted: bool,
	queues: BTreeMap<usize, VecDeque<DescendantTask>>,
	pub(super) unresolved: HashSet<Key>,
	visited: HashSet<Key>,
}

impl Search {
	#[must_use]
	pub(super) fn new(
		config: crate::authorize::SearchConfig,
		principal: &tg::Principal,
		state: &State,
		subjects: &[tg::authorization::Subject],
		targets: Vec<Key>,
		token: Option<(&tg::authorization::Body, &tg::Id)>,
	) -> Self {
		let authorization_revision = state.authorization_revision();
		let budget = Budget::with_root_total(config, targets.len());
		let complete = true;
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
			};
		}

		let mut queues = BTreeMap::<_, VecDeque<_>>::new();
		let visited = HashSet::new();
		for subject in subjects {
			queues
				.entry(0)
				.or_default()
				.push_back(DescendantTask::SubjectGrants {
					after: None,
					subject: subject.clone(),
				});
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
		}
	}

	pub(super) fn add_targets(
		&mut self,
		config: crate::authorize::SearchConfig,
		targets: Vec<Key>,
	) {
		let previous = self.unresolved.len();
		self.unresolved.extend(targets);
		let added = self.unresolved.len() - previous;
		self.budget.add_root_total(config, added);
		if added > 0 && (config.max_edges > 0 || config.max_nodes > 0) {
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
				DescendantTask::Node { depth, key } => self.expand_node(state, depth, key),
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
				DescendantTask::ProcessGrants {
					after,
					depth,
					process,
				} => {
					let limit = self.budget.config.page_size;
					reads.push(Read::ProcessGrants {
						after,
						depth,
						limit,
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
				DescendantTask::SubjectGrants { after, subject } => {
					let limit = self.budget.config.page_size;
					reads.push(Read::SubjectGrants {
						after,
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
		_state: &mut State,
		read: Read,
		output: ReadOutput,
	) -> tg::Result<()> {
		if self.exhausted {
			self.requeue_read(read)?;

			return Ok(());
		}
		let retry = read.clone();
		let (depth, next_depth, continuation, neighbors) = match read {
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
				let permissions = match permission {
					tg::authorization::permission::process::Permission::Parent => {
						vec![permission]
					},
					permission => vec![permission, super::process_node_permission(permission)],
				};
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
			Read::ProcessGrants { depth, process, .. } => {
				let (after, grants) = output.into_grants()?;
				let subject = tg::authorization::Subject::Process(process.clone());
				let continuation = after.map(|after| DescendantTask::ProcessGrants {
					after: Some(after),
					depth,
					process,
				});
				let neighbors = grants
					.into_iter()
					.filter(|grant| grant.is_process_implicit() && grant.subject == subject)
					.flat_map(|grant| {
						crate::authorize::permissions_implied_by(grant.permission)
							.into_iter()
							.map(move |permission| (grant.resource.clone(), permission))
					})
					.collect();

				(depth, depth + 1, continuation, neighbors)
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
			Read::SubjectGrants { subject, .. } => {
				let (after, grants) = output.into_grants()?;
				let continuation = after.map(|after| DescendantTask::SubjectGrants {
					after: Some(after),
					subject,
				});
				let neighbors = grants
					.into_iter()
					.map(|grant| (grant.resource, grant.permission))
					.collect();

				(0, 0, continuation, neighbors)
			},
			Read::AncestorNode { .. }
			| Read::Member { .. }
			| Read::ObjectParents { .. }
			| Read::Process { .. }
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
		let implied = if matches!(permission, tg::authorization::Permission::Object(_)) {
			Vec::new()
		} else {
			crate::authorize::permissions_implied_by(permission)
		};
		let implied = implied
			.into_iter()
			.rev()
			.filter(|implied| *implied != permission)
			.map(|permission| (resource.clone(), permission))
			.filter(|key| !self.visited.contains(key))
			.collect::<Vec<_>>();
		let next_depth = depth + 1;
		if !self.budget.add(implied.len(), implied.len(), next_depth) {
			self.exhausted = true;
			self.queues
				.entry(depth)
				.or_default()
				.push_front(DescendantTask::Node { depth, key });

			return;
		}
		for key in implied {
			self.visited.insert(key.clone());
			self.queues
				.entry(next_depth)
				.or_default()
				.push_back(DescendantTask::Node {
					depth: next_depth,
					key,
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
				self.queues
					.entry(depth)
					.or_default()
					.push_back(DescendantTask::ObjectChildren {
						after: None,
						depth,
						object,
					});
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
					self.queues.entry(depth).or_default().push_back(
						DescendantTask::ProcessChildren {
							after: None,
							depth,
							permission,
							process: process.clone(),
						},
					);
				}
				if permission == tg::authorization::permission::process::Permission::Parent {
					self.queues.entry(depth).or_default().push_back(
						DescendantTask::ProcessGrants {
							after: None,
							depth,
							process,
						},
					);
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

	fn requeue_read(&mut self, read: Read) -> tg::Result<()> {
		let (depth, task) = match read {
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
			Read::ProcessGrants {
				after,
				depth,
				process,
				..
			} => (
				depth,
				DescendantTask::ProcessGrants {
					after,
					depth,
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
			Read::SubjectGrants { after, subject, .. } => {
				(0, DescendantTask::SubjectGrants { after, subject })
			},
			Read::AncestorNode { .. }
			| Read::Member { .. }
			| Read::ObjectParents { .. }
			| Read::Process { .. }
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
