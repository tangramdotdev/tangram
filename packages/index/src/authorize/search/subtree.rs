use {
	super::{Key, Outcome, Read, ReadOutput, State},
	std::collections::{HashMap, HashSet, VecDeque},
	tangram_client::prelude::*,
};

pub(crate) enum Action {
	AuthorizeAncestorOrDescendant { roots: Vec<Key> },
	AuthorizeProcessNodes { roots: Vec<(Key, Outcome)> },
	Complete { outcome: Outcome },
	Read { reads: Vec<Read> },
}

struct Budget {
	max_depth: usize,
	remaining: usize,
}

#[derive(Clone, Copy)]
enum Kind {
	Object,
	Process {
		node: tg::authorization::permission::process::Permission,
		subtree: tg::authorization::permission::process::Permission,
	},
}

enum Phase {
	AuthorizeNodes { nodes: Vec<tg::Id> },
	AuthorizeProcessNodes { nodes: Vec<tg::Id> },
	AuthorizeSubtrees { nodes: Vec<tg::Id> },
	Children,
	Complete { outcome: Outcome },
	Frontier,
}

pub(crate) struct Search {
	ancestor_or_descendant: HashMap<Key, Outcome>,
	budget: Budget,
	depth: usize,
	frontier: Vec<tg::Id>,
	kind: Kind,
	next: Vec<tg::Id>,
	pending: VecDeque<(tg::Id, Option<Vec<u8>>)>,
	phase: Phase,
	root: Key,
	visited: HashSet<tg::Id>,
}

impl Search {
	pub(crate) fn new_object(
		config: crate::authorize::SubtreeConfig,
		resource: &tg::Id,
	) -> tg::Result<Self> {
		let root = tg::object::Id::try_from(resource.clone())?;
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let root = tg::Id::from(root);
		let budget = Budget {
			max_depth: config.max_depth,
			remaining: config.max_objects,
		};

		Ok(Self::new(budget, Kind::Object, root, permission))
	}

	pub(crate) fn new_process(
		config: crate::authorize::SubtreeConfig,
		permission: tg::authorization::permission::process::Permission,
		resource: &tg::Id,
	) -> tg::Result<Self> {
		let node = match permission {
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
			_ => return Err(tg::error!("expected a process subtree permission")),
		};
		let root = tg::process::Id::try_from(resource.clone())?;
		let subtree = permission;
		let permission = tg::authorization::Permission::Process(permission);
		let root = tg::Id::from(root);
		let budget = Budget {
			max_depth: config.max_depth,
			remaining: config.max_processes,
		};
		let kind = Kind::Process { node, subtree };

		Ok(Self::new(budget, kind, root, permission))
	}

	#[must_use]
	fn new(
		budget: Budget,
		kind: Kind,
		root: tg::Id,
		permission: tg::authorization::Permission,
	) -> Self {
		let frontier = vec![root.clone()];
		let root_key = (root.clone(), permission);
		let visited = HashSet::from([root]);

		Self {
			ancestor_or_descendant: HashMap::new(),
			budget,
			depth: 0,
			frontier,
			kind,
			next: Vec::new(),
			pending: VecDeque::new(),
			phase: Phase::Frontier,
			root: root_key,
			visited,
		}
	}

	pub(crate) fn next_action(
		&mut self,
		state: &mut State,
		max_reads: usize,
		page_size: usize,
	) -> tg::Result<Action> {
		assert!(max_reads > 0);
		assert!(page_size > 0);
		loop {
			let phase = std::mem::replace(&mut self.phase, Phase::Frontier);
			match phase {
				Phase::AuthorizeNodes { nodes } => match self.kind {
					Kind::Object => {
						let mut incomplete = false;
						for node in &nodes {
							let key = self.node_key(node);
							match state.ancestor_or_descendant(&key) {
								Outcome::Authorized => {},
								Outcome::Denied => {
									let key = self.subtree_key(node);
									state.deny_derived(&key);

									return Ok(self.complete(state, Outcome::Denied));
								},
								Outcome::Exhausted => unreachable!(),
								Outcome::Pending => incomplete = true,
							}
						}
						if incomplete {
							return Ok(self.complete(state, Outcome::Exhausted));
						}
						self.prepare_children(state, nodes);
					},
					Kind::Process { .. } => {
						let roots = nodes
							.iter()
							.map(|node| self.node_key(node))
							.filter(|key| !state.is_authorized(key))
							.map(|key| {
								let outcome = self
									.ancestor_or_descendant
									.get(&key)
									.copied()
									.unwrap_or_else(|| state.ancestor_or_descendant(&key));

								(key, outcome)
							})
							.collect::<Vec<_>>();
						if roots.is_empty() {
							self.prepare_children(state, nodes);
							continue;
						}
						self.phase = Phase::AuthorizeProcessNodes { nodes };

						return Ok(Action::AuthorizeProcessNodes { roots });
					},
				},
				Phase::AuthorizeProcessNodes { nodes } => {
					let mut incomplete = false;
					for node in &nodes {
						let key = self.node_key(node);
						match state.outcome(&key) {
							Outcome::Authorized => {},
							Outcome::Denied => {
								let key = self.subtree_key(node);
								state.deny_derived(&key);

								return Ok(self.complete(state, Outcome::Denied));
							},
							Outcome::Exhausted => unreachable!(),
							Outcome::Pending => incomplete = true,
						}
					}
					if incomplete {
						return Ok(self.complete(state, Outcome::Exhausted));
					}
					self.prepare_children(state, nodes);
				},
				Phase::AuthorizeSubtrees { nodes } => {
					let nodes = nodes
						.into_iter()
						.filter(|node| !state.is_authorized(&self.subtree_key(node)))
						.collect::<Vec<_>>();
					if nodes.is_empty() {
						return Ok(self.complete(state, Outcome::Authorized));
					}
					let roots = nodes
						.iter()
						.map(|node| self.node_key(node))
						.filter(|key| {
							state.ancestor_or_descendant(key) == Outcome::Pending
								&& !self.ancestor_or_descendant.contains_key(key)
						})
						.collect::<Vec<_>>();
					self.phase = Phase::AuthorizeNodes { nodes };
					if roots.is_empty() {
						continue;
					}

					return Ok(Action::AuthorizeAncestorOrDescendant { roots });
				},
				Phase::Children => {
					if self.pending.is_empty() {
						self.frontier = std::mem::take(&mut self.next);
						self.depth += 1;
						self.phase = Phase::Frontier;
						continue;
					}
					let reads = self.take_child_reads(max_reads, page_size)?;
					self.phase = Phase::Children;

					return Ok(Action::Read { reads });
				},
				Phase::Complete { outcome } => {
					self.phase = Phase::Complete { outcome };

					return Ok(Action::Complete { outcome });
				},
				Phase::Frontier => {
					if self.frontier.is_empty() {
						return Ok(self.complete(state, Outcome::Authorized));
					}
					let mut nodes = Vec::new();
					for node in &self.frontier {
						let key = self.subtree_key(node);
						match state.outcome(&key) {
							Outcome::Authorized => {},
							Outcome::Denied => {
								state.deny_derived(&key);

								return Ok(self.complete(state, Outcome::Denied));
							},
							Outcome::Exhausted => unreachable!(),
							Outcome::Pending => nodes.push(node.clone()),
						}
					}
					if nodes.is_empty() {
						return Ok(self.complete(state, Outcome::Authorized));
					}
					if nodes.len() > self.budget.remaining {
						return Ok(self.complete(state, Outcome::Exhausted));
					}
					self.budget.remaining -= nodes.len();
					let roots = nodes
						.iter()
						.map(|node| self.subtree_key(node))
						.filter(|key| {
							state.ancestor_or_descendant(key) == Outcome::Pending
								&& !self.ancestor_or_descendant.contains_key(key)
						})
						.collect::<Vec<_>>();
					self.phase = Phase::AuthorizeSubtrees { nodes };
					if roots.is_empty() {
						continue;
					}

					return Ok(Action::AuthorizeAncestorOrDescendant { roots });
				},
			}
		}
	}

	pub(crate) fn apply_ancestor_or_descendant(
		&mut self,
		roots: &[Key],
		outcomes: &[Outcome],
	) -> tg::Result<()> {
		if roots.len() != outcomes.len() {
			return Err(tg::error!(
				"received the wrong number of ancestor or descendant search outcomes"
			));
		}
		self.ancestor_or_descendant
			.extend(std::iter::zip(roots, outcomes).map(|(key, outcome)| (key.clone(), *outcome)));

		Ok(())
	}

	pub(crate) fn apply(
		&mut self,
		state: &mut State,
		read: Read,
		output: ReadOutput,
	) -> tg::Result<()> {
		let (depth, parent) = match (&self.kind, read) {
			(Kind::Object, Read::SubtreeObjectChildren { depth, object, .. }) => {
				(depth, tg::Id::from(object))
			},
			(Kind::Process { .. }, Read::SubtreeProcessChildren { depth, process, .. }) => {
				(depth, tg::Id::from(process))
			},
			_ => return Err(tg::error!("received an invalid read for a subtree search")),
		};
		let (after, children) = output.into_ids()?;
		let parent = self.subtree_key(&parent);
		let complete = matches!(self.phase, Phase::Complete { .. });
		let covered = state.is_authorized(&parent);
		let mut denied = false;
		let mut exhausted = false;
		for child in children {
			let child = self.subtree_key(&child);
			state.add_derived_dependency(&child, parent.clone());
			match state.outcome(&child) {
				Outcome::Authorized => continue,
				Outcome::Denied => {
					denied |= !covered;
					continue;
				},
				Outcome::Exhausted => unreachable!(),
				Outcome::Pending => {},
			}
			if !complete && !exhausted && !covered && !self.try_schedule_child(child.0, depth) {
				exhausted = true;
			}
		}
		if let Some(after) = after.clone() {
			state.set_derived_cursor(&parent, &after);
		} else {
			state.complete_derived(&parent);
		}
		if complete {
			if denied
				&& matches!(
					self.phase,
					Phase::Complete {
						outcome: Outcome::Exhausted
					}
				) {
				self.complete(state, Outcome::Denied);
			}

			return Ok(());
		}
		if denied {
			self.complete(state, Outcome::Denied);
		} else if exhausted {
			self.complete(state, Outcome::Exhausted);
		} else if let Some(after) = after {
			self.pending.push_back((parent.0, Some(after)));
		}

		Ok(())
	}

	fn complete(&mut self, state: &mut State, outcome: Outcome) -> Action {
		match outcome {
			Outcome::Authorized => {
				for node in &self.visited {
					let key = self.subtree_key(node);
					state.authorize_derived(key);
				}
			},
			Outcome::Denied => state.deny_derived(&self.root),
			Outcome::Exhausted => {},
			Outcome::Pending => unreachable!(),
		}
		let outcome = match state.outcome(&self.root) {
			Outcome::Authorized => Outcome::Authorized,
			Outcome::Denied => Outcome::Denied,
			Outcome::Exhausted => unreachable!(),
			Outcome::Pending => Outcome::Exhausted,
		};
		self.phase = Phase::Complete { outcome };

		Action::Complete { outcome }
	}

	fn node_key(&self, node: &tg::Id) -> Key {
		let permission = match self.kind {
			Kind::Object => tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			),
			Kind::Process { node, .. } => tg::authorization::Permission::Process(node),
		};

		(node.clone(), permission)
	}

	fn prepare_children(&mut self, state: &mut State, nodes: Vec<tg::Id>) {
		self.next.clear();
		self.pending.clear();
		for node in nodes {
			let dependency = self.node_key(&node);
			let dependent = self.subtree_key(&node);
			state.add_derived_dependency(&dependency, dependent.clone());
			if state.is_authorized(&dependent) {
				continue;
			}
			for child in state.derived_children(&dependent) {
				match state.outcome(&child) {
					Outcome::Authorized => {},
					Outcome::Denied => {
						self.complete(state, Outcome::Denied);

						return;
					},
					Outcome::Exhausted => unreachable!(),
					Outcome::Pending => {
						if !self.try_schedule_child(child.0, self.depth) {
							self.complete(state, Outcome::Exhausted);

							return;
						}
					},
				}
			}
			if !state.derived_is_complete(&dependent) {
				let cursor = state.derived_cursor(&dependent);
				self.pending.push_back((node, cursor));
			}
		}
		self.phase = Phase::Children;
	}

	fn subtree_key(&self, node: &tg::Id) -> Key {
		let permission = match self.kind {
			Kind::Object => tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			),
			Kind::Process { subtree, .. } => tg::authorization::Permission::Process(subtree),
		};

		(node.clone(), permission)
	}

	fn try_schedule_child(&mut self, child: tg::Id, depth: usize) -> bool {
		if !self.visited.insert(child.clone()) {
			return true;
		}
		if depth == self.budget.max_depth {
			return false;
		}
		self.next.push(child);

		self.next.len() <= self.budget.remaining
	}

	fn take_child_reads(&mut self, max_reads: usize, page_size: usize) -> tg::Result<Vec<Read>> {
		let available = self
			.budget
			.remaining
			.saturating_sub(self.next.len())
			.saturating_add(1);
		let read_total = max_reads.min(self.pending.len()).min(available);
		let mut allowance = available;
		let mut reads = Vec::with_capacity(read_total);
		for index in 0..read_total {
			let remaining = read_total - index;
			let limit = allowance.div_ceil(remaining).min(page_size).max(1);
			allowance -= limit;
			let (node, after) = self.pending.pop_front().unwrap();
			let read = match self.kind {
				Kind::Object => Read::SubtreeObjectChildren {
					after,
					depth: self.depth,
					limit,
					object: tg::object::Id::try_from(node)?,
				},
				Kind::Process { .. } => Read::SubtreeProcessChildren {
					after,
					depth: self.depth,
					limit,
					process: tg::process::Id::try_from(node)?,
				},
			};
			reads.push(read);
		}

		Ok(reads)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_definitive_denial_wins_over_an_incomplete_frontier_node() {
		let root = object(0);
		let incomplete = object(1);
		let denied = object(2);
		let mut search = Search::new_object(
			crate::authorize::SubtreeConfig::default(),
			&root.clone().into(),
		)
		.unwrap();
		search.frontier = vec![incomplete.into(), denied.clone().into()];
		search.phase = Phase::Frontier;

		let root = search.subtree_key(&root.into());
		let denied = search.subtree_key(&denied.into());
		let mut state = State::default();
		state.deny_ancestor_or_descendant(&root);
		state.deny_ancestor_or_descendant(&denied);
		state.deny_derived(&denied);

		let action = search.next_action(&mut state, 1, 1).unwrap();

		assert!(matches!(
			action,
			Action::Complete {
				outcome: Outcome::Denied
			}
		));
	}

	#[test]
	fn a_late_page_denial_overrides_exhaustion() {
		let root = object(0);
		let child = object(1);
		let resource = tg::Id::from(root.clone());
		let mut search =
			Search::new_object(crate::authorize::SubtreeConfig::default(), &resource).unwrap();
		search.phase = Phase::Complete {
			outcome: Outcome::Exhausted,
		};
		let root_key = search.subtree_key(&resource);
		let child_key = search.subtree_key(&child.clone().into());
		let mut state = State::default();
		state.deny_ancestor_or_descendant(&root_key);
		state.deny_ancestor_or_descendant(&child_key);
		state.deny_derived(&child_key);
		let read = Read::SubtreeObjectChildren {
			after: None,
			depth: 0,
			limit: 1,
			object: root,
		};
		let output = ReadOutput::Ids {
			after: None,
			ids: vec![child_key.0],
		};

		search.apply(&mut state, read, output).unwrap();
		let action = search.next_action(&mut state, 1, 1).unwrap();

		assert!(matches!(
			action,
			Action::Complete {
				outcome: Outcome::Denied
			}
		));
	}

	#[test]
	fn an_initial_ancestor_or_descendant_outcome_is_not_requested_again() {
		let root = object(0);
		let resource = tg::Id::from(root);
		let mut search =
			Search::new_object(crate::authorize::SubtreeConfig::default(), &resource).unwrap();
		let subtree = search.subtree_key(&resource);
		let node = search.node_key(&resource);
		search
			.apply_ancestor_or_descendant(std::slice::from_ref(&subtree), &[Outcome::Exhausted])
			.unwrap();
		let mut state = State::default();

		let action = search.next_action(&mut state, 1, 1).unwrap();

		assert!(matches!(
			action,
			Action::AuthorizeAncestorOrDescendant { roots } if roots == [node]
		));
	}

	#[test]
	fn an_overlapping_search_resumes_a_partially_read_node() {
		let root = object(0);
		let child = object(1);
		let resource = tg::Id::from(root);
		let mut search =
			Search::new_object(crate::authorize::SubtreeConfig::default(), &resource).unwrap();
		let parent = search.subtree_key(&resource);
		let child = search.subtree_key(&child.into());
		let node = search.node_key(&resource);
		let cursor = vec![1, 2, 3];
		let mut state = State::default();
		state.deny_ancestor_or_descendant(&parent);
		state.add_derived_dependency(&child, parent.clone());
		state.set_derived_cursor(&parent, &cursor);
		state.authorize_ancestor_or_descendant(node);

		let action = search.next_action(&mut state, 1, 1).unwrap();

		assert!(matches!(
			action,
			Action::Read { reads }
				if matches!(
					reads.as_slice(),
					[Read::SubtreeObjectChildren { after: Some(after), .. }] if after == &cursor
				)
		));
	}

	#[test]
	fn a_process_subtree_reuses_its_batched_ancestor_or_descendant_outcome() {
		let process = tg::process::Id::new();
		let resource = tg::Id::from(process);
		let permission = tg::authorization::permission::process::Permission::SubtreeCommand;
		let mut search = Search::new_process(
			crate::authorize::SubtreeConfig::default(),
			permission,
			&resource,
		)
		.unwrap();
		let mut state = State::default();

		let Action::AuthorizeAncestorOrDescendant { roots } =
			search.next_action(&mut state, 1, 1).unwrap()
		else {
			panic!("expected the process subtree ancestor or descendant search");
		};
		state.deny_ancestor_or_descendant(&roots[0]);
		search
			.apply_ancestor_or_descendant(&roots, &[Outcome::Denied])
			.unwrap();
		let Action::AuthorizeAncestorOrDescendant { roots } =
			search.next_action(&mut state, 1, 1).unwrap()
		else {
			panic!("expected the process node ancestor or descendant search");
		};
		search
			.apply_ancestor_or_descendant(&roots, &[Outcome::Exhausted])
			.unwrap();

		let action = search.next_action(&mut state, 1, 1).unwrap();

		assert!(matches!(
			action,
			Action::AuthorizeProcessNodes { roots }
				if matches!(roots.as_slice(), [(_, Outcome::Exhausted)])
		));
	}

	fn object(value: u8) -> tg::object::Id {
		tg::object::Id::new(tg::object::Kind::Blob, &vec![value].into())
	}
}
