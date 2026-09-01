use {
	ancestor::Search as AncestorSearch,
	descendant::Search as DescendantSearch,
	std::{
		collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
		sync::Arc,
	},
	tangram_client::prelude::*,
};

mod ancestor;
mod descendant;
mod subtree;

pub(crate) use {
	crate::grant::Fact as Grant,
	subtree::{Action as SubtreeAction, Search as SubtreeSearch},
};

pub(crate) type Key = (tg::Id, tg::authorization::Permission);

#[derive(Clone, Debug, Default)]
pub(crate) struct AncestorNodeFacts {
	pub grants: Vec<Grant>,
	pub object_processes: Vec<(tg::process::Id, crate::process::object::Kind)>,
	pub parent: Option<tg::Id>,
	pub process_sandbox: Option<tg::sandbox::Id>,
	pub sandbox_owner: Option<tg::Principal>,
	pub tags: Vec<(tg::tag::Id, Vec<tg::authorization::Permission>)>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ProcessFacts {
	pub objects: Vec<(tg::object::Id, crate::process::object::Kind)>,
	pub process: Option<crate::process::Process>,
}

#[derive(Clone, Debug)]
pub(crate) enum AncestorNodeRead {
	Group {
		group: tg::group::Id,
	},
	ObjectProcesses {
		after: Option<Vec<u8>>,
		limit: usize,
		object: tg::object::Id,
	},
	Process {
		process: tg::process::Id,
	},
	ResourceGrants {
		after: Option<Vec<u8>>,
		limit: usize,
		resource: tg::Id,
	},
	SandboxOwner {
		sandbox: tg::sandbox::Id,
	},
	Tag {
		tag: tg::tag::Id,
	},
	TargetTag {
		tag: tg::tag::Id,
	},
	TargetTags {
		after: Option<Vec<u8>>,
		limit: usize,
		target: tg::Id,
	},
}

#[derive(Clone, Debug)]
pub(crate) enum MemberRead {
	Groups { after: Option<Vec<u8>> },
	Organizations { after: Option<Vec<u8>> },
}

#[derive(Clone, Debug)]
pub(crate) enum Read {
	AncestorNode {
		depth: usize,
		key: Key,
		read: AncestorNodeRead,
	},
	GroupMembers {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		group: tg::group::Id,
		limit: usize,
	},
	Member {
		depth: usize,
		limit: usize,
		member: tg::Id,
		read: MemberRead,
	},
	ObjectChildren {
		after: Option<Vec<u8>>,
		depth: usize,
		limit: usize,
		object: tg::object::Id,
	},
	ObjectParents {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		limit: usize,
		object: tg::object::Id,
	},
	OrganizationMembers {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		limit: usize,
		organization: tg::organization::Id,
	},
	OwnerSandboxes {
		after: Option<Vec<u8>>,
		depth: usize,
		limit: usize,
		owner: tg::Principal,
	},
	Process {
		process: tg::process::Id,
	},
	ProcessChildren {
		after: Option<Vec<u8>>,
		depth: usize,
		limit: usize,
		permission: tg::authorization::permission::process::Permission,
		process: tg::process::Id,
	},
	ProcessGrants {
		after: Option<Vec<u8>>,
		depth: usize,
		limit: usize,
		process: tg::process::Id,
	},
	ProcessObjects {
		after: Option<Vec<u8>>,
		limit: usize,
		process: tg::process::Id,
	},
	ProcessParents {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		limit: usize,
		permission: tg::authorization::permission::process::Permission,
		process: tg::process::Id,
	},
	Resolve {
		index: usize,
		selector: tg::Selector<tg::Id>,
	},
	SandboxProcesses {
		after: Option<Vec<u8>>,
		depth: usize,
		limit: usize,
		permission: tg::authorization::permission::sandbox::Permission,
		sandbox: tg::sandbox::Id,
	},
	SubjectGrants {
		after: Option<Vec<u8>>,
		depth: usize,
		limit: usize,
		subject: tg::authorization::Subject,
	},
	SubtreeObjectChildren {
		after: Option<Vec<u8>>,
		depth: usize,
		limit: usize,
		object: tg::object::Id,
	},
	SubtreeProcessChildren {
		after: Option<Vec<u8>>,
		depth: usize,
		limit: usize,
		process: tg::process::Id,
	},
}

pub(crate) enum ReadOutput {
	Grants {
		after: Option<Vec<u8>>,
		grants: Vec<Grant>,
	},
	Group(Option<crate::group::Group>),
	Ids {
		after: Option<Vec<u8>>,
		ids: Vec<tg::Id>,
	},
	MemberGroups {
		after: Option<Vec<u8>>,
		groups: Vec<tg::group::Id>,
	},
	MemberOrganizations {
		after: Option<Vec<u8>>,
		organizations: Vec<tg::organization::Id>,
	},
	ObjectProcesses {
		after: Option<Vec<u8>>,
		processes: Vec<(tg::process::Id, crate::process::object::Kind)>,
	},
	Process(Option<crate::process::Process>),
	ProcessObjects {
		after: Option<Vec<u8>>,
		objects: Vec<(tg::object::Id, crate::process::object::Kind)>,
	},
	Resolved(Option<(tg::Id, bool)>),
	SandboxOwner(Option<tg::Principal>),
	Tag(Option<crate::tag::Tag>),
	Tags {
		after: Option<Vec<u8>>,
		tags: Vec<tg::tag::Id>,
	},
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
	Authorized,
	Denied,
	Exhausted,
	Pending,
}

struct Budget {
	config: crate::authorize::SearchConfig,
	edges: usize,
	nodes: usize,
}

pub(crate) struct AncestorOrDescendantSearch {
	ancestor: Option<AncestorSearch>,
	ancestor_exhausted: HashSet<Key>,
	complete: bool,
	descendant: Option<DescendantSearch>,
	descendant_exhausted: HashSet<Key>,
	next: Direction,
	roots: Vec<Key>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Direction {
	Ancestor,
	Descendant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProofStatus {
	Denied,
	Pending,
}

struct KeyEvaluation {
	// A proof from either evaluation authorizes the key for every search strategy.
	ancestor_or_descendant: ProofStatus,
	authorized: bool,
	derived: Option<ProofStatus>,
}

#[derive(Default)]
pub(crate) struct State {
	// Retain admitted proof edges, evaluations, and traversal cursors, never database pages.
	ancestor_complete: BTreeSet<Key>,
	ancestor_cursors: BTreeMap<Key, Vec<u8>>,
	ancestor_facts: HashMap<tg::Id, Arc<AncestorNodeFacts>>,
	ancestor_nodes: BTreeSet<Key>,
	authorization_dependencies: BTreeMap<Key, BTreeSet<Key>>,
	authorization_dependents: BTreeMap<Key, BTreeSet<Key>>,
	authorization_log: Vec<Key>,
	authorized_subjects: BTreeSet<tg::authorization::Subject>,
	derived_complete: BTreeSet<Key>,
	derived_cursors: BTreeMap<Key, Vec<u8>>,
	derived_dependencies: BTreeMap<Key, BTreeSet<Key>>,
	derived_dependents: BTreeMap<Key, BTreeSet<Key>>,
	derived_unresolved: BTreeMap<Key, usize>,
	descendant: Option<DescendantSearch>,
	evaluations: HashMap<Key, KeyEvaluation>,
	newly_evaluated: BTreeSet<Key>,
	process_facts: HashMap<tg::process::Id, Arc<ProcessFacts>>,
	subject_key_dependents: BTreeMap<tg::authorization::Subject, BTreeSet<Key>>,
	subject_subject_dependents:
		BTreeMap<tg::authorization::Subject, BTreeSet<tg::authorization::Subject>>,
}

pub(crate) struct FinalSearch {
	deferred: BTreeSet<Key>,
	outcomes: BTreeMap<Key, Outcome>,
	pending: VecDeque<Key>,
	queued: BTreeSet<Key>,
}

#[must_use]
pub(crate) fn process_node_permission(
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

impl ReadOutput {
	fn into_grants(self) -> tg::Result<(Option<Vec<u8>>, Vec<Grant>)> {
		let Self::Grants { after, grants } = self else {
			return Err(tg::error!("received a non-grant result for a grant read"));
		};

		Ok((after, grants))
	}

	fn into_group(self) -> tg::Result<Option<crate::group::Group>> {
		let Self::Group(group) = self else {
			return Err(tg::error!("received a non-group result for a group read"));
		};

		Ok(group)
	}

	fn into_ids(self) -> tg::Result<(Option<Vec<u8>>, Vec<tg::Id>)> {
		let Self::Ids { after, ids } = self else {
			return Err(tg::error!("received a non-ID result for an ID read"));
		};

		Ok((after, ids))
	}

	pub(crate) fn into_member_groups(self) -> tg::Result<(Option<Vec<u8>>, Vec<tg::group::Id>)> {
		let Self::MemberGroups { after, groups } = self else {
			return Err(tg::error!(
				"received a non-group result for a member group read"
			));
		};

		Ok((after, groups))
	}

	pub(crate) fn into_member_organizations(
		self,
	) -> tg::Result<(Option<Vec<u8>>, Vec<tg::organization::Id>)> {
		let Self::MemberOrganizations {
			after,
			organizations,
		} = self
		else {
			return Err(tg::error!(
				"received a non-organization result for a member organization read"
			));
		};

		Ok((after, organizations))
	}

	fn into_object_processes(
		self,
	) -> tg::Result<(
		Option<Vec<u8>>,
		Vec<(tg::process::Id, crate::process::object::Kind)>,
	)> {
		let Self::ObjectProcesses { after, processes } = self else {
			return Err(tg::error!(
				"received a non-process result for an object process read"
			));
		};

		Ok((after, processes))
	}

	pub(crate) fn into_process(self) -> tg::Result<Option<crate::process::Process>> {
		let Self::Process(process) = self else {
			return Err(tg::error!(
				"received a non-process result for a process read"
			));
		};

		Ok(process)
	}

	pub(crate) fn into_process_objects(
		self,
	) -> tg::Result<(
		Option<Vec<u8>>,
		Vec<(tg::object::Id, crate::process::object::Kind)>,
	)> {
		let Self::ProcessObjects { after, objects } = self else {
			return Err(tg::error!(
				"received a non-object result for a process object read"
			));
		};

		Ok((after, objects))
	}

	pub(crate) fn into_resolved(self) -> tg::Result<Option<(tg::Id, bool)>> {
		let Self::Resolved(resource) = self else {
			return Err(tg::error!(
				"received a non-resolution result for a resolution read"
			));
		};

		Ok(resource)
	}

	fn into_sandbox_owner(self) -> tg::Result<Option<tg::Principal>> {
		let Self::SandboxOwner(owner) = self else {
			return Err(tg::error!(
				"received a non-owner result for a sandbox owner read"
			));
		};

		Ok(owner)
	}

	fn into_tag(self) -> tg::Result<Option<crate::tag::Tag>> {
		let Self::Tag(tag) = self else {
			return Err(tg::error!("received a non-tag result for a tag read"));
		};

		Ok(tag)
	}

	fn into_tags(self) -> tg::Result<(Option<Vec<u8>>, Vec<tg::tag::Id>)> {
		let Self::Tags { after, tags } = self else {
			return Err(tg::error!("received a non-tag result for a tag list read"));
		};

		Ok((after, tags))
	}
}

impl Budget {
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

	fn add_root_total(&mut self, config: crate::authorize::SearchConfig, root_total: usize) {
		debug_assert_eq!(self.config.max_depth, config.max_depth);
		debug_assert_eq!(self.config.page_size, config.page_size);
		self.config.max_edges = self
			.config
			.max_edges
			.saturating_add(config.max_edges.saturating_mul(root_total));
		self.config.max_nodes = self
			.config
			.max_nodes
			.saturating_add(config.max_nodes.saturating_mul(root_total));
	}

	fn add_edge(&mut self) -> bool {
		self.add(1, 0, 0)
	}

	fn add_node(&mut self, depth: usize) -> bool {
		self.add(0, 1, depth)
	}

	fn add(&mut self, edges: usize, nodes: usize, depth: usize) -> bool {
		if (nodes > 0 && depth > self.config.max_depth)
			|| self.edges.saturating_add(edges) > self.config.max_edges
			|| self.nodes.saturating_add(nodes) > self.config.max_nodes
		{
			return false;
		}
		self.edges += edges;
		self.nodes += nodes;

		true
	}
}

impl AncestorOrDescendantSearch {
	#[must_use]
	pub(crate) fn new(
		config: crate::authorize::Config,
		principal: &tg::Principal,
		roots: &[Key],
		token: Option<&(tg::authorization::Body, tg::Id)>,
		state: &mut State,
	) -> Self {
		if let Ok(subject) = principal.try_to_subject() {
			state.authorize_subject(subject);
		}
		let mut seen = HashSet::new();
		let roots = roots
			.iter()
			.filter(|root| {
				state.ancestor_or_descendant(root) == Outcome::Pending
					&& seen.insert((*root).clone())
			})
			.cloned()
			.collect::<Vec<_>>();
		let complete = roots.is_empty();
		let (ancestor, descendant) = if complete {
			(None, None)
		} else {
			let ancestor =
				AncestorSearch::new(config.ancestor, principal, &roots, token.cloned(), state);
			let descendant = if let Some(mut descendant) = state.take_descendant() {
				descendant.add_targets(config.descendant, roots.clone());
				descendant
			} else {
				let token = token.map(|(body, resource)| (body, resource));
				DescendantSearch::new(config.descendant, principal, state, roots.clone(), token)
			};

			(Some(ancestor), Some(descendant))
		};

		Self {
			ancestor,
			ancestor_exhausted: HashSet::new(),
			complete,
			descendant,
			descendant_exhausted: HashSet::new(),
			next: Direction::Descendant,
			roots,
		}
	}

	#[must_use]
	pub(crate) fn complete(&self) -> bool {
		self.complete
	}

	#[must_use]
	pub(crate) fn outcome(&self, state: &State, root: &Key) -> Outcome {
		match state.ancestor_or_descendant(root) {
			Outcome::Pending
				if self.ancestor_exhausted.contains(root)
					&& self.descendant_exhausted.contains(root) =>
			{
				Outcome::Exhausted
			},
			outcome => outcome,
		}
	}

	pub(crate) fn take_reads(&mut self, state: &mut State, limit: usize) -> tg::Result<Vec<Read>> {
		assert!(limit > 0);
		loop {
			if self.complete {
				return Ok(Vec::new());
			}

			if self.roots.iter().all(|root| {
				state.ancestor_or_descendant(root) != Outcome::Pending
					|| (self.ancestor_exhausted.contains(root)
						&& self.descendant_exhausted.contains(root))
			}) {
				self.finish(state);

				return Ok(Vec::new());
			}

			let mut reads = Vec::new();
			let both = self.ancestor.is_some() && self.descendant.is_some();
			let direction = self.next;
			if both && limit == 1 {
				self.next = match direction {
					Direction::Ancestor => Direction::Descendant,
					Direction::Descendant => Direction::Ancestor,
				};
			}
			let descendant_limit = if both && limit == 1 {
				usize::from(direction == Direction::Descendant)
			} else if both {
				limit.div_ceil(2)
			} else if self.descendant.is_some() {
				limit
			} else {
				0
			};
			if descendant_limit > 0
				&& let Some(search) = &mut self.descendant
			{
				let descendant_reads = search.take_reads(state, descendant_limit);
				if descendant_reads.is_empty() {
					let mut search = self.descendant.take().unwrap();
					if search.finish(state) == Outcome::Exhausted {
						self.descendant_exhausted
							.extend(search.unresolved.iter().cloned());
					}
					search.reset_visited_if_complete();
					state.set_descendant(search);
				} else {
					reads.extend(descendant_reads);
				}
			}

			let remaining = limit - reads.len();
			if remaining > 0
				&& let Some(search) = &mut self.ancestor
			{
				let ancestor_reads = search.take_reads(state, remaining)?;
				if ancestor_reads.is_empty() {
					let mut search = self.ancestor.take().unwrap();
					search.finish(state);
					for root in &self.roots {
						if state.is_authorized(root) {
							continue;
						}
						if search.incomplete.contains(root) {
							self.ancestor_exhausted.insert(root.clone());
						} else {
							state.deny_ancestor_or_descendant(root);
						}
					}
				} else {
					reads.extend(ancestor_reads);
				}
			}

			if !reads.is_empty() {
				return Ok(reads);
			}
		}
	}

	pub(crate) fn apply(
		&mut self,
		state: &mut State,
		read: Read,
		output: ReadOutput,
	) -> tg::Result<()> {
		match read {
			read @ (Read::AncestorNode { .. }
			| Read::GroupMembers { .. }
			| Read::ObjectParents { .. }
			| Read::OrganizationMembers { .. }
			| Read::ProcessParents { .. }) => self
				.ancestor
				.as_mut()
				.ok_or_else(|| tg::error!("received a read after the ancestor search completed"))?
				.apply(state, read, output),
			read @ (Read::Member { .. }
			| Read::ObjectChildren { .. }
			| Read::OwnerSandboxes { .. }
			| Read::ProcessChildren { .. }
			| Read::ProcessGrants { .. }
			| Read::SandboxProcesses { .. }
			| Read::SubjectGrants { .. }) => self
				.descendant
				.as_mut()
				.ok_or_else(|| tg::error!("received a read after the descendant search completed"))?
				.apply(state, read, output),
			Read::Process { .. }
			| Read::ProcessObjects { .. }
			| Read::Resolve { .. }
			| Read::SubtreeObjectChildren { .. }
			| Read::SubtreeProcessChildren { .. } => Err(tg::error!(
				"received an invalid read for an ancestor or descendant search"
			)),
		}
	}

	fn finish(&mut self, state: &mut State) {
		self.ancestor = None;
		if let Some(mut descendant) = self.descendant.take() {
			descendant.reset_visited_if_complete();
			state.set_descendant(descendant);
		}
		self.complete = true;
	}
}

impl KeyEvaluation {
	#[must_use]
	fn new(permission: tg::authorization::Permission) -> Self {
		let derived = has_derived_proof(permission).then_some(ProofStatus::Pending);

		Self {
			ancestor_or_descendant: ProofStatus::Pending,
			authorized: false,
			derived,
		}
	}

	#[must_use]
	fn outcome(&self) -> Outcome {
		if self.authorized {
			return Outcome::Authorized;
		}
		let evaluations = [Some(self.ancestor_or_descendant), self.derived];
		if evaluations
			.into_iter()
			.flatten()
			.any(|evaluation| evaluation == ProofStatus::Pending)
		{
			return Outcome::Pending;
		}
		Outcome::Denied
	}

	#[must_use]
	fn ancestor_or_descendant(&self) -> Outcome {
		if self.authorized {
			return Outcome::Authorized;
		}

		match self.ancestor_or_descendant {
			ProofStatus::Denied => Outcome::Denied,
			ProofStatus::Pending => Outcome::Pending,
		}
	}
}

impl State {
	#[must_use]
	pub(crate) fn process_facts(&self, process: &tg::process::Id) -> Option<Arc<ProcessFacts>> {
		self.process_facts.get(process).cloned()
	}

	pub(crate) fn set_process_facts(
		&mut self,
		process: tg::process::Id,
		facts: ProcessFacts,
	) -> Arc<ProcessFacts> {
		let facts = Arc::new(facts);
		self.process_facts.insert(process, facts.clone());

		facts
	}

	#[must_use]
	pub(crate) fn ancestor_facts(&self, resource: &tg::Id) -> Option<Arc<AncestorNodeFacts>> {
		self.ancestor_facts.get(resource).cloned()
	}

	pub(crate) fn set_ancestor_facts(
		&mut self,
		resource: tg::Id,
		facts: AncestorNodeFacts,
	) -> Arc<AncestorNodeFacts> {
		let facts = Arc::new(facts);
		self.ancestor_facts.insert(resource, facts.clone());

		facts
	}

	pub(crate) fn complete_ancestor_node(&mut self, key: &Key) {
		self.ancestor_nodes.insert(key.clone());
	}

	pub(crate) fn complete_ancestor_parents(&mut self, key: &Key) {
		self.ancestor_complete.insert(key.clone());
		self.ancestor_cursors.remove(key);
	}

	#[must_use]
	pub(crate) fn ancestor_cursor(&self, key: &Key) -> Option<Vec<u8>> {
		self.ancestor_cursors.get(key).cloned()
	}

	#[must_use]
	pub(crate) fn ancestor_node_is_complete(&self, key: &Key) -> bool {
		self.ancestor_nodes.contains(key)
	}

	#[must_use]
	pub(crate) fn ancestor_parents_are_complete(&self, key: &Key) -> bool {
		self.ancestor_complete.contains(key)
	}

	pub(crate) fn set_ancestor_cursor(&mut self, key: &Key, cursor: &[u8]) {
		if !self.ancestor_complete.contains(key) {
			self.ancestor_cursors.insert(key.clone(), cursor.to_vec());
		}
	}

	#[must_use]
	pub(crate) fn has_membership_dependency(
		&self,
		member: &tg::authorization::Subject,
		container: &tg::authorization::Subject,
	) -> bool {
		self.subject_subject_dependents
			.get(member)
			.is_some_and(|containers| containers.contains(container))
	}

	pub(crate) fn add_membership_dependency(
		&mut self,
		member: &tg::authorization::Subject,
		container: tg::authorization::Subject,
	) -> bool {
		let inserted = self
			.subject_subject_dependents
			.entry(member.clone())
			.or_default()
			.insert(container.clone());
		if self.is_subject_authorized(member) {
			self.authorize_subject(container);
		}

		inserted
	}

	#[must_use]
	pub(crate) fn has_subject_dependency(
		&self,
		subject: &tg::authorization::Subject,
		dependent: &Key,
	) -> bool {
		self.subject_key_dependents
			.get(subject)
			.is_some_and(|dependents| dependents.contains(dependent))
	}

	pub(crate) fn add_subject_dependency(
		&mut self,
		subject: &tg::authorization::Subject,
		dependent: Key,
	) -> bool {
		let inserted = self
			.subject_key_dependents
			.entry(subject.clone())
			.or_default()
			.insert(dependent.clone());
		if self.is_subject_authorized(subject) {
			self.authorize(dependent);
		}

		inserted
	}

	pub(crate) fn authorize_subject(&mut self, subject: tg::authorization::Subject) {
		let mut stack = vec![subject];
		while let Some(subject) = stack.pop() {
			if !self.authorized_subjects.insert(subject.clone()) {
				continue;
			}
			let dependents = self
				.subject_key_dependents
				.get(&subject)
				.map_or_else(Vec::new, |dependents| dependents.iter().cloned().collect());
			for dependent in dependents {
				self.authorize(dependent);
			}
			stack.extend(
				self.subject_subject_dependents
					.get(&subject)
					.into_iter()
					.flatten()
					.cloned(),
			);
		}
	}

	#[must_use]
	pub(crate) fn is_subject_authorized(&self, subject: &tg::authorization::Subject) -> bool {
		self.authorized_subjects.contains(subject)
	}

	#[must_use]
	pub(crate) fn has_authorization_dependency(&self, dependency: &Key, dependent: &Key) -> bool {
		self.authorization_dependents
			.get(dependency)
			.is_some_and(|dependents| dependents.contains(dependent))
	}

	pub(crate) fn add_authorization_dependency(
		&mut self,
		dependency: &Key,
		dependent: Key,
	) -> bool {
		let inserted = self
			.authorization_dependents
			.entry(dependency.clone())
			.or_default()
			.insert(dependent.clone());
		if inserted {
			self.authorization_dependencies
				.entry(dependent.clone())
				.or_default()
				.insert(dependency.clone());
		}
		if self.is_authorized(dependency) {
			self.authorize(dependent);
		}

		inserted
	}

	pub(crate) fn add_derived_dependency(&mut self, dependency: &Key, dependent: Key) {
		let inserted = self
			.derived_dependents
			.entry(dependency.clone())
			.or_default()
			.insert(dependent.clone());
		if inserted {
			self.derived_dependencies
				.entry(dependent.clone())
				.or_default()
				.insert(dependency.clone());
			if !self.is_authorized(dependency) {
				let unresolved = self
					.derived_unresolved
					.entry(dependent.clone())
					.or_default();
				*unresolved = unresolved.saturating_add(1);
			}
		}
		self.propagate_derived_outcome(dependency);
		self.try_authorize_derived(dependent);
	}

	pub(crate) fn authorize_derived(&mut self, key: Key) {
		self.authorize(key);
	}

	pub(crate) fn authorize_ancestor_or_descendant(&mut self, key: Key) {
		self.authorize(key);
	}

	pub(crate) fn complete_derived(&mut self, key: &Key) {
		self.derived_cursors.remove(key);
		self.derived_complete.insert(key.clone());
		self.try_authorize_derived(key.clone());
	}

	#[must_use]
	pub(crate) fn derived_children(&self, key: &Key) -> Vec<Key> {
		self.derived_dependencies
			.get(key)
			.map_or_else(Vec::new, |dependencies| {
				dependencies
					.iter()
					.filter(|dependency| dependency.1 == key.1)
					.cloned()
					.collect()
			})
	}

	#[must_use]
	pub(crate) fn derived_cursor(&self, key: &Key) -> Option<Vec<u8>> {
		self.derived_cursors.get(key).cloned()
	}

	#[must_use]
	pub(crate) fn derived_is_complete(&self, key: &Key) -> bool {
		self.derived_complete.contains(key)
	}

	pub(crate) fn set_derived_cursor(&mut self, key: &Key, cursor: &[u8]) {
		if !self.derived_complete.contains(key) {
			self.derived_cursors.insert(key.clone(), cursor.to_vec());
		}
	}

	pub(crate) fn deny_derived(&mut self, key: &Key) {
		if self.is_authorized(key) {
			return;
		}
		if self.evaluation_mut(key).derived == Some(ProofStatus::Denied) {
			return;
		}
		self.evaluation_mut(key).derived = Some(ProofStatus::Denied);
		self.newly_evaluated.insert(key.clone());
		self.propagate_derived_outcome(key);
	}

	pub(crate) fn deny_ancestor_or_descendant(&mut self, key: &Key) {
		if self.is_authorized(key)
			|| self.evaluation_mut(key).ancestor_or_descendant == ProofStatus::Denied
		{
			return;
		}
		self.evaluation_mut(key).ancestor_or_descendant = ProofStatus::Denied;
		self.newly_evaluated.insert(key.clone());
		self.propagate_derived_outcome(key);
	}

	#[must_use]
	pub(crate) fn is_authorized(&self, key: &Key) -> bool {
		self.evaluations
			.get(key)
			.is_some_and(|evaluation| evaluation.authorized)
	}

	#[must_use]
	pub(crate) fn authorization_dependents(&self, key: &Key) -> Vec<Key> {
		self.authorization_dependents
			.get(key)
			.map_or_else(Vec::new, |dependents| dependents.iter().cloned().collect())
	}

	#[must_use]
	pub(crate) fn authorization_dependencies(&self, key: &Key) -> Vec<Key> {
		self.authorization_dependencies
			.get(key)
			.map_or_else(Vec::new, |dependencies| {
				dependencies.iter().cloned().collect()
			})
	}

	#[must_use]
	pub(crate) fn outcome(&self, key: &Key) -> Outcome {
		self.evaluations
			.get(key)
			.map_or(Outcome::Pending, KeyEvaluation::outcome)
	}

	#[must_use]
	pub(crate) fn ancestor_or_descendant(&self, key: &Key) -> Outcome {
		self.evaluations
			.get(key)
			.map_or(Outcome::Pending, KeyEvaluation::ancestor_or_descendant)
	}

	pub(crate) fn take_changed(&mut self) -> BTreeSet<Key> {
		let mut changed = std::mem::take(&mut self.newly_evaluated);
		let mut stack = changed.iter().cloned().collect::<Vec<_>>();
		while let Some(key) = stack.pop() {
			let Some(dependents) = self.derived_dependents.get(&key) else {
				continue;
			};
			for dependent in dependents {
				if changed.insert(dependent.clone()) {
					stack.push(dependent.clone());
				}
			}
		}

		changed
	}

	#[must_use]
	fn authorization_revision(&self) -> usize {
		self.authorization_log.len()
	}

	fn authorization_changes_since(&self, revision: &mut usize) -> Vec<Key> {
		let changes = self.authorization_log[*revision..].to_vec();
		*revision = self.authorization_log.len();

		changes
	}

	fn take_descendant(&mut self) -> Option<DescendantSearch> {
		self.descendant.take()
	}

	fn set_descendant(&mut self, descendant: DescendantSearch) {
		self.descendant = Some(descendant);
	}

	fn authorize(&mut self, key: Key) {
		let mut stack = vec![key];
		let mut visited = HashSet::new();
		while let Some(key) = stack.pop() {
			if !visited.insert(key.clone()) {
				continue;
			}
			let evaluation = self.evaluation_mut(&key);
			if evaluation.authorized {
				continue;
			}
			evaluation.authorized = true;
			self.authorization_log.push(key.clone());
			self.newly_evaluated.insert(key.clone());
			stack.extend(
				crate::authorize::permissions_implied_by(key.1)
					.into_iter()
					.filter(|permission| *permission != key.1)
					.map(|permission| (key.0.clone(), permission)),
			);
			if let Some(dependents) = self.authorization_dependents.get(&key) {
				stack.extend(dependents.iter().cloned());
			}
			let derived = self
				.derived_dependents
				.get(&key)
				.map_or_else(Vec::new, |dependents| dependents.iter().cloned().collect());
			for dependent in derived {
				let unresolved = self
					.derived_unresolved
					.entry(dependent.clone())
					.or_default();
				*unresolved = unresolved.saturating_sub(1);
				if self.derived_is_authorized(&dependent) {
					stack.push(dependent);
				}
			}
		}
	}

	fn evaluation_mut(&mut self, key: &Key) -> &mut KeyEvaluation {
		self.evaluations
			.entry(key.clone())
			.or_insert_with(|| KeyEvaluation::new(key.1))
	}

	#[must_use]
	fn derived_is_authorized(&self, key: &Key) -> bool {
		self.derived_complete.contains(key)
			&& self.derived_unresolved.get(key).copied().unwrap_or(0) == 0
			&& self
				.evaluations
				.get(key)
				.and_then(|evaluation| evaluation.derived)
				!= Some(ProofStatus::Denied)
	}

	fn propagate_derived_outcome(&mut self, key: &Key) {
		let mut stack = vec![key.clone()];
		while let Some(key) = stack.pop() {
			if self.outcome(&key) != Outcome::Denied {
				continue;
			}
			let dependents = self
				.derived_dependents
				.get(&key)
				.map_or_else(Vec::new, |dependents| {
					dependents.iter().cloned().collect::<Vec<_>>()
				});
			for dependent in dependents {
				if self.is_authorized(&dependent) {
					continue;
				}
				let evaluation = self.evaluation_mut(&dependent);
				let changed = evaluation.derived != Some(ProofStatus::Denied);
				if changed {
					evaluation.derived = Some(ProofStatus::Denied);
					self.newly_evaluated.insert(dependent.clone());
				}
				if changed {
					stack.push(dependent);
				}
			}
		}
	}

	fn try_authorize_derived(&mut self, key: Key) {
		if !self.is_authorized(&key) && self.derived_is_authorized(&key) {
			self.authorize(key);
		}
	}
}

impl FinalSearch {
	#[must_use]
	pub(crate) fn new(roots: impl IntoIterator<Item = Key>) -> Self {
		let mut pending = VecDeque::new();
		let mut queued = BTreeSet::new();
		for root in roots {
			if queued.insert(root.clone()) {
				pending.push_back(root);
			}
		}

		Self {
			deferred: BTreeSet::new(),
			outcomes: BTreeMap::new(),
			pending,
			queued,
		}
	}

	pub(crate) fn next(&mut self, state: &mut State) -> Option<Key> {
		self.enqueue_changed(state, None);
		while let Some(key) = self.pending.pop_front() {
			self.queued.remove(&key);
			match state.outcome(&key) {
				outcome @ (Outcome::Authorized | Outcome::Denied) => {
					self.deferred.remove(&key);
					self.outcomes.insert(key, outcome);
				},
				Outcome::Exhausted => unreachable!(),
				Outcome::Pending => return Some(key),
			}
		}

		None
	}

	pub(crate) fn apply(&mut self, state: &mut State, key: &Key, outcome: Outcome) {
		let outcome = match state.outcome(key) {
			outcome @ (Outcome::Authorized | Outcome::Denied) => outcome,
			Outcome::Exhausted => unreachable!(),
			Outcome::Pending => outcome,
		};
		self.outcomes.insert(key.clone(), outcome);
		match outcome {
			Outcome::Authorized | Outcome::Denied => {
				self.deferred.remove(key);
			},
			Outcome::Exhausted | Outcome::Pending => {
				self.deferred.insert(key.clone());
			},
		}
		self.enqueue_changed(state, Some(key));
	}

	#[must_use]
	pub(crate) fn outcome(&self, state: &State, key: &Key) -> Outcome {
		match state.outcome(key) {
			outcome @ (Outcome::Authorized | Outcome::Denied) => outcome,
			Outcome::Exhausted => unreachable!(),
			Outcome::Pending => self
				.outcomes
				.get(key)
				.copied()
				.unwrap_or(Outcome::Exhausted),
		}
	}

	#[must_use]
	pub(crate) fn permissions(
		&self,
		state: &State,
		resource: &tg::Id,
		permissions: tg::authorization::permission::Set,
	) -> (
		tg::authorization::permission::Set,
		tg::authorization::permission::Set,
	) {
		let mut authorized = permissions.empty_like();
		let mut exhausted = permissions.empty_like();
		for permission in crate::authorize::permissions_in_search_order(permissions) {
			let key = (resource.clone(), permission);
			match self.outcome(state, &key) {
				Outcome::Authorized => {
					crate::authorize::insert_implied_permissions(
						&mut authorized,
						permissions,
						permission,
					);
					if authorized.contains(permissions) {
						break;
					}
				},
				Outcome::Denied => {},
				Outcome::Exhausted | Outcome::Pending => {
					exhausted.insert(tg::authorization::permission::Set::from_permission(
						permission,
					));
				},
			}
		}

		(authorized, exhausted)
	}

	fn enqueue_changed(&mut self, state: &mut State, current: Option<&Key>) {
		for key in state.take_changed() {
			if current == Some(&key)
				|| !self.deferred.remove(&key)
				|| !self.queued.insert(key.clone())
			{
				continue;
			}
			self.pending.push_back(key);
		}
	}
}

#[must_use]
fn has_derived_proof(permission: tg::authorization::Permission) -> bool {
	matches!(
		permission,
		tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree
		) | tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeCommand
				| tg::authorization::permission::process::Permission::NodeError
				| tg::authorization::permission::process::Permission::NodeLog
				| tg::authorization::permission::process::Permission::NodeOutput
				| tg::authorization::permission::process::Permission::Subtree
				| tg::authorization::permission::process::Permission::SubtreeCommand
				| tg::authorization::permission::process::Permission::SubtreeError
				| tg::authorization::permission::process::Permission::SubtreeLog
				| tg::authorization::permission::process::Permission::SubtreeOutput
		)
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ancestor_and_descendant_searches_share_a_read_batch() {
		let root = key();
		let principal = tg::Principal::User(tg::user::Id::new());
		let mut state = State::default();
		let mut search = AncestorOrDescendantSearch::new(
			crate::authorize::Config::default(),
			&principal,
			std::slice::from_ref(&root),
			None,
			&mut state,
		);

		let reads = search.take_reads(&mut state, 2).unwrap();

		assert!(
			reads
				.iter()
				.any(|read| matches!(read, Read::AncestorNode { .. }))
		);
		assert!(
			reads
				.iter()
				.any(|read| !matches!(read, Read::AncestorNode { .. }))
		);
	}

	#[test]
	fn ancestor_and_descendant_searches_alternate_with_one_read() {
		let root = key();
		let principal = tg::Principal::User(tg::user::Id::new());
		let mut state = State::default();
		let mut search = AncestorOrDescendantSearch::new(
			crate::authorize::Config::default(),
			&principal,
			std::slice::from_ref(&root),
			None,
			&mut state,
		);
		let mut reads = search.take_reads(&mut state, 1).unwrap();
		let read = reads.pop().unwrap();
		let output = match &read {
			Read::Member {
				read: MemberRead::Groups { .. },
				..
			} => ReadOutput::MemberGroups {
				after: None,
				groups: Vec::new(),
			},
			Read::Member {
				read: MemberRead::Organizations { .. },
				..
			} => ReadOutput::MemberOrganizations {
				after: None,
				organizations: Vec::new(),
			},
			Read::OwnerSandboxes { .. } => ReadOutput::Ids {
				after: None,
				ids: Vec::new(),
			},
			Read::SubjectGrants { .. } => ReadOutput::Grants {
				after: None,
				grants: Vec::new(),
			},
			_ => panic!("expected a descendant read"),
		};
		search.apply(&mut state, read, output).unwrap();

		let reads = search.take_reads(&mut state, 1).unwrap();

		assert!(matches!(reads.as_slice(), [Read::AncestorNode { .. }]));
	}

	#[test]
	fn an_ancestor_grant_page_authorizes_before_the_node_is_complete() {
		let root = key();
		let mut state = State::default();
		let mut search = AncestorOrDescendantSearch::new(
			crate::authorize::Config::default(),
			&tg::Principal::Anonymous,
			std::slice::from_ref(&root),
			None,
			&mut state,
		);
		let mut reads = search.take_reads(&mut state, 1).unwrap();
		let read = reads.pop().unwrap();
		assert!(matches!(
			&read,
			Read::AncestorNode {
				read: AncestorNodeRead::ResourceGrants { .. },
				..
			}
		));
		let grant = Grant {
			creator: None,
			implicit: false,
			permission: root.1,
			resource: root.0.clone(),
			subject: tg::authorization::Subject::Public,
		};
		let output = ReadOutput::Grants {
			after: None,
			grants: vec![grant],
		};
		search.apply(&mut state, read, output).unwrap();

		assert!(state.is_authorized(&root));
	}

	#[test]
	fn authorization_propagates_across_an_edge_added_after_the_proof() {
		let dependency = key();
		let dependent = key();
		let mut state = State::default();
		state.authorize_ancestor_or_descendant(dependency.clone());

		state.add_authorization_dependency(&dependency, dependent.clone());

		assert!(state.is_authorized(&dependent));
	}

	#[test]
	fn a_completed_derived_conjunction_authorizes_after_its_last_dependency() {
		let child = subtree_key(0);
		let parent = subtree_key(1);
		let node = (
			parent.0.clone(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			),
		);
		let mut state = State::default();
		state.add_derived_dependency(&child, parent.clone());
		state.add_derived_dependency(&node, parent.clone());
		state.complete_derived(&parent);
		state.authorize_ancestor_or_descendant(node);
		assert!(!state.is_authorized(&parent));

		state.authorize_derived(child);

		assert!(state.is_authorized(&parent));
	}

	#[test]
	fn a_final_search_preserves_permission_order() {
		let subtree = subtree_key(0);
		let node = (
			subtree.0.clone(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			),
		);
		let mut search = FinalSearch::new([subtree.clone(), node]);
		let mut state = State::default();

		assert_eq!(search.next(&mut state), Some(subtree));
	}

	#[test]
	fn a_final_search_retries_an_exhausted_dependent_after_a_proof() {
		let mut keys = [subtree_key(0), subtree_key(1)];
		keys.sort();
		let [parent, child] = keys;
		let mut state = State::default();
		state.deny_ancestor_or_descendant(&child);
		state.deny_ancestor_or_descendant(&parent);
		state.add_derived_dependency(&child, parent.clone());
		let mut search = FinalSearch::new([parent.clone(), child.clone()]);

		assert_eq!(search.next(&mut state), Some(parent.clone()));
		search.apply(&mut state, &parent, Outcome::Exhausted);
		assert_eq!(search.next(&mut state), Some(child.clone()));
		state.authorize_derived(child.clone());
		search.apply(&mut state, &child, Outcome::Authorized);

		assert_eq!(search.next(&mut state), Some(parent.clone()));
		state.authorize_derived(parent.clone());
		search.apply(&mut state, &parent, Outcome::Authorized);
		assert_eq!(search.next(&mut state), None);
		assert_eq!(search.outcome(&state, &parent), Outcome::Authorized);
	}

	#[test]
	fn incomplete_derived_evaluation_does_not_propagate() {
		let dependency = subtree_key(0);
		let dependent = subtree_key(1);
		let mut state = State::default();
		state.deny_ancestor_or_descendant(&dependency);
		state.deny_ancestor_or_descendant(&dependent);

		state.add_derived_dependency(&dependency, dependent.clone());

		assert_eq!(state.outcome(&dependent), Outcome::Pending);
	}

	#[test]
	fn derived_denial_waits_for_the_ancestor_or_descendant_evaluation() {
		let dependency = subtree_key(0);
		let dependent = subtree_key(1);
		let mut state = State::default();
		state.deny_ancestor_or_descendant(&dependent);
		state.add_derived_dependency(&dependency, dependent.clone());

		state.deny_derived(&dependency);

		assert_eq!(state.outcome(&dependent), Outcome::Pending);

		state.deny_ancestor_or_descendant(&dependency);

		assert_eq!(state.outcome(&dependent), Outcome::Denied);
	}

	#[test]
	fn a_denial_propagates_through_a_derived_diamond() {
		let source = subtree_key(0);
		let denied_path = subtree_key(1);
		let second_path = subtree_key(2);
		let parent = subtree_key(3);
		let root = subtree_key(4);
		let mut state = State::default();
		for key in [&source, &denied_path, &parent, &root] {
			state.deny_ancestor_or_descendant(key);
		}
		state.deny_ancestor_or_descendant(&second_path);
		state.add_derived_dependency(&source, denied_path.clone());
		state.add_derived_dependency(&source, second_path.clone());
		state.add_derived_dependency(&denied_path, parent.clone());
		state.add_derived_dependency(&second_path, parent.clone());
		state.add_derived_dependency(&parent, root.clone());

		state.deny_derived(&source);

		assert_eq!(state.outcome(&root), Outcome::Denied);
	}

	fn key() -> Key {
		let resource = tg::Id::from(tg::user::Id::new());
		let permission = tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Read,
		);

		(resource, permission)
	}

	fn subtree_key(value: u8) -> Key {
		let resource = tg::Id::from(tg::object::Id::new(
			tg::object::Kind::Blob,
			&vec![value].into(),
		));
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);

		(resource, permission)
	}
}
