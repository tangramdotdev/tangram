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
mod sync;

pub(crate) use {
	crate::permission::Fact as Permission,
	subtree::{Action as SubtreeAction, Search as SubtreeSearch},
};

// A missing subject uses the caller; delegation sources carry their own verification context.
pub(crate) type Key = (
	tg::Id,
	tg::authorization::Permission,
	Option<tg::authorization::Subject>,
);

#[derive(Clone, Debug)]
pub(crate) struct AncestorCandidate {
	checks: Vec<super::Check>,
	dependency: Key,
	edges: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct AncestorChecks {
	candidates: Vec<AncestorCandidate>,
	dependent: Key,
	depth: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct DescendantCandidate {
	edges: usize,
	neighbor: Key,
	proofs: Vec<Vec<super::Check>>,
}

#[derive(Clone, Debug)]
pub(crate) struct DescendantChecks {
	candidates: Vec<DescendantCandidate>,
	depth: usize,
	fallback: DescendantFallback,
	source: Key,
}

#[derive(Clone, Debug)]
pub(crate) enum DescendantFallback {
	None,
	ObjectChildren {
		object: tg::object::Id,
	},
	ProcessChildren {
		permission: tg::authorization::permission::process::Permission,
		process: tg::process::Id,
	},

	ProcessObjects {
		after: Option<Vec<u8>>,
		permission: tg::authorization::permission::process::Permission,
		process: tg::process::Id,
	},
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AncestorNodeFacts {
	pub delegations: Vec<crate::delegation::put::Arg>,
	pub permissions: Vec<Permission>,
	pub object_processes: Vec<(tg::process::Id, crate::process::object::Kind)>,
	pub parent: Option<tg::Id>,
	pub sandbox_owner: Option<tg::Principal>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ProcessFacts {
	pub objects: Vec<(tg::object::Id, crate::process::object::Kind)>,
	pub process: Option<crate::process::Process>,
}

#[derive(Clone, Debug)]
pub(crate) enum AncestorNodeRead {
	Delegations {
		after: Option<Vec<u8>>,
		limit: usize,
		resource: tg::Id,
	},

	Group {
		group: tg::group::Id,
	},
	ObjectProcesses {
		after: Option<Vec<u8>>,
		limit: usize,
		object: tg::object::Id,
	},
	ResourcePermissions {
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
}

#[derive(Clone, Debug)]
pub(crate) enum MemberRead {
	Groups { after: Option<Vec<u8>> },
	Organizations { after: Option<Vec<u8>> },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum DelegationRead {
	Delegations,
	ObjectParents,
	ObjectProcesses,
	ProcessParents,
}

#[derive(Clone, Debug)]
pub(crate) enum Read {
	AncestorChecks(AncestorChecks),
	AncestorNode {
		depth: usize,
		key: Key,
		read: AncestorNodeRead,
	},
	Delegation {
		after: Option<Vec<u8>>,
		dependent: Key,
		depth: usize,
		limit: usize,
		read: DelegationRead,
		resource: tg::Id,
	},
	DescendantChecks(DescendantChecks),
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
	ProcessObjectChildren {
		after: Option<Vec<u8>>,
		depth: usize,
		limit: usize,
		permission: tg::authorization::permission::process::Permission,
		process: tg::process::Id,
	},
	ProcessObjectDescendants {
		after: Option<Vec<u8>>,
		depth: usize,
		limit: usize,
		object: tg::object::Id,
		source: Key,
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
	StorageChildren {
		after: Option<Vec<u8>>,
		depth: usize,
		key: Key,
		limit: usize,
		objects: bool,
	},

	SubjectPermissions {
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
	Delegations {
		after: Option<Vec<u8>>,
		delegations: Vec<crate::delegation::put::Arg>,
	},

	Bools(Vec<bool>),
	Permissions {
		after: Option<Vec<u8>>,
		permissions: Vec<Permission>,
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
	Missing,
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
	Verified,
	Denied,
	Exhausted,
	Pending,
}

struct Budget {
	config: crate::verify::SearchConfig,
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
	// A proof from either evaluation verifys the key for every search strategy.
	ancestor_or_descendant: ProofStatus,
	verified: bool,
	derived: Option<ProofStatus>,
	expires_at: i64,
}

#[derive(Default)]
pub(crate) struct State {
	// Retain admitted proof edges, evaluations, and traversal cursors, never database pages.
	ancestor_complete: BTreeSet<Key>,
	ancestor_cursors: BTreeMap<Key, Vec<u8>>,
	ancestor_facts: HashMap<tg::Id, Arc<AncestorNodeFacts>>,
	ancestor_nodes: BTreeSet<Key>,
	verification_conjunctions: BTreeMap<Key, BTreeSet<(Key, Key)>>,
	verification_dependencies: BTreeMap<Key, BTreeSet<Key>>,
	verification_expirations: BTreeMap<(Key, Key), i64>,
	verification_dependents: BTreeMap<Key, BTreeSet<Key>>,
	verification_log: Vec<Key>,
	verified_subjects: BTreeSet<(
		Option<tg::authorization::Subject>,
		tg::authorization::Subject,
	)>,
	derived_complete: BTreeSet<Key>,
	derived_cursors: BTreeMap<Key, Vec<u8>>,
	derived_dependencies: BTreeMap<Key, BTreeSet<Key>>,
	derived_dependents: BTreeMap<Key, BTreeSet<Key>>,
	derived_unresolved: BTreeMap<Key, usize>,
	descendant: Option<DescendantSearch>,
	evaluations: HashMap<Key, KeyEvaluation>,
	newly_evaluated: BTreeSet<Key>,
	process_facts: HashMap<tg::process::Id, Arc<ProcessFacts>>,
	process_parent_delegation: bool,
	pub(crate) storage: BTreeSet<Key>,
	pub(crate) storage_children: BTreeSet<Key>,
	pub(crate) storage_exhausted: BTreeSet<Key>,
	sync_access: BTreeSet<Key>,
	sync_dependents: BTreeMap<Key, BTreeSet<Key>>,
	sync_reads: BTreeMap<Key, BTreeSet<(Key, tg::sync::Id)>>,
	syncs: BTreeMap<Key, BTreeSet<crate::verify::Sync>>,

	token_subject: Option<tg::authorization::Subject>,
	subject_key_dependents: BTreeMap<
		(
			Option<tg::authorization::Subject>,
			tg::authorization::Subject,
		),
		BTreeSet<Key>,
	>,
	subject_subject_dependents: BTreeMap<
		(
			Option<tg::authorization::Subject>,
			tg::authorization::Subject,
		),
		BTreeSet<tg::authorization::Subject>,
	>,
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
		tg::authorization::permission::process::Permission::SubtreeCommandObjects => {
			tg::authorization::permission::process::Permission::NodeCommandObjects
		},
		tg::authorization::permission::process::Permission::SubtreeErrorObjects => {
			tg::authorization::permission::process::Permission::NodeErrorObjects
		},
		tg::authorization::permission::process::Permission::SubtreeLogObjects => {
			tg::authorization::permission::process::Permission::NodeLogObjects
		},
		tg::authorization::permission::process::Permission::SubtreeOutputObjects => {
			tg::authorization::permission::process::Permission::NodeOutputObjects
		},
		_ => unreachable!(),
	}
}

impl AncestorChecks {
	#[must_use]
	pub(crate) fn candidates(&self) -> &[AncestorCandidate] {
		&self.candidates
	}
}

impl AncestorCandidate {
	#[must_use]
	pub(crate) fn checks(&self) -> &[super::Check] {
		&self.checks
	}
}

impl DescendantChecks {
	#[must_use]
	pub(crate) fn candidates(&self) -> &[DescendantCandidate] {
		&self.candidates
	}
}

impl DescendantCandidate {
	#[must_use]
	pub(crate) fn proofs(&self) -> &[Vec<super::Check>] {
		&self.proofs
	}
}

impl ReadOutput {
	fn into_bools(self) -> tg::Result<Vec<bool>> {
		let Self::Bools(values) = self else {
			return Err(tg::error!("received a non-boolean result for checks"));
		};

		Ok(values)
	}

	fn into_permissions(self) -> tg::Result<(Option<Vec<u8>>, Vec<Permission>)> {
		let Self::Permissions { after, permissions } = self else {
			return Err(tg::error!(
				"received a non-permission result for a permission read"
			));
		};

		Ok((after, permissions))
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
}

impl Budget {
	#[must_use]
	fn new(config: crate::verify::SearchConfig) -> Self {
		Self {
			config,
			edges: 0,
			nodes: 0,
		}
	}

	#[must_use]
	fn with_root_total(mut config: crate::verify::SearchConfig, root_total: usize) -> Self {
		config.max_edges = config.max_edges.saturating_mul(root_total);
		config.max_nodes = config.max_nodes.saturating_mul(root_total);

		Self::new(config)
	}

	fn add_root_total(&mut self, config: crate::verify::SearchConfig, root_total: usize) {
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
		config: crate::verify::Config,
		principal: &tg::Principal,
		roots: &[Key],
		tokens: &[tg::authorization::Body],
		state: &mut State,
	) -> Self {
		if let Ok(subject) = principal.try_to_subject() {
			state.verify_subject(subject, None);
		}
		let mut seen = HashSet::new();
		let roots = roots
			.iter()
			.filter(|root| {
				state.search_outcome(root) == Outcome::Pending && seen.insert((*root).clone())
			})
			.cloned()
			.collect::<Vec<_>>();
		let complete = roots.is_empty();
		let (ancestor, descendant) = if complete {
			(None, None)
		} else {
			let ancestor = AncestorSearch::new(
				config.permissions.ancestor,
				principal,
				&roots,
				tokens.to_vec(),
				state,
			);
			let descendant = if let Some(mut descendant) = state.take_descendant() {
				descendant.add_targets(config.permissions.descendant, roots.clone());
				descendant
			} else {
				DescendantSearch::new(
					config.permissions.descendant,
					principal,
					state,
					roots.clone(),
					tokens,
				)
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
				(state.ancestor_or_descendant(root) != Outcome::Pending
					&& (!state.storage.contains(root) || self.ancestor.is_none()))
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
					for key in &search.incomplete {
						state.exhaust_syncs(key);
					}
					for root in &self.roots {
						if state.is_verified(root) {
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
			read @ (Read::AncestorChecks(_)
			| Read::AncestorNode { .. }
			| Read::Delegation { .. }
			| Read::GroupMembers { .. }
			| Read::ObjectParents { .. }
			| Read::OrganizationMembers { .. }
			| Read::ProcessParents { .. }
			| Read::StorageChildren { .. }) => self
				.ancestor
				.as_mut()
				.ok_or_else(|| tg::error!("received a read after the ancestor search completed"))?
				.apply(state, read, output),
			read @ (Read::DescendantChecks(_)
			| Read::Member { .. }
			| Read::ObjectChildren { .. }
			| Read::OwnerSandboxes { .. }
			| Read::ProcessChildren { .. }
			| Read::ProcessObjectChildren { .. }
			| Read::ProcessObjectDescendants { .. }
			| Read::SubjectPermissions { .. }) => self
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
			verified: false,
			derived,
			expires_at: i64::MIN,
		}
	}

	#[must_use]
	fn outcome(&self) -> Outcome {
		if self.verified {
			return Outcome::Verified;
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
		if self.verified {
			return Outcome::Verified;
		}

		match self.ancestor_or_descendant {
			ProofStatus::Denied => Outcome::Denied,
			ProofStatus::Pending => Outcome::Pending,
		}
	}
}

impl State {
	pub(crate) fn set_token_subject(&mut self, subject: Option<tg::authorization::Subject>) {
		self.token_subject = subject;
	}

	#[must_use]
	pub(crate) fn token_subject(&self) -> Option<&tg::authorization::Subject> {
		self.token_subject.as_ref()
	}

	#[must_use]
	pub(crate) fn process_parent_delegation(&self) -> bool {
		self.process_parent_delegation
	}

	pub(crate) fn set_process_parent_delegation(&mut self, value: bool) {
		self.process_parent_delegation = value;
	}

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

	pub(crate) fn has_graph_scopes(&self) -> bool {
		self.ancestor_facts
			.values()
			.any(|facts| !facts.delegations.is_empty() || !facts.object_processes.is_empty())
	}

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
		scope: Option<&tg::authorization::Subject>,
	) -> bool {
		self.subject_subject_dependents
			.get(&(scope.cloned(), member.clone()))
			.is_some_and(|containers| containers.contains(container))
	}

	pub(crate) fn add_membership_dependency(
		&mut self,
		member: &tg::authorization::Subject,
		container: tg::authorization::Subject,
		scope: Option<&tg::authorization::Subject>,
	) -> bool {
		let inserted = self
			.subject_subject_dependents
			.entry((scope.cloned(), member.clone()))
			.or_default()
			.insert(container.clone());
		if self.is_subject_verified(member, scope) {
			self.verify_subject(container, scope);
		}
		inserted
	}

	pub(crate) fn has_subject_dependency(
		&self,
		subject: &tg::authorization::Subject,
		dependent: &Key,
	) -> bool {
		self.subject_key_dependents
			.get(&(dependent.2.clone(), subject.clone()))
			.is_some_and(|dependents| dependents.contains(dependent))
	}

	pub(crate) fn add_subject_dependency(
		&mut self,
		subject: &tg::authorization::Subject,
		dependent: Key,
	) -> bool {
		let inserted = self
			.subject_key_dependents
			.entry((dependent.2.clone(), subject.clone()))
			.or_default()
			.insert(dependent.clone());
		if self.is_subject_verified(subject, dependent.2.as_ref()) {
			self.verify_with_expiration(dependent, i64::MAX);
		}
		inserted
	}

	pub(crate) fn verify_subject(
		&mut self,
		subject: tg::authorization::Subject,
		scope: Option<&tg::authorization::Subject>,
	) {
		let mut stack = vec![subject];
		while let Some(subject) = stack.pop() {
			let subject_key = (scope.cloned(), subject);
			if !self.verified_subjects.insert(subject_key.clone()) {
				continue;
			}
			let dependents = self
				.subject_key_dependents
				.get(&subject_key)
				.map_or_else(Vec::new, |dependents| dependents.iter().cloned().collect());
			for dependent in dependents {
				self.verify_with_expiration(dependent, i64::MAX);
			}
			stack.extend(
				self.subject_subject_dependents
					.get(&subject_key)
					.into_iter()
					.flatten()
					.cloned(),
			);
		}
	}

	pub(crate) fn is_subject_verified(
		&self,
		subject: &tg::authorization::Subject,
		scope: Option<&tg::authorization::Subject>,
	) -> bool {
		scope == Some(subject)
			|| *subject == tg::authorization::Subject::Public
			|| self
				.verified_subjects
				.contains(&(scope.cloned(), subject.clone()))
	}

	#[must_use]
	pub(crate) fn has_verification_dependency(&self, dependency: &Key, dependent: &Key) -> bool {
		self.verification_dependents
			.get(dependency)
			.is_some_and(|dependents| dependents.contains(dependent))
	}

	pub(crate) fn add_verification_dependency(&mut self, dependency: &Key, dependent: Key) -> bool {
		let inserted = self
			.verification_dependents
			.entry(dependency.clone())
			.or_default()
			.insert(dependent.clone());
		if inserted {
			self.verification_dependencies
				.entry(dependent.clone())
				.or_default()
				.insert(dependency.clone());
		}
		self.inherit_syncs(dependency, &dependent);
		if self.is_verified(dependency) {
			let expires_at = self.expires_at(dependency).min(
				self.verification_expirations
					.get(&(dependency.clone(), dependent.clone()))
					.copied()
					.unwrap_or(i64::MAX),
			);
			self.verify_with_expiration(dependent, expires_at);
		}

		inserted
	}

	#[must_use]
	pub(crate) fn has_verification_conjunction(
		&self,
		first: &Key,
		second: &Key,
		dependent: &Key,
	) -> bool {
		self.verification_conjunctions
			.get(first)
			.is_some_and(|conjunctions| conjunctions.contains(&(second.clone(), dependent.clone())))
	}

	pub(crate) fn add_verification_conjunction(
		&mut self,
		first: &Key,
		second: &Key,
		dependent: &Key,
	) {
		for (dependency, other) in [(first, second), (second, first)] {
			self.verification_conjunctions
				.entry(dependency.clone())
				.or_default()
				.insert((other.clone(), dependent.clone()));
			self.verification_dependencies
				.entry(dependent.clone())
				.or_default()
				.insert(dependency.clone());
		}
		if self.is_verified(first) {
			self.inherit_syncs(second, dependent);
		}
		if self.is_verified(second) {
			self.inherit_syncs(first, dependent);
		}
		if self.is_verified(first) && self.is_verified(second) {
			let expires_at = self.expires_at(first).min(self.expires_at(second));
			self.verify_with_expiration(dependent.clone(), expires_at);
		}
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
			if !self.is_verified(dependency) {
				let unresolved = self
					.derived_unresolved
					.entry(dependent.clone())
					.or_default();
				*unresolved = unresolved.saturating_add(1);
			}
		}
		self.inherit_derived_syncs(dependency, &dependent);
		self.propagate_derived_outcome(dependency);
		self.try_verify_derived(dependent);
	}

	pub(crate) fn verify_derived(&mut self, key: Key) {
		let expires_at = self.derived_expires_at(&key);
		self.verify_with_expiration(key, expires_at);
	}

	pub(crate) fn verify_ancestor_or_descendant(&mut self, key: Key) {
		self.verify_with_expiration(key, i64::MAX);
	}

	pub(crate) fn complete_derived(&mut self, key: &Key) {
		self.derived_cursors.remove(key);
		self.derived_complete.insert(key.clone());
		self.try_verify_derived(key.clone());
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
		if self.is_verified(key) {
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
		if self.is_verified(key)
			|| self.evaluation_mut(key).ancestor_or_descendant == ProofStatus::Denied
		{
			return;
		}
		self.evaluation_mut(key).ancestor_or_descendant = ProofStatus::Denied;
		self.newly_evaluated.insert(key.clone());
		self.propagate_derived_outcome(key);
	}

	#[must_use]
	pub(crate) fn is_verified(&self, key: &Key) -> bool {
		self.evaluations
			.get(key)
			.is_some_and(|evaluation| evaluation.verified)
	}

	#[must_use]
	pub(crate) fn verification_dependents(&self, key: &Key) -> Vec<Key> {
		self.verification_dependents
			.get(key)
			.into_iter()
			.flatten()
			.cloned()
			.chain(
				self.verification_conjunctions
					.get(key)
					.into_iter()
					.flatten()
					.map(|(_, dependent)| dependent.clone()),
			)
			.collect()
	}

	#[must_use]
	pub(crate) fn verification_dependencies(&self, key: &Key) -> Vec<Key> {
		self.verification_dependencies
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
	fn verification_revision(&self) -> usize {
		self.verification_log.len()
	}

	fn verification_changes_since(&self, revision: &mut usize) -> Vec<Key> {
		let changes = self.verification_log[*revision..].to_vec();
		*revision = self.verification_log.len();

		changes
	}

	fn take_descendant(&mut self) -> Option<DescendantSearch> {
		self.descendant.take()
	}

	fn set_descendant(&mut self, descendant: DescendantSearch) {
		self.descendant = Some(descendant);
	}

	pub(crate) fn expires_at(&self, key: &Key) -> i64 {
		self.evaluations
			.get(key)
			.filter(|evaluation| evaluation.verified)
			.map_or(i64::MIN, |evaluation| evaluation.expires_at)
	}

	fn derived_expires_at(&self, key: &Key) -> i64 {
		self.derived_dependencies
			.get(key)
			.into_iter()
			.flatten()
			.map(|dependency| self.expires_at(dependency))
			.min()
			.unwrap_or(i64::MAX)
	}

	pub(crate) fn verify_with_expiration(&mut self, key: Key, expires_at: i64) {
		let mut stack = vec![(key, expires_at)];
		while let Some((key, expires_at)) = stack.pop() {
			let evaluation = self.evaluation_mut(&key);
			let verified = evaluation.verified;
			if verified && evaluation.expires_at >= expires_at {
				continue;
			}
			evaluation.verified = true;
			evaluation.expires_at = expires_at;
			if !verified {
				self.verification_log.push(key.clone());
			}
			self.newly_evaluated.insert(key.clone());
			self.activate_syncs(&key);
			stack.extend(
				crate::verify::permissions_implied_by(key.1)
					.into_iter()
					.filter(|permission| *permission != key.1)
					.map(|permission| ((key.0.clone(), permission, key.2.clone()), expires_at)),
			);
			if let Some(dependents) = self.verification_dependents.get(&key) {
				stack.extend(dependents.iter().cloned().map(|dependent| {
					let expires_at = expires_at.min(
						self.verification_expirations
							.get(&(key.clone(), dependent.clone()))
							.copied()
							.unwrap_or(i64::MAX),
					);
					(dependent, expires_at)
				}));
			}
			if let Some(conjunctions) = self.verification_conjunctions.get(&key) {
				for (other, dependent) in conjunctions {
					if self.is_verified(other) {
						stack.push((dependent.clone(), expires_at.min(self.expires_at(other))));
					}
				}
			}

			let derived = self
				.derived_dependents
				.get(&key)
				.map_or_else(Vec::new, |dependents| dependents.iter().cloned().collect());
			for dependent in derived {
				if !verified {
					let unresolved = self
						.derived_unresolved
						.entry(dependent.clone())
						.or_default();
					*unresolved = unresolved.saturating_sub(1);
				}
				if self.derived_is_verified(&dependent) {
					let expires_at = self.derived_expires_at(&dependent);
					stack.push((dependent, expires_at));
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
	fn derived_is_verified(&self, key: &Key) -> bool {
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
				if self.is_verified(&dependent) {
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

	fn try_verify_derived(&mut self, key: Key) {
		if !self.is_verified(&key) && self.derived_is_verified(&key) {
			self.verify_derived(key);
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
				Outcome::Verified => {
					self.deferred.remove(&key);
					self.outcomes.insert(key.clone(), Outcome::Verified);
					if state.storage.contains(&key) {
						self.enqueue_dependencies(state, &key);
					}
				},
				Outcome::Denied => {
					self.outcomes.insert(key.clone(), Outcome::Denied);
					self.enqueue_dependencies(state, &key);
				},
				Outcome::Exhausted => unreachable!(),
				Outcome::Pending => return Some(key),
			}
		}

		None
	}

	pub(crate) fn apply(&mut self, state: &mut State, key: &Key, outcome: Outcome) {
		let outcome = match state.outcome(key) {
			outcome @ (Outcome::Verified | Outcome::Denied) => outcome,
			Outcome::Exhausted => unreachable!(),
			Outcome::Pending => outcome,
		};
		self.outcomes.insert(key.clone(), outcome);
		match outcome {
			Outcome::Verified => {
				self.deferred.remove(key);
			},
			Outcome::Denied | Outcome::Exhausted | Outcome::Pending => {
				self.deferred.insert(key.clone());
			},
		}
		if outcome != Outcome::Verified {
			self.enqueue_dependencies(state, key);
		}

		self.enqueue_changed(state, Some(key));
	}

	#[must_use]
	pub(crate) fn outcome(&self, state: &State, key: &Key) -> Outcome {
		match state.outcome(key) {
			outcome @ (Outcome::Verified | Outcome::Denied) => outcome,
			Outcome::Exhausted => unreachable!(),
			Outcome::Pending => self
				.outcomes
				.get(key)
				.copied()
				.unwrap_or(Outcome::Exhausted),
		}
	}

	fn enqueue_dependencies(&mut self, state: &State, key: &Key) {
		for dependency in state.verification_dependencies(key) {
			if (dependency.2.is_some() || state.sync_access.contains(&dependency))
				&& !self.outcomes.contains_key(&dependency)
				&& self.queued.insert(dependency.clone())
			{
				self.pending.push_back(dependency);
			}
		}
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
			tg::authorization::permission::process::Permission::NodeCommandObjects
				| tg::authorization::permission::process::Permission::NodeErrorObjects
				| tg::authorization::permission::process::Permission::NodeLogObjects
				| tg::authorization::permission::process::Permission::NodeOutputObjects
				| tg::authorization::permission::process::Permission::Subtree
				| tg::authorization::permission::process::Permission::SubtreeCommandObjects
				| tg::authorization::permission::process::Permission::SubtreeErrorObjects
				| tg::authorization::permission::process::Permission::SubtreeLogObjects
				| tg::authorization::permission::process::Permission::SubtreeOutputObjects
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
			crate::verify::Config::default(),
			&principal,
			std::slice::from_ref(&root),
			&[],
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
			crate::verify::Config::default(),
			&principal,
			std::slice::from_ref(&root),
			&[],
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
			Read::SubjectPermissions { .. } => ReadOutput::Permissions {
				after: None,
				permissions: Vec::new(),
			},
			_ => panic!("expected a descendant read"),
		};
		search.apply(&mut state, read, output).unwrap();

		let reads = search.take_reads(&mut state, 1).unwrap();

		assert!(matches!(reads.as_slice(), [Read::AncestorNode { .. }]));
	}

	#[test]
	fn an_ancestor_permission_page_verifys_before_the_node_is_complete() {
		let root = key();
		let mut state = State::default();
		let mut search = AncestorOrDescendantSearch::new(
			crate::verify::Config::default(),
			&tg::Principal::Anonymous,
			std::slice::from_ref(&root),
			&[],
			&mut state,
		);
		let mut reads = search.take_reads(&mut state, 1).unwrap();
		let read = reads.pop().unwrap();
		assert!(matches!(
			&read,
			Read::AncestorNode {
				read: AncestorNodeRead::ResourcePermissions { .. },
				..
			}
		));
		let permission = Permission {
			creator: None,
			direct: false,
			permission: root.1,
			resource: root.0.clone(),
			subject: tg::authorization::Subject::Public,
		};
		let output = ReadOutput::Permissions {
			after: None,
			permissions: vec![permission],
		};
		search.apply(&mut state, read, output).unwrap();

		assert!(state.is_verified(&root));
	}

	#[test]
	fn verification_propagates_across_an_edge_added_after_the_proof() {
		let dependency = key();
		let dependent = key();
		let mut state = State::default();
		state.verify_ancestor_or_descendant(dependency.clone());

		state.add_verification_dependency(&dependency, dependent.clone());

		assert!(state.is_verified(&dependent));
	}

	#[test]
	fn verification_conjunctions_require_both_proofs_in_either_order() {
		for reverse in [false, true] {
			for already_verified in [false, true] {
				let first = subtree_key(0);
				let second = subtree_key(1);
				let dependent = subtree_key(2);
				let mut state = State::default();
				let proofs = if reverse {
					[(&second, 200), (&first, 100)]
				} else {
					[(&first, 100), (&second, 200)]
				};
				if already_verified {
					for (key, expires_at) in proofs {
						state.verify_with_expiration(key.clone(), expires_at);
					}
				}
				state.add_verification_conjunction(&first, &second, &dependent);
				if !already_verified {
					assert!(!state.is_verified(&dependent));
					state.verify_with_expiration(proofs[0].0.clone(), proofs[0].1);
					assert!(!state.is_verified(&dependent));
					state.verify_with_expiration(proofs[1].0.clone(), proofs[1].1);
				}
				assert!(state.is_verified(&dependent));
				assert_eq!(state.expires_at(&dependent), 100);
			}
		}
	}

	#[test]
	fn a_completed_derived_conjunction_verifys_after_its_last_dependency() {
		let child = subtree_key(0);
		let parent = subtree_key(1);
		let node = (
			parent.0.clone(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			),
			None,
		);
		let mut state = State::default();
		state.add_derived_dependency(&child, parent.clone());
		state.add_derived_dependency(&node, parent.clone());
		state.complete_derived(&parent);
		state.verify_ancestor_or_descendant(node);
		assert!(!state.is_verified(&parent));

		state.verify_derived(child);

		assert!(state.is_verified(&parent));
	}

	#[test]
	fn a_final_search_preserves_permission_order() {
		let subtree = subtree_key(0);
		let node = (
			subtree.0.clone(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			),
			None,
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
		state.verify_derived(child.clone());
		search.apply(&mut state, &child, Outcome::Verified);

		assert_eq!(search.next(&mut state), Some(parent.clone()));
		state.verify_derived(parent.clone());
		search.apply(&mut state, &parent, Outcome::Verified);
		assert_eq!(search.next(&mut state), None);
		assert_eq!(search.outcome(&state, &parent), Outcome::Verified);
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

		(resource, permission, None)
	}

	fn subtree_key(value: u8) -> Key {
		let resource = tg::Id::from(tg::object::Id::new(
			tg::object::Kind::Blob,
			&vec![value].into(),
		));
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);

		(resource, permission, None)
	}
}
