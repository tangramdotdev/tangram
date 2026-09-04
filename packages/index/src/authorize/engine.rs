use {
	super::{
		facts,
		search::{
			AncestorOrDescendantSearch, FinalSearch, Key, Outcome, ProcessFacts, Read, ReadOutput,
			State, SubtreeAction, SubtreeSearch,
		},
	},
	std::{
		collections::{BTreeMap, VecDeque},
		ops::ControlFlow,
	},
	tangram_client::prelude::*,
};

pub(crate) struct Batch {
	args: Vec<super::Arg>,
	config: super::Config,
	outcomes: Option<Vec<super::Outcome>>,
	phase: BatchPhase,
	principal: tg::Principal,
	requested: Vec<Option<tg::authorization::permission::Set>>,
	required: Vec<Option<tg::authorization::permission::Set>>,
	resources: Vec<Resolution>,
	search_indices: BTreeMap<(Option<tg::authorization::Body>, bool), usize>,
	searches: Vec<TokenSearch>,
}

enum BatchPhase {
	Complete,
	Resolve { next: usize },
	Search { next: usize },
}

#[derive(Clone)]
enum Resolution {
	Complete(Option<(tg::Id, bool)>),
	Pending,
}

struct TokenSearch {
	active: Option<(Key, PermissionSearch)>,
	config: super::Config,
	final_search: FinalSearch,
	initial: BTreeMap<Key, Outcome>,
	phase: TokenPhase,
	principal: tg::Principal,
	roots: Vec<Key>,
	state: State,
	token: Option<(tg::authorization::Body, tg::Id)>,
}

enum TokenPhase {
	Complete,
	Final,
	Initial(AncestorOrDescendantSearch),
}

struct PermissionSearch {
	phase: PermissionPhase,
}

enum PermissionPhase {
	Complete(Outcome),
	Process(Box<ProcessSearch>),
	Subtree(Box<SubtreeEvaluation>),
}

struct SubtreeEvaluation {
	phase: SubtreePhase,
	search: SubtreeSearch,
}

enum SubtreePhase {
	AncestorOrDescendant {
		roots: Vec<Key>,
		search: AncestorOrDescendantSearch,
	},
	Complete(Outcome),
	ProcessNodes {
		current: Option<Box<ProcessSearch>>,
		pending: VecDeque<(Key, Outcome)>,
	},
	Ready,
}

struct ProcessSearch {
	incomplete: bool,
	initial: Outcome,
	kind: crate::process::object::Kind,
	objects: Vec<tg::object::Id>,
	phase: ProcessPhase,
	root: Key,
}

enum ProcessFactRead {
	Objects { after: Option<Vec<u8>> },
	Process,
}

enum ProcessValue {
	Complete(Option<crate::process::Process>),
	Pending,
}

enum ProcessPhase {
	Complete(Outcome),
	Facts {
		objects: Vec<(tg::object::Id, crate::process::object::Kind)>,
		pending: VecDeque<ProcessFactRead>,
		value: ProcessValue,
	},
	ObjectFinal {
		current: Option<Box<SubtreeEvaluation>>,
		pending: VecDeque<(tg::object::Id, Key, Outcome)>,
	},
	ObjectInitial {
		roots: Vec<Key>,
		search: AncestorOrDescendantSearch,
	},
}

impl Batch {
	pub(crate) async fn authorize<E>(
		args: &[super::Arg],
		client: facts::Client<E>,
		config: super::Config,
		principal: &tg::Principal,
	) -> Result<ControlFlow<Vec<super::Outcome>, E>, tg::Error>
	where
		E: Clone + Send + Sync + 'static,
	{
		let client_for_reads = client.clone();
		let result = Self::authorize_inner(args, client, config, principal).await;
		let reads = client_for_reads.reads();
		for arg in args {
			tracing::debug!(
				args = args.len(),
				reads,
				resource = %arg.resource,
				"authorize batch"
			);
		}

		result
	}

	async fn authorize_inner<E>(
		args: &[super::Arg],
		client: facts::Client<E>,
		config: super::Config,
		principal: &tg::Principal,
	) -> Result<ControlFlow<Vec<super::Outcome>, E>, tg::Error>
	where
		E: Clone + Send + Sync + 'static,
	{
		let mut batch = Self::new(args, config, principal)?;

		// Resolve the resources.
		while !batch.complete() && !matches!(batch.phase, BatchPhase::Search { .. }) {
			let reads = batch.take_reads(client.concurrency())?;
			let results = match execute_reads(&client, reads).await? {
				ControlFlow::Break(results) => results,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			for (read, output) in results {
				batch.apply(read, output)?;
			}
		}
		if batch.complete() {
			let outcomes = batch.into_outcomes()?;

			return Ok(ControlFlow::Break(outcomes));
		}

		// Search independent token contexts concurrently while sharing their datastore facts.
		let searches = std::mem::take(&mut batch.searches);
		let searches = futures::future::try_join_all(searches.into_iter().map(|search| {
			let client = client.clone();
			async move { execute_token_search(&client, search).await }
		}))
		.await?;
		for search in searches {
			let search = match search {
				ControlFlow::Break(search) => search,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			batch.searches.push(search);
		}
		batch.finish();
		let outcomes = batch.into_outcomes()?;

		Ok(ControlFlow::Break(outcomes))
	}

	pub(crate) fn new(
		args: &[super::Arg],
		config: super::Config,
		principal: &tg::Principal,
	) -> tg::Result<Self> {
		config.validate()?;
		args.iter().try_for_each(super::Arg::validate)?;
		let outcomes = if args.is_empty() {
			Some(Vec::new())
		} else if matches!(principal, tg::Principal::Root) {
			let outcomes = args
				.iter()
				.map(|arg| {
					super::Outcome::Authorized(super::Output {
						permissions: arg.requested,
					})
				})
				.collect();
			Some(outcomes)
		} else {
			None
		};
		let phase = if outcomes.is_some() {
			BatchPhase::Complete
		} else {
			BatchPhase::Resolve { next: 0 }
		};
		Ok(Self {
			args: args.to_vec(),
			config,
			outcomes,
			phase,
			principal: principal.clone(),
			requested: Vec::new(),
			required: Vec::new(),
			resources: vec![Resolution::Pending; args.len()],
			search_indices: BTreeMap::new(),
			searches: Vec::new(),
		})
	}

	#[must_use]
	pub(crate) fn complete(&self) -> bool {
		matches!(self.phase, BatchPhase::Complete)
	}

	pub(crate) fn take_reads(&mut self, limit: usize) -> tg::Result<Vec<Read>> {
		assert!(limit > 0);
		loop {
			match &mut self.phase {
				BatchPhase::Complete => return Ok(Vec::new()),
				BatchPhase::Resolve { next } => {
					let end = next.saturating_add(limit).min(self.args.len());
					let reads = (*next..end)
						.map(|index| Read::Resolve {
							index,
							selector: self.args[index].resource.clone(),
						})
						.collect::<Vec<_>>();
					*next = end;
					if !reads.is_empty() {
						return Ok(reads);
					}
					self.prepare_searches()?;

					return Ok(Vec::new());
				},
				BatchPhase::Search { next } => {
					let Some(search) = self.searches.get_mut(*next) else {
						self.finish();
						continue;
					};
					let reads = search.take_reads(limit)?;
					if !reads.is_empty() {
						return Ok(reads);
					}
					if search.complete() {
						*next += 1;
					}
				},
			}
		}
	}

	pub(crate) fn apply(&mut self, read: Read, output: ReadOutput) -> tg::Result<()> {
		match read {
			Read::Resolve { index, .. } => {
				let resource = output.into_resolved()?;
				let slot = self
					.resources
					.get_mut(index)
					.ok_or_else(|| tg::error!("received an invalid resolution index"))?;
				*slot = Resolution::Complete(resource);
			},
			read => {
				let BatchPhase::Search { next } = self.phase else {
					return Err(tg::error!(
						"received an authorization fact outside a search"
					));
				};
				let search = self
					.searches
					.get_mut(next)
					.ok_or_else(|| tg::error!("received an authorization fact after the search"))?;
				search.apply(read, output)?;
			},
		}

		Ok(())
	}

	pub(crate) fn into_outcomes(self) -> tg::Result<Vec<super::Outcome>> {
		self.outcomes
			.ok_or_else(|| tg::error!("the authorization batch is incomplete"))
	}

	fn prepare_searches(&mut self) -> tg::Result<()> {
		let resources = self
			.resources
			.iter()
			.map(|resource| {
				let Resolution::Complete(resource) = resource else {
					return Err(tg::error!("a resource resolution is missing"));
				};

				Ok(resource.clone())
			})
			.collect::<tg::Result<Vec<_>>>()?;
		self.requested = std::iter::zip(&self.args, &resources)
			.map(|(arg, resource)| normalize_permissions(resource.as_ref(), arg.requested))
			.collect::<tg::Result<Vec<_>>>()?;
		self.required = std::iter::zip(&self.args, &resources)
			.map(|(arg, resource)| normalize_permissions(resource.as_ref(), arg.required))
			.collect::<tg::Result<Vec<_>>>()?;

		let mut roots = BTreeMap::<(Option<tg::authorization::Body>, bool), Vec<Key>>::new();
		for (index, (arg, resource)) in std::iter::zip(&self.args, &resources).enumerate() {
			let Some((id, _)) = resource else {
				continue;
			};
			let Some(requested) = self.requested[index] else {
				continue;
			};
			if super::validate(id, requested).is_err() || principal_is_resource(&self.principal, id)
			{
				continue;
			}
			for permission in super::permissions_in_search_order(requested) {
				let process_parent_delegation = permission.is_read_like();
				roots
					.entry((arg.token.clone(), process_parent_delegation))
					.or_default()
					.push((id.clone(), permission));
			}
		}

		for ((token, process_parent_delegation), roots) in roots {
			let index = self.searches.len();
			self.search_indices
				.insert((token.clone(), process_parent_delegation), index);
			self.searches.push(TokenSearch::new(
				self.config,
				&self.principal,
				process_parent_delegation,
				roots,
				token,
			));
		}
		self.phase = BatchPhase::Search { next: 0 };

		Ok(())
	}

	fn finish(&mut self) {
		let resources = self
			.resources
			.iter()
			.map(|resource| match resource {
				Resolution::Complete(resource) => resource.clone(),
				Resolution::Pending => unreachable!(),
			})
			.collect::<Vec<_>>();
		let mut outcomes = Vec::with_capacity(self.args.len());
		for (index, (arg, resource)) in std::iter::zip(&self.args, resources).enumerate() {
			let Some((id, _)) = resource else {
				outcomes.push(super::Outcome::Denied(None));
				continue;
			};
			let Some(requested) = self.requested[index] else {
				outcomes.push(super::Outcome::Denied(None));
				continue;
			};
			let Some(required) = self.required[index] else {
				outcomes.push(super::Outcome::Denied(None));
				continue;
			};
			if super::validate(&id, requested).is_err() {
				outcomes.push(super::Outcome::Denied(None));
				continue;
			}
			if principal_is_resource(&self.principal, &id) {
				let output = super::Output {
					permissions: arg.requested,
				};
				outcomes.push(super::Outcome::Authorized(output));
				continue;
			}
			let mut authorized = requested.empty_like();
			let mut exhausted = requested.empty_like();
			for permission in super::permissions_in_search_order(requested) {
				let process_parent_delegation = permission.is_read_like();
				let search_index =
					self.search_indices[&(arg.token.clone(), process_parent_delegation)];
				let search = &self.searches[search_index];
				let key = (id.clone(), permission);
				match search.final_search.outcome(&search.state, &key) {
					Outcome::Authorized => {
						super::insert_implied_permissions(&mut authorized, requested, permission);
						if authorized.contains(requested) {
							break;
						}
					},
					Outcome::Denied => {},
					Outcome::Exhausted | Outcome::Pending => exhausted.insert(
						tg::authorization::permission::Set::from_permission(permission),
					),
				}
			}
			let required_exhausted = required.iter().any(|permission| {
				let permission = tg::authorization::permission::Set::from_permission(permission);
				!authorized.contains(permission) && exhausted.contains(permission)
			});
			if required_exhausted {
				outcomes.push(super::Outcome::Exhausted);
				continue;
			}
			let permissions = if requested == arg.requested {
				authorized
			} else if authorized.contains(requested) {
				arg.requested
			} else {
				arg.requested.empty_like()
			};
			let output = super::Output { permissions };
			let outcome = super::Outcome::from_output(Some(output), arg.requested);
			outcomes.push(outcome);
		}
		self.outcomes = Some(outcomes);
		self.phase = BatchPhase::Complete;
	}
}

impl TokenSearch {
	#[must_use]
	fn new(
		config: super::Config,
		principal: &tg::Principal,
		process_parent_delegation: bool,
		roots: Vec<Key>,
		token: Option<tg::authorization::Body>,
	) -> Self {
		let mut state = State::default();
		state.set_process_parent_delegation(process_parent_delegation);
		let token = token.map(|body| {
			let resource = body.resource.clone();
			(body, resource)
		});
		let initial =
			AncestorOrDescendantSearch::new(config, principal, &roots, token.as_ref(), &mut state);
		let final_search = FinalSearch::new(roots.iter().cloned());

		Self {
			active: None,
			config,
			final_search,
			initial: BTreeMap::new(),
			phase: TokenPhase::Initial(initial),
			principal: principal.clone(),
			roots,
			state,
			token,
		}
	}

	#[must_use]
	fn complete(&self) -> bool {
		matches!(self.phase, TokenPhase::Complete)
	}

	fn take_reads(&mut self, limit: usize) -> tg::Result<Vec<Read>> {
		loop {
			match &mut self.phase {
				TokenPhase::Complete => return Ok(Vec::new()),
				TokenPhase::Final => {
					if let Some((key, search)) = &mut self.active {
						let reads = search.take_reads(
							self.config,
							&self.principal,
							self.token.as_ref(),
							&mut self.state,
							limit,
						)?;
						if !reads.is_empty() {
							return Ok(reads);
						}
						if let Some(outcome) = search.outcome() {
							self.final_search.apply(&mut self.state, key, outcome);
							self.active = None;
							continue;
						}
					}
					let Some(key) = self.final_search.next(&mut self.state) else {
						self.phase = TokenPhase::Complete;
						continue;
					};
					let initial = self.initial[&key];
					let search = PermissionSearch::new(self.config, &key, initial, &self.state)?;
					self.active = Some((key, search));
				},
				TokenPhase::Initial(search) => {
					let reads = search.take_reads(&mut self.state, limit)?;
					if !reads.is_empty() {
						return Ok(reads);
					}
					if search.complete() {
						for root in &self.roots {
							self.initial
								.insert(root.clone(), search.outcome(&self.state, root));
						}
						self.phase = TokenPhase::Final;
					}
				},
			}
		}
	}

	fn apply(&mut self, read: Read, output: ReadOutput) -> tg::Result<()> {
		match &mut self.phase {
			TokenPhase::Complete => Err(tg::error!("received a fact after the token search")),
			TokenPhase::Final => self
				.active
				.as_mut()
				.ok_or_else(|| tg::error!("received a fact without an active final search"))?
				.1
				.apply(&mut self.state, read, output),
			TokenPhase::Initial(search) => search.apply(&mut self.state, read, output),
		}
	}
}

impl PermissionSearch {
	fn new(config: super::Config, key: &Key, initial: Outcome, state: &State) -> tg::Result<Self> {
		let current = state.outcome(key);
		let phase = match current {
			Outcome::Authorized | Outcome::Denied => PermissionPhase::Complete(current),
			Outcome::Exhausted => unreachable!(),
			Outcome::Pending => match key.1 {
				tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				) => PermissionPhase::Subtree(Box::new(SubtreeEvaluation::new_object(
					config.subtree,
					&key.0,
					initial,
				)?)),
				tg::authorization::Permission::Process(
					permission
						@ (tg::authorization::permission::process::Permission::NodeCommand
						| tg::authorization::permission::process::Permission::NodeError
						| tg::authorization::permission::process::Permission::NodeLog
						| tg::authorization::permission::process::Permission::NodeOutput),
				) => PermissionPhase::Process(Box::new(ProcessSearch::new(key, permission, initial)?)),
				tg::authorization::Permission::Process(
					permission
						@ (tg::authorization::permission::process::Permission::Subtree
						| tg::authorization::permission::process::Permission::SubtreeCommand
						| tg::authorization::permission::process::Permission::SubtreeError
						| tg::authorization::permission::process::Permission::SubtreeLog
						| tg::authorization::permission::process::Permission::SubtreeOutput),
				) => PermissionPhase::Subtree(Box::new(SubtreeEvaluation::new_process(
					config.subtree,
					&key.0,
					permission,
					initial,
				)?)),
				_ => PermissionPhase::Complete(initial),
			},
		};

		Ok(Self { phase })
	}

	fn take_reads(
		&mut self,
		config: super::Config,
		principal: &tg::Principal,
		token: Option<&(tg::authorization::Body, tg::Id)>,
		state: &mut State,
		limit: usize,
	) -> tg::Result<Vec<Read>> {
		let (reads, outcome) = match &mut self.phase {
			PermissionPhase::Complete(_) => return Ok(Vec::new()),
			PermissionPhase::Process(search) => {
				let reads = search.take_reads(config, principal, token, state, limit)?;
				(reads, search.outcome())
			},
			PermissionPhase::Subtree(search) => {
				let reads = search.take_reads(config, principal, token, state, limit)?;
				(reads, search.outcome())
			},
		};
		if let Some(outcome) = outcome {
			self.phase = PermissionPhase::Complete(outcome);
		}

		Ok(reads)
	}

	fn apply(&mut self, state: &mut State, read: Read, output: ReadOutput) -> tg::Result<()> {
		match &mut self.phase {
			PermissionPhase::Complete(_) => {
				Err(tg::error!("received a fact after the final search"))
			},
			PermissionPhase::Process(search) => search.apply(state, read, output),
			PermissionPhase::Subtree(search) => search.apply(state, read, output),
		}
	}

	#[must_use]
	fn outcome(&self) -> Option<Outcome> {
		match self.phase {
			PermissionPhase::Complete(outcome) => Some(outcome),
			PermissionPhase::Process(_) | PermissionPhase::Subtree(_) => None,
		}
	}
}

impl SubtreeEvaluation {
	fn new_object(
		config: super::SubtreeConfig,
		resource: &tg::Id,
		initial: Outcome,
	) -> tg::Result<Self> {
		let mut search = SubtreeSearch::new_object(config, resource)?;
		let root = (
			resource.clone(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			),
		);
		search.apply_ancestor_or_descendant(std::slice::from_ref(&root), &[initial])?;

		Ok(Self {
			phase: SubtreePhase::Ready,
			search,
		})
	}

	fn new_process(
		config: super::SubtreeConfig,
		resource: &tg::Id,
		permission: tg::authorization::permission::process::Permission,
		initial: Outcome,
	) -> tg::Result<Self> {
		let mut search = SubtreeSearch::new_process(config, permission, resource)?;
		let root = (
			resource.clone(),
			tg::authorization::Permission::Process(permission),
		);
		search.apply_ancestor_or_descendant(std::slice::from_ref(&root), &[initial])?;

		Ok(Self {
			phase: SubtreePhase::Ready,
			search,
		})
	}

	fn take_reads(
		&mut self,
		config: super::Config,
		principal: &tg::Principal,
		token: Option<&(tg::authorization::Body, tg::Id)>,
		state: &mut State,
		limit: usize,
	) -> tg::Result<Vec<Read>> {
		loop {
			match &mut self.phase {
				SubtreePhase::AncestorOrDescendant { roots, search } => {
					let reads = search.take_reads(state, limit)?;
					if !reads.is_empty() {
						return Ok(reads);
					}
					if search.complete() {
						let outcomes = roots
							.iter()
							.map(|root| search.outcome(state, root))
							.collect::<Vec<_>>();
						self.search.apply_ancestor_or_descendant(roots, &outcomes)?;
						self.phase = SubtreePhase::Ready;
					}
				},
				SubtreePhase::Complete(_) => return Ok(Vec::new()),
				SubtreePhase::ProcessNodes { current, pending } => {
					if let Some(search) = current {
						let reads = search.take_reads(config, principal, token, state, limit)?;
						if !reads.is_empty() {
							return Ok(reads);
						}
						if search.outcome().is_some() {
							*current = None;
							continue;
						}
					}
					let Some((key, initial)) = pending.pop_front() else {
						self.phase = SubtreePhase::Ready;
						continue;
					};
					let tg::authorization::Permission::Process(permission) = key.1 else {
						return Err(tg::error!("expected a process permission"));
					};
					if permission == tg::authorization::permission::process::Permission::Node {
						continue;
					}
					*current = Some(Box::new(ProcessSearch::new(&key, permission, initial)?));
				},
				SubtreePhase::Ready => {
					match self
						.search
						.next_action(state, limit, config.ancestor.page_size)?
					{
						SubtreeAction::AuthorizeAncestorOrDescendant { roots } => {
							let search = AncestorOrDescendantSearch::new(
								config, principal, &roots, token, state,
							);
							self.phase = SubtreePhase::AncestorOrDescendant { roots, search };
						},
						SubtreeAction::AuthorizeProcessNodes { roots } => {
							self.phase = SubtreePhase::ProcessNodes {
								current: None,
								pending: roots.into(),
							};
						},
						SubtreeAction::Complete { outcome } => {
							self.phase = SubtreePhase::Complete(outcome);
						},
						SubtreeAction::Read { reads } => return Ok(reads),
					}
				},
			}
		}
	}

	fn apply(&mut self, state: &mut State, read: Read, output: ReadOutput) -> tg::Result<()> {
		match &mut self.phase {
			SubtreePhase::AncestorOrDescendant { search, .. } => search.apply(state, read, output),
			SubtreePhase::Complete(_) => Err(tg::error!("received a fact after a subtree search")),
			SubtreePhase::ProcessNodes { current, .. } => current
				.as_mut()
				.ok_or_else(|| tg::error!("received a fact without an active process search"))?
				.apply(state, read, output),
			SubtreePhase::Ready => self.search.apply(state, read, output),
		}
	}

	#[must_use]
	fn outcome(&self) -> Option<Outcome> {
		match self.phase {
			SubtreePhase::Complete(outcome) => Some(outcome),
			SubtreePhase::AncestorOrDescendant { .. }
			| SubtreePhase::ProcessNodes { .. }
			| SubtreePhase::Ready => None,
		}
	}
}

impl ProcessSearch {
	fn new(
		root: &Key,
		permission: tg::authorization::permission::process::Permission,
		initial: Outcome,
	) -> tg::Result<Self> {
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
			_ => return Err(tg::error!("expected a process node aspect permission")),
		};

		Ok(Self {
			incomplete: false,
			initial,
			kind,
			objects: Vec::new(),
			phase: ProcessPhase::Facts {
				objects: Vec::new(),
				pending: VecDeque::from([
					ProcessFactRead::Objects { after: None },
					ProcessFactRead::Process,
				]),
				value: ProcessValue::Pending,
			},
			root: root.clone(),
		})
	}

	fn take_reads(
		&mut self,
		config: super::Config,
		principal: &tg::Principal,
		token: Option<&(tg::authorization::Body, tg::Id)>,
		state: &mut State,
		limit: usize,
	) -> tg::Result<Vec<Read>> {
		loop {
			match state.outcome(&self.root) {
				outcome @ (Outcome::Authorized | Outcome::Denied) => {
					self.phase = ProcessPhase::Complete(outcome);
				},
				Outcome::Exhausted => unreachable!(),
				Outcome::Pending => {},
			}
			match &mut self.phase {
				ProcessPhase::Complete(_) => return Ok(Vec::new()),
				ProcessPhase::Facts {
					objects,
					pending,
					value,
				} => {
					if self.initial == Outcome::Authorized {
						self.phase = ProcessPhase::Complete(Outcome::Authorized);
						continue;
					}
					if config.subtree.max_objects == 0 {
						self.phase = ProcessPhase::Complete(Outcome::Exhausted);
						continue;
					}
					let process = tg::process::Id::try_from(self.root.0.clone())?;
					if let Some(facts) = state.process_facts(&process) {
						self.prepare_facts(config, principal, token, state, &facts);
						continue;
					}
					let mut reads = Vec::new();
					while reads.len() < limit {
						let Some(read) = pending.pop_front() else {
							break;
						};
						match read {
							ProcessFactRead::Objects { after } => {
								let limit = config.ancestor.page_size;
								reads.push(Read::ProcessObjects {
									after,
									limit,
									process: process.clone(),
								});
							},
							ProcessFactRead::Process => {
								reads.push(Read::Process {
									process: process.clone(),
								});
							},
						}
					}
					if !reads.is_empty() {
						return Ok(reads);
					}
					let ProcessValue::Complete(process_value) =
						std::mem::replace(value, ProcessValue::Pending)
					else {
						return Err(tg::error!("the process facts are incomplete"));
					};
					let objects = std::mem::take(objects);
					let facts = ProcessFacts {
						objects,
						process: process_value,
					};
					let facts = state.set_process_facts(process, facts);
					self.prepare_facts(config, principal, token, state, &facts);
				},
				ProcessPhase::ObjectFinal { current, pending } => {
					if let Some(search) = current {
						let reads = search.take_reads(config, principal, token, state, limit)?;
						if !reads.is_empty() {
							return Ok(reads);
						}
						if let Some(outcome) = search.outcome() {
							match outcome {
								Outcome::Authorized => {},
								Outcome::Denied => {
									let outcome = finish_process(state, &self.root, false);
									self.phase = ProcessPhase::Complete(outcome);
									continue;
								},
								Outcome::Exhausted | Outcome::Pending => self.incomplete = true,
							}
							*current = None;
							continue;
						}
					}
					let Some((object, root, initial)) = pending.pop_front() else {
						let outcome = if self.incomplete {
							Outcome::Exhausted
						} else {
							finish_process(state, &self.root, true)
						};
						self.phase = ProcessPhase::Complete(outcome);
						continue;
					};
					if state.is_authorized(&root) {
						continue;
					}
					let resource = tg::Id::from(object);
					*current = Some(Box::new(SubtreeEvaluation::new_object(
						config.subtree,
						&resource,
						initial,
					)?));
				},
				ProcessPhase::ObjectInitial { roots, search } => {
					let reads = search.take_reads(state, limit)?;
					if !reads.is_empty() {
						return Ok(reads);
					}
					if search.complete() {
						let pending = std::iter::zip(&self.objects, roots.iter())
							.map(|(object, root)| {
								(object.clone(), root.clone(), search.outcome(state, root))
							})
							.collect();
						self.phase = ProcessPhase::ObjectFinal {
							current: None,
							pending,
						};
					}
				},
			}
		}
	}

	fn apply(&mut self, state: &mut State, read: Read, output: ReadOutput) -> tg::Result<()> {
		match &mut self.phase {
			ProcessPhase::Complete(_) => Err(tg::error!("received a fact after a process search")),
			ProcessPhase::Facts {
				objects,
				pending,
				value,
			} => match read {
				Read::Process { .. } => {
					*value = ProcessValue::Complete(output.into_process()?);

					Ok(())
				},
				Read::ProcessObjects { .. } => {
					let (after, page) = output.into_process_objects()?;
					objects.extend(page);
					if let Some(after) = after {
						pending.push_back(ProcessFactRead::Objects { after: Some(after) });
					}

					Ok(())
				},
				_ => Err(tg::error!(
					"received a non-process fact for a process search"
				)),
			},
			ProcessPhase::ObjectFinal { current, .. } => current
				.as_mut()
				.ok_or_else(|| tg::error!("received a fact without an active object search"))?
				.apply(state, read, output),
			ProcessPhase::ObjectInitial { search, .. } => search.apply(state, read, output),
		}
	}

	#[must_use]
	fn outcome(&self) -> Option<Outcome> {
		match self.phase {
			ProcessPhase::Complete(outcome) => Some(outcome),
			ProcessPhase::Facts { .. }
			| ProcessPhase::ObjectFinal { .. }
			| ProcessPhase::ObjectInitial { .. } => None,
		}
	}

	fn prepare_facts(
		&mut self,
		config: super::Config,
		principal: &tg::Principal,
		token: Option<&(tg::authorization::Body, tg::Id)>,
		state: &mut State,
		facts: &ProcessFacts,
	) {
		self.objects = facts
			.objects
			.iter()
			.filter_map(|(object, kind)| aspect_matches(self.kind, *kind).then_some(object.clone()))
			.collect();
		let aspect_is_set = facts
			.process
			.as_ref()
			.is_some_and(|process| match self.kind {
				crate::process::object::Kind::Command => true,
				crate::process::object::Kind::Error => process.set.error,
				crate::process::object::Kind::Log => process.set.log,
				crate::process::object::Kind::Output => process.set.output,
			});
		if !aspect_is_set || (self.kind.is_command() && self.objects.is_empty()) {
			let outcome = finish_process(state, &self.root, false);
			self.phase = ProcessPhase::Complete(outcome);

			return;
		}

		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let roots = self
			.objects
			.iter()
			.map(|object| (tg::Id::from(object.clone()), permission))
			.collect::<Vec<_>>();
		for root in &roots {
			state.add_derived_dependency(root, self.root.clone());
		}
		state.complete_derived(&self.root);
		if state.is_authorized(&self.root) {
			self.phase = ProcessPhase::Complete(Outcome::Authorized);

			return;
		}
		let search = AncestorOrDescendantSearch::new(config, principal, &roots, token, state);
		self.phase = ProcessPhase::ObjectInitial { roots, search };
	}
}

async fn execute_reads<E>(
	client: &facts::Client<E>,
	reads: Vec<Read>,
) -> Result<ControlFlow<Vec<(Read, ReadOutput)>, E>, tg::Error>
where
	E: Clone + Send + Sync + 'static,
{
	let results = futures::future::try_join_all(reads.into_iter().map(|read| {
		let client = client.clone();
		async move {
			let output = execute_read(&client, &read).await?;

			Ok::<_, tg::Error>((read, output))
		}
	}))
	.await?;
	let mut outputs = Vec::with_capacity(results.len());
	for (read, output) in results {
		let output = match output {
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		outputs.push((read, output));
	}

	Ok(ControlFlow::Break(outputs))
}

async fn execute_token_search<E>(
	client: &facts::Client<E>,
	mut search: TokenSearch,
) -> Result<ControlFlow<TokenSearch, E>, tg::Error>
where
	E: Clone + Send + Sync + 'static,
{
	while !search.complete() {
		let reads = search.take_reads(client.concurrency())?;
		let results = match execute_reads(client, reads).await? {
			ControlFlow::Break(results) => results,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		for (read, output) in results {
			search.apply(read, output)?;
		}
	}

	Ok(ControlFlow::Break(search))
}

async fn execute_read<E>(
	client: &facts::Client<E>,
	read: &Read,
) -> Result<ControlFlow<ReadOutput, E>, tg::Error>
where
	E: Clone + Send + Sync + 'static,
{
	macro_rules! read {
		($request:expr) => {{
			match client.read($request).await? {
				ControlFlow::Break(output) => output,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		}};
	}

	let output = match read {
		Read::AncestorChecks(checks) => {
			let mut values = Vec::with_capacity(checks.candidates().len());
			for candidate in checks.candidates() {
				let mut value = true;
				for check in candidate.checks() {
					let output = read!(check.request());
					if !check_matches_output(check, output)? {
						value = false;

						break;
					}
				}
				values.push(value);
			}

			ReadOutput::Bools(values)
		},
		Read::AncestorNode { read, .. } => match read {
			super::search::AncestorNodeRead::Group { group } => {
				let output = read!(facts::Request::Group {
					group: group.clone(),
				});
				let group = output.into_group()?;

				ReadOutput::Group(group)
			},
			super::search::AncestorNodeRead::ObjectProcesses {
				after,
				limit,
				object,
			} => {
				let output = read!(facts::Request::ObjectProcesses {
					after: after.clone(),
					limit: *limit,
					object: object.clone(),
				});
				let (after, processes) = output.into_object_processes()?;

				ReadOutput::ObjectProcesses { after, processes }
			},
			super::search::AncestorNodeRead::Process { process } => {
				let output = read!(facts::Request::Process {
					process: process.clone(),
				});
				let process = output.into_process()?;

				ReadOutput::Process(process)
			},
			super::search::AncestorNodeRead::ResourceGrants {
				after,
				limit,
				resource,
			} => {
				let output = read!(facts::Request::ResourceGrants {
					after: after.clone(),
					limit: *limit,
					resource: resource.clone(),
				});
				let (after, grants) = output.into_grants()?;

				ReadOutput::Grants { after, grants }
			},
			super::search::AncestorNodeRead::SandboxOwner { sandbox } => {
				let output = read!(facts::Request::SandboxOwner {
					sandbox: sandbox.clone(),
				});
				let owner = output.into_sandbox_owner()?;

				ReadOutput::SandboxOwner(owner)
			},
			super::search::AncestorNodeRead::Tag { tag }
			| super::search::AncestorNodeRead::TargetTag { tag } => {
				let output = read!(facts::Request::Tag { tag: tag.clone() });
				let tag = output.into_tag()?;

				ReadOutput::Tag(tag)
			},
			super::search::AncestorNodeRead::TargetTags {
				after,
				limit,
				target,
			} => {
				let output = read!(facts::Request::TargetTags {
					after: after.clone(),
					limit: *limit,
					target: target.clone(),
				});
				let (after, tags) = output.into_tags()?;

				ReadOutput::Tags { after, tags }
			},
		},
		Read::DescendantChecks(checks) => {
			let mut values = Vec::with_capacity(checks.candidates().len());
			for candidate in checks.candidates() {
				let mut value = false;
				for proof in candidate.proofs() {
					value = true;
					for check in proof {
						let output = read!(check.request());
						if !check_matches_output(check, output)? {
							value = false;

							break;
						}
					}
					if value {
						break;
					}
				}
				values.push(value);
			}

			ReadOutput::Bools(values)
		},
		Read::GroupMembers {
			after,
			group,
			limit,
			..
		} => {
			let output = read!(facts::Request::GroupMembers {
				after: after.clone(),
				group: group.clone(),
				limit: *limit,
			});
			let (after, ids) = output.into_ids()?;

			ReadOutput::Ids { after, ids }
		},
		Read::Member {
			limit,
			member,
			read,
			..
		} => match read {
			super::search::MemberRead::Groups { after } => {
				let output = read!(facts::Request::MemberGroups {
					after: after.clone(),
					limit: *limit,
					member: member.clone(),
				});
				let (after, groups) = output.into_member_groups()?;

				ReadOutput::MemberGroups { after, groups }
			},
			super::search::MemberRead::Organizations { after } => {
				let output = read!(facts::Request::MemberOrganizations {
					after: after.clone(),
					limit: *limit,
					member: member.clone(),
				});
				let (after, organizations) = output.into_member_organizations()?;

				ReadOutput::MemberOrganizations {
					after,
					organizations,
				}
			},
		},
		Read::ObjectChildren {
			after,
			limit,
			object,
			..
		}
		| Read::SubtreeObjectChildren {
			after,
			limit,
			object,
			..
		} => {
			let output = read!(facts::Request::ObjectChildren {
				after: after.clone(),
				limit: *limit,
				object: object.clone(),
			});
			let (after, ids) = output.into_ids()?;

			ReadOutput::Ids { after, ids }
		},
		Read::ObjectParents {
			after,
			limit,
			object,
			..
		} => {
			let output = read!(facts::Request::ObjectParents {
				after: after.clone(),
				limit: *limit,
				object: object.clone(),
			});
			let (after, ids) = output.into_ids()?;

			ReadOutput::Ids { after, ids }
		},
		Read::OrganizationMembers {
			after,
			limit,
			organization,
			..
		} => {
			let output = read!(facts::Request::OrganizationMembers {
				after: after.clone(),
				limit: *limit,
				organization: organization.clone(),
			});
			let (after, ids) = output.into_ids()?;

			ReadOutput::Ids { after, ids }
		},
		Read::OwnerSandboxes {
			after,
			limit,
			owner,
			..
		} => {
			let output = read!(facts::Request::OwnerSandboxes {
				after: after.clone(),
				limit: *limit,
				owner: owner.clone(),
			});
			let (after, ids) = output.into_ids()?;

			ReadOutput::Ids { after, ids }
		},
		Read::Process { process } => {
			let output = read!(facts::Request::Process {
				process: process.clone(),
			});
			let process = output.into_process()?;

			ReadOutput::Process(process)
		},
		Read::ProcessChildren {
			after,
			limit,
			process,
			..
		}
		| Read::SubtreeProcessChildren {
			after,
			limit,
			process,
			..
		} => {
			let output = read!(facts::Request::ProcessChildren {
				after: after.clone(),
				limit: *limit,
				process: process.clone(),
			});
			let (after, ids) = output.into_ids()?;

			ReadOutput::Ids { after, ids }
		},
		Read::ProcessObjectChildren {
			after,
			limit,
			process,
			..
		}
		| Read::ProcessObjects {
			after,
			limit,
			process,
		} => {
			let output = read!(facts::Request::ProcessObjects {
				after: after.clone(),
				limit: *limit,
				process: process.clone(),
			});
			let (after, objects) = output.into_process_objects()?;

			ReadOutput::ProcessObjects { after, objects }
		},
		Read::ProcessParents {
			after,
			limit,
			process,
			..
		} => {
			let output = read!(facts::Request::ProcessParents {
				after: after.clone(),
				limit: *limit,
				process: process.clone(),
			});
			let (after, ids) = output.into_ids()?;

			ReadOutput::Ids { after, ids }
		},
		Read::Resolve { selector, .. } => {
			let resource = match resolve(client, selector).await? {
				ControlFlow::Break(resource) => resource,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};

			ReadOutput::Resolved(resource)
		},
		Read::SandboxProcesses {
			after,
			limit,
			sandbox,
			..
		} => {
			let output = read!(facts::Request::SandboxProcesses {
				after: after.clone(),
				limit: *limit,
				sandbox: sandbox.clone(),
			});
			let (after, ids) = output.into_ids()?;

			ReadOutput::Ids { after, ids }
		},
		Read::SubjectGrants {
			after,
			limit,
			subject,
			..
		} => {
			let output = read!(facts::Request::SubjectGrants {
				after: after.clone(),
				limit: *limit,
				subject: subject.clone(),
			});
			let (after, grants) = output.into_grants()?;

			ReadOutput::Grants { after, grants }
		},
	};

	Ok(ControlFlow::Break(output))
}

fn check_matches_output(check: &super::Check, output: facts::Output) -> tg::Result<bool> {
	let value = check.matches(output)?;

	Ok(value)
}

async fn resolve<E>(
	client: &facts::Client<E>,
	selector: &tg::Selector<tg::Id>,
) -> Result<ControlFlow<Option<(tg::Id, bool)>, E>, tg::Error>
where
	E: Clone + Send + Sync + 'static,
{
	match selector {
		tg::Selector::Id(id) => {
			let output = client.read(facts::Request::Id { id: id.clone() }).await?;
			let id = match output {
				ControlFlow::Break(output) => output.into_id()?,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};

			Ok(ControlFlow::Break(id.map(|id| (id, true))))
		},
		tg::Selector::Specifier(specifier) => {
			let mut prefixes = specifier.prefixes().collect::<Vec<_>>();
			prefixes.reverse();
			for prefixes in prefixes.chunks(client.concurrency()) {
				let results =
					futures::future::try_join_all(prefixes.iter().cloned().map(|specifier| {
						let client = client.clone();
						let request = facts::Request::Specifier {
							specifier: specifier.clone(),
						};
						async move {
							let output = client.read(request).await?;

							Ok::<_, tg::Error>((specifier, output))
						}
					}))
					.await?;
				for (prefix, output) in results {
					let id = match output {
						ControlFlow::Break(output) => output.into_id()?,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					if let Some(id) = id {
						let exact = &prefix == specifier;

						return Ok(ControlFlow::Break(Some((id, exact))));
					}
				}
			}

			Ok(ControlFlow::Break(None))
		},
	}
}

fn aspect_matches(
	wanted: crate::process::object::Kind,
	actual: crate::process::object::Kind,
) -> bool {
	match wanted {
		crate::process::object::Kind::Command => actual.is_command(),
		crate::process::object::Kind::Error => actual.is_error(),
		crate::process::object::Kind::Log => actual.is_log(),
		crate::process::object::Kind::Output => actual.is_output(),
	}
}

fn finish_process(state: &mut State, key: &Key, authorized: bool) -> Outcome {
	if authorized {
		state.authorize_derived(key.clone());
	} else {
		state.deny_derived(key);
	}
	match state.outcome(key) {
		outcome @ (Outcome::Authorized | Outcome::Denied) => outcome,
		Outcome::Exhausted => unreachable!(),
		Outcome::Pending => Outcome::Exhausted,
	}
}

fn normalize_permissions(
	resource: Option<&(tg::Id, bool)>,
	permissions: tg::authorization::permission::Set,
) -> tg::Result<Option<tg::authorization::permission::Set>> {
	let Some((resource, exact)) = resource else {
		return Ok(None);
	};
	if *exact {
		return Ok(Some(permissions));
	}

	super::permissions_for_specifier_prefix(resource, permissions)
}

fn principal_is_resource(principal: &tg::Principal, resource: &tg::Id) -> bool {
	matches!(principal, tg::Principal::Process(process) if tg::Id::from(process.clone()) == *resource)
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		crate::authorize::search::{AncestorNodeRead, Grant, MemberRead},
		std::{
			sync::{
				Arc,
				atomic::{AtomicUsize, Ordering},
			},
			time::Duration,
		},
		tokio::sync::Barrier,
	};

	#[test]
	fn a_direct_grant_is_evaluated_by_the_shared_policy() {
		let object = object(0);
		let user = tg::user::Id::new();
		let principal = tg::Principal::User(user.clone());
		let outcome = run(&[arg(object.clone().into(), None)], &principal, |read| {
			let Read::AncestorNode {
				key,
				read: AncestorNodeRead::ResourceGrants { .. },
				..
			} = read
			else {
				return default_output(read);
			};
			let grant = Grant {
				creator: None,
				implicit: false,
				permission: key.1,
				resource: key.0.clone(),
				subject: tg::authorization::Subject::User(user.clone()),
			};
			ReadOutput::Grants {
				after: None,
				grants: vec![grant],
			}
		});

		assert!(matches!(outcome[0], super::super::Outcome::Authorized(_)));
	}

	#[test]
	fn a_group_membership_is_evaluated_by_the_shared_policy() {
		let group = tg::group::Id::new();
		let object = object(0);
		let user = tg::user::Id::new();
		let principal = tg::Principal::User(user);
		let outcome = run(
			&[arg(object.clone().into(), None)],
			&principal,
			|read| match read {
				Read::AncestorNode {
					key,
					read: AncestorNodeRead::ResourceGrants { .. },
					..
				} => {
					let grant = Grant {
						creator: None,
						implicit: false,
						permission: key.1,
						resource: key.0.clone(),
						subject: tg::authorization::Subject::Group(group.clone()),
					};
					ReadOutput::Grants {
						after: None,
						grants: vec![grant],
					}
				},
				Read::Member {
					read: MemberRead::Groups { .. },
					..
				} => ReadOutput::MemberGroups {
					after: None,
					groups: vec![group.clone()],
				},
				_ => default_output(read),
			},
		);

		assert!(matches!(outcome[0], super::super::Outcome::Authorized(_)));
	}

	#[test]
	fn ancestor_and_descendant_membership_searches_meet() {
		let descendant_group = tg::group::Id::new();
		let ancestor_group = tg::group::Id::new();
		let object = object(0);
		let user = tg::user::Id::new();
		let principal = tg::Principal::User(user.clone());
		let outcome = run(
			&[arg(object.clone().into(), None)],
			&principal,
			|read| match read {
				Read::AncestorNode {
					key,
					read: AncestorNodeRead::ResourceGrants { .. },
					..
				} if key.0 == tg::Id::from(object.clone()) => {
					let grant = Grant {
						creator: None,
						implicit: false,
						permission: key.1,
						resource: key.0.clone(),
						subject: tg::authorization::Subject::Group(ancestor_group.clone()),
					};

					ReadOutput::Grants {
						after: None,
						grants: vec![grant],
					}
				},
				Read::GroupMembers { group, .. } if group == &ancestor_group => ReadOutput::Ids {
					after: None,
					ids: vec![descendant_group.clone().into()],
				},
				Read::Member {
					member,
					read: MemberRead::Groups { .. },
					..
				} if member == &tg::Id::from(user.clone()) => ReadOutput::MemberGroups {
					after: None,
					groups: vec![descendant_group.clone()],
				},
				_ => default_output(read),
			},
		);

		assert!(matches!(outcome[0], super::super::Outcome::Authorized(_)));
	}

	#[test]
	fn ancestor_search_traverses_memberships_in_reverse() {
		let group = tg::group::Id::new();
		let object = object(0);
		let user = tg::user::Id::new();
		let principal = tg::Principal::User(user.clone());
		let mut config = super::super::Config::default();
		config.ancestor.max_depth = 1;
		config.descendant.max_nodes = 0;
		let outcome = run_with_config(
			&[arg(object.clone().into(), None)],
			config,
			&principal,
			|read| match read {
				Read::AncestorNode {
					key,
					read: AncestorNodeRead::ResourceGrants { .. },
					..
				} => {
					let grant = Grant {
						creator: None,
						implicit: false,
						permission: key.1,
						resource: key.0.clone(),
						subject: tg::authorization::Subject::Group(group.clone()),
					};

					ReadOutput::Grants {
						after: None,
						grants: vec![grant],
					}
				},
				Read::GroupMembers {
					group: read_group, ..
				} if read_group == &group => ReadOutput::Ids {
					after: None,
					ids: vec![user.clone().into()],
				},
				_ => default_output(read),
			},
		);

		assert!(matches!(outcome[0], super::super::Outcome::Authorized(_)));
	}

	#[test]
	fn descendant_search_traverses_memberships_forward() {
		let group = tg::group::Id::new();
		let object = object(0);
		let user = tg::user::Id::new();
		let principal = tg::Principal::User(user.clone());
		let mut config = super::super::Config::default();
		config.ancestor.max_nodes = 0;
		config.descendant.max_depth = 1;
		let outcome = run_with_config(
			&[arg(object.clone().into(), None)],
			config,
			&principal,
			|read| match read {
				Read::Member {
					member,
					read: MemberRead::Groups { .. },
					..
				} if member == &tg::Id::from(user.clone()) => ReadOutput::MemberGroups {
					after: None,
					groups: vec![group.clone()],
				},
				Read::SubjectGrants {
					subject: tg::authorization::Subject::Group(read_group),
					..
				} if read_group == &group => {
					let grant = Grant {
						creator: None,
						implicit: false,
						permission: tg::authorization::Permission::Object(
							tg::authorization::permission::object::Permission::Node,
						),
						resource: object.clone().into(),
						subject: tg::authorization::Subject::Group(group.clone()),
					};

					ReadOutput::Grants {
						after: None,
						grants: vec![grant],
					}
				},
				_ => default_output(read),
			},
		);

		assert!(matches!(outcome[0], super::super::Outcome::Authorized(_)));
	}

	#[test]
	fn a_missing_resource_is_denied_without_starting_a_search() {
		let object = object(0);
		let outcome = run(
			&[arg(object.into(), None)],
			&tg::Principal::Anonymous,
			|read| match read {
				Read::Resolve { .. } => ReadOutput::Resolved(None),
				_ => panic!("a missing resource must not start an authorization search"),
			},
		);

		assert!(matches!(outcome[0], super::super::Outcome::Denied(None)));
	}

	#[test]
	fn an_invalid_resource_permission_pair_is_denied_by_the_shared_policy() {
		let object = object(0);
		let permissions = tg::authorization::permission::Set::from_permission(
			tg::authorization::Permission::User(
				tg::authorization::permission::user::Permission::Read,
			),
		);
		let arg = super::super::Arg {
			requested: permissions,
			required: permissions,
			resource: tg::Selector::Id(object.into()),
			token: None,
		};
		let outcome = run(&[arg], &tg::Principal::Anonymous, default_output);

		assert!(matches!(outcome[0], super::super::Outcome::Denied(None)));
	}

	#[tokio::test]
	async fn independent_token_contexts_request_facts_concurrently() {
		let first = object(0);
		let second = object(1);
		let args = [
			arg(first.clone().into(), Some(empty_token(object(2)))),
			arg(second.clone().into(), Some(empty_token(object(3)))),
		];
		let active = Arc::new(AtomicUsize::new(0));
		let barrier = Arc::new(Barrier::new(2));
		let maximum = Arc::new(AtomicUsize::new(0));
		let (client, receiver) = facts::channel::<facts::LmdbError>(2);
		let authorize = Batch::authorize(
			&args,
			client,
			super::super::Config::default(),
			&tg::Principal::Anonymous,
		);
		let provide = facts::serve(receiver, 2, {
			let active = active.clone();
			let barrier = barrier.clone();
			let maximum = maximum.clone();
			move |request| {
				let active = active.clone();
				let barrier = barrier.clone();
				let maximum = maximum.clone();
				async move {
					let output = match request {
						facts::Request::Id { id } => facts::Output::Id(Some(id)),
						facts::Request::ObjectParents { .. } => facts::Output::Ids {
							after: None,
							ids: Vec::new(),
						},
						facts::Request::ObjectProcesses { .. } => facts::Output::ObjectProcesses {
							after: None,
							processes: Vec::new(),
						},
						facts::Request::ResourceGrants { .. } => {
							let current = active.fetch_add(1, Ordering::SeqCst) + 1;
							maximum.fetch_max(current, Ordering::SeqCst);
							barrier.wait().await;
							active.fetch_sub(1, Ordering::SeqCst);

							facts::Output::Grants {
								after: None,
								grants: Vec::new(),
							}
						},
						facts::Request::SubjectGrants { .. } => facts::Output::Grants {
							after: None,
							grants: Vec::new(),
						},
						facts::Request::TargetTags { .. } => facts::Output::Tags {
							after: None,
							tags: Vec::new(),
						},
						request => panic!("received an unexpected fact request: {request:?}"),
					};

					Ok(ControlFlow::Break(output))
				}
			}
		});
		let (outcome, ()) = tokio::time::timeout(
			Duration::from_secs(1),
			futures::future::join(authorize, provide),
		)
		.await
		.unwrap();
		let outcome = match outcome.unwrap() {
			ControlFlow::Break(outcome) => outcome,
			ControlFlow::Continue(error) => match error {},
		};

		assert!(
			outcome
				.iter()
				.all(|outcome| matches!(outcome, super::super::Outcome::Denied(_)))
		);
		assert_eq!(maximum.load(Ordering::SeqCst), 2);
	}

	#[test]
	fn process_subtree_uses_the_general_node_outcome_without_deriving_an_aspect() {
		let process = tg::process::Id::new();
		let resource = tg::Id::from(process);
		let node = (
			resource.clone(),
			tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Node,
			),
		);
		let subtree = (
			resource.clone(),
			tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Subtree,
			),
		);
		let mut state = State::default();
		state.deny_ancestor_or_descendant(&node);
		state.deny_ancestor_or_descendant(&subtree);
		let mut search = SubtreeEvaluation::new_process(
			super::super::SubtreeConfig::default(),
			&resource,
			tg::authorization::permission::process::Permission::Subtree,
			Outcome::Denied,
		)
		.unwrap();

		let reads = search
			.take_reads(
				super::super::Config::default(),
				&tg::Principal::Anonymous,
				None,
				&mut state,
				1,
			)
			.unwrap();

		assert!(reads.is_empty());
		assert_eq!(search.outcome(), Some(Outcome::Denied));
	}

	#[test]
	fn process_parent_delegation_ignores_the_grant_source() {
		let matching = object(0);
		let mismatching = object(1);
		let process = tg::process::Id::new();
		let other = tg::process::Id::new();
		let user = tg::user::Id::new();
		let principal = tg::Principal::User(user.clone());
		let args = [
			arg(matching.clone().into(), None),
			arg(mismatching.clone().into(), None),
		];
		let outcome = run(&args, &principal, |read| {
			let Read::AncestorNode {
				key,
				read: AncestorNodeRead::ResourceGrants { .. },
				..
			} = read
			else {
				return default_output(read);
			};
			let grants = if key.0 == tg::Id::from(matching.clone()) {
				vec![process_grant(&matching, &process, &process, key.1)]
			} else if key.0 == tg::Id::from(mismatching.clone()) {
				vec![process_grant(&mismatching, &process, &other, key.1)]
			} else if key.0 == tg::Id::from(process.clone()) {
				vec![Grant {
					creator: None,
					implicit: false,
					permission: key.1,
					resource: key.0.clone(),
					subject: tg::authorization::Subject::User(user.clone()),
				}]
			} else {
				Vec::new()
			};
			ReadOutput::Grants {
				after: None,
				grants,
			}
		});

		assert!(matches!(outcome[0], super::super::Outcome::Authorized(_)));
		assert!(matches!(outcome[1], super::super::Outcome::Authorized(_)));
	}

	#[test]
	fn root_authorization_completes_without_requesting_facts() {
		let object = object(0);
		let outcome = run(&[arg(object.into(), None)], &tg::Principal::Root, |_| {
			panic!("root authorization must not request datastore facts")
		});

		assert!(matches!(outcome[0], super::super::Outcome::Authorized(_)));
	}

	#[test]
	fn token_policy_is_isolated_between_batch_arguments() {
		let parent = object(0);
		let child = object(1);
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let token = tg::authorization::Body {
			expires_at: i64::MAX,
			permissions: vec![permission],
			resource: parent.clone().into(),
		};
		let args = [
			arg(child.clone().into(), Some(token)),
			arg(child.clone().into(), None),
		];
		let outcome = run(&args, &tg::Principal::Anonymous, |read| match read {
			Read::ObjectChildren { object, .. } if object == &parent => ReadOutput::Ids {
				after: None,
				ids: vec![child.clone().into()],
			},
			Read::ObjectParents { object, .. } if object == &child => ReadOutput::Ids {
				after: None,
				ids: vec![parent.clone().into()],
			},
			_ => default_output(read),
		});

		assert!(matches!(outcome[0], super::super::Outcome::Authorized(_)));
		assert!(matches!(outcome[1], super::super::Outcome::Denied(_)));
	}

	fn arg(resource: tg::Id, token: Option<tg::authorization::Body>) -> super::super::Arg {
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let permissions = tg::authorization::permission::Set::from_permission(permission);

		super::super::Arg {
			requested: permissions,
			required: permissions,
			resource: tg::Selector::Id(resource),
			token,
		}
	}

	fn empty_token(resource: tg::object::Id) -> tg::authorization::Body {
		tg::authorization::Body {
			expires_at: i64::MAX,
			permissions: Vec::new(),
			resource: resource.into(),
		}
	}

	fn default_output(read: &Read) -> ReadOutput {
		match read {
			Read::AncestorChecks(checks) => {
				ReadOutput::Bools(vec![false; checks.candidates().len()])
			},
			Read::DescendantChecks(checks) => {
				ReadOutput::Bools(vec![false; checks.candidates().len()])
			},
			Read::AncestorNode { read, .. } => match read {
				AncestorNodeRead::Group { .. } => ReadOutput::Group(None),
				AncestorNodeRead::ObjectProcesses { .. } => ReadOutput::ObjectProcesses {
					after: None,
					processes: Vec::new(),
				},
				AncestorNodeRead::Process { .. } => ReadOutput::Process(None),
				AncestorNodeRead::ResourceGrants { .. } => ReadOutput::Grants {
					after: None,
					grants: Vec::new(),
				},
				AncestorNodeRead::SandboxOwner { .. } => ReadOutput::SandboxOwner(None),
				AncestorNodeRead::Tag { .. } | AncestorNodeRead::TargetTag { .. } => {
					ReadOutput::Tag(None)
				},
				AncestorNodeRead::TargetTags { .. } => ReadOutput::Tags {
					after: None,
					tags: Vec::new(),
				},
			},
			Read::Member { read, .. } => match read {
				MemberRead::Groups { .. } => ReadOutput::MemberGroups {
					after: None,
					groups: Vec::new(),
				},
				MemberRead::Organizations { .. } => ReadOutput::MemberOrganizations {
					after: None,
					organizations: Vec::new(),
				},
			},
			Read::GroupMembers { .. } | Read::OrganizationMembers { .. } => ReadOutput::Ids {
				after: None,
				ids: Vec::new(),
			},
			Read::ObjectChildren { .. }
			| Read::ObjectParents { .. }
			| Read::OwnerSandboxes { .. }
			| Read::ProcessChildren { .. }
			| Read::ProcessParents { .. }
			| Read::Resolve { .. }
			| Read::SandboxProcesses { .. }
			| Read::SubtreeObjectChildren { .. }
			| Read::SubtreeProcessChildren { .. } => match read {
				Read::Resolve { selector, .. } => {
					let resource = match selector {
						tg::Selector::Id(id) => Some((id.clone(), true)),
						tg::Selector::Specifier(_) => None,
					};
					ReadOutput::Resolved(resource)
				},
				_ => ReadOutput::Ids {
					after: None,
					ids: Vec::new(),
				},
			},
			Read::Process { .. } => ReadOutput::Process(None),
			Read::ProcessObjectChildren { .. } | Read::ProcessObjects { .. } => {
				ReadOutput::ProcessObjects {
					after: None,
					objects: Vec::new(),
				}
			},
			Read::SubjectGrants { .. } => ReadOutput::Grants {
				after: None,
				grants: Vec::new(),
			},
		}
	}

	fn object(value: u8) -> tg::object::Id {
		tg::object::Id::new(tg::object::Kind::Blob, &vec![value].into())
	}

	fn process_grant(
		resource: &tg::object::Id,
		subject: &tg::process::Id,
		creator: &tg::process::Id,
		permission: tg::authorization::Permission,
	) -> Grant {
		Grant {
			creator: Some(tg::Principal::Process(creator.clone())),
			implicit: true,
			permission,
			resource: resource.clone().into(),
			subject: tg::authorization::Subject::Process(subject.clone()),
		}
	}

	fn run(
		args: &[super::super::Arg],
		principal: &tg::Principal,
		output: impl FnMut(&Read) -> ReadOutput,
	) -> Vec<super::super::Outcome> {
		run_with_config(args, super::super::Config::default(), principal, output)
	}

	fn run_with_config(
		args: &[super::super::Arg],
		config: super::super::Config,
		principal: &tg::Principal,
		mut output: impl FnMut(&Read) -> ReadOutput,
	) -> Vec<super::super::Outcome> {
		let mut batch = Batch::new(args, config, principal).unwrap();
		while !batch.complete() {
			for read in batch.take_reads(4).unwrap() {
				let value = output(&read);
				batch.apply(read, value).unwrap();
			}
		}

		batch.into_outcomes().unwrap()
	}
}
