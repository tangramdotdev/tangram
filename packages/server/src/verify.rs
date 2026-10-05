use {
	crate::{
		Session,
		authorization::{IntoAuthorizationResource, Outcome, Output as Authorization, Proofs},
	},
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	std::{
		collections::{BTreeMap, BTreeSet},
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_index::prelude::*,
	tokio::time::Instant,
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub(crate) struct Output {
	pub expires_at: Option<i64>,
	pub outcome: Outcome,
	pub permissions: tg::authorization::permission::Set,
	pub storage: tg::storage::Set,
}

type RequestKey = (
	usize,
	tg::sync::Id,
	tg::Id,
	Vec<tg::authorization::Permission>,
	Vec<tg::authorization::Permission>,
);
type SyncKey = (usize, tg::sync::Id);

struct Requests<'a> {
	cancelled: BTreeSet<usize>,
	client: Option<Arc<crate::sync::control::Client>>,
	entries: BTreeMap<RequestKey, Entry>,
	errors: BTreeMap<SyncKey, tg::Error>,
	futures: FuturesUnordered<
		futures::future::BoxFuture<
			'a,
			(
				Request,
				Option<tg::Result<Option<tg::sync::control::VerifyServerResponseOutput>>>,
			),
		>,
	>,
	session: &'a Session,
}

struct Entry {
	request: Request,
	state: RequestState,
}

enum RequestState {
	Complete,
	Indexing {
		deadline: Instant,
	},
	InFlight {
		abort: futures::future::AbortHandle,
		deadline: Option<Instant>,
	},
	Queued {
		deadline: Option<Instant>,
	},
	Retry {
		deadline: Instant,
	},
}

#[derive(Clone)]
struct Request {
	arg: tg::sync::control::VerifyClientRequestArg,
	index_position: usize,
	position: usize,
	sync: tg::sync::Id,
}

enum Mode {
	Initial,
	Normal,
}

impl Output {
	pub(crate) fn check_exhaustion(self) -> tg::Result<Self> {
		if self.outcome == Outcome::Exhausted {
			return Err(tangram_index::verify::search_exhausted_error(
				"the verification search exhausted",
			));
		}
		Ok(self)
	}
}

impl Session {
	pub(crate) async fn verify(
		&self,
		resource: impl IntoAuthorizationResource,
		permissions: tg::authorization::permission::Set,
		storage: tg::storage::Set,
	) -> tg::Result<Output> {
		let mut outputs = self
			.verify_batch([(resource, permissions, storage)])
			.await?;
		Ok(outputs.pop().unwrap())
	}

	pub(crate) async fn verify_with_subject(
		&self,
		resource: impl IntoAuthorizationResource,
		requested: tg::authorization::permission::Set,
		required: tg::authorization::permission::Set,
		storage: tg::storage::Set,
		subject: tg::authorization::Subject,
	) -> tg::Result<Output> {
		let mut outputs = self
			.verify_batch_inner_with_subject(
				[(resource, requested, None, storage)],
				Some(required),
				true,
				Some(subject),
				Mode::Normal,
			)
			.await?;
		Ok(outputs.pop().unwrap())
	}

	pub(crate) async fn verify_batch<R, I>(&self, args: I) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set, tg::storage::Set)>,
	{
		let args = args.into_iter().collect::<Vec<_>>();

		let args = args
			.into_iter()
			.map(|(resource, permissions, storage)| (resource, permissions, None, storage));
		let outputs = self.verify_batch_inner(args, None, false).await?;
		Ok(outputs)
	}

	pub(crate) async fn verify_batch_initial<R, I>(
		&self,
		args: I,
		required: tg::authorization::permission::Set,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		let args = args
			.into_iter()
			.map(|(resource, requested)| (resource, requested, None, empty_storage(requested)));
		self.verify_batch_inner_with_subject(args, Some(required), false, None, Mode::Initial)
			.await
	}

	pub(crate) async fn verify_batch_with_required<R, I>(
		&self,
		args: I,
		required: tg::authorization::permission::Set,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set, tg::storage::Set)>,
	{
		let args = args
			.into_iter()
			.map(|(resource, permissions, storage)| (resource, permissions, None, storage));
		self.verify_batch_inner(args, Some(required), false).await
	}

	pub(crate) async fn verify_batch_inner<R, I>(
		&self,
		args: I,
		required: Option<tg::authorization::permission::Set>,
		wait_for_requested_permissions: bool,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<
			Item = (
				R,
				tg::authorization::permission::Set,
				Option<tg::authorization::permission::Set>,
				tg::storage::Set,
			),
		>,
	{
		self.verify_batch_inner_with_subject(
			args,
			required,
			wait_for_requested_permissions,
			None,
			Mode::Normal,
		)
		.await
	}

	async fn verify_batch_inner_with_subject<R, I>(
		&self,
		args: I,
		required: Option<tg::authorization::permission::Set>,
		wait_for_requested_permissions: bool,
		subject: Option<tg::authorization::Subject>,
		mode: Mode,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<
			Item = (
				R,
				tg::authorization::permission::Set,
				Option<tg::authorization::permission::Set>,
				tg::storage::Set,
			),
		>,
	{
		let wait_for_requested_proofs =
			wait_for_requested_permissions && (subject.is_some() || required.is_none());

		let args = args
			.into_iter()
			.map(|(resource, permissions, trusted, storage)| {
				let (resource, tokens) = resource.into_authorization_resource();
				(resource, tokens, permissions, trusted, storage)
			})
			.collect::<Vec<_>>();
		let requirements = args
			.iter()
			.map(|(_, _, permissions, _, storage)| (required.unwrap_or(*permissions), *storage))
			.collect::<Vec<_>>();
		let mut exhausted = vec![false; args.len()];
		let resources = args
			.iter()
			.map(|(resource, _, _, _, storage)| (resource.clone(), *storage))
			.collect::<Vec<_>>();
		let mut stored = resources
			.iter()
			.map(|(_, storage)| storage.empty_like())
			.collect::<Vec<_>>();
		let mut outputs = Vec::new();
		let mut index_args = Vec::new();
		let mut index_positions = Vec::new();
		let mut requests = Requests::new(self);

		for (position, (resource, mut tokens, permissions, trusted, storage)) in
			args.into_iter().enumerate()
		{
			let required = required.unwrap_or(permissions);
			// Scoped verification must not borrow the caller's authority.
			let trusted = if subject.is_none() {
				trusted
			} else {
				tokens.clear();
				None
			};
			if !permissions.contains(required) {
				return Err(tg::error!(
					"the required permissions must be contained in the requested permissions"
				));
			}
			if permissions.is_empty() && storage.is_empty() {
				outputs.push(Some(Authorization {
					expires_at: None,
					outcome: Outcome::Satisfied,
					permissions,
				}));
				continue;
			}
			let mut exact_output = permissions.is_empty().then_some(Authorization {
				expires_at: None,
				outcome: Outcome::Satisfied,
				permissions,
			});

			// Try exact proofs first, without verifying unrelated ancestor tokens.
			tokens.sort_by_key(|token| std::cmp::Reverse(token.body.expires_at));
			let mut verified = vec![None; tokens.len()];
			if let tg::Selector::Id(id) = &resource {
				let mut proven = permissions.empty_like();
				if let Some(trusted) = trusted {
					for permission in permissions
						.iter()
						.filter(|permission| trusted.iter().any(|proof| proof.implies(*permission)))
					{
						proven.insert(tg::authorization::permission::Set::from_permission(
							permission,
						));
					}
				}
				let mut expires_at = i64::MAX;
				for (index, token) in tokens.iter().enumerate() {
					if &token.body.resource != id
						|| !permissions.iter().any(|permission| {
							!proven.contains(permission) && token.body.authorizes(permission)
						}) {
						continue;
					}
					let valid = self.verify_token(token);
					verified[index] = Some(valid);
					if !valid {
						continue;
					}
					for permission in permissions
						.iter()
						.filter(|permission| token.body.authorizes(*permission))
					{
						proven.insert(tg::authorization::permission::Set::from_permission(
							permission,
						));
					}
					expires_at = expires_at.min(token.body.expires_at);
					if proven.contains(permissions) {
						break;
					}
				}
				if !proven.is_empty() {
					exact_output = Some(Authorization {
						expires_at: (expires_at != i64::MAX).then_some(expires_at),
						outcome: Outcome::Satisfied,
						permissions: proven,
					});
				}
				if proven.contains(permissions)
					|| (proven.contains(required)
						&& (trusted.is_some()
							|| matches!(required, tg::authorization::permission::Set::Process(_)))
						&& !wait_for_requested_permissions
						&& !matches!(self.context.principal, tg::Principal::Root))
				{
					let output = Authorization {
						expires_at: (expires_at != i64::MAX).then_some(expires_at),
						outcome: Outcome::Satisfied,
						permissions: proven,
					};
					if stored[position].contains(storage) {
						outputs.push(Some(output));
						continue;
					}
					exact_output = Some(output);
				}
			}

			// Authorize the root principal for all resources.
			if subject.is_none() && matches!(self.context.principal, tg::Principal::Root) {
				let output = Authorization {
					expires_at: None,
					outcome: Outcome::Satisfied,
					permissions,
				};
				if stored[position].contains(storage) {
					outputs.push(Some(output));
					continue;
				}
				exact_output = Some(output);
			}

			// Authorize a sandbox for its own processes.
			if let (
				tg::Selector::Id(id),
				tg::authorization::permission::Set::Process(_),
				tg::Principal::Sandbox(sandbox),
			) = (&resource, permissions, &self.context.principal)
				&& subject.is_none()
				&& let Ok(process) = tg::process::Id::try_from(id.clone())
				&& match self.server.runner.state().try_get_process_sandbox(&process) {
					Some(process_sandbox) => process_sandbox == *sandbox,
					None => self
						.try_get_process_local_inner(&process, false, tg::process::Source::Auto)
						.boxed()
						.await?
						.is_some_and(|output| output.data.sandbox.as_ref() == Some(sandbox)),
				} {
				let output = Authorization {
					expires_at: None,
					outcome: Outcome::Satisfied,
					permissions,
				};
				if stored[position].contains(storage) {
					outputs.push(Some(output));
					continue;
				}
				exact_output = Some(output);
			}

			outputs.push(exact_output);
			// Ask each verified sync for permissions independently of storage and optional proofs.
			if let tg::Selector::Id(id) = &resource {
				let mut syncs = BTreeMap::new();
				for token in &tokens {
					if let Some(sync) = self.try_get_sync_id_from_token(token)
						&& self.sync.as_ref() != Some(&sync)
					{
						syncs.entry(sync).or_insert(token);
					}
				}
				for sync in syncs.into_keys() {
					let requested = if wait_for_requested_permissions {
						permissions
					} else {
						required
					};
					let mut sync_args = requested
						.iter()
						.filter(|permission| {
							!exact_output
								.is_some_and(|output| output.permissions.contains(*permission))
						})
						.map(|permission| {
							(
								tg::authorization::permission::Set::from_permission(permission),
								empty_storage(tg::authorization::permission::Set::from_permission(
									permission,
								)),
							)
						})
						.collect::<Vec<_>>();
					if !stored[position].contains(storage) {
						sync_args.push((permissions.empty_like(), storage));
					}
					for (requested, storage) in sync_args {
						if !matches!(
							requested,
							tg::authorization::permission::Set::Object(_)
								| tg::authorization::permission::Set::Process(_)
						) {
							continue;
						}
						let arg = tg::sync::control::VerifyClientRequestArg {
							node: id.clone(),
							permissions: requested,
							storage,
						};
						let request = Request {
							arg,
							index_position: index_args.len(),
							position,
							sync: sync.clone(),
						};
						requests.push(request);
					}
				}
			}
			let mut tokens: Vec<_> = std::iter::zip(tokens, verified)
				.filter_map(|(token, verified)| {
					verified
						.unwrap_or_else(|| self.verify_token(&token))
						.then_some(token.body)
				})
				.collect();
			if let Some(permissions) = trusted {
				let tg::Selector::Id(resource) = &resource else {
					return Err(tg::error!("expected an ID for the authorization proof"));
				};
				let proof = tg::authorization::Body {
					expires_at: i64::MAX,
					permissions: permissions.iter().collect(),
					resource: resource.clone(),
				};
				tokens.push(proof);
			}

			index_positions.push(position);

			index_args.push(tangram_index::verify::Arg {
				required,
				requested: permissions,
				resource,
				storage,
				subject: subject.clone(),
				tokens,
			});
		}

		if index_args.is_empty() {
			return Ok(Self::verify_outputs(
				outputs,
				stored,
				&requirements,
				&exhausted,
			));
		}
		// Race the index searches with the sync proofs, retaining partial permissions from both.
		let mut proofs = outputs
			.iter()
			.map(|output| {
				let mut proofs = Proofs::default();
				if let Some(output) = output {
					proofs.insert(output.permissions, *output);
				}
				proofs
			})
			.collect::<Vec<_>>();
		// A receiving sync can request a node from the sender instead of waiting for another incoming sync.
		if matches!(mode, Mode::Initial) {
			let config = crate::verification_search_config(
				&self.server.config.verification.permissions.initial,
			);
			let results = self
				.server
				.index
				.verify_batch(&index_args, config, &self.context.principal)
				.await?;
			for ((position, arg), result) in
				std::iter::zip(std::iter::zip(&index_positions, &index_args), results)
			{
				{
					let output = &result;
					stored[*position].insert(result.storage);
					exhausted[*position] = output.outcome == Outcome::Exhausted;
					let output = Authorization {
						expires_at: output.expires_at,
						outcome: Outcome::Satisfied,
						permissions: output.permissions,
					};
					proofs[*position].insert(arg.requested, output);
				}
				outputs[*position] = proofs[*position].output(arg.requested);
			}
			return Ok(Self::verify_outputs(
				outputs,
				stored,
				&requirements,
				&exhausted,
			));
		}

		// Query the syncs supplied by authorization tokens.
		requests.start_queued();

		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		let index = self
			.verify_index_batch(
				index_args.clone(),
				wait_for_requested_permissions,
				sender.clone(),
			)
			.boxed();
		tokio::pin!(index);
		let mut index_done = false;
		let mut refresh_index = false;
		let mut retry_index = false;
		let mut errors = vec![None; outputs.len()];
		let poll_interval = self
			.server
			.config
			.sync
			.control
			.retry_interval
			.max(Duration::from_millis(100));
		let poll = tokio::time::sleep(poll_interval);
		tokio::pin!(poll);
		loop {
			// Stop storage discovery once availability is proven, including after an exhausted search.
			for (position, arg) in std::iter::zip(&index_positions, &mut index_args) {
				if stored[*position].contains(resources[*position].1) {
					requests.resolve_indexing(*position);
				}
				if stored[*position].contains(resources[*position].1) && !arg.storage.is_empty() {
					arg.storage = arg.storage.empty_like();
					refresh_index = true;
				}
			}

			// Apply new authorization or availability without waiting for a stale index attempt.
			if refresh_index || index_done && retry_index {
				index.set(
					self.verify_index_batch(
						index_args.clone(),
						wait_for_requested_permissions,
						sender.clone(),
					)
					.boxed(),
				);
				index_done = false;
				refresh_index = false;
				retry_index = false;
			}
			let mut sufficient = true;
			for (position, arg) in std::iter::zip(&index_positions, &index_args) {
				outputs[*position] = proofs[*position].output(arg.requested);
				let requested = wait_for_requested_permissions
					&& (!index_done
						|| retry_index || (wait_for_requested_proofs
						&& (requests.pending(*position)
							|| requests.error(*position).is_some()
							|| errors[*position].is_some())));
				let permissions = if requested {
					arg.requested
				} else {
					arg.required
				};
				if !retry_index
					&& outputs[*position]
						.is_some_and(|output| output.permissions.contains(permissions))
					&& stored[*position].contains(resources[*position].1)
				{
					requests.cancel(*position);
				} else {
					sufficient = false;
				}
			}
			if sufficient {
				return Ok(Self::verify_outputs(
					outputs,
					stored,
					&requirements,
					&exhausted,
				));
			}
			if index_done
				&& requests.futures.is_empty()
				&& !requests.entries.values().any(Entry::pending)
				&& receiver.is_empty()
			{
				for (position, arg) in std::iter::zip(&index_positions, &index_args) {
					outputs[*position] = proofs[*position].output(arg.requested);
					let permissions = if wait_for_requested_proofs {
						arg.requested
					} else {
						arg.required
					};
					let missing_permissions = !outputs[*position]
						.is_some_and(|output| output.permissions.contains(permissions));
					let missing_storage = !stored[*position].contains(resources[*position].1);
					if (((wait_for_requested_proofs || requests.error(*position).is_some())
						&& missing_permissions)
						|| missing_storage)
						&& let Some(error) = requests.error(*position).cloned()
					{
						return Err(tg::error!(
							!error,
							"failed to verify the resource through a sync"
						));
					}
					if (missing_storage || missing_permissions)
						&& let Some(error) = errors[*position].take()
					{
						return Err(error);
					}
				}
				return Ok(Self::verify_outputs(
					outputs,
					stored,
					&requirements,
					&exhausted,
				));
			}
			let results = tokio::select! {
				biased;
				result = &mut index, if !index_done => {
					index_done = true;
					match result {
						Ok(results) => {
							for position in &index_positions { errors[*position] = None; }
							Some((true, results))
						},
						Err(error) => {
							for position in &index_positions { errors[*position] = Some(error.clone()); }
							None
						},
					}
				},
				Some(results) = receiver.recv() => Some((false, results)),
				() = &mut poll, if requests.polling() => {
					poll.as_mut().reset(Instant::now() + poll_interval);
					requests.retry();
					retry_index = true;
					None
				},
				Some((request, result)) = requests.futures.next(), if !requests.futures.is_empty() => {
					if result.is_some() { retry_index = true; }
					match result {
						Some(Ok(Some(output))) => {
							let storage = match &output {
								tg::sync::control::VerifyServerResponseOutput::Object(output) => tg::storage::Set::Object(output.storage),
								tg::sync::control::VerifyServerResponseOutput::Process(output) => tg::storage::Set::Process(output.storage),
							};
							if !output.permissions().contains(request.arg.permissions)
								|| !storage.contains(request.arg.storage) {
								requests.defer(&request);
							} else {
								requests.resolve(&request);
								let arg = &index_args[request.index_position];
								if !request.arg.storage.is_empty()
									&& arg.resource != tg::Selector::Id(request.arg.node.clone())
									&& !stored[request.position].contains(resources[request.position].1)
								{
									requests.await_indexing(&request);
								}
							}
							let arg = &mut index_args[request.index_position];
							// A child's storage does not prove the parent's storage.
							if arg.resource == tg::Selector::Id(request.arg.node.clone()) { stored[request.position].insert(storage); }
							let now = self.server.clock.unix_timestamp()?;
							for body in output.tokens() {
								if body.resource != request.arg.node {
									return Err(tg::error!("received an authorization body for the wrong resource from a sync"));
								}
								body.validate_at(now).map_err(|error| tg::error!(!error, "received an invalid authorization body from a sync"))?;
								if !arg.tokens.contains(body) {
									arg.tokens.push(body.clone());
									refresh_index = true;
								}
							}
						},
						None => {},
						Some(Ok(None)) => { requests.resolve(&request); },
						Some(Err(error)) => {
							tracing::trace!(%error, "a sync verification request failed");
							requests.set_error(&request, error);
						},
					}
					None
				},
			};
			if let Some((_, results)) = results {
				for (index_position, (position, result)) in
					std::iter::zip(&index_positions, results).enumerate()
				{
					let arg = &index_args[index_position];
					stored[*position].insert(result.storage);
					exhausted[*position] = result.outcome == Outcome::Exhausted;
					let output = Authorization {
						expires_at: result.expires_at,
						outcome: result.outcome,
						permissions: result.permissions,
					};
					proofs[*position].insert(arg.requested, output);

					// Discovery remains useful after permission success and on search exhaustion.
					let output = proofs[*position].output(arg.requested);
					let requested = if wait_for_requested_permissions {
						arg.requested
					} else {
						arg.required
					};
					let missing_permissions =
						!output.is_some_and(|output| output.permissions.contains(requested));
					let missing_storage = !stored[*position].contains(resources[*position].1);
					for sync in result.syncs {
						if self.sync.as_ref() == Some(&sync.sync) {
							continue;
						}
						let mut args = Vec::new();
						let exact = arg.resource == tg::Selector::Id(sync.resource.clone());
						if missing_permissions {
							let permissions = if exact {
								requested
									.iter()
									.filter(|permission| {
										!output.is_some_and(|output| {
											output.permissions.contains(*permission)
										}) && sync.permission.implies(*permission)
									})
									.collect::<Vec<_>>()
							} else {
								vec![sync.permission]
							};
							for permission in permissions {
								let permissions =
									tg::authorization::permission::Set::from_permission(permission);
								args.push(tg::sync::control::VerifyClientRequestArg {
									node: sync.resource.clone(),
									permissions,
									storage: empty_storage(permissions),
								});
							}
						}
						if missing_storage {
							let permissions = if exact {
								tangram_index::verify::storage_permissions(
									resources[*position].1,
									requested,
								)
								.iter()
								.filter(|permission| sync.permission.implies(*permission))
								.collect::<Vec<_>>()
							} else {
								vec![sync.permission]
							};
							for permission in permissions {
								let Some(storage) = permission_storage(permission) else {
									continue;
								};
								if exact && stored[*position].contains(storage) {
									continue;
								}
								let permissions =
									tg::authorization::permission::Set::from_permission(permission)
										.empty_like();
								args.push(tg::sync::control::VerifyClientRequestArg {
									node: sync.resource.clone(),
									permissions,
									storage,
								});
							}
						}

						for arg in args {
							let request = Request {
								arg,
								index_position,
								position: *position,
								sync: sync.sync.clone(),
							};
							requests.push(request);
						}
					}
				}
			}
			// Query newly discovered syncs and retry partial responses within their deadlines.
			requests.start_queued();
		}
	}

	pub(crate) async fn verify_index_batch(
		&self,
		index_args: Vec<tangram_index::verify::Arg>,
		wait_for_requested_permissions: bool,
		sender: tokio::sync::mpsc::UnboundedSender<Vec<tangram_index::verify::Output>>,
	) -> tg::Result<Vec<tangram_index::verify::Output>> {
		for arg in &index_args {
			let token_resource = arg
				.tokens
				.iter()
				.map(|body| body.resource.to_string())
				.collect::<Vec<_>>()
				.join(",");
			crate::checkpoint!(
				self.server,
				"verification.index",
				resource = %arg.resource,
				storage = !arg.storage.is_empty(),
				token_resource,
			)
			.await;
		}

		// Run at most one authorization search at a time while indexing catches up.
		let verification = &self.server.config.verification;
		let delay = verification.index.delay;
		let initial_config = crate::verification_search_config(&verification.permissions.initial);
		let initial_is_sufficient = |outcomes: &[tangram_index::verify::Output]| {
			outcomes.len() == index_args.len()
				&& std::iter::zip(outcomes, &index_args).all(|(outcome, arg)| {
					let permissions = if wait_for_requested_permissions {
						arg.requested
					} else {
						arg.required
					};
					let permissions_satisfied = outcome.permissions.contains(permissions);
					permissions_satisfied && outcome.storage.contains(arg.storage)
				})
		};
		let initial =
			self.server
				.index
				.verify_batch(&index_args, initial_config, &self.context.principal);
		tokio::pin!(initial);
		let initial_result = match delay {
			Some(delay) => tokio::select! {
				result = &mut initial => Some(result),
				() = tokio::time::sleep(delay) => None,
			},
			None => Some((&mut initial).await),
		};
		if let Some(Ok(outcomes)) = &initial_result {
			sender.send(outcomes.clone()).ok();
		}
		let index_wait = async {
			crate::checkpoint!(self.server, "verification.index.wait").await;
			self.index()
				.await
				.map_err(|error| tg::error!(!error, "failed to index"))?
				.try_last()
				.await
				.map_err(|error| tg::error!(!error, "failed to index"))?;

			Ok::<_, tg::Error>(())
		};
		let final_config = crate::verification_search_config(&verification.permissions.final_);
		let final_authorization = || async {
			let outcomes = self
				.server
				.index
				.verify_batch(&index_args, final_config, &self.context.principal)
				.await?;

			Ok::<_, tg::Error>(outcomes)
		};
		let index_outcomes = match initial_result {
			Some(Ok(outcomes)) if initial_is_sufficient(&outcomes) => outcomes,
			Some(Ok(_)) => {
				index_wait.await?;
				final_authorization().await?
			},
			Some(Err(error)) => return Err(error),
			None => {
				tokio::pin!(index_wait);
				tokio::select! {
					result = &mut initial => match result {
						Ok(outcomes) if initial_is_sufficient(&outcomes) => outcomes,
						Ok(outcomes) => {
							sender.send(outcomes).ok();
							index_wait.await?;
							final_authorization().await?
						},
						Err(error) => return Err(error),
					},
					result = &mut index_wait => match result {
						Ok(()) => match initial.await {
							Ok(outcomes) if initial_is_sufficient(&outcomes) => outcomes,
							Ok(_) | Err(_) => final_authorization().await?,
						},
						Err(error) => match initial.await {
							Ok(outcomes) if initial_is_sufficient(&outcomes) => outcomes,
							Ok(_) | Err(_) => return Err(error),
						},
					},
				}
			},
		};
		Ok(index_outcomes)
	}

	fn verify_outputs(
		outputs: Vec<Option<Authorization>>,
		stored: Vec<tg::storage::Set>,
		requirements: &[(tg::authorization::permission::Set, tg::storage::Set)],
		exhausted: &[bool],
	) -> Vec<Output> {
		std::iter::zip(outputs, stored)
			.enumerate()
			.map(|(position, (output, storage))| {
				let (required_permissions, required_storage) = requirements[position];
				let output = output.unwrap_or(Authorization {
					expires_at: None,
					outcome: Outcome::Unsatisfied,
					permissions: required_permissions.empty_like(),
				});
				let permissions_satisfied = output.permissions.contains(required_permissions);
				let outcome = if permissions_satisfied && storage.contains(required_storage) {
					Outcome::Satisfied
				} else if exhausted[position] {
					Outcome::Exhausted
				} else {
					Outcome::Unsatisfied
				};
				Output {
					expires_at: output.expires_at,
					outcome,
					permissions: output.permissions,
					storage,
				}
			})
			.collect()
	}
}

impl<'a> Requests<'a> {
	fn new(session: &'a Session) -> Self {
		Self {
			cancelled: BTreeSet::new(),
			client: None,
			entries: BTreeMap::new(),
			errors: BTreeMap::new(),
			futures: FuturesUnordered::new(),
			session,
		}
	}

	fn push(&mut self, request: Request) {
		if self.session.sync.as_ref() == Some(&request.sync) {
			return;
		}
		let key = request.key();
		let entry = Entry {
			request,
			state: RequestState::Queued { deadline: None },
		};
		self.entries.entry(key).or_insert(entry);
	}

	fn start_queued(&mut self) {
		let requests = self
			.entries
			.values()
			.filter(|entry| matches!(entry.state, RequestState::Queued { .. }))
			.map(|entry| entry.request.clone())
			.collect::<Vec<_>>();
		for request in requests {
			if !self.cancelled.contains(&request.position) {
				self.start(request);
			}
		}
	}

	fn start(&mut self, request: Request) {
		if self.errors.contains_key(&request.sync_key()) {
			return;
		}
		let (abort, registration) = futures::future::AbortHandle::new_pair();
		if !self.entries.get_mut(&request.key()).unwrap().start(abort) {
			return;
		}
		let client = self
			.client
			.get_or_insert_with(|| {
				self.session
					.sync_control
					.clone()
					.unwrap_or_else(|| Arc::new(crate::sync::control::Client::default()))
			})
			.clone();
		let session = self.session;
		self.futures.push(
			async move {
				let future = async {
					let mut control = client.request(
						session,
						&request.sync,
						tg::sync::control::ClientRequestArg::Verify(request.arg.clone()),
					);
					control.wait().await
				};
				let result = futures::future::Abortable::new(future, registration)
					.await
					.ok();
				(request, result)
			}
			.boxed(),
		);
	}

	fn pending(&self, position: usize) -> bool {
		self.entries
			.values()
			.any(|entry| entry.request.position == position && entry.pending())
	}

	fn defer(&mut self, request: &Request) {
		if self.cancelled.contains(&request.position)
			|| self.errors.contains_key(&request.sync_key())
		{
			return;
		}
		let timeout = self.session.server.config.sync.control.request_timeout;
		self.entries
			.get_mut(&request.key())
			.unwrap()
			.defer(Instant::now() + timeout);
	}

	fn resolve(&mut self, request: &Request) {
		self.entries.get_mut(&request.key()).unwrap().cancel();
	}

	fn await_indexing(&mut self, request: &Request) {
		// Keep a descendant's storage proof pending until indexing proves the root's storage.
		let timeout = self.session.server.config.sync.control.request_timeout;
		self.entries
			.get_mut(&request.key())
			.unwrap()
			.await_indexing(Instant::now() + timeout);
	}

	fn resolve_indexing(&mut self, position: usize) {
		for entry in self.entries.values_mut() {
			if entry.request.position == position
				&& matches!(entry.state, RequestState::Indexing { .. })
			{
				entry.cancel();
			}
		}
	}

	fn polling(&self) -> bool {
		self.entries
			.values()
			.any(|entry| entry.deadline().is_some())
	}

	fn retry(&mut self) {
		let now = Instant::now();
		let expired = self
			.entries
			.values()
			.filter(|entry| entry.deadline().is_some_and(|deadline| now >= deadline))
			.map(|entry| entry.request.clone())
			.collect::<Vec<_>>();
		for request in expired {
			let error = tg::error!(sync = %request.sync, "the sync and indexing did not prove the requested requirements before the timeout");
			self.set_error(&request, error);
		}
		for entry in self.entries.values_mut() {
			entry.retry();
		}
	}

	fn error(&self, position: usize) -> Option<&tg::Error> {
		self.errors
			.iter()
			.find_map(|((request_position, _), error)| {
				(*request_position == position).then_some(error)
			})
	}

	fn set_error(&mut self, request: &Request, error: tg::Error) {
		if self.cancelled.contains(&request.position) {
			return;
		}
		let key = request.sync_key();
		for entry in self.entries.values_mut() {
			if entry.request.sync_key() == key {
				entry.cancel();
			}
		}
		self.errors.insert(key, error);
	}

	fn cancel(&mut self, position: usize) {
		self.cancelled.insert(position);
		for entry in self.entries.values_mut() {
			if entry.request.position == position {
				entry.cancel();
			}
		}
	}
}

impl Entry {
	fn start(&mut self, abort: futures::future::AbortHandle) -> bool {
		let RequestState::Queued { deadline } = self.state else {
			return false;
		};
		self.state = RequestState::InFlight { abort, deadline };
		true
	}

	fn pending(&self) -> bool {
		match self.state {
			RequestState::Complete => false,
			RequestState::Indexing { .. }
			| RequestState::InFlight { .. }
			| RequestState::Retry { .. } => true,
			RequestState::Queued { deadline } => deadline.is_some(),
		}
	}

	fn defer(&mut self, deadline: Instant) {
		let deadline = self.deadline().unwrap_or(deadline);
		self.state = RequestState::Retry { deadline };
	}

	fn await_indexing(&mut self, deadline: Instant) {
		let deadline = self.deadline().unwrap_or(deadline);
		self.state = RequestState::Indexing { deadline };
	}

	fn deadline(&self) -> Option<Instant> {
		match self.state {
			RequestState::Complete => None,
			RequestState::Indexing { deadline } | RequestState::Retry { deadline } => {
				Some(deadline)
			},
			RequestState::InFlight { deadline, .. } | RequestState::Queued { deadline } => deadline,
		}
	}

	fn retry(&mut self) {
		if let RequestState::Retry { deadline } = self.state {
			self.state = RequestState::Queued {
				deadline: Some(deadline),
			};
		}
	}

	fn cancel(&mut self) {
		if let RequestState::InFlight { abort, .. } = &self.state {
			abort.abort();
		}
		self.state = RequestState::Complete;
	}
}

impl Request {
	fn key(&self) -> RequestKey {
		let storage = self.arg.storage;
		(
			self.position,
			self.sync.clone(),
			self.arg.node.clone(),
			self.arg.permissions.iter().collect(),
			tangram_index::verify::storage_permissions(storage, self.arg.permissions)
				.iter()
				.collect(),
		)
	}

	fn sync_key(&self) -> SyncKey {
		(self.position, self.sync.clone())
	}
}

pub(crate) fn empty_storage(permissions: tg::authorization::permission::Set) -> tg::storage::Set {
	match permissions {
		tg::authorization::permission::Set::Process(_) => {
			tg::storage::Set::Process(tg::process::storage::Set::empty())
		},
		_ => tg::storage::Set::Object(tg::object::storage::Set::empty()),
	}
}

fn permission_storage(permission: tg::authorization::Permission) -> Option<tg::storage::Set> {
	match permission {
		tg::authorization::Permission::Object(permission) => {
			let storage = match permission {
				tg::authorization::permission::object::Permission::Node => {
					tg::object::storage::Storage::Node
				},
				tg::authorization::permission::object::Permission::Subtree => {
					tg::object::storage::Storage::Subtree
				},
			};
			Some(tg::storage::Set::Object(
				tg::object::storage::Set::from_storage(storage),
			))
		},
		tg::authorization::Permission::Process(permission) => {
			let storage = match permission {
				tg::authorization::permission::process::Permission::Node => {
					tg::process::storage::Storage::Node
				},
				tg::authorization::permission::process::Permission::NodeCommandObjects => {
					tg::process::storage::Storage::NodeCommandObjects
				},
				tg::authorization::permission::process::Permission::NodeErrorObjects => {
					tg::process::storage::Storage::NodeErrorObjects
				},
				tg::authorization::permission::process::Permission::NodeLogObjects => {
					tg::process::storage::Storage::NodeLogObjects
				},
				tg::authorization::permission::process::Permission::NodeOutputObjects => {
					tg::process::storage::Storage::NodeOutputObjects
				},
				tg::authorization::permission::process::Permission::Subtree => {
					tg::process::storage::Storage::Subtree
				},
				tg::authorization::permission::process::Permission::SubtreeCommandObjects => {
					tg::process::storage::Storage::SubtreeCommandObjects
				},
				tg::authorization::permission::process::Permission::SubtreeErrorObjects => {
					tg::process::storage::Storage::SubtreeErrorObjects
				},
				tg::authorization::permission::process::Permission::SubtreeLogObjects => {
					tg::process::storage::Storage::SubtreeLogObjects
				},
				tg::authorization::permission::process::Permission::SubtreeOutputObjects => {
					tg::process::storage::Storage::SubtreeOutputObjects
				},
				tg::authorization::permission::process::Permission::Parent => return None,
			};
			Some(tg::storage::Set::Process(
				tg::process::storage::Set::from_storage(storage),
			))
		},
		_ => None,
	}
}
