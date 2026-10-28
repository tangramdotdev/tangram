use {
	self::proofs::Proofs,
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture, stream::FuturesUnordered},
	num::ToPrimitive as _,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_index::prelude::*,
	tangram_server_core::{
		Session,
		authorization::{Outcome, Output as Authorization},
		verify::{Arg, Mode, Output, VerifyBatchArg, empty_storage},
	},
};

mod proofs;
#[cfg(test)]
mod tests;

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
	current_sync: Option<&'a tg::sync::Id>,
	entries: BTreeMap<RequestKey, Entry>,
	errors: BTreeMap<SyncKey, tg::Error>,
	futures: FuturesUnordered<
		futures::future::BoxFuture<
			'a,
			(
				Request,
				Option<tg::Result<Option<tg::sync::service::Output>>>,
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
	InFlight { abort: futures::future::AbortHandle },
	Queued,
}

#[derive(Clone)]
struct Request {
	arg: tg::sync::service::Arg,
	index_position: usize,
	position: usize,
	sync: tg::sync::Id,
}

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn verify_batch(
		&self,
		session: &Session,
		arg: VerifyBatchArg<'_>,
	) -> tg::Result<Vec<Output>> {
		let VerifyBatchArg {
			args,
			current_sync,
			mode,
			required,
			subject,
			wait_for_requested_permissions,
		} = arg;
		let wait_for_requested_proofs =
			wait_for_requested_permissions && (subject.is_some() || required.is_none());

		let args = args
			.into_iter()
			.map(|arg| {
				let Arg {
					permissions,
					resource,
					storage,
					tokens,
					trusted,
				} = arg;
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
		let mut requests = Requests::new(session, current_sync);

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
					let valid = session
						.server
						.components
						.authorization
						.verify_token(session, token);
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
						&& !matches!(session.context.principal, tg::Principal::Root))
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
			if subject.is_none() && matches!(session.context.principal, tg::Principal::Root) {
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

			// Authorize a principal for its own node using its inherent permissions.
			if let tg::Selector::Id(id) = &resource
				&& subject.is_none()
				&& session.context.principal.to_id().as_ref() == Some(id)
				&& matches!(
					(permissions, &session.context.principal),
					(
						tg::authorization::permission::Set::Process(_),
						tg::Principal::Process(_)
					) | (
						tg::authorization::permission::Set::Sandbox(_),
						tg::Principal::Sandbox(_)
					) | (
						tg::authorization::permission::Set::User(_),
						tg::Principal::User(_)
					)
				) {
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
			) = (&resource, permissions, &session.context.principal)
				&& subject.is_none()
				&& let Ok(process) = tg::process::Id::try_from(id.clone())
				&& match session
					.server
					.components
					.runner
					.state()
					.try_get_process_sandbox(&process)
				{
					Some(process_sandbox) => process_sandbox == *sandbox,
					None => session
						.server
						.components
						.process
						.try_get_process_local_inner(
							session,
							&process,
							false,
							tg::process::Source::Auto,
						)
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
					if let Some(sync) = session
						.server
						.components
						.authorization
						.try_get_sync_id_from_token(session, token)
						&& current_sync != Some(&sync)
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
						let arg = tg::sync::service::Arg {
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
						.unwrap_or_else(|| {
							session
								.server
								.components
								.authorization
								.verify_token(session, &token)
						})
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
		// Retain partial permissions from the index searches and the sync proofs.
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
			let config = tangram_server_core::config::verification_search_config(
				&session.server.config.verification.permissions.initial,
			);
			let results = session
				.server
				.index
				.verify_batch(&index_args, config, &session.context.principal)
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

		let (_, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		let index = futures::future::pending().boxed();
		tokio::pin!(index);
		let mut searching = false;
		let mut search_positions = Vec::<usize>::new();
		let mut searches = vec![true; index_args.len()];
		let mut restart = false;
		let mut errors = vec![None; outputs.len()];
		loop {
			// Stop searching for storage once it is proven.
			for (index_position, (position, arg)) in
				std::iter::zip(&index_positions, &mut index_args).enumerate()
			{
				if stored[*position].contains(resources[*position].1) && !arg.storage.is_empty() {
					arg.storage = arg.storage.empty_like();
					searches[index_position] = true;
					restart |= search_positions.contains(&index_position);
				}
			}

			// Abandon a search whose arguments are stale.
			if searching && restart {
				index.set(futures::future::pending().boxed());
				searching = false;
				for index_position in &search_positions {
					searches[*index_position] = true;
				}
			}
			restart = false;

			let mut sufficient = true;
			for (index_position, (position, arg)) in
				std::iter::zip(&index_positions, &index_args).enumerate()
			{
				outputs[*position] = proofs[*position].output(arg.requested);
				let pending_search = searches[index_position]
					|| (searching && search_positions.contains(&index_position));
				let requested = wait_for_requested_permissions
					&& (pending_search
						|| (wait_for_requested_proofs
							&& (requests.pending(*position)
								|| requests.error(*position).is_some()
								|| errors[*position].is_some())));
				let permissions = if requested {
					arg.requested
				} else {
					arg.required
				};
				if outputs[*position].is_some_and(|output| output.permissions.contains(permissions))
					&& stored[*position].contains(resources[*position].1)
				{
					requests.cancel(*position);
					searches[index_position] = false;
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

			// Search the index only for the positions that are not awaiting a sync, so that the search is a last resort.
			if !searching {
				let positions = (0..index_args.len())
					.filter(|index_position| {
						searches[*index_position]
							&& !requests.awaiting(index_positions[*index_position])
					})
					.collect::<Vec<_>>();
				if !positions.is_empty() {
					for index_position in &positions {
						searches[*index_position] = false;
					}
					let args = positions
						.iter()
						.map(|index_position| index_args[*index_position].clone())
						.collect();
					let sender;
					(sender, receiver) = tokio::sync::mpsc::unbounded_channel();
					index.set(
						self.verify_index_batch(
							session,
							args,
							wait_for_requested_permissions,
							sender,
						)
						.boxed(),
					);
					searching = true;
					search_positions = positions;
				}
			}

			if !searching
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
				result = &mut index, if searching => {
					searching = false;
					match result {
						Ok(results) => {
							for index_position in &search_positions { errors[index_positions[*index_position]] = None; }
							Some(results)
						},
						Err(error) => {
							for index_position in &search_positions { errors[index_positions[*index_position]] = Some(error.clone()); }
							None
						},
					}
				},
				Some(results) = receiver.recv() => Some(results),
				Some((request, result)) = requests.futures.next(), if !requests.futures.is_empty() => {
					if result.is_some() { searches[request.index_position] = true; }
					match result {
						Some(Ok(Some(output))) => {
							let storage = match &output {
								tg::sync::service::Output::Object(output) => tg::storage::Set::Object(output.storage),
								tg::sync::service::Output::Process(output) => tg::storage::Set::Process(output.storage),
							};
							requests.resolve(&request);
							let arg = &mut index_args[request.index_position];
							stored[request.position].insert(storage);
							let now = session.server.clock.unix_timestamp();
							for body in output.tokens() {
								if body.resource != request.arg.node {
									return Err(tg::error!("received an authorization body for the wrong resource from a sync"));
								}
								body.validate_at(now).map_err(|error| tg::error!(!error, "received an invalid authorization body from a sync"))?;

								// Prove the permissions that a sync authorizes for the resource without searching the index.
								let mut permissions = arg.requested.empty_like();
								for permission in arg.requested.iter().filter(|permission| body.authorizes(*permission)) {
									permissions.insert(tg::authorization::permission::Set::from_permission(permission));
								}
								let output = Authorization {
									expires_at: Some(body.expires_at),
									outcome: Outcome::Satisfied,
									permissions,
								};
								proofs[request.position].insert(arg.requested, output);

								if !arg.tokens.contains(body) {
									arg.tokens.push(body.clone());
									restart |= searching && search_positions.contains(&request.index_position);
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
			if let Some(results) = results {
				for (index_position, result) in
					std::iter::zip(search_positions.iter().copied(), results)
				{
					let position = index_positions[index_position];
					let arg = &index_args[index_position];
					stored[position].insert(result.storage);
					exhausted[position] = result.outcome == Outcome::Exhausted;
					let output = Authorization {
						expires_at: result.expires_at,
						outcome: result.outcome,
						permissions: result.permissions,
					};
					proofs[position].insert(arg.requested, output);
				}
			}
		}
	}

	pub(crate) async fn verify_index_batch(
		&self,
		session: &Session,
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
			tangram_server_core::checkpoint!(
				session.server,
				"verification.index",
				resource = %arg.resource,
				storage = !arg.storage.is_empty(),
				token_resource,
			)
			.await;
		}

		// Run at most one authorization search at a time while indexing catches up.
		let verification = &session.server.config.verification;
		let delay = verification.index.delay;
		let initial_config = tangram_server_core::config::verification_search_config(
			&verification.permissions.initial,
		);
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
		let initial = session.server.index.verify_batch(
			&index_args,
			initial_config,
			&session.context.principal,
		);
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
			tangram_server_core::checkpoint!(session.server, "verification.index.wait").await;
			session
				.server
				.components
				.indexer
				.index(session, tg::index::Arg::default())
				.await
				.map_err(|error| tg::error!(!error, "failed to index"))?
				.try_last()
				.await
				.map_err(|error| tg::error!(!error, "failed to index"))?;

			Ok::<_, tg::Error>(())
		};
		let final_config = tangram_server_core::config::verification_search_config(
			&verification.permissions.final_,
		);
		let final_authorization = || async {
			let outcomes = session
				.server
				.index
				.verify_batch(&index_args, final_config, &session.context.principal)
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

		// Verify the process permissions that the index cannot yet prove through each process's parent, which a process on the shortcut path records before the index does.
		let mut index_outcomes = index_outcomes;
		for (arg, outcome) in std::iter::zip(&index_args, &mut index_outcomes) {
			self.verify_index_process_parent(session, arg, outcome, wait_for_requested_permissions)
				.await?;
		}

		Ok(index_outcomes)
	}

	/// Grant the permissions on a process that the index has not recorded whose subtree forms the process's parent grants, finding the parent in the runner's state or the process's control service.
	fn verify_index_process_parent<'a>(
		&'a self,
		session: &'a Session,
		arg: &'a tangram_index::verify::Arg,
		outcome: &'a mut tangram_index::verify::Output,
		wait_for_requested_permissions: bool,
	) -> futures::future::BoxFuture<'a, tg::Result<()>> {
		async move {
			// Skip a satisfied verification, and every verification other than one of process permissions without storage.
			let permissions = if wait_for_requested_permissions {
				arg.requested
			} else {
				arg.required
			};
			if outcome.permissions.contains(permissions) && outcome.storage.contains(arg.storage) {
				return Ok(());
			}
			let tg::Selector::Id(resource) = &arg.resource else {
				return Ok(());
			};
			let Ok(process) = tg::process::Id::try_from(resource.clone()) else {
				return Ok(());
			};
			let (
				tg::authorization::permission::Set::Process(requested),
				tg::authorization::permission::Set::Process(required),
			) = (arg.requested, arg.required)
			else {
				return Ok(());
			};
			if !arg.storage.is_empty() {
				return Ok(());
			}

			// Find the parent of a process that the index has not recorded.
			let Some(parent) = self
				.try_get_unindexed_process_parent(session, &process)
				.await?
			else {
				return Ok(());
			};

			// Verify the subtree forms of the permissions on the parent.
			let subtree = |permissions: tg::authorization::permission::process::Set| {
				let mut subtree = tg::authorization::permission::process::Set::empty();
				for permission in permissions.iter() {
					subtree.insert(
						tg::authorization::permission::process::Set::from_permission(
							permission.to_subtree(),
						),
					);
				}
				subtree
			};
			let parent_arg = tangram_index::verify::Arg {
				requested: tg::authorization::permission::Set::Process(subtree(requested)),
				required: tg::authorization::permission::Set::Process(subtree(required)),
				resource: tg::Selector::Id(parent.into()),
				storage: arg.storage,
				subject: arg.subject.clone(),
				tokens: arg.tokens.clone(),
			};
			let (sender, _) = tokio::sync::mpsc::unbounded_channel();
			let parent_outcome = self
				.verify_index_batch(
					session,
					vec![parent_arg],
					wait_for_requested_permissions,
					sender,
				)
				.await?
				.into_iter()
				.next()
				.ok_or_else(|| tg::error!("expected a parent verification outcome"))?;
			let tg::authorization::permission::Set::Process(parent_permissions) =
				parent_outcome.permissions
			else {
				return Ok(());
			};

			// Grant each permission whose subtree form the parent grants.
			let mut granted = tg::authorization::permission::process::Set::empty();
			for permission in requested.iter() {
				let subtree = tg::authorization::permission::process::Set::from_permission(
					permission.to_subtree(),
				);
				if parent_permissions.contains(subtree) {
					granted.insert(
						tg::authorization::permission::process::Set::from_permission(permission),
					);
				}
			}
			if granted.is_empty() {
				return Ok(());
			}
			outcome
				.permissions
				.insert(tg::authorization::permission::Set::Process(granted));
			outcome.expires_at = match (outcome.expires_at, parent_outcome.expires_at) {
				(Some(left), Some(right)) => Some(left.min(right)),
				(left, right) => left.or(right),
			};
			if outcome.permissions.contains(arg.required) {
				outcome.outcome = tangram_index::verify::Outcome::Satisfied;
			}

			Ok(())
		}
		.boxed()
	}

	/// Get the parent of a process that the index has not recorded, from the runner's state or, waiting for the process's start, from its control service.
	async fn try_get_unindexed_process_parent(
		&self,
		session: &Session,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Id>> {
		// Use the parent of a process that this runner runs.
		let state = session.server.components.runner.state();
		if let Some(sandbox) = state
			.processes()
			.get(process)
			.map(|entry| entry.value().clone())
			&& let Some(sandbox) = state.sandboxes().get_by_id(&sandbox)
			&& let Some(entry) = sandbox.processes.get(process)
		{
			return Ok(entry.parent.clone());
		}

		// Skip a process that the index has recorded, whose parent the index search already follows.
		if session
			.server
			.index
			.try_get_process(process)
			.await?
			.is_some()
		{
			return Ok(None);
		}

		// Skip a process whose ID is outside the proof window, since a process that is still starting has a recent ID.
		let now = session.server.clock.unix_timestamp();
		let now = now.to_u64().unwrap_or_default() * 1000;
		let window = session.server.config.process.id_proof_window;
		let window = window.as_millis().try_into().unwrap_or(u64::MAX);
		if process
			.timestamp()
			.is_none_or(|timestamp| timestamp.abs_diff(now) > window)
		{
			return Ok(None);
		}

		// Ask the process's control service.
		session
			.server
			.components
			.process
			.try_get_process_parent_from_control_service(session, process)
			.await
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
	fn new(session: &'a Session, current_sync: Option<&'a tg::sync::Id>) -> Self {
		Self {
			cancelled: BTreeSet::new(),
			current_sync,
			entries: BTreeMap::new(),
			errors: BTreeMap::new(),
			futures: FuturesUnordered::new(),
			session,
		}
	}

	fn push(&mut self, request: Request) {
		// A sync does not ask itself for permissions.
		if self.current_sync == Some(&request.sync) {
			return;
		}
		let key = request.key();
		let entry = Entry {
			request,
			state: RequestState::Queued,
		};
		self.entries.entry(key).or_insert(entry);
	}

	fn start_queued(&mut self) {
		let requests = self
			.entries
			.values()
			.filter(|entry| matches!(entry.state, RequestState::Queued))
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
		let session = self.session;
		self.futures.push(
			async move {
				let future = session
					.server
					.components
					.sync
					.request_sync_verification(&request.sync, request.arg.clone());
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

	fn awaiting(&self, position: usize) -> bool {
		self.entries
			.values()
			.any(|entry| entry.request.position == position && entry.awaiting())
	}

	fn resolve(&mut self, request: &Request) {
		self.entries.get_mut(&request.key()).unwrap().cancel();
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
		let RequestState::Queued = self.state else {
			return false;
		};
		self.state = RequestState::InFlight { abort };
		true
	}

	fn pending(&self) -> bool {
		match self.state {
			RequestState::Complete | RequestState::Queued => false,
			RequestState::InFlight { .. } => true,
		}
	}

	fn awaiting(&self) -> bool {
		matches!(self.state, RequestState::InFlight { .. })
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

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::verify::Component for Component {
	fn verify_batch<'a>(
		&'a self,
		session: &'a Session,
		arg: VerifyBatchArg<'a>,
	) -> BoxFuture<'a, tg::Result<Vec<Output>>> {
		self.verify_batch(session, arg).boxed()
	}
}
