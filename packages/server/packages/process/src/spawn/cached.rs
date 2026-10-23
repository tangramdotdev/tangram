use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	num::ToPrimitive as _,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_server_core::Session,
};

type Candidate = (
	tg::process::Id,
	tangram_index::process::Process,
	tg::process::Data,
);

enum CandidateOutcome {
	Active,
	Failure,
	Success,
}

pub(super) struct Output {
	guard: Option<super::lease::LeaseGuard>,
	output: tg::process::spawn::Output,
}

struct TryGetCachedProcessWithMismatchedChecksumLocalArg<'a> {
	candidate_indices: &'a [usize],
	candidates: &'a [Candidate],
	command: &'a tg::Referent<tg::command::Id>,
	cycle: &'a mut Option<tg::process::Id>,
	public: bool,
	spawn_arg: &'a tg::process::spawn::Arg,
}

impl Output {
	pub fn new(session: &Session, output: tg::process::spawn::Output) -> Self {
		let guard = super::lease::LeaseGuard::new(session, &output);
		Self { guard, output }
	}

	pub fn into_output(mut self) -> tg::process::spawn::Output {
		if let Some(mut guard) = self.guard.take() {
			guard.disarm();
		}
		self.output
	}

	pub fn process(&self) -> Option<&tg::process::Id> {
		self.output.process.as_ref().right()
	}
}

impl Component {
	pub(super) async fn try_get_cached_process_local(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		command: &tg::Referent<tg::command::Id>,
	) -> tg::Result<Option<super::local::Output>> {
		match self
			.try_get_cached_process_local_inner(session, arg, command)
			.await
		{
			Ok(output) => Ok(output),
			Err(error) if matches!(error.code(), Some(tg::error::Code::CycleDetection)) => {
				Err(error)
			},
			Err(error) => {
				tracing::error!(error = %error.trace(), "failed to get a cached process");
				Ok(None)
			},
		}
	}

	async fn try_get_cached_process_local_inner(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		command: &tg::Referent<tg::command::Id>,
	) -> tg::Result<Option<super::local::Output>> {
		let public = arg.public && arg.cached == Some(true);

		// List the candidates.
		let candidates = session
			.server
			.index
			.try_get_cached_processes(&command.node.clone().into())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to query the index for cached processes")
			})?;
		// A running remote candidate cannot acquire a lease through local process control.
		let candidates = candidates
			.into_iter()
			.filter(|(_, process, data)| {
				!process
					.location
					.as_ref()
					.is_some_and(tg::Location::is_remote)
					|| data.status.is_finished()
			})
			.collect::<Vec<_>>();
		let mut cycle = None;

		// Try the normal candidates.
		let normal_candidate_indices =
			Self::cached_process_normal_candidate_indices(arg, &candidates);
		let output = self
			.try_get_cached_process_normal_local(
				session,
				arg,
				&candidates,
				&normal_candidate_indices,
				&mut cycle,
				public,
			)
			.await?;
		if output.is_some() {
			return Ok(output);
		}

		// Try the mismatched checksum candidates.
		let mismatched_checksum_candidate_indices =
			Self::cached_process_mismatched_checksum_candidate_indices(arg, &candidates);
		let mismatched_arg = TryGetCachedProcessWithMismatchedChecksumLocalArg {
			candidate_indices: &mismatched_checksum_candidate_indices,
			candidates: &candidates,
			command,
			cycle: &mut cycle,
			public,
			spawn_arg: arg,
		};
		let output = Box::pin(
			self.try_get_cached_process_with_mismatched_checksum_local(session, mismatched_arg),
		)
		.await?;
		if output.is_some() {
			return Ok(output);
		}

		// Report a cycle if no candidate succeeded.
		if let Some(child) = cycle {
			let parent = arg.parent.as_ref().unwrap();
			return Err(tg::error!(
				code = tg::error::Code::CycleDetection,
				%child,
				%parent,
				"adding this child process creates a cycle"
			));
		}

		Ok(None)
	}

	fn cached_process_normal_candidate_indices(
		arg: &tg::process::spawn::Arg,
		candidates: &[Candidate],
	) -> Vec<usize> {
		let candidate_outcomes = candidates
			.iter()
			.enumerate()
			.filter(|(_, (_, _, data))| data.cacheable && data.checksum.expected == arg.checksum)
			.map(|(index, (_, _, data))| (index, Self::cached_process_outcome(data)));

		Self::prioritize_cached_process_candidate_indices(candidate_outcomes, arg.retry)
	}

	async fn try_get_cached_process_normal_local(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		candidates: &[Candidate],
		candidate_indices: &[usize],
		cycle: &mut Option<tg::process::Id>,
		public: bool,
	) -> tg::Result<Option<super::local::Output>> {
		let batch_size = session.server.config.process.spawn.authorization_batch_size;
		for candidate_indices in candidate_indices.chunks(batch_size) {
			let authorizations = self
				.authorize_cached_process_candidates(session, candidates, candidate_indices, public)
				.await?;
			for (index, authorized) in std::iter::zip(candidate_indices, authorizations) {
				if !authorized {
					continue;
				}
				let (id, process, data) = &candidates[*index];
				if !self
					.touch_cached_process_if_needed(session, id, process, data)
					.await?
				{
					continue;
				}
				if self
					.cached_process_creates_cycle(session, arg.parent.as_ref(), id)
					.await?
				{
					cycle.get_or_insert_with(|| id.clone());
					continue;
				}
				let output = Self::cached_process_output(session, id.clone(), data.clone())?;
				if let Some(output) = self
					.acquire_cached_process_lease(session, output)
					.boxed()
					.await?
				{
					if !self.put_cached_process_account(session, id).await? {
						continue;
					}
					return Ok(Some(output));
				}
			}
		}

		Ok(None)
	}

	fn cached_process_mismatched_checksum_candidate_indices(
		arg: &tg::process::spawn::Arg,
		candidates: &[Candidate],
	) -> Vec<usize> {
		let Some(expected_checksum) = &arg.checksum else {
			return Vec::new();
		};
		let candidate_outcomes =
			candidates
				.iter()
				.enumerate()
				.filter_map(|(index, (_, _, data))| {
					let actual_checksum = data.checksum.actual.as_ref()?;
					let source_expected_checksum = data.checksum.expected.as_ref()?;
					if !data.cacheable
						|| source_expected_checksum == actual_checksum
						|| !matches!(
							Self::cached_process_error_code(data),
							Some(tg::error::Code::ChecksumMismatch)
						) || actual_checksum.algorithm() != expected_checksum.algorithm()
					{
						return None;
					}
					let outcome = if actual_checksum == expected_checksum {
						CandidateOutcome::Success
					} else {
						CandidateOutcome::Failure
					};

					Some((index, outcome))
				});

		Self::prioritize_cached_process_candidate_indices(candidate_outcomes, arg.retry)
	}

	async fn try_get_cached_process_with_mismatched_checksum_local(
		&self,
		session: &Session,
		arg: TryGetCachedProcessWithMismatchedChecksumLocalArg<'_>,
	) -> tg::Result<Option<super::local::Output>> {
		let TryGetCachedProcessWithMismatchedChecksumLocalArg {
			candidate_indices,
			candidates,
			command,
			cycle,
			public,
			spawn_arg,
		} = arg;
		let batch_size = session.server.config.process.spawn.authorization_batch_size;
		for candidate_indices in candidate_indices.chunks(batch_size) {
			let authorizations = self
				.authorize_cached_process_candidates(session, candidates, candidate_indices, public)
				.await?;
			for (index, authorized) in std::iter::zip(candidate_indices, authorizations) {
				if !authorized {
					continue;
				}
				let (source, process, data) = &candidates[*index];
				if !self
					.touch_cached_process_if_needed(session, source, process, data)
					.await?
				{
					continue;
				}
				if self
					.cached_process_creates_cycle(session, spawn_arg.parent.as_ref(), source)
					.await?
				{
					cycle.get_or_insert_with(|| source.clone());
					continue;
				}
				let mut data = data.clone();
				self.set_process_children_from_index(
					session,
					source,
					process.set.children,
					&mut data,
				)
				.await?;
				let host = data.host.clone();
				let output =
					Box::pin(self.create_mismatched_checksum_process(
						session, spawn_arg, command, &host, data,
					))
					.await?;

				return Ok(Some(output));
			}
		}

		Ok(None)
	}

	async fn authorize_cached_process_candidates(
		&self,
		session: &Session,
		candidates: &[Candidate],
		candidate_indices: &[usize],
		public: bool,
	) -> tg::Result<Vec<bool>> {
		let mut permissions = tg::authorization::permission::process::Set::NODE;
		permissions.insert(tg::authorization::permission::process::Set::NODE_ERROR_OBJECTS);
		permissions.insert(tg::authorization::permission::process::Set::NODE_OUTPUT_OBJECTS);
		let permissions = tg::authorization::permission::Set::Process(permissions);
		let args = candidate_indices
			.iter()
			.map(|index| {
				let resource = tg::Selector::<tg::Id>::Id(candidates[*index].0.clone().into());
				(resource, permissions)
			})
			.collect::<Vec<_>>();
		let principal = if public {
			tg::Principal::Anonymous
		} else {
			self.cache_principal(session).await?
		};
		let context = tangram_server_core::context::Context {
			principal,
			token: None,
			..session.context.clone()
		};
		let authorizations = session
			.server
			.session(&context)
			.authorize_batch(args, None)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the cached processes"))?;
		tangram_server_core::authorization::check_exhaustion(&authorizations)?;
		let authorized = authorizations
			.into_iter()
			.map(|authorization| authorization.permissions.contains(permissions))
			.collect();

		Ok(authorized)
	}

	async fn cache_principal(&self, session: &Session) -> tg::Result<tg::Principal> {
		let tg::Principal::Process(id) = &session.context.principal else {
			return Ok(session.context.principal.clone());
		};
		let process = session
			.try_get_authenticated_process(id)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the authenticated process"))?;
		let sandbox = process.sandbox;
		let owner = if let Some(sandbox) = session
			.server
			.runner
			.state()
			.sandboxes()
			.get_by_id(&sandbox)
		{
			sandbox.create.owner.clone()
		} else {
			session
				.server
				.components
				.sandbox
				.get_sandbox_from_index(session, &sandbox)
				.await
				.map_err(|error| tg::error!(!error, %sandbox, "failed to get the sandbox"))?
				.data
				.ok_or_else(|| tg::error!(%sandbox, "missing the sandbox data"))?
				.data
				.owner
		};
		let owner = owner.unwrap_or(tg::Principal::Root);

		Ok(owner)
	}

	fn prioritize_cached_process_candidate_indices(
		candidate_outcomes: impl IntoIterator<Item = (usize, CandidateOutcome)>,
		retry: bool,
	) -> Vec<usize> {
		let mut active = Vec::new();
		let mut failed = Vec::new();
		let mut successful = Vec::new();
		for (index, outcome) in candidate_outcomes {
			match outcome {
				CandidateOutcome::Active => active.push(index),
				CandidateOutcome::Failure if !retry => failed.push(index),
				CandidateOutcome::Failure => (),
				CandidateOutcome::Success => successful.push(index),
			}
		}
		successful.extend(active);
		successful.extend(failed);

		successful
	}

	fn cached_process_outcome(data: &tg::process::Data) -> CandidateOutcome {
		let failed = data.error.is_some() || data.exit.is_some_and(|exit| exit != 0);
		if failed {
			CandidateOutcome::Failure
		} else if data.status.is_finished() {
			CandidateOutcome::Success
		} else {
			CandidateOutcome::Active
		}
	}

	async fn acquire_cached_process_lease(
		&self,
		session: &Session,
		mut output: super::local::Output,
	) -> tg::Result<Option<super::local::Output>> {
		if output.data.status.is_finished() {
			return Ok(Some(output));
		}
		let request = tg::process::control::ServerRequestArg::AcquireLease(
			tg::process::control::AcquireLeaseServerRequestArg {},
		);
		let retry = session.server.config.control.request_retry.clone().into();
		let timeout = session.server.config.control.request_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		let location = tg::Location::Local(tg::location::Local::default()).into();
		let runner = Self::try_get_process_runner_inner(session, &output.id, Some(&location));
		let control = runner.as_ref().and_then(|runner| {
			let process = runner.processes.get(&output.id)?;
			let data = process.data.status.is_finished().then(|| process.data());
			Some((process.control_sender.clone(), data))
		});
		let response = if let Some((control_sender, data)) = control {
			if let Some(data) = data {
				output.data = data;
				return Ok(output.data.cacheable.then_some(output));
			}
			match control_sender.send_request(request).await {
				Err(error) => Err(error),
				Ok(response) => response.await,
			}
		} else {
			self.request_process_control(session, &output.id, request, options)
				.await
		};
		let response = match response {
			Err(error) => {
				if let Some(data) = runner.as_ref().and_then(|runner| {
					let process = runner.processes.get(&output.id)?;
					process.data.status.is_finished().then(|| process.data())
				}) {
					output.data = data;
					return Ok(output.data.cacheable.then_some(output));
				}
				if let Some(process) = self
					.try_get_process_local_inner(
						session,
						&output.id,
						false,
						tg::process::Source::Auto,
					)
					.await? && process.data.status.is_finished()
				{
					output.data = process.data;
					return Ok(output.data.cacheable.then_some(output));
				}
				return Err(
					tg::error!(!error, process = %output.id, "failed to acquire a process lease"),
				);
			},
			Ok(response) => response,
		}
		.map_err(
			|error| tg::error!(!error, process = %output.id, "the acquire process lease request failed"),
		)?;
		let response = response
			.try_unwrap_acquire_lease()
			.map_err(|_| tg::error!("expected an acquire process lease response"))?;
		output.data = response.data;
		output.lease = response.lease;
		tangram_server_core::checkpoint!(
			session.server,
			"process.spawn.cached.lease.acquire",
			process = %output.id,
		)
		.await;
		if !output.data.cacheable || (!output.data.status.is_finished() && output.lease.is_none()) {
			return Ok(None);
		}
		Ok(Some(output))
	}

	async fn cached_process_creates_cycle(
		&self,
		session: &Session,
		parent: Option<&tg::process::Id>,
		child: &tg::process::Id,
	) -> tg::Result<bool> {
		let Some(parent) = parent else {
			return Ok(false);
		};
		let cycle = session
			.server
			.index
			.process_has_ancestor(parent, child)
			.await
			.map_err(
				|error| tg::error!(!error, %parent, %child, "failed to check for a process cycle"),
			)?;

		Ok(cycle)
	}

	fn cached_process_error_code(data: &tg::process::Data) -> Option<tg::error::Code> {
		let Some(error) = &data.error else {
			return None;
		};
		match &error.node {
			tg::Either::Left(data) => data.code,
			tg::Either::Right(_) => None,
		}
	}

	async fn touch_cached_process_if_needed(
		&self,
		session: &Session,
		id: &tg::process::Id,
		process: &tangram_index::process::Process,
		data: &tg::process::Data,
	) -> tg::Result<bool> {
		if !data.status.is_finished() {
			return Ok(true);
		}
		let now = session.server.clock.unix_timestamp();
		let time_to_index = session
			.server
			.config
			.process
			.time_to_index
			.as_secs()
			.to_i64()
			.unwrap();
		if process.touched_at > now - time_to_index {
			return Ok(true);
		}
		let process = session
			.server
			.index
			.touch_process(id, now, session.server.config.process.time_to_touch)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to touch the process"))?;
		Ok(process.is_some())
	}

	async fn put_cached_process_account(
		&self,
		session: &Session,
		id: &tg::process::Id,
	) -> tg::Result<bool> {
		let Some(account) = session.usage_account(&session.context.principal).await? else {
			return Ok(true);
		};
		let touched_at = session.server.clock.unix_timestamp();
		let process = session
			.server
			.index
			.touch_process_and_put_account(
				id,
				&account,
				touched_at,
				session.server.config.process.time_to_touch,
			)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to own the cached process"))?;

		Ok(process.is_some())
	}

	fn cached_process_output(
		session: &Session,
		id: tg::process::Id,
		data: tg::process::Data,
	) -> tg::Result<super::local::Output> {
		let now = session.server.clock.unix_timestamp();
		let token = Self::create_process_wait_token(session, &id, now)?;
		Ok(super::local::Output {
			allocation: None,
			open: None,
			cached: true,
			data,
			id,
			lease: None,
			parent: None,
			parent_sandbox: None,
			process_token: None,
			sandbox_arg: None,
			sandbox_token: None,
			scheduler: None,
			tokens: tg::authorization::Tokens::with_authorization(token),
		})
	}

	async fn create_mismatched_checksum_process(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		command: &tg::Referent<tg::command::Id>,
		host: &str,
		source: tg::process::Data,
	) -> tg::Result<super::local::Output> {
		let expected_checksum = arg.checksum.clone().unwrap();
		let actual_checksum = source.checksum.actual.clone().unwrap();
		let (exit, error) = if expected_checksum == actual_checksum {
			(0, None)
		} else {
			let error = tg::error::Data {
				code: Some(tg::error::Code::ChecksumMismatch),
				message: Some("checksum mismatch".into()),
				values: [
					("expected".into(), expected_checksum.to_string()),
					("actual".into(), actual_checksum.to_string()),
				]
				.into(),
				..Default::default()
			};
			(1, Some(error))
		};
		let tty = arg
			.tty
			.as_ref()
			.map(|tty| {
				tty.as_ref()
					.right()
					.copied()
					.ok_or_else(|| tg::error!("invalid tty"))
			})
			.transpose()?;
		let now = session.server.clock.unix_timestamp();
		let id = tg::process::Id::new();
		let checksum = tg::process::Checksum {
			actual: Some(actual_checksum),
			expected: Some(expected_checksum),
		};
		let data = tg::process::Data {
			cacheable: true,
			checksum,
			children: source.children,
			command: tg::Referent::new(source.command.node, command.options.clone()),
			created_at: now,
			debug: arg.debug.clone(),
			error: error
				.clone()
				.map(|error| tg::Referent::with_node(tg::Either::Left(Box::new(error)))),
			exit: Some(exit),
			finished_at: Some(now),
			host: host.to_owned(),
			log: None,
			output: source.output,
			retry: arg.retry,
			sandbox: None,
			started_at: None,
			status: tg::process::Status::Finished,
			stderr: arg.stderr.clone(),
			stdin: arg.stdin.clone(),
			stdout: arg.stdout.clone(),
			tty,
		};
		let entry = tg::process::put::Arg {
			data: data.clone(),
			location: None,
		};
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let options = tangram_server_core::process::put::Options {
			command_sync: None,
			defer_index: false,
			location: Some(location),
			outcome_sync: None,
			store_data: true,
		};
		let output = self
			.put_process_local(session, &id, entry, options)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to store the process"))?;
		Ok(super::local::Output {
			allocation: None,
			open: None,
			cached: true,
			data,
			id,
			lease: None,
			parent: None,
			parent_sandbox: None,
			process_token: None,
			sandbox_arg: None,
			sandbox_token: None,
			scheduler: None,
			tokens: output.tokens,
		})
	}

	pub(super) async fn try_get_cached_process_regions(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		regions: &[String],
	) -> tg::Result<Option<Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_cached_process_region(session, arg, region))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(error) => result = Err(error),
			}
		}
		result
	}

	pub(super) async fn try_get_cached_process_region(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		region: &str,
	) -> tg::Result<Option<Output>> {
		let client = session
			.get_region_session_for_process(region)
			.await
			.map_err(|error| tg::error!(!error, %region, "failed to get the region client"))?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let mut arg = tg::process::spawn::Arg {
			cached: Some(true),
			cache_location: Some(tg::location::Arg::default()),
			location: Some(location.clone().into()),
			..arg.clone()
		};
		Self::update_spawn_process_arg_for_location(&mut arg, &location);
		let stream = client
			.try_spawn_process(arg)
			.await
			.map_err(|error| tg::error!(!error, %region, "failed to get the cached process"))?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.next().await {
			let event = event?;
			let Some(mut output) = event.try_unwrap_output().ok().flatten() else {
				continue;
			};
			Self::update_spawn_process_output_referents_for_location(
				session,
				&mut output,
				&location,
				false,
			)?;
			return Ok(Some(Output::new(session, output)));
		}
		Ok(None)
	}

	pub(super) async fn try_get_cached_process_remotes(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		remotes: &[tangram_server_core::location::Remote],
	) -> tg::Result<Option<Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_cached_process_remote(session, arg, &remote.name, None))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(error) => result = Err(error),
			}
		}
		result
	}

	pub(super) async fn try_get_cached_process_remote(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		remote: &str,
		region: Option<&str>,
	) -> tg::Result<Option<Output>> {
		let client = session
			.get_remote_session_for_process(remote)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote client"))?;
		let trusted = client.trusted();
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.to_owned(),
			region: region.map(ToOwned::to_owned),
		});
		let mut arg = tg::process::spawn::Arg {
			cached: Some(true),
			cache_location: Some(tg::location::Arg::default()),
			location: Some(
				tg::Location::Local(tg::location::Local {
					region: region.map(ToOwned::to_owned),
				})
				.into(),
			),
			..arg.clone()
		};
		Self::update_spawn_process_arg_for_location(&mut arg, &location);
		let stream = client
			.try_spawn_process(arg)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the cached process"))?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.next().await {
			let event = event?;
			let Some(mut output) = event.try_unwrap_output().ok().flatten() else {
				continue;
			};
			let region = match output.location.take() {
				Some(tg::Location::Local(local)) => local.region,
				Some(tg::Location::Remote(remote)) => remote.region,
				None => None,
			};
			let location = tg::Location::Remote(tg::location::Remote {
				name: remote.to_owned(),
				region,
			});
			Self::update_spawn_process_output_referents_for_location(
				session,
				&mut output,
				&location,
				trusted,
			)?;
			return Ok(Some(Output::new(session, output)));
		}
		Ok(None)
	}
}
