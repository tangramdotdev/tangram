use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	num::ToPrimitive as _,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

const AUTHORIZATION_BATCH_SIZE: usize = 16;

type Candidate = (tg::process::Id, tangram_index::process::Process);

enum CandidateOutcome {
	Active,
	Failure,
	Success,
}

pub(super) struct Output {
	guard: Option<super::lease::LeaseGuard>,
	output: tg::process::spawn::Output,
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

impl Session {
	pub(super) async fn try_get_cached_process_local(
		&self,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<Option<super::local::Output>> {
		let public = arg.public && arg.cached == Some(true);

		// List the candidates.
		let candidates = self
			.server
			.index
			.try_get_cached_processes(&arg.command.item.clone().into())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to query the index for cached processes")
			})?;
		let mut cycle = None;

		// Try the normal candidates.
		let normal_candidate_indices =
			Self::cached_process_normal_candidate_indices(arg, &candidates);
		let output = self
			.try_get_cached_process_normal_local(
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
		let output = self
			.try_get_cached_process_with_mismatched_checksum_local(
				arg,
				&candidates,
				&mismatched_checksum_candidate_indices,
				&mut cycle,
				public,
			)
			.await?;
		if output.is_some() {
			return Ok(output);
		}

		// Report a cycle if no candidate succeeded.
		if let Some(child) = cycle {
			let parent = arg.parent.as_ref().unwrap();
			return Err(tg::error!(
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
		let candidate_outcomes =
			candidates
				.iter()
				.enumerate()
				.filter_map(|(index, (_, process))| {
					let data = process.data.as_ref().unwrap();
					(data.cacheable && data.expected_checksum == arg.checksum)
						.then(|| (index, Self::cached_process_outcome(data)))
				});

		Self::prioritize_cached_process_candidate_indices(candidate_outcomes, arg.retry)
	}

	async fn try_get_cached_process_normal_local(
		&self,
		arg: &tg::process::spawn::Arg,
		candidates: &[Candidate],
		candidate_indices: &[usize],
		cycle: &mut Option<tg::process::Id>,
		public: bool,
	) -> tg::Result<Option<super::local::Output>> {
		for candidate_indices in candidate_indices.chunks(AUTHORIZATION_BATCH_SIZE) {
			let authorizations = self
				.authorize_cached_process_candidates(candidates, candidate_indices, public)
				.await?;
			for (index, authorized) in std::iter::zip(candidate_indices, authorizations) {
				if !authorized {
					continue;
				}
				let (id, process) = &candidates[*index];
				if !self.touch_cached_process_if_needed(id, process).await? {
					continue;
				}
				if self
					.cached_process_creates_cycle(arg.parent.as_ref(), id)
					.await?
				{
					cycle.get_or_insert_with(|| id.clone());
					continue;
				}
				let data = process.data.as_ref().unwrap();
				let output = self.cached_process_output(id.clone(), data.clone())?;
				if let Some(output) = self.acquire_cached_process_lease(output).boxed().await? {
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
				.filter_map(|(index, (_, process))| {
					let data = process.data.as_ref().unwrap();
					let actual_checksum = data.actual_checksum.as_ref()?;
					let source_expected_checksum = data.expected_checksum.as_ref()?;
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
		arg: &tg::process::spawn::Arg,
		candidates: &[Candidate],
		candidate_indices: &[usize],
		cycle: &mut Option<tg::process::Id>,
		public: bool,
	) -> tg::Result<Option<super::local::Output>> {
		for candidate_indices in candidate_indices.chunks(AUTHORIZATION_BATCH_SIZE) {
			let authorizations = self
				.authorize_cached_process_candidates(candidates, candidate_indices, public)
				.await?;
			for (index, authorized) in std::iter::zip(candidate_indices, authorizations) {
				if !authorized {
					continue;
				}
				let (source, process) = &candidates[*index];
				if !self.touch_cached_process_if_needed(source, process).await? {
					continue;
				}
				if self
					.cached_process_creates_cycle(arg.parent.as_ref(), source)
					.await?
				{
					cycle.get_or_insert_with(|| source.clone());
					continue;
				}
				let data = process.data.as_ref().unwrap();
				let host = data.host.clone();
				let output = self
					.create_mismatched_checksum_process(arg, &host, data.clone())
					.await?;

				return Ok(Some(output));
			}
		}

		Ok(None)
	}

	async fn authorize_cached_process_candidates(
		&self,
		candidates: &[Candidate],
		candidate_indices: &[usize],
		public: bool,
	) -> tg::Result<Vec<bool>> {
		let permission =
			tg::grant::Permission::Process(tg::grant::permission::process::Permission::Node);
		let args = candidate_indices
			.iter()
			.map(|index| {
				let permissions = tg::grant::permission::Set::from_permission(permission);
				let resource = tg::grant::Resource::Id(candidates[*index].0.clone().into());
				(resource, permissions)
			})
			.collect::<Vec<_>>();
		let principal = if public {
			tg::Principal::Anonymous
		} else {
			self.cache_principal().await?
		};
		let context = crate::Context {
			principal,
			token: None,
			..self.context.clone()
		};
		let authorizations = self
			.server
			.session(&context)
			.authorize_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the cached processes"))?;
		let authorized = authorizations
			.into_iter()
			.map(|authorization| {
				authorization.is_some_and(|permissions| permissions.contains(permission))
			})
			.collect();

		Ok(authorized)
	}

	async fn cache_principal(&self) -> tg::Result<tg::Principal> {
		let tg::Principal::Process(id) = &self.context.principal else {
			return Ok(self.context.principal.clone());
		};
		let process = self
			.try_get_authenticated_process(id)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the authenticated process"))?;
		let sandbox = process.sandbox;
		let owner = if let Some(sandbox) = self.server.runner.state.sandboxes.get(&sandbox) {
			sandbox.data.owner.clone()
		} else {
			self.get_sandbox_from_index(&sandbox)
				.await
				.map_err(|error| tg::error!(!error, %sandbox, "failed to get the sandbox"))?
				.data
				.ok_or_else(|| tg::error!(%sandbox, "missing the sandbox data"))?
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
		mut output: super::local::Output,
	) -> tg::Result<Option<super::local::Output>> {
		if output.data.status.is_finished() {
			return Ok(Some(output));
		}
		let request = tg::process::control::ServerRequestArg::AcquireLease(
			tg::process::control::AcquireLeaseServerRequestArg {},
		);
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options::default(),
			timeout: std::time::Duration::from_secs(10),
		};
		let response = self
			.send_process_control_request(&output.id, request, options)
			.await
			.map_err(
				|error| tg::error!(!error, process = %output.id, "failed to acquire a process lease"),
			)?
			.map_err(
				|error| tg::error!(!error, process = %output.id, "the acquire process lease request failed"),
			)?;
		let response = response
			.try_unwrap_acquire_lease()
			.map_err(|_| tg::error!("expected an acquire process lease response"))?;
		output.data = response.data;
		output.lease = response.lease;
		if !output.data.cacheable || (!output.data.status.is_finished() && output.lease.is_none()) {
			return Ok(None);
		}
		Ok(Some(output))
	}

	async fn cached_process_creates_cycle(
		&self,
		parent: Option<&tg::process::Id>,
		child: &tg::process::Id,
	) -> tg::Result<bool> {
		let Some(parent) = parent else {
			return Ok(false);
		};
		let cycle = self
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
		match error {
			tg::Either::Left(data) => data.code,
			tg::Either::Right(_) => None,
		}
	}

	async fn touch_cached_process_if_needed(
		&self,
		id: &tg::process::Id,
		process: &tangram_index::process::Process,
	) -> tg::Result<bool> {
		let data = process.data.as_ref().unwrap();
		if !data.status.is_finished() {
			return Ok(true);
		}
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let time_to_index = self
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
		let process = self
			.server
			.index
			.touch_process(id, now, self.server.config.process.time_to_touch)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to touch the process"))?;
		Ok(process.is_some())
	}

	fn cached_process_output(
		&self,
		id: tg::process::Id,
		data: tg::process::Data,
	) -> tg::Result<super::local::Output> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let token = self.create_process_wait_token(&id, now)?;
		Ok(super::local::Output {
			allocation: None,
			cached: true,
			data,
			id,
			lease: None,
			parent: None,
			parent_sandbox: None,
			process_token: None,
			sandbox_arg: None,
			sandbox_token: None,
			token,
		})
	}

	async fn create_mismatched_checksum_process(
		&self,
		arg: &tg::process::spawn::Arg,
		host: &str,
		source: tg::process::Data,
	) -> tg::Result<super::local::Output> {
		let expected_checksum = arg.checksum.clone().unwrap();
		let actual_checksum = source.actual_checksum.clone().unwrap();
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
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let id = tg::process::Id::new();
		let data = tg::process::Data {
			actual_checksum: Some(actual_checksum),
			cacheable: true,
			children: source.children,
			command: arg.command.item.clone(),
			created_at: now,
			debug: arg.debug.clone(),
			error: error.clone().map(tg::Either::Left),
			exit: Some(exit),
			expected_checksum: Some(expected_checksum),
			finished_at: Some(now),
			host: host.to_owned(),
			log: None,
			output: source.output,
			retry: arg.retry,
			sandbox: source.sandbox,
			started_at: None,
			status: tg::process::Status::Finished,
			stderr: arg.stderr.clone(),
			stdin: arg.stdin.clone(),
			stdout: arg.stdout.clone(),
			tty,
		};
		let output = self
			.put_process_local(
				&id,
				tg::process::put::Arg {
					data: data.clone(),
					location: None,
				},
			)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to store the process"))?;
		Ok(super::local::Output {
			allocation: None,
			cached: true,
			data,
			id,
			lease: None,
			parent: None,
			parent_sandbox: None,
			process_token: None,
			sandbox_arg: None,
			sandbox_token: None,
			token: output.token,
		})
	}

	pub(super) async fn try_get_cached_process_regions(
		&self,
		arg: &tg::process::spawn::Arg,
		regions: &[String],
	) -> tg::Result<Option<Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_cached_process_region(arg, region))
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
		arg: &tg::process::spawn::Arg,
		region: &str,
	) -> tg::Result<Option<Output>> {
		let client = self
			.get_region_session(region)
			.await
			.map_err(|error| tg::error!(!error, %region, "failed to get the region client"))?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::spawn::Arg {
			cached: Some(true),
			cache_location: Some(tg::location::Arg::default()),
			location: Some(location.clone().into()),
			..arg.clone()
		};
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
			output.location = Some(location);
			return Ok(Some(Output::new(self, output)));
		}
		Ok(None)
	}

	pub(super) async fn try_get_cached_process_remotes(
		&self,
		arg: &tg::process::spawn::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_cached_process_remote(arg, &remote.name, None))
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
		arg: &tg::process::spawn::Arg,
		remote: &str,
		region: Option<&str>,
	) -> tg::Result<Option<Output>> {
		let client = self
			.get_remote_session(remote)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote client"))?;
		let arg = tg::process::spawn::Arg {
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
			output.location = Some(tg::Location::Remote(tg::location::Remote {
				name: remote.to_owned(),
				region,
			}));
			return Ok(Some(Output::new(self, output)));
		}
		Ok(None)
	}
}
