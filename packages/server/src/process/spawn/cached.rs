use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	num::ToPrimitive as _,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

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
		parent_sandbox: Option<&tg::sandbox::Id>,
	) -> tg::Result<Option<super::local::Output>> {
		let owner = self.cached_process_owner(arg, parent_sandbox)?;
		let candidates = self
			.server
			.index
			.try_get_cached_processes(&arg.command.item.clone().into())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to query the index for cached processes")
			})?;
		let permission =
			tg::grant::Permission::Process(tg::grant::permission::process::Permission::Node);
		let principal = owner.clone().unwrap_or(tg::Principal::Root);
		let authorizations = self
			.server
			.index
			.authorize_batch(
				&candidates
					.iter()
					.map(|(id, _)| tangram_index::authorize::Arg {
						permissions: permission.into(),
						resource: tg::grant::Resource::Id(id.clone().into()),
						token: None,
					})
					.collect::<Vec<_>>(),
				&principal,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the cached processes"))?;
		let candidates = std::iter::zip(candidates, authorizations)
			.filter_map(|(candidate, authorization)| {
				authorization
					.is_some_and(|output| output.permissions.contains(permission))
					.then_some(candidate)
			})
			.collect::<Vec<_>>();

		for (id, process) in &candidates {
			let data = process.data.as_ref().unwrap();
			if !data.cacheable || data.expected_checksum != arg.checksum {
				continue;
			}
			let error_code = self.cached_process_error_code(data).await?;
			if matches!(
				error_code,
				Some(
					tg::error::Code::Cancellation
						| tg::error::Code::HeartbeatExpiration
						| tg::error::Code::Internal
				)
			) {
				continue;
			}
			let failed = data.error.is_some() || data.exit.is_some_and(|exit| exit != 0);
			if failed && arg.retry {
				continue;
			}
			if !self.touch_cached_process_if_needed(id, process).await? {
				continue;
			}
			self.check_cached_process_cycle(arg.parent.as_ref(), id)
				.await?;
			let output = self.cached_process_output(id.clone(), data.clone())?;
			return self
				.acquire_cached_process_lease(output)
				.boxed()
				.await
				.map(Some);
		}

		let Some(expected_checksum) = &arg.checksum else {
			return Ok(None);
		};
		for (source, process) in candidates {
			let data = process.data.as_ref().unwrap();
			if !data.cacheable
				|| !matches!(
					self.cached_process_error_code(data).await?,
					Some(tg::error::Code::ChecksumMismatch)
				) {
				continue;
			}
			let Some(actual_checksum) = &data.actual_checksum else {
				continue;
			};
			if actual_checksum.algorithm() != expected_checksum.algorithm() {
				continue;
			}
			if !self
				.touch_cached_process_if_needed(&source, &process)
				.await?
			{
				continue;
			}
			self.check_cached_process_cycle(arg.parent.as_ref(), &source)
				.await?;
			let host = data.host.clone();
			return self
				.create_mismatched_checksum_process(arg, &host, data.clone())
				.await
				.map(Some);
		}

		Ok(None)
	}

	async fn acquire_cached_process_lease(
		&self,
		mut output: super::local::Output,
	) -> tg::Result<super::local::Output> {
		if output.data.status.is_finished() {
			return Ok(output);
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
		if !output.data.status.is_finished() && output.lease.is_none() {
			return Err(tg::error!(
				process = %output.id,
				"the active process lease was not acquired"
			));
		}
		Ok(output)
	}

	async fn check_cached_process_cycle(
		&self,
		parent: Option<&tg::process::Id>,
		child: &tg::process::Id,
	) -> tg::Result<()> {
		let Some(parent) = parent else {
			return Ok(());
		};
		let cycle = self
			.server
			.index
			.process_has_ancestor(parent, child)
			.await
			.map_err(
				|error| tg::error!(!error, %parent, %child, "failed to check for a process cycle"),
			)?;
		if cycle {
			return Err(tg::error!(
				%parent,
				%child,
				"a cache hit would create a process cycle"
			));
		}
		Ok(())
	}

	fn cached_process_owner(
		&self,
		arg: &tg::process::spawn::Arg,
		parent_sandbox: Option<&tg::sandbox::Id>,
	) -> tg::Result<Option<tg::Principal>> {
		let owner = match &arg.sandbox {
			Some(tg::Either::Left(sandbox)) => sandbox.owner.clone(),
			Some(tg::Either::Right(sandbox)) => {
				self.server
					.runner
					.state
					.try_get_sandbox(sandbox)
					.ok_or_else(|| tg::error!(%sandbox, "failed to find the sandbox"))?
					.owner
			},
			None => return Err(tg::error!("expected the sandbox to be set")),
		};
		if owner.is_some() {
			return Ok(owner.filter(|owner| !matches!(owner, tg::Principal::Root)));
		}
		if let Some(parent_sandbox) = parent_sandbox
			&& let Some(owner) = self
				.server
				.runner
				.state
				.try_get_sandbox(parent_sandbox)
				.and_then(|sandbox| sandbox.owner)
		{
			return Ok(Some(owner));
		}
		Ok(None)
	}

	async fn cached_process_error_code(
		&self,
		data: &tg::process::Data,
	) -> tg::Result<Option<tg::error::Code>> {
		let Some(error) = &data.error else {
			return Ok(None);
		};
		match error {
			tg::Either::Left(data) => Ok(data.code),
			tg::Either::Right(id) => {
				let id = id.clone().map_right(|id| id.id).into_inner();
				let data = tg::Error::with_id(id)
					.data_with_handle(self)
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to get the cached process error")
					})?;
				Ok(data.code)
			},
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
