use {
	super::{CandidateEvent, Event, cached, lease::LeaseGuard, local::Output},
	crate::Component,
	futures::{FutureExt as _, future},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_server_core::Session,
};

impl Component {
	pub(super) async fn try_spawn_process_local_or_get_cached(
		&self,
		session: &Session,
		mut arg: tg::process::spawn::Arg,
		parent_sandbox: Option<tg::sandbox::Id>,
		allocation: Option<tangram_server_runner_core::capacity::Allocation>,
		open: Option<crate::attach::service::Open>,
		cache_location: Option<&tg::Location>,
	) -> tg::Result<Option<(tg::process::spawn::Output, Option<tg::sandbox::Id>)>> {
		// Spawn the process on this server, and return its output if the spawn cannot get a cached process from another region or remote.
		let command = Self::spawn_process_resolve_command(session, &mut arg)?;
		let cacheable =
			Self::spawn_process_is_cacheable(&arg) && matches!(arg.cached, None | Some(true));
		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		let mut local_future = self
			.try_spawn_process_local(
				session,
				arg.clone(),
				parent_sandbox,
				allocation,
				open,
				cache_location,
				sender,
			)
			.boxed();
		if !cacheable {
			return local_future.await;
		}
		let mut fan_out_future =
			pin!(self.try_get_cached_process_regions_and_remotes(session, &arg));

		// Wait for the local spawn to return or for its cache lookup to miss.
		let mut candidate = None;
		let mut missed = false;
		let local = loop {
			tokio::select! {
				result = &mut local_future => break Some(result?),
				Some(event) = receiver.recv() => match event {
					Event::Candidate(event) => candidate = Some(event),
					Event::Missed => {
						missed = true;
						break None;
					},
				},
			}
		};

		let cached = 'cached: {
			let local = 'local: {
				if let Some(local) = local {
					// Record a miss that the local spawn sent before it returned.
					while let Ok(event) = receiver.try_recv() {
						missed |= matches!(event, Event::Missed);
					}
					break 'local local;
				}

				// Race the local spawn against the fan-out.
				let result = loop {
					tokio::select! {
						result = &mut local_future => break tg::Either::Left(result?),
						Some(event) = receiver.recv() => {
							if let Event::Candidate(event) = event {
								candidate = Some(event);
							}
						},
						result = fan_out_future.as_mut() => break tg::Either::Right(result),
					}
				};
				let cached = match result {
					tg::Either::Left(local) => break 'local local,
					tg::Either::Right(Ok(Some(cached))) => cached,
					tg::Either::Right(result) => {
						if let Err(error) = result {
							tracing::debug!(
								error = %error.trace(),
								"failed to get a cached process from another region or remote"
							);
						}
						return local_future.await;
					},
				};

				// Wait for the candidate or for the local spawn to return.
				let local = if candidate.is_some() {
					None
				} else {
					loop {
						tokio::select! {
							result = &mut local_future => break Some(result?),
							Some(event) = receiver.recv() => {
								if let Event::Candidate(event) = event {
									candidate = Some(event);
									break None;
								}
							},
						}
					}
				};
				if let Some(Some((output, _))) = &local
					&& !output.cached
					&& let Some(process) = output.process.as_ref().right()
					&& let Some(lease) = output.lease
				{
					candidate = Some(CandidateEvent {
						lease,
						process: process.clone(),
					});
				}

				// Return the local output if the cached process is the candidate.
				if candidate
					.as_ref()
					.is_some_and(|candidate| cached.process() == Some(&candidate.process))
				{
					return match local {
						Some(local) => Ok(local),
						None => local_future.await,
					};
				}

				// Release the lease of a local cached process, and cancel the candidate, logging a failure, because the spawn already has the cached process.
				if let Some(Some((output, _))) = &local
					&& output.cached
				{
					drop(LeaseGuard::new(session, output));
				}
				if let Some(CandidateEvent { lease, process }) = candidate {
					let result = self
						.spawn_process_cancel_candidate(
							session,
							process.clone(),
							Some(lease),
							arg.location.clone(),
						)
						.await;
					if let Err(error) = result {
						tracing::warn!(error = %error.trace(), %process, "failed to cancel the candidate process");
					}
				}
				break 'cached cached;
			};

			// Return the cached process from another region or remote if the local spawn has no process.
			let Some((mut output, sandbox)) = local else {
				let Some(cached) = fan_out_future.await? else {
					return Ok(None);
				};
				break 'cached cached;
			};

			// Return a finished local process, or a local process whose spawn does not get a cached process from another region or remote.
			if output.outcome.is_some() || !(missed || arg.cached == Some(true)) {
				return Ok(Some((output, sandbox)));
			}

			// Race the local process's finish against the fan-out.
			let mut lease_guard = if output.cached {
				LeaseGuard::new(session, &output)
			} else {
				None
			};
			let id = output.process.as_ref().right().cloned();
			let wait_future = self.spawn_process_wait_local(
				session,
				id.clone(),
				output.tokens.clone(),
				None,
				false,
			);
			match future::select(pin!(wait_future), fan_out_future).await {
				future::Either::Left((result, _)) => {
					let outcome = result?;
					if let Some(guard) = &mut lease_guard {
						guard.disarm();
					}
					output.lease = None;
					output.outcome = outcome;
					Self::spawn_process_add_tokens(session, &mut output).await?;
					return Ok(Some((output, sandbox)));
				},
				future::Either::Right((Ok(Some(cached)), _)) if cached.process() != id.as_ref() => {
					// Cancel the candidate, logging a failure, because the spawn already has the cached process.
					if !output.cached
						&& let Some(process) = id
						&& output.lease.is_some()
					{
						let result = self
							.spawn_process_cancel_candidate(
								session,
								process.clone(),
								output.lease,
								arg.location.clone(),
							)
							.await;
						if let Err(error) = result {
							tracing::warn!(error = %error.trace(), %process, "failed to cancel the candidate process");
						}
					}
					drop(lease_guard);
					cached
				},
				future::Either::Right((result, _)) => {
					if let Err(error) = result {
						tracing::debug!(
							error = %error.trace(),
							"failed to get a cached process from another region or remote"
						);
					}
					if let Some(guard) = &mut lease_guard {
						guard.disarm();
					}
					return Ok(Some((output, sandbox)));
				},
			}
		};

		// Finish the cached process from another region or remote.
		let mut output = cached.into_output();
		let mut lease_guard = LeaseGuard::new(session, &output);
		output.command = Some(command.node);
		Self::spawn_process_add_tokens(session, &mut output).await?;
		self.spawn_process_create_public_grant_if_requested(session, &arg, &output)
			.await?;
		if let Some(guard) = &mut lease_guard {
			guard.disarm();
		}

		Ok(Some((output, None)))
	}

	pub(super) async fn spawn_process_local_in_sandbox_or_get_cached(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		command: &tg::Referent<tg::command::Id>,
		output: Option<Output>,
		location: Option<&tg::Location>,
		events: &tokio::sync::mpsc::UnboundedSender<Event>,
	) -> tg::Result<Option<(tg::process::spawn::Output, Option<tg::sandbox::Id>)>> {
		let candidate = output.as_ref().map(|output| output.id.clone());
		let scheduled_sandbox = output.as_ref().and_then(|output| {
			(output.sandbox_arg.is_some() && output.allocation.is_none())
				.then(|| output.data.sandbox.clone())
				.flatten()
		});
		let (scheduler_sender, scheduler_receiver) = if scheduled_sandbox.is_some() {
			let (sender, receiver) = tokio::sync::oneshot::channel();
			(Some(sender), Some(receiver))
		} else {
			(None, None)
		};
		let cache_future = self.spawn_process_local_get_cached_process(
			session,
			arg,
			command,
			location,
			candidate.as_ref(),
			events,
		);
		let create_delay = session.server.config.process.spawn.create_delay;
		let spawn_future = self.spawn_process_local_in_new_or_existing_sandbox(
			session,
			arg,
			output,
			scheduler_sender,
			Some(events),
		);
		let spawn_future = if create_delay.is_zero() {
			spawn_future.left_future()
		} else {
			tokio::time::sleep(create_delay)
				.then(|()| spawn_future)
				.right_future()
		};
		let (cache, output) = match future::select(pin!(spawn_future), pin!(cache_future)).await {
			future::Either::Left((result, cache_future)) => {
				let output = result?;
				let cache = cache_future.await?;
				(cache, output)
			},
			future::Either::Right((result, mut spawn_future)) => {
				let cache = result?;
				if cache.is_some()
					&& let Some(sandbox) = &scheduled_sandbox
				{
					let scheduler_receiver = scheduler_receiver.unwrap();
					let scheduler_future = async {
						scheduler_receiver
							.await
							.map_err(|_| tg::error!("failed to receive the scheduler"))
					};
					let scheduler_future = pin!(scheduler_future);
					match future::select(spawn_future.as_mut(), scheduler_future).await {
						future::Either::Left((result, _)) => {
							if let Some(output) = result? {
								self.destroy_process_candidate_sandbox(
									session,
									output.data.sandbox.as_ref().ok_or_else(|| {
										tg::error!("the scheduled process has no sandbox")
									})?,
								)
								.await?;
							}
						},
						future::Either::Right((result, _)) => {
							let scheduler = result?;
							Self::spawn_process_local_dequeue_sandbox_candidate(
								session,
								sandbox.clone(),
								scheduler,
							);
						},
					}
					let output = cache.map(|cache| {
						let sandbox = cache.sandbox.clone();
						(cache.into_output(), sandbox)
					});
					return Ok(output);
				}
				let output = spawn_future.await?;
				(cache, output)
			},
		};
		let cache = cache.filter(|cache| {
			output
				.as_ref()
				.is_none_or(|output| cache.process() != Some(&output.id))
		});
		if let Some(cache) = cache {
			// Cancel the candidate, logging a failure, because the spawn already has the cached process.
			let output = output.as_ref().unwrap();
			let result = self
				.spawn_process_cancel_candidate(
					session,
					output.id.clone(),
					output.lease,
					arg.location.clone(),
				)
				.await;
			if let Err(error) = result {
				tracing::warn!(error = %error.trace(), process = %output.id, "failed to cancel the candidate process");
			}
			let sandbox = cache.sandbox.clone();
			return Ok(Some((cache.into_output(), sandbox)));
		}
		let Some(output) = output else {
			return Ok(None);
		};
		let output = Self::spawn_process_local_output(output, None)?;

		Ok(Some((output, None)))
	}

	async fn spawn_process_wait_local(
		&self,
		session: &Session,
		id: Option<tg::process::Id>,
		tokens: tg::authorization::Tokens,
		outcome: Option<tg::process::outcome::Data>,
		finished: bool,
	) -> tg::Result<Option<tg::process::outcome::Data>> {
		if finished {
			return Ok(outcome);
		}
		if let Some(id) = id {
			let arg = tg::process::wait::Arg {
				tokens,
				..Default::default()
			};
			let outcome = session
				.wait_process(&id, arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
			Ok(Some(outcome))
		} else {
			Ok(None)
		}
	}

	async fn spawn_process_local_get_cached_process(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		command: &tg::Referent<tg::command::Id>,
		location: Option<&tg::Location>,
		exclude: Option<&tg::process::Id>,
		events: &tokio::sync::mpsc::UnboundedSender<Event>,
	) -> tg::Result<Option<cached::Output>> {
		let output = if let Some(location) = location
			&& !matches!(
				location,
				tg::Location::Local(tg::location::Local { region })
				if region.as_deref().is_none_or(|region| Some(region) == session.server.config.region.as_deref())
			) {
			match location {
				tg::Location::Local(tg::location::Local {
					region: Some(region),
				}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
					self.try_get_cached_process_region(session, arg, region)
						.await?
				},
				tg::Location::Local(_) => unreachable!(),
				tg::Location::Remote(tg::location::Remote { name, region }) => {
					self.try_get_cached_process_remote(session, arg, name, region.as_deref())
						.await?
				},
			}
		} else {
			self.try_get_cached_process_local(session, arg, command, exclude)
				.boxed()
				.await
				.map_err(|error| tg::error!(!error, "failed to get a cached process"))?
				.map(|output| -> tg::Result<_> {
					let sandbox = output
						.data
						.sandbox
						.clone()
						.filter(|_| output.data.status.is_queued());
					let mut output = Self::spawn_process_local_output(output, None)?;
					if let Some(location) = location {
						output.location = Some(location.clone());
					}
					let mut output = cached::Output::new(session, output);
					output.sandbox = sandbox;
					Ok(output)
				})
				.transpose()?
		};
		if let Some(output) = output
			&& output
				.process()
				.is_none_or(|process| Some(process) != exclude)
		{
			return Ok(Some(output));
		}

		// Report the miss, so that the spawn gets a cached process from another region or remote.
		tangram_server_core::checkpoint!(
			session.server,
			"process.spawn.cached.region_or_remote",
			command = %command.node,
		)
		.await;
		events.send(Event::Missed).ok();

		Ok(None)
	}

	fn spawn_process_local_dequeue_sandbox_candidate(
		session: &Session,
		sandbox: tg::sandbox::Id,
		scheduler: tg::scheduler::Id,
	) {
		let session = session.clone();
		tokio::spawn(async move {
			let output = session
				.server
				.components
				.scheduler
				.dequeue_sandbox(&session, &sandbox, &scheduler)
				.boxed()
				.await;
			let destroy_required = match output {
				Ok(dequeued) => !dequeued,
				Err(error) => {
					tracing::error!(
						error = %error.trace(),
						%sandbox,
						%scheduler,
						"failed to dequeue the sandbox for the unused process candidate"
					);
					true
				},
			};
			if !destroy_required {
				return;
			}

			let arg = process_candidate_sandbox_destroy_arg();
			let session = session.server.session(&session.server.context);
			session
				.try_destroy_sandbox(&sandbox, arg)
				.await
				.inspect_err(|error| {
					tracing::error!(
						error = %error.trace(),
						%sandbox,
						%scheduler,
						"failed to destroy the connected sandbox for the unused process candidate"
					);
				})
				.ok();
		});
	}

	async fn destroy_process_candidate_sandbox(
		&self,
		session: &Session,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let arg = process_candidate_sandbox_destroy_arg();
		let session = session.server.session(&session.server.context);
		let output = session
			.try_destroy_sandbox(sandbox, arg)
			.await
			.map_err(|error| tg::error!(!error, %sandbox, "failed to destroy the sandbox"))?;
		if output.is_none() {
			return Err(tg::error!(%sandbox, "failed to find the sandbox"));
		}

		Ok(())
	}

	async fn spawn_process_cancel_candidate(
		&self,
		session: &Session,
		id: tg::process::Id,
		lease: Option<tg::process::Lease>,
		location: Option<tg::location::Arg>,
	) -> tg::Result<()> {
		tangram_server_core::checkpoint!(
			session.server,
			"process.spawn.candidate.cancel",
			process = %id,
		)
		.await;
		let lease = lease.ok_or_else(|| tg::error!("missing the process lease"))?;
		let arg = tg::process::cancel::Arg { lease, location };
		// The candidate may belong to a remote even when it ran here.
		self.try_cancel_process(session, &tg::Either::Right(id.clone()), arg)
			.await
			.map_err(|error| tg::error!(!error, process = %id, "failed to cancel the process"))?
			.ok_or_else(|| tg::error!(process = %id, "failed to find the process"))?;
		tangram_server_core::checkpoint!(
			session.server,
			"process.spawn.candidate.cancelled",
			process = %id,
		)
		.await;
		Ok(())
	}

	pub(super) fn spawn_process_local_output(
		output: Output,
		outcome: Option<tg::process::outcome::Data>,
	) -> tg::Result<tg::process::spawn::Output> {
		let outcome = outcome.or(output.try_outcome()?);
		let lease = if outcome.is_some() {
			None
		} else {
			output.lease
		};
		let output = tg::process::spawn::Output {
			cached: output.cached,
			command: None,
			lease,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			outcome,
			process: tg::Either::Right(output.id),
			tokens: output.tokens,
		};

		Ok(output)
	}
}

fn process_candidate_sandbox_destroy_arg() -> tg::sandbox::destroy::Arg {
	let error = tg::error::Data {
		code: Some(tg::error::Code::Cancellation),
		message: Some("a cached process was found".into()),
		..Default::default()
	};
	tg::sandbox::destroy::Arg {
		error: Some(tg::Either::Left(error)),
		location: Some(tg::Location::Local(tg::location::Local::default()).into()),
		tokens: tg::authorization::Tokens::default(),
		wait: false,
	}
}
