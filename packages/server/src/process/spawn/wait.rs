use {
	super::{cached, lease::LeaseGuard, local::Output},
	crate::Session,
	futures::{FutureExt as _, future},
	std::pin::pin,
	tangram_client::prelude::*,
};

impl Session {
	pub(super) async fn spawn_process_in_sandbox_or_get_cached(
		&self,
		arg: &tg::process::spawn::Arg,
		output: Option<Output>,
		location: Option<&tg::Location>,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let candidate = output.as_ref().map(|output| output.id.clone());
		let scheduled_sandbox = output.as_ref().and_then(|output| {
			(output.sandbox_arg.is_some() && output.allocation.is_none())
				.then(|| output.data.sandbox.clone())
		});
		let sandbox_connection_future = if let Some(sandbox) = &scheduled_sandbox {
			Some(self.subscribe_sandbox_connection(sandbox).await?)
		} else {
			None
		};
		let (scheduler_sender, scheduler_receiver) = if scheduled_sandbox.is_some() {
			let (sender, receiver) = tokio::sync::oneshot::channel();
			(Some(sender), Some(receiver))
		} else {
			(None, None)
		};
		let cache_future = self.spawn_process_get_cached_process(arg, location, candidate.as_ref());
		let spawn_future = self
			.spawn_process_in_new_or_existing_sandbox(arg, output, scheduler_sender)
			.boxed();
		let (cache, wait, output) = match future::select(spawn_future, pin!(cache_future)).await {
			future::Either::Left((result, cache_future)) => {
				let output = result?;
				let local_id = output.as_ref().map(|output| output.id.clone());
				let local_token = output.as_ref().and_then(|output| output.token.clone());
				let wait_future = self.spawn_process_wait_local(local_id, local_token, None, false);
				match future::select(pin!(wait_future), cache_future).await {
					future::Either::Left((result, _)) => (None, result?, output),
					future::Either::Right((result, _)) => (result?, None, output),
				}
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
								self.destroy_process_candidate_sandbox(&output.data.sandbox)
									.await?;
							}
						},
						future::Either::Right((result, _)) => {
							let scheduler = result?;
							self.spawn_process_dequeue_sandbox_candidate(
								sandbox.clone(),
								scheduler,
								sandbox_connection_future.unwrap(),
							);
						},
					}
					return Ok(cache.map(cached::Output::into_output));
				}
				let output = spawn_future.await?;
				(cache, None, output)
			},
		};
		let cache = cache.filter(|cache| {
			output
				.as_ref()
				.is_none_or(|output| cache.process() != Some(&output.id))
		});
		if let Some(cache) = cache {
			let output = output.as_ref().unwrap();
			self.spawn_process_cancel_candidate(output.id.clone(), output.lease.clone())
				.await?;
			return Ok(Some(cache.into_output()));
		}
		let Some(output) = output else {
			return Ok(None);
		};
		Ok(Some(Self::spawn_process_output(output, wait)?))
	}

	pub(super) async fn spawn_process_wait_or_get_cached(
		&self,
		arg: &tg::process::spawn::Arg,
		output: Option<Output>,
		cacheable: bool,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let finished = output
			.as_ref()
			.is_some_and(|output| output.data.status.is_finished());
		let local_id = output.as_ref().map(|output| output.id.clone());
		let local_token = output.as_ref().and_then(|output| output.token.clone());
		let local_wait = output.as_ref().map(Output::wait).transpose()?.flatten();
		let mut local_cache_guard = output
			.as_ref()
			.filter(|output| output.cached)
			.and_then(|output| LeaseGuard::new_local(self, output));
		let local_future =
			self.spawn_process_wait_local(local_id, local_token, local_wait, finished);
		let cached_future =
			self.spawn_process_get_cached_process_region_or_remote(arg, cacheable, finished);
		let output = match future::select(pin!(local_future), pin!(cached_future)).await {
			future::Either::Left((result, cached_future)) => {
				if let Some(wait) = result? {
					if let Some(guard) = &mut local_cache_guard {
						guard.disarm();
					}
					let output = output.unwrap();
					Self::spawn_process_output(output, Some(wait))?
				} else {
					let Some(cached_output) = cached_future.await? else {
						return Ok(None);
					};
					cached_output.into_output()
				}
			},
			future::Either::Right((result, _)) => {
				if let Ok(Some(cached_output)) = result {
					if let Some(output) = &output
						&& !output.cached && output.lease.is_some()
					{
						self.spawn_process_cancel_candidate(
							output.id.clone(),
							output.lease.clone(),
						)
						.await?;
					}
					cached_output.into_output()
				} else {
					if let Some(guard) = &mut local_cache_guard {
						guard.disarm();
					}
					let Some(output) = output else {
						return Ok(None);
					};
					Self::spawn_process_output(output, None)?
				}
			},
		};
		Ok(Some(output))
	}

	async fn spawn_process_wait_local(
		&self,
		id: Option<tg::process::Id>,
		token: Option<tg::grant::Token>,
		wait: Option<tg::process::wait::Output>,
		finished: bool,
	) -> tg::Result<Option<tg::process::wait::Output>> {
		if finished {
			return Ok(wait);
		}
		if let Some(id) = id {
			let arg = tg::process::wait::Arg {
				tokens: tg::grant::Tokens::with_local(token),
				..Default::default()
			};
			let wait = self
				.wait_process(&id, arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
			Ok(Some(wait))
		} else {
			Ok(None)
		}
	}

	async fn spawn_process_get_cached_process(
		&self,
		arg: &tg::process::spawn::Arg,
		location: Option<&tg::Location>,
		exclude: Option<&tg::process::Id>,
	) -> tg::Result<Option<cached::Output>> {
		let output = if let Some(location) = location
			&& !matches!(
				location,
				tg::Location::Local(tg::location::Local { region: None })
			) {
			match location {
				tg::Location::Local(tg::location::Local { region: None }) => unreachable!(),
				tg::Location::Local(tg::location::Local {
					region: Some(region),
				}) => self.try_get_cached_process_region(arg, region).await?,
				tg::Location::Remote(tg::location::Remote { name, region }) => {
					self.try_get_cached_process_remote(arg, name, region.as_deref())
						.await?
				},
			}
		} else {
			self.try_get_cached_process_local(arg)
				.boxed()
				.await
				.map_err(|error| tg::error!(!error, "failed to get a cached process"))?
				.map(|output| -> tg::Result<_> {
					let mut output = Self::spawn_process_output(output, None)?;
					if let Some(location) = location {
						output.location = Some(location.clone());
					}
					Ok(cached::Output::new(self, output))
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
		let output = self
			.spawn_process_get_cached_process_region_or_remote(arg, true, false)
			.await?;
		let output = output.filter(|output| {
			output
				.process()
				.is_none_or(|process| Some(process) != exclude)
		});
		Ok(output)
	}

	fn spawn_process_dequeue_sandbox_candidate(
		&self,
		sandbox: tg::sandbox::Id,
		scheduler: tg::scheduler::Id,
		connection_future: crate::sandbox::ConnectionFuture,
	) {
		let session = self.clone();
		tokio::spawn(async move {
			let output = session.dequeue_sandbox(&sandbox, &scheduler).await;
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
				.destroy_sandbox_when_available(&sandbox, arg, connection_future)
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

	async fn destroy_process_candidate_sandbox(&self, sandbox: &tg::sandbox::Id) -> tg::Result<()> {
		let arg = process_candidate_sandbox_destroy_arg();
		let session = self.server.session(&self.server.context);
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
		id: tg::process::Id,
		lease: Option<String>,
	) -> tg::Result<()> {
		crate::checkpoint!(
			self.server,
			"process.spawn.candidate.cancel",
			process = %id,
		)
		.await;
		let lease = lease.ok_or_else(|| tg::error!("missing the process lease"))?;
		let arg = tg::process::cancel::Arg {
			lease,
			location: None,
		};
		self.try_cancel_process_local(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, process = %id, "failed to cancel the process"))?
			.ok_or_else(|| tg::error!(process = %id, "failed to find the process"))?;
		crate::checkpoint!(
			self.server,
			"process.spawn.candidate.cancelled",
			process = %id,
		)
		.await;
		Ok(())
	}

	fn spawn_process_output(
		output: Output,
		wait: Option<tg::process::wait::Output>,
	) -> tg::Result<tg::process::spawn::Output> {
		let wait = wait.or(output.wait()?);
		let lease = if wait.is_some() { None } else { output.lease };
		let output = tg::process::spawn::Output {
			cached: output.cached,
			lease,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			process: tg::Either::Right(output.id),
			tokens: tg::grant::Tokens::with_local(output.token),
			wait,
		};

		Ok(output)
	}

	async fn spawn_process_get_cached_process_region_or_remote(
		&self,
		arg: &tg::process::spawn::Arg,
		cacheable: bool,
		finished: bool,
	) -> tg::Result<Option<cached::Output>> {
		if finished {
			return Ok(None);
		}
		if cacheable && matches!(arg.cached, None | Some(true)) {
			let locations = self
				.locations(arg.cache_location.as_ref())
				.await
				.map_err(|error| tg::error!(!error, "failed to resolve the cache locations"))?;
			let regions = locations.local.map_or_else(Vec::new, |local| local.regions);
			if let Some(output) = self
				.try_get_cached_process_regions(arg, &regions)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to get a cached process from another region")
				})? {
				return Ok(Some(output));
			}
			let output = self
				.try_get_cached_process_remotes(arg, &locations.remotes)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to get a cached process from a remote")
				})?;
			Ok(output)
		} else {
			Ok(None)
		}
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
	}
}
