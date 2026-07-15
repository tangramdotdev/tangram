use {
	super::{cached, lease::LeaseGuard, local::Output},
	crate::Session,
	futures::{FutureExt as _, future},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_messenger::Messenger as _,
};

impl Session {
	pub(super) async fn spawn_process_start_or_get_cached(
		&self,
		arg: &tg::process::spawn::Arg,
		mut output: Option<Output>,
		parent_sandbox: Option<&tg::sandbox::Id>,
		location: Option<&tg::Location>,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let local_id = output.as_ref().map(|output| output.id.clone());
		let local_token = output.as_ref().and_then(|output| output.token.clone());
		let scheduled_sandbox = output.as_ref().and_then(|output| {
			(output.sandbox_arg.is_some() && output.allocation.is_none())
				.then(|| output.data.sandbox.clone())
		});
		let cache_future =
			self.spawn_process_get_cached_process(arg, parent_sandbox, location, local_id.as_ref());
		let start_future = self.spawn_process_start_local(arg, output.as_mut());
		let (cache, wait) = match future::select(pin!(start_future), pin!(cache_future)).await {
			future::Either::Left((result, cache_future)) => {
				result?;
				let wait_future =
					self.spawn_process_wait_local(local_id.clone(), local_token, None, false);
				match future::select(pin!(wait_future), cache_future).await {
					future::Either::Left((result, _)) => (None, result?),
					future::Either::Right((result, _)) => (result?, None),
				}
			},
			future::Either::Right((result, start_future)) => {
				let cache = result?;
				if cache.is_some()
					&& let Some(sandbox) = &scheduled_sandbox
				{
					self.spawn_process_cancel_scheduled_candidate(sandbox)
						.await?;
					return Ok(cache.map(cached::Output::into_output));
				}
				start_future.await?;
				(cache, None)
			},
		};
		if let Some(cache) = cache {
			self.spawn_process_cancel_candidate(output.as_ref().unwrap())
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
						self.spawn_process_cancel_candidate(output).await?;
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
				token,
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
		parent_sandbox: Option<&tg::sandbox::Id>,
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
			let mut local_arg = arg.clone();
			local_arg.cached = Some(true);
			self.spawn_process_get_or_create_local_process(&local_arg, parent_sandbox, true)
				.boxed()
				.await?
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

	async fn spawn_process_cancel_candidate(&self, output: &Output) -> tg::Result<()> {
		let lease = output
			.lease
			.clone()
			.ok_or_else(|| tg::error!("missing the process lease"))?;
		let arg = tg::process::cancel::Arg {
			lease,
			location: None,
		};
		self.try_cancel_process_local(&output.id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, process = %output.id, "failed to cancel the process"),
			)?
			.ok_or_else(|| tg::error!(process = %output.id, "failed to find the process"))?;
		Ok(())
	}

	async fn spawn_process_cancel_scheduled_candidate(
		&self,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let notification =
			crate::scheduler::Message::Notification(crate::scheduler::Notification::CancelSandbox(
				crate::scheduler::CancelSandboxNotification {
					sandbox: sandbox.clone(),
				},
			));
		self.server
			.messenger
			.publish("scheduler.server".to_owned(), notification)
			.await
			.map_err(
				|error| tg::error!(!error, %sandbox, "failed to cancel the scheduled sandbox"),
			)?;
		Ok(())
	}

	fn spawn_process_output(
		output: Output,
		wait: Option<tg::process::wait::Output>,
	) -> tg::Result<tg::process::spawn::Output> {
		let data_wait = output.wait()?;
		Ok(tg::process::spawn::Output {
			cached: output.cached,
			lease: output.lease,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			process: tg::Either::Right(output.id),
			token: output.token,
			wait: wait.or(data_wait),
		})
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
