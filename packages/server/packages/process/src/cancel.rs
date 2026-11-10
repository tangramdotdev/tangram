use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
	tangram_server_core::{Session, location::await_local_future_over_regions_remotes_futures},
};

impl Component {
	pub async fn try_cancel_process(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		mut arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		let mut process = process.clone();
		if self
			.try_resolve_process(session, &mut process, &mut arg.location, None, &[])
			.await?
			.is_none()
		{
			return Ok(None);
		}
		let id = process.clone().right();
		let process = &process;
		if let Some(id) = &id
			&& let Some(output) = self.try_cancel_process_runner(session, id, &arg).await?
		{
			return Ok(Some(output));
		}
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		let current = locations.local.as_ref().is_some_and(|local| local.current);

		// Cancel the process locally without a search.
		if current
			&& let Some(id) = &id
			&& let Some(output) = self
				.try_cancel_process_local(session, id, arg.clone(), false)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to cancel the process"))?
		{
			return Ok(Some(output));
		}

		// Search locally while trying the regions and then the remotes.
		let local_future = async {
			let (true, Some(id)) = (current, &id) else {
				return Ok(None);
			};
			self.try_cancel_process_local(session, id, arg.clone(), true)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to cancel the process"))
		};
		let regions_and_remotes_future = async {
			if let Some(local) = &locations.local
				&& let Some(output) = self
					.try_cancel_process_regions(session, process, arg.clone(), &local.regions)
					.await
					.map_err(
						|error| tg::error!(!error, %process, "failed to cancel the process in another region"),
					)? {
				return Ok(Some(output));
			}
			self.try_cancel_process_remotes(session, process, arg.clone(), &locations.remotes)
				.await
				.map_err(
					|error| tg::error!(!error, %process, "failed to cancel the process in a remote"),
				)
		};
		let output = await_local_future_over_regions_remotes_futures(
			local_future,
			regions_and_remotes_future,
		)
		.boxed()
		.await?;

		Ok(output)
	}

	async fn try_cancel_process_runner(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: &tg::process::cancel::Arg,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		let Some(control) =
			Self::try_get_process_control_runner_inner(session, id, arg.location.as_ref())
		else {
			return Ok(None);
		};
		if control.data.status.is_finished() {
			let output = tg::process::cancel::Output { released: false };
			return Ok(Some(output));
		}
		tangram_server_core::checkpoint!(session.server, "process.cancel.runner", process = %id)
			.await;
		self.cancel_process_with_control(session, id, arg.lease, Some(control.control_sender))
			.boxed()
			.await
	}

	pub(crate) async fn try_cancel_process_local(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
		exhaustive: bool,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		// Leave a process that this server does not hold for the search.
		if !exhaustive {
			let process = session.server.index.try_get_process(id).await?;
			let held = process.is_some_and(|process| {
				!process
					.location
					.as_ref()
					.is_some_and(tg::Location::is_remote)
			});
			if !held {
				return Ok(None);
			}
		}

		// Release a process that is starting at its sandbox's scheduler, because it has no control connection.
		if let Some(output) = self
			.try_cancel_starting_process(session, id, arg.lease)
			.await?
		{
			return Ok(Some(output));
		}
		self.cancel_process_with_control(session, id, arg.lease, None)
			.boxed()
			.await
	}

	/// Release a lease at the scheduler while it holds the process's leases, and wait for the process to connect once its request is sent to a runner.
	async fn try_cancel_starting_process(
		&self,
		session: &Session,
		id: &tg::process::Id,
		lease: tg::process::Lease,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		let mut wakeups = None;
		loop {
			let Some((scheduler, sandbox)) =
				self.try_get_starting_process_owner(session, id).await?
			else {
				return Ok(None);
			};
			let released = session
				.server
				.components
				.scheduler
				.release_process_lease(session, &scheduler, &sandbox, lease)
				.await?;
			if let Some(released) = released {
				let output = tg::process::cancel::Output { released };
				return Ok(Some(output));
			}

			// Subscribe only for a process that is starting, and read again after subscribing, so that no change is missed.
			let Some(wakeups) = &mut wakeups else {
				let stream = self
					.create_process_status_wakeup_stream(
						session,
						id,
						session.context.stopper.clone(),
						None,
					)
					.await?;
				wakeups = Some(stream);
				continue;
			};
			if wakeups.next().await.is_none() {
				return Err(tg::error!("the process status wakeup stream ended"));
			}
		}
	}

	/// Get the scheduler and sandbox of a process that is starting.
	pub(crate) async fn try_get_starting_process_owner(
		&self,
		session: &Session,
		id: &tg::process::Id,
	) -> tg::Result<Option<(tg::scheduler::Id, tg::sandbox::Id)>> {
		let Some((process, data)) = self
			.try_get_process_with_data_from_index(session, id)
			.await?
		else {
			return Ok(None);
		};
		if !process.status.is_some_and(|status| status.is_starting()) {
			return Ok(None);
		}
		let Some(sandbox) = data.and_then(|data| data.sandbox) else {
			return Ok(None);
		};
		let scheduler = session
			.server
			.index
			.try_get_sandbox_scheduler(&sandbox)
			.await
			.map_err(
				|error| tg::error!(!error, %sandbox, "failed to get the sandbox's scheduler"),
			)?;
		let output = scheduler.map(|scheduler| (scheduler, sandbox));

		Ok(output)
	}

	async fn cancel_process_with_control(
		&self,
		session: &Session,
		id: &tg::process::Id,
		lease: tg::process::Lease,
		control_sender: Option<tangram_server_process_core::control::local::Local>,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		let request = tg::process::control::ServerRequestArg::ReleaseLease(
			tg::process::control::ReleaseLeaseServerRequestArg { lease },
		);
		let retry = session.server.config.control.request_retry.clone().into();
		let timeout = session.server.config.control.request_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		let runner = control_sender.is_some();
		let response = if let Some(control_sender) = control_sender {
			match control_sender.send_request(request).await {
				Err(error) => Err(error),
				Ok(response) => response.await,
			}
		} else {
			let wakeups = self
				.create_process_status_wakeup_stream(
					session,
					id,
					session.context.stopper.clone(),
					None,
				)
				.await?;
			let cancel_future = async {
				tangram_server_core::checkpoint!(session.server, "process.cancel.control", process = %id).await;
				self.request_process_control(session, id, request, options)
					.await
			};
			let get_future = self.wait_for_process_cancel_local(session, id, wakeups);
			match future::select(pin!(cancel_future), pin!(get_future)).await {
				future::Either::Left((response, _)) => response,
				future::Either::Right((output, _)) => return output,
			}
		};
		if response.is_err() {
			if runner
				&& Self::try_get_process_control_runner_inner(session, id, None)
					.is_some_and(|control| control.data.status.is_finished())
			{
				let output = tg::process::cancel::Output { released: false };
				return Ok(Some(output));
			}
			if self
				.try_get_process_from_index(session, id)
				.await?
				.is_some_and(|process| {
					(runner
						|| !process
							.location
							.as_ref()
							.is_some_and(tg::Location::is_remote))
						&& process.status.is_some_and(|status| status.is_finished())
				}) {
				let output = tg::process::cancel::Output { released: false };
				return Ok(Some(output));
			}
		}
		let response = response
			.map_err(
				|error| tg::error!(!error, %id, "failed to send the release process lease control request"),
			)?
			.map_err(
				|error| tg::error!(!error, %id, "the release process lease control request failed"),
			)?;
		let output = response
			.try_unwrap_release_lease()
			.map_err(|_| tg::error!("expected a release process lease response"))?;
		let output = tg::process::cancel::Output {
			released: output.released,
		};
		Ok(Some(output))
	}

	async fn wait_for_process_cancel_local(
		&self,
		session: &Session,
		id: &tg::process::Id,
		mut wakeups: futures::stream::BoxStream<'static, ()>,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		loop {
			let process = self.try_get_process_from_index(session, id).await;
			tangram_server_core::checkpoint!(session.server, "process.cancel.index", process = %id)
				.await;
			let Some(process) =
				process.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
			else {
				return Ok(None);
			};
			if process
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
			{
				return Ok(None);
			}
			if process.status.is_some_and(|status| status.is_finished()) {
				let output = tg::process::cancel::Output { released: false };
				return Ok(Some(output));
			}
			if wakeups.next().await.is_none() {
				return Err(tg::error!("the process status wakeup stream ended"));
			}
		}
	}

	async fn try_cancel_process_regions(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::cancel::Arg,
		regions: &[String],
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_cancel_process_region(session, process, arg.clone(), region))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_cancel_process_region(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::cancel::Arg,
		region: &str,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		let client = session
			.get_region_session_for_process(region)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, %process, "failed to get the region client"),
			)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::cancel::Arg {
			location: Some(location.into()),
			..arg
		};
		let Some(output) = client.try_cancel_process(process, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to cancel the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_cancel_process_remotes(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::cancel::Arg,
		remotes: &[tangram_server_core::location::Remote],
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_cancel_process_remote(session, process, arg.clone(), remote))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_cancel_process_remote(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::cancel::Arg,
		remote: &tangram_server_core::location::Remote,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session_for_process(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, %process, "failed to get the remote client"),
			)?;
		let arg = tg::process::cancel::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			..arg
		};
		let Some(output) = client.try_cancel_process(process, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to cancel the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	pub(crate) async fn try_cancel_process_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		process: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Parse the process.
		let process = process
			.parse::<tg::Either<tg::process::Nonce, tg::process::Id>>()
			.map_err(|_| tg::error!(argument, "failed to parse the process"))?;

		// Parse the arg.
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.ok_or_else(|| tg::error!(argument, "missing the arg"))?;

		let Some(output) = self
			.try_cancel_process(session, &process, arg)
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to cancel the process"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder()
			.json(output)
			.map_err(|error| tg::error!(!error, "failed to serialize the response"))?
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
