use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
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

		// Create the request's ID, which the passes without and with a search share, so that the holder acts on the request once.
		let request_id = tangram_server_core::control::id();

		// Cancel the process locally without a search.
		if current
			&& let Some(id) = &id
			&& let Some(output) = self
				.try_cancel_process_local(session, id, &request_id, arg.clone(), false)
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
			self.try_cancel_process_local(session, id, &request_id, arg.clone(), true)
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

		// Send the request to the runner. A runner that no longer holds the process has finished it, so there is no lease left to release.
		let request = tg::process::control::ServerRequestArg::ReleaseLease(
			tg::process::control::ReleaseLeaseServerRequestArg { lease: arg.lease },
		);
		let reply = match control.control_sender.send_request(request).await {
			Err(error) => Err(error),
			Ok(response) => response.await,
		};
		let finished = || {
			Self::try_get_process_control_runner_inner(session, id, None)
				.is_some_and(|control| control.data.status.is_finished())
		};
		let released = match reply {
			Ok(Ok(std::ops::ControlFlow::Break(
				tg::process::control::ClientResponseOutput::ReleaseLease(output),
			))) => output.released,
			Ok(Ok(std::ops::ControlFlow::Continue(_))) => false,
			Ok(Ok(std::ops::ControlFlow::Break(_))) => {
				return Err(tg::error!("expected a release process lease response"));
			},
			Ok(Err(_)) | Err(_) if finished() => false,
			Ok(Err(error)) => {
				return Err(
					tg::error!(!error, %id, "the release process lease control request failed"),
				);
			},
			Err(error) => {
				return Err(
					tg::error!(!error, %id, "failed to send the release process lease control request"),
				);
			},
		};
		let output = tg::process::cancel::Output { released };

		Ok(Some(output))
	}

	pub(crate) async fn try_cancel_process_local(
		&self,
		session: &Session,
		id: &tg::process::Id,
		request_id: &str,
		arg: tg::process::cancel::Arg,
		exhaustive: bool,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		// Subscribe before sending the request, so that a finish between the request and the wait cannot be missed.
		let mut wakeups = self
			.create_process_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;

		// Send the request to the process's holder. A cancel is authorized by its lease.
		let request = tg::process::control::ServerRequestArg::ReleaseLease(
			tg::process::control::ReleaseLeaseServerRequestArg { lease: arg.lease },
		);
		let retry = session.server.config.control.request_retry.clone().into();
		let timeout = session.server.config.control.request_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		let arg = crate::holder::Arg {
			checkpoint: Some("process.cancel"),
			deadline: session.server.control_read_deadline(),
			exhaustive,
			id: request_id.to_owned(),
			mode: tangram_server_core::holder::Mode::Write,
			options,
			request,
			wakeups: Some(&mut wakeups),
		};
		let output = self
			.request_process_holder(session, id, arg, |_| false)
			.boxed()
			.await?
			.ok_or_else(|| tg::error!("the process status wakeup stream ended"))?;

		// A process in a remote is not canceled here.
		let (process, _) = output.indexed.unzip();
		if process
			.as_ref()
			.and_then(|process| process.location.as_ref())
			.is_some_and(tg::Location::is_remote)
		{
			return Ok(None);
		}

		// A finished process has no lease left to release, and a process that neither the index nor a holder has does not exist.
		let finished = process
			.and_then(|process| process.status)
			.is_some_and(|status| status.is_finished());
		let released = match output.reply {
			Some(Ok(tg::process::control::ClientResponseOutput::ReleaseLease(output))) => {
				output.released
			},
			Some(Ok(_)) => return Err(tg::error!("expected a release process lease response")),
			Some(Err(_)) | None if finished => false,
			Some(Err(error)) => {
				return Err(
					tg::error!(!error, %id, "the release process lease control request failed"),
				);
			},
			None => return Ok(None),
		};
		let output = tg::process::cancel::Output { released };

		Ok(Some(output))
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
