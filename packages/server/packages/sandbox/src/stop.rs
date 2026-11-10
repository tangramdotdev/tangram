use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_server_core::{Session, location::await_local_future_over_regions_remotes_futures},
};

impl Component {
	#[tracing::instrument(name = "sandbox.stop_request", level = "info", skip_all, fields(sandbox = %id), err(level = "debug"))]
	pub(crate) async fn try_stop_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::stop::Arg,
	) -> tg::Result<Option<bool>> {
		if let Some(output) = self.try_stop_sandbox_runner(session, id, &arg).await? {
			return Ok(Some(output));
		}
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		let current = locations.local.as_ref().is_some_and(|local| local.current);

		// Stop the sandbox locally without a search.
		if current
			&& let Some(output) = self
				.try_stop_sandbox_local(session, id, &arg, false)
				.boxed()
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to stop the sandbox"))?
		{
			return Ok(Some(output));
		}

		// Search locally while trying the regions and then the remotes.
		let local_future = async {
			if !current {
				return Ok(None);
			}
			self.try_stop_sandbox_local(session, id, &arg, true)
				.boxed()
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to stop the sandbox"))
		};
		let regions_and_remotes_future = async {
			if let Some(local) = &locations.local
				&& let Some(output) = self
					.try_stop_sandbox_regions(session, id, &arg, &local.regions)
					.await
					.map_err(
						|error| tg::error!(!error, %id, "failed to stop the sandbox in another region"),
					)? {
				return Ok(Some(output));
			}
			self.try_stop_sandbox_remotes(session, id, &arg, &locations.remotes)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to stop the sandbox in a remote"))
		};
		let output = await_local_future_over_regions_remotes_futures(
			local_future,
			regions_and_remotes_future,
		)
		.boxed()
		.await?;

		Ok(output)
	}

	async fn try_stop_sandbox_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::stop::Arg,
	) -> tg::Result<Option<bool>> {
		let Some(runner) = session
			.server
			.components
			.runner
			.try_get_sandbox_runner(id, arg.location.as_ref())
		else {
			return Ok(None);
		};
		if !self
			.authorize_sandbox_runner(
				session,
				id,
				arg.tokens.local_authorization(),
				tg::authorization::permission::sandbox::Permission::Parent,
			)
			.await?
		{
			return Ok(None);
		}
		let Some((status, control_sender)) = session
			.server
			.components
			.runner
			.state()
			.sandboxes()
			.get(runner.index)
			.map(|sandbox| (sandbox.status, sandbox.control_sender.clone()))
		else {
			return Ok(None);
		};
		self.stop_sandbox_with_runner(session, id, status, control_sender, arg.wait)
			.boxed()
			.await
	}

	async fn try_stop_sandbox_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::stop::Arg,
		exhaustive: bool,
	) -> tg::Result<Option<bool>> {
		// Authorize the stop.
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Parent,
		);
		let resource = tg::Referent::with_node_and_tokens(id.clone(), arg.tokens.clone());
		let authorized = session
			.authorize_with_exhaustive(resource, permission, exhaustive)
			.await?
			.check_exhaustion()?;
		if !authorized.permissions.contains(permission) {
			return Ok(None);
		}

		// Subscribe before sending the request, so that the stop cannot be missed.
		let mut wakeups = self
			.create_sandbox_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;

		// Send the request to the sandbox's holder.
		let request = tg::sandbox::control::ServerRequestArg::Stop(
			tg::sandbox::control::StopServerRequestArg {},
		);
		let retry = session.server.config.control.request_retry.clone().into();
		let timeout = session.server.config.control.request_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		let holder_arg = crate::holder::Arg {
			deadline: session.server.control_read_deadline(),
			exhaustive,
			fallback: false,
			options,
			request,
			source: tg::sandbox::Source::Auto,
			wakeups: Some(&mut wakeups),
		};
		let output = self
			.request_sandbox_holder(session, id, holder_arg, |_| false)
			.boxed()
			.await?
			.ok_or_else(|| tg::error!("the sandbox status wakeup stream ended"))?;

		// A sandbox in a remote is not stopped here.
		if output
			.indexed
			.as_ref()
			.and_then(|sandbox| sandbox.location.as_ref())
			.is_some_and(tg::Location::is_remote)
		{
			return Ok(None);
		}

		// A stopped sandbox has nothing left to stop, a destroyed sandbox cannot be stopped, and a sandbox that neither the index nor a holder has does not exist.
		let status = output
			.indexed
			.and_then(|sandbox| sandbox.data)
			.map(|data| data.data.status);
		let stopped = match (output.reply, status) {
			(Some(Ok(tg::sandbox::control::ClientResponseOutput::Stop(output))), _) => {
				output.stopped
			},
			(Some(Ok(_)), _) => {
				return Err(tg::error!(%id, "expected a stop sandbox response"));
			},
			(_, Some(tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying)) => {
				return Err(tg::error!(%id, "the sandbox was destroyed"));
			},
			(_, Some(tg::sandbox::Status::Stopped)) => false,
			(Some(Err(error)), _) => {
				return Err(tg::error!(!error, %id, "the stop sandbox control request failed"));
			},
			(None, _) => return Ok(None),
		};

		// Wait for the sandbox to be stopped if requested.
		if arg.wait {
			self.wait_for_sandbox_stop(session, id, &mut wakeups)
				.await?;
		}

		Ok(Some(stopped))
	}

	/// Stop a sandbox that this server's runner holds.
	async fn stop_sandbox_with_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		status: tg::sandbox::Status,
		control_sender: tangram_server_sandbox_core::control::local::Local,
		wait: bool,
	) -> tg::Result<Option<bool>> {
		// Subscribe before sending the request, so that the stop cannot be missed.
		let mut wakeups = self
			.create_sandbox_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;

		// Request the stop unless the sandbox is already stopping or stopped.
		let stopped = match status {
			tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying => {
				return Err(tg::error!(%id, "the sandbox was destroyed"));
			},
			tg::sandbox::Status::Stopped => return Ok(Some(false)),
			tg::sandbox::Status::Stopping => false,
			tg::sandbox::Status::Started | tg::sandbox::Status::Starting => {
				let request = tg::sandbox::control::ServerRequestArg::Stop(
					tg::sandbox::control::StopServerRequestArg {},
				);
				let response = match control_sender.send_request(request).await {
					Err(error) => Err(error),
					Ok(response) => response.await,
				};
				let response = response
					.map_err(
						|error| tg::error!(!error, %id, "failed to send the stop sandbox control request"),
					)?
					.map_err(
						|error| tg::error!(!error, %id, "the stop sandbox control request failed"),
					)?;

				// Stop the sandbox through its holder if this server's runner no longer holds it.
				if matches!(
					response,
					tg::sandbox::control::ClientResponseOutput::NotHolder(_)
				) {
					return Ok(None);
				}
				response
					.try_unwrap_stop()
					.map_err(|_| tg::error!(%id, "expected a stop sandbox response"))?
					.stopped
			},
		};

		// Wait for the sandbox to be stopped if requested.
		if wait {
			self.wait_for_sandbox_stop(session, id, &mut wakeups)
				.await?;
		}

		Ok(Some(stopped))
	}

	async fn wait_for_sandbox_stop(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		wakeups: &mut futures::stream::BoxStream<'static, ()>,
	) -> tg::Result<()> {
		loop {
			let arg = tg::sandbox::get::Arg::default();
			let output = self
				.try_get_sandbox(session, id, arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox"))?
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
			match output.data.status {
				tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying => {
					return Err(tg::error!(%id, "the sandbox was destroyed"));
				},
				tg::sandbox::Status::Stopped => return Ok(()),
				tg::sandbox::Status::Started
				| tg::sandbox::Status::Starting
				| tg::sandbox::Status::Stopping => {},
			}
			if wakeups.next().await.is_none() {
				return Err(tg::error!("the sandbox status wakeup stream ended"));
			}
		}
	}

	async fn try_stop_sandbox_regions(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::stop::Arg,
		regions: &[String],
	) -> tg::Result<Option<bool>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_stop_sandbox_region(session, id, arg, region))
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

	async fn try_stop_sandbox_region(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::stop::Arg,
		region: &str,
	) -> tg::Result<Option<bool>> {
		let client = session.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::stop::Arg {
			location: Some(location.clone().into()),
			tokens: arg.tokens.for_location(&location),
			wait: arg.wait,
		};
		let Some(stopped) = client
			.try_stop_sandbox(id, arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to stop the sandbox"))?
		else {
			return Ok(None);
		};
		Ok(Some(stopped))
	}

	async fn try_stop_sandbox_remotes(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::stop::Arg,
		remotes: &[tangram_server_core::location::Remote],
	) -> tg::Result<Option<bool>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_stop_sandbox_remote(session, id, arg, remote))
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

	async fn try_stop_sandbox_remote(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::stop::Arg,
		remote: &tangram_server_core::location::Remote,
	) -> tg::Result<Option<bool>> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, %id, "failed to get the remote client"),
			)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let arg = tg::sandbox::stop::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			tokens: arg.tokens.for_location(&location),
			wait: arg.wait,
		};
		let Some(stopped) = client.try_stop_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to stop the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(stopped))
	}

	pub(crate) async fn try_stop_sandbox_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let id = id
			.parse::<tg::sandbox::Id>()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the sandbox id"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		let Some(stopped) = self
			.try_stop_sandbox(session, &id, arg)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to stop the sandbox"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		if !stopped {
			return Ok(http::Response::builder()
				.status(http::StatusCode::CONFLICT)
				.empty()
				.unwrap()
				.boxed_body());
		}

		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}
}
