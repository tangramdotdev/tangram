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
	#[tracing::instrument(name = "sandbox.start_request", level = "info", skip_all, fields(sandbox = %id), err(level = "debug"))]
	pub(crate) async fn try_start_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::start::Arg,
	) -> tg::Result<Option<bool>> {
		if let Some(output) = self.try_start_sandbox_runner(session, id, &arg).await? {
			return Ok(Some(output));
		}
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		let current = locations.local.as_ref().is_some_and(|local| local.current);

		// Start the sandbox locally without a search.
		if current
			&& let Some(output) = self
				.try_start_sandbox_local(session, id, &arg, false)
				.boxed()
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to start the sandbox"))?
		{
			return Ok(Some(output));
		}

		// Search locally while trying the regions and then the remotes.
		let local_future = async {
			if !current {
				return Ok(None);
			}
			self.try_start_sandbox_local(session, id, &arg, true)
				.boxed()
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to start the sandbox"))
		};
		let regions_and_remotes_future = async {
			if let Some(local) = &locations.local
				&& let Some(output) = self
					.try_start_sandbox_regions(session, id, &arg, &local.regions)
					.await
					.map_err(
						|error| tg::error!(!error, %id, "failed to start the sandbox in another region"),
					)? {
				return Ok(Some(output));
			}
			self.try_start_sandbox_remotes(session, id, &arg, &locations.remotes)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to start the sandbox in a remote"))
		};
		let output = await_local_future_over_regions_remotes_futures(
			local_future,
			regions_and_remotes_future,
		)
		.boxed()
		.await?;

		Ok(output)
	}

	async fn try_start_sandbox_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
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
		let Some(status) = session
			.server
			.components
			.runner
			.state()
			.sandboxes()
			.get(runner.index)
			.map(|sandbox| sandbox.status)
		else {
			return Ok(None);
		};

		// A started sandbox has nothing left to start. Start any other sandbox through its holder, because a scheduler may hold a stop of a boot that is starting, and starts the next boot of one that is stopped.
		match status {
			tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying => {
				Err(tg::error!(%id, "the sandbox was destroyed"))
			},
			tg::sandbox::Status::Started => Ok(Some(false)),
			tg::sandbox::Status::Starting
			| tg::sandbox::Status::Stopped
			| tg::sandbox::Status::Stopping => Ok(None),
		}
	}

	async fn try_start_sandbox_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
		exhaustive: bool,
	) -> tg::Result<Option<bool>> {
		// Authorize the start.
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

		// Subscribe before sending the request, so that a start or stop between the request and the wait cannot be missed.
		let mut wakeups = self
			.create_sandbox_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;

		// Send the request to the sandbox's holder until the sandbox is starting or started, or the index records that it is stopped.
		let indexed = loop {
			let request = tg::sandbox::control::ServerRequestArg::Start(
				tg::sandbox::control::StartServerRequestArg {},
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

			// A sandbox in a remote is not started here.
			if output
				.indexed
				.as_ref()
				.and_then(|sandbox| sandbox.location.as_ref())
				.is_some_and(tg::Location::is_remote)
			{
				return Ok(None);
			}

			// A boot that is starting or started has nothing left to start, a destroyed sandbox cannot be started, a stopped sandbox starts its next boot, and a sandbox that neither the index nor a holder has does not exist.
			let status = output
				.indexed
				.as_ref()
				.and_then(|sandbox| sandbox.data.as_ref())
				.map(|data| data.data.status);
			match (output.reply, status) {
				(Some(Ok(tg::sandbox::control::ClientResponseOutput::Start(output))), _) => {
					match output.data.data.status {
						tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying => {
							return Err(tg::error!(%id, "the sandbox was destroyed"));
						},
						tg::sandbox::Status::Started => return Ok(Some(false)),
						tg::sandbox::Status::Starting => {
							if arg.wait {
								self.wait_for_sandbox_start(session, id, &mut wakeups)
									.await?;
							}
							return Ok(Some(false));
						},
						tg::sandbox::Status::Stopped | tg::sandbox::Status::Stopping => {},
					}
				},
				(Some(Ok(_)), _) => {
					return Err(tg::error!(%id, "expected a start sandbox response"));
				},
				(_, Some(tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying)) => {
					return Err(tg::error!(%id, "the sandbox was destroyed"));
				},
				(_, Some(tg::sandbox::Status::Stopped)) => break output.indexed.unwrap(),
				(Some(Err(error)), _) => {
					return Err(
						tg::error!(!error, %id, "the start sandbox control request failed"),
					);
				},
				(None, _) => return Ok(None),
			}

			// Wait for the index to record the stop of a boot that is stopping.
			if wakeups.next().await.is_none() {
				return Err(tg::error!("the sandbox status wakeup stream ended"));
			}
		};

		// Start the next boot on the runner that holds the sandbox's directory, or on any runner if the sandbox was stopped before it was placed.
		let data = indexed.data.unwrap().data;
		let boot = data.boot + 1;
		self.start_sandbox_with_runner(session, id, data, indexed.runner, indexed.created_at)
			.await?;

		// Wait for the boot to start if requested, because a connection alone does not identify the boot.
		if arg.wait {
			self.wait_for_sandbox_boot(session, id, boot, &mut wakeups)
				.await?;
		}

		Ok(Some(true))
	}

	async fn start_sandbox_with_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		data: tg::sandbox::Data,
		runner: Option<tg::runner::Id>,
		created_at: i64,
	) -> tg::Result<()> {
		// Create the sandbox's arg from its data, and a new control token.
		let boot = data.boot + 1;
		let arg = tg::sandbox::create::Arg {
			cpu: data.cpu,
			host: None,
			hostname: data.hostname.clone(),
			location: None,
			memory: data.memory,
			mounts: data.mounts.clone(),
			network: data.network.clone(),
			owner: Some(data.owner.clone()),
			storage: data.storage,
			time_to_live: data.time_to_live,
			time_to_stop: data.time_to_stop,
			wait: false,
		};
		let token = session
			.server
			.components
			.authentication
			.create_sandbox_authentication_token(&session.server, id.clone())?;

		// Get the scheduler of the runner that holds the sandbox's directory, because only that scheduler can place a boot on the runner. A sandbox that was stopped before it was placed has no runner, so any scheduler starts it.
		let scheduler = if let Some(runner) = &runner {
			session
				.server
				.index
				.try_get_runner(runner)
				.await
				.map_err(|error| tg::error!(!error, %runner, "failed to get the runner"))?
				.and_then(|runner| runner.scheduler)
		} else {
			None
		};

		// Enqueue the boot, which the scheduler accepts once its record is durable.
		let request = tangram_server_scheduler_core::EnqueueSandboxRequestArg {
			arg,
			boot,
			created_at,
			creator: data.creator.clone(),
			owner: data.owner.clone(),
			parent: None,
			process: None,
			runner,
			sandbox: id.clone(),
			scheduler,
			stop: false,
			token: Some(token),
		};
		session
			.server
			.components
			.scheduler
			.enqueue_sandbox(session, request)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to enqueue the sandbox"))?;

		Ok(())
	}

	pub(crate) async fn wait_for_sandbox_boot(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		boot: u64,
		wakeups: &mut futures::stream::BoxStream<'static, ()>,
	) -> tg::Result<()> {
		let sandboxes = session.server.components.runner.state().sandboxes();
		loop {
			let updated = sandboxes.updated();
			let output = self
				.try_get_sandbox(session, id, tg::sandbox::get::Arg::default())
				.await?
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
			let data = output.data;
			if data.status.is_destroyed() || data.status.is_destroying() {
				let error = match data.error {
					Some(tg::Either::Left(data)) => tg::Error::try_from(data).ok(),
					Some(tg::Either::Right(_)) | None => None,
				};
				let error = if let Some(error) = error {
					tg::error!(!error, %id, "the sandbox was destroyed")
				} else {
					tg::error!(%id, "the sandbox was destroyed")
				};
				return Err(error);
			}
			if data.boot == boot {
				match data.status {
					tg::sandbox::Status::Started => return Ok(()),
					tg::sandbox::Status::Stopped | tg::sandbox::Status::Stopping => {
						let error = match data.error {
							Some(tg::Either::Left(data)) => tg::Error::try_from(data).ok(),
							Some(tg::Either::Right(_)) | None => None,
						};
						let error = if let Some(error) = error {
							tg::error!(!error, %id, "the sandbox failed to start")
						} else {
							tg::error!(%id, "the sandbox failed to start")
						};
						return Err(error);
					},
					tg::sandbox::Status::Destroyed
					| tg::sandbox::Status::Destroying
					| tg::sandbox::Status::Starting => {},
				}
			}
			tokio::select! {
				wakeup = wakeups.next() => {
					if wakeup.is_none() {
						return Err(tg::error!("the sandbox status wakeup stream ended"));
					}
				},
				() = updated => {},
			}
		}
	}

	pub(crate) async fn wait_for_sandbox_start(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		wakeups: &mut futures::stream::BoxStream<'static, ()>,
	) -> tg::Result<()> {
		loop {
			let output = self
				.try_get_sandbox(session, id, tg::sandbox::get::Arg::default())
				.await?
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
			match output.data.status {
				tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying => {
					return Err(tg::error!(%id, "the sandbox was destroyed"));
				},
				tg::sandbox::Status::Started => return Ok(()),
				tg::sandbox::Status::Stopped | tg::sandbox::Status::Stopping => {
					return Err(tg::error!(%id, "the sandbox failed to start"));
				},
				tg::sandbox::Status::Starting => {},
			}
			if wakeups.next().await.is_none() {
				return Err(tg::error!("the sandbox status wakeup stream ended"));
			}
		}
	}

	async fn try_start_sandbox_regions(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
		regions: &[String],
	) -> tg::Result<Option<bool>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_start_sandbox_region(session, id, arg, region))
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

	async fn try_start_sandbox_region(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
		region: &str,
	) -> tg::Result<Option<bool>> {
		let client = session.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::start::Arg {
			location: Some(location.clone().into()),
			tokens: arg.tokens.for_location(&location),
			wait: arg.wait,
		};
		let Some(started) = client
			.try_start_sandbox(id, arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to start the sandbox"))?
		else {
			return Ok(None);
		};
		Ok(Some(started))
	}

	async fn try_start_sandbox_remotes(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
		remotes: &[tangram_server_core::location::Remote],
	) -> tg::Result<Option<bool>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_start_sandbox_remote(session, id, arg, remote))
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

	async fn try_start_sandbox_remote(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
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
		let arg = tg::sandbox::start::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			tokens: arg.tokens.for_location(&location),
			wait: arg.wait,
		};
		let Some(started) = client.try_start_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to start the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(started))
	}

	pub(crate) async fn try_start_sandbox_request(
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

		let Some(started) = self
			.try_start_sandbox(session, &id, arg)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to start the sandbox"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		if !started {
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
